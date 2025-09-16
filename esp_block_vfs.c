/*
** This file implements a VFS for SQLite that directly accesses an SD card
** using the ESP-IDF SDMMC block access API. It supports WAL mode through a
** partitioned block device layout and an in-memory shared-memory simulation
** using FreeRTOS mutexes.
*/

#include "sqlite3.h"
#include <string.h>
#include <assert.h>

/* ESP-IDF and FreeRTOS headers */
#include "driver/sdmmc_host.h"
#include "sdmmc_cmd.h"
#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"

/* VFS-specific constants */
#define ESP_BLOCK_VFS_NAME "esp32-sdmmc-wal"
#define BLOCK_SIZE 512

/*
** Define the layout of the block device. All values are in blocks.
**
** - The first block (Block 0) is unused, reserved for a potential master boot record.
** - The DB region starts at block 1.
** - The WAL region starts after the DB region.
**
** Example for a 128MB card with a 16MB DB and 4MB WAL:
**   DB_REGION_START_BLOCK: 1
**   DB_REGION_MAX_SIZE_BLOCKS: (16*1024*1024)/512 = 32768
**   WAL_REGION_START_BLOCK: 1 + 32768 = 32769
**   WAL_REGION_MAX_SIZE_BLOCKS: (4*1024*1024)/512 = 8192
*/
#define DB_REGION_START_BLOCK       1
#define DB_REGION_MAX_SIZE_BLOCKS   (32 * 1024 * 1024 / BLOCK_SIZE) // 32 MB
#define WAL_REGION_START_BLOCK      (DB_REGION_START_BLOCK + DB_REGION_MAX_SIZE_BLOCKS)
#define WAL_REGION_MAX_SIZE_BLOCKS  (4 * 1024 * 1024 / BLOCK_SIZE)  // 4 MB

/* Limit for in-memory SHM region */
#define MAX_SHM_SIZE (32 * 1024) // 32 KiB

/* File types managed by this VFS */
#define DB_FILE        1
#define WAL_FILE       2

/* Forward declarations */
typedef struct esp_block_file esp_block_file;
typedef struct shm_node shm_node;

/* An open file handle */
struct esp_block_file {
  sqlite3_file base;          /* Base class. Must be first */
  sdmmc_card_t *p_card;         /* Card handle */
  int file_type;              /* DB_FILE or WAL_FILE */
  uint32_t start_block;       /* Starting block of the region for this file */
  uint32_t max_blocks;        /* Maximum size of this region in blocks */
  sqlite3_int64 current_size; /* Current size of the file in bytes */
};

/* A node in the list of shared memory regions */
struct shm_node {
  char *z_name;               /* Name of the database */
  int n_ref;                  /* Number of connections using this region */
  void *p_mem;                /* The shared memory region */
  SemaphoreHandle_t a_mutex[SQLITE_SHM_NLOCK]; /* Array of mutexes */
  shm_node *p_next;           /* Next in the list */
};

/* Global list of shared memory regions */
static shm_node *shm_list = 0;

/* Method declarations for esp_block_file */
static int esp_block_close(sqlite3_file*);
static int esp_block_read(sqlite3_file*, void*, int i_amt, sqlite3_int64 i_ofst);
static int esp_block_write(sqlite3_file*, const void*, int i_amt, sqlite3_int64 i_ofst);
static int esp_block_truncate(sqlite3_file*, sqlite3_int64 size);
static int esp_block_sync(sqlite3_file*, int flags);
static int esp_block_file_size(sqlite3_file*, sqlite3_int64 *p_size);
static int esp_block_lock(sqlite3_file*, int);
static int esp_block_unlock(sqlite3_file*, int);
static int esp_block_check_reserved_lock(sqlite3_file*, int *p_res_out);
static int esp_block_file_control(sqlite3_file*, int op, void *p_arg);
static int esp_block_sector_size(sqlite3_file*);
static int esp_block_device_characteristics(sqlite3_file*);

/* Method declarations for shared memory */
static int esp_block_shm_map(sqlite3_file*, int i_pg, int pgsz, int, void volatile**);
static int esp_block_shm_lock(sqlite3_file*, int offset, int n, int flags);
static void esp_block_shm_barrier(sqlite3_file*);
static int esp_block_shm_unmap(sqlite3_file*, int delete_flag);

/* Method declarations for the VFS */
static int esp_block_open(sqlite3_vfs*, const char *, sqlite3_file*, int , int *);
static int esp_block_delete(sqlite3_vfs*, const char *z_name, int sync_dir);
static int esp_block_access(sqlite3_vfs*, const char *z_name, int flags, int *);
static int esp_block_full_pathname(sqlite3_vfs*, const char *z_name, int n_out,char *z_out);
static void *esp_block_dl_open(sqlite3_vfs*, const char *z_filename);
static void esp_block_dl_error(sqlite3_vfs*, int n_byte, char *z_err_msg);
static void (*esp_block_dl_sym(sqlite3_vfs*,void*, const char *z_symbol))(void);
static void esp_block_dl_close(sqlite3_vfs*, void*);
static int esp_block_randomness(sqlite3_vfs*, int n_byte, char *z_out);
static int esp_block_sleep(sqlite3_vfs*, int microseconds);
static int esp_block_current_time(sqlite3_vfs*, double*);
static int esp_block_current_time_int64(sqlite3_vfs*, sqlite3_int64*);

/* The VFS object itself */
static sqlite3_vfs esp_block_vfs = {
  3,                                  /* iVersion */
  sizeof(esp_block_file),             /* szOsFile */
  1024,                               /* mxPathname */
  0,                                  /* pNext */
  ESP_BLOCK_VFS_NAME,                 /* zName */
  0,                                  /* pAppData */
  esp_block_open,                     /* xOpen */
  esp_block_delete,                   /* xDelete */
  esp_block_access,                   /* xAccess */
  esp_block_full_pathname,            /* xFullPathname */
  esp_block_dl_open,                  /* xDlOpen */
  esp_block_dl_error,                 /* xDlError */
  esp_block_dl_sym,                   /* xDlSym */
  esp_block_dl_close,                 /* xDlClose */
  esp_block_randomness,               /* xRandomness */
  esp_block_sleep,                    /* xSleep */
  esp_block_current_time,             /* xCurrentTime */
  esp_block_current_time_int64        /* xCurrentTimeInt64 */
};

/* I/O methods for block device files */
static const sqlite3_io_methods esp_block_io_methods = {
  2,                                  /* iVersion (must be 2 for WAL) */
  esp_block_close,
  esp_block_read,
  esp_block_write,
  esp_block_truncate,
  esp_block_sync,
  esp_block_file_size,
  esp_block_lock,
  esp_block_unlock,
  esp_block_check_reserved_lock,
  esp_block_file_control,
  esp_block_sector_size,
  esp_block_device_characteristics,
  esp_block_shm_map,
  esp_block_shm_lock,
  esp_block_shm_barrier,
  esp_block_shm_unmap
};

#define MIN(x,y) ((x)<(y)?(x):(y))
#define MAX(x,y) ((x)>(y)?(x):(y))

/******************************************************************************
** Block Device File I/O
******************************************************************************/

static int esp_block_close(sqlite3_file *p_file) {
  return SQLITE_OK;
}

static int esp_block_read(sqlite3_file *p_file, void *z_buf, int i_amt, sqlite3_int64 i_ofst) {
  esp_block_file *p = (esp_block_file *)p_file;
  if ((i_ofst + i_amt) > p->current_size) {
    memset(z_buf, 0, i_amt);
    return SQLITE_IOERR_SHORT_READ;
  }

  uint32_t start_sector = p->start_block + (i_ofst / BLOCK_SIZE);
  uint32_t end_sector = p->start_block + ((i_ofst + i_amt - 1) / BLOCK_SIZE);
  uint32_t sector_count = end_sector - start_sector + 1;

  char *p_block_buf = sqlite3_malloc(sector_count * BLOCK_SIZE);
  if (!p_block_buf) return SQLITE_NOMEM;

  esp_err_t err = sdmmc_read_sectors(p->p_card, p_block_buf, start_sector, sector_count);
  if (err != ESP_OK) {
    sqlite3_free(p_block_buf);
    return SQLITE_IOERR_READ;
  }

  memcpy(z_buf, p_block_buf + (i_ofst % BLOCK_SIZE), i_amt);
  sqlite3_free(p_block_buf);
  return SQLITE_OK;
}

static int esp_block_write(sqlite3_file *p_file, const void *z_buf, int i_amt, sqlite3_int64 i_ofst) {
  esp_block_file *p = (esp_block_file *)p_file;
  if ((i_ofst + i_amt) / BLOCK_SIZE >= p->max_blocks) {
    return SQLITE_FULL;
  }

  uint32_t start_sector = p->start_block + (i_ofst / BLOCK_SIZE);
  uint32_t end_sector = p->start_block + ((i_ofst + i_amt - 1) / BLOCK_SIZE);
  uint32_t sector_count = end_sector - start_sector + 1;

  char *p_block_buf = sqlite3_malloc(sector_count * BLOCK_SIZE);
  if (!p_block_buf) return SQLITE_NOMEM;

  uint32_t first_offset = i_ofst % BLOCK_SIZE;
  if (first_offset != 0 || (i_amt % BLOCK_SIZE) != 0) {
    esp_err_t err = sdmmc_read_sectors(p->p_card, p_block_buf, start_sector, sector_count);
    if (err != ESP_OK) {
      sqlite3_free(p_block_buf);
      return SQLITE_IOERR_READ;
    }
  }

  memcpy(p_block_buf + first_offset, z_buf, i_amt);

  esp_err_t err = sdmmc_write_sectors(p->p_card, p_block_buf, start_sector, sector_count);
  sqlite3_free(p_block_buf);
  if (err != ESP_OK) {
    return SQLITE_IOERR_WRITE;
  }

  p->current_size = MAX(p->current_size, i_ofst + i_amt);
  return SQLITE_OK;
}

static int esp_block_truncate(sqlite3_file *p_file, sqlite_int64 size) {
  ((esp_block_file *)p_file)->current_size = size;
  return SQLITE_OK;
}

static int esp_block_sync(sqlite3_file *p_file, int flags) {
  return SQLITE_OK; /* Writes are synchronous with the SD card driver */
}

static int esp_block_file_size(sqlite3_file *p_file, sqlite3_int64 *p_size) {
  *p_size = ((esp_block_file *)p_file)->current_size;
  return SQLITE_OK;
}

static int esp_block_lock(sqlite3_file *p_file, int e_lock) { return SQLITE_OK; }
static int esp_block_unlock(sqlite3_file *p_file, int e_lock) { return SQLITE_OK; }
static int esp_block_check_reserved_lock(sqlite3_file *p_file, int *p_res_out) {
  *p_res_out = 0;
  return SQLITE_OK;
}
static int esp_block_file_control(sqlite3_file *p_file, int op, void *p_arg) {
    if( op==SQLITE_FCNTL_VFSNAME ){
        *(char**)p_arg = sqlite3_mprintf("%s", esp_block_vfs.zName);
        return SQLITE_OK;
    }
    if (op == SQLITE_FCNTL_SIZE_HINT) {
        ((esp_block_file *)p_file)->current_size = *(sqlite3_int64 *)p_arg;
        return SQLITE_OK;
    }
    return SQLITE_NOTFOUND;
}
static int esp_block_sector_size(sqlite3_file *p_file) { return BLOCK_SIZE; }
static int esp_block_device_characteristics(sqlite3_file *p_file) {
  return SQLITE_IOCAP_ATOMIC512 | SQLITE_IOCAP_SEQUENTIAL_REQS;
}

/******************************************************************************
** Shared Memory Routines
******************************************************************************/

static int esp_block_shm_map(sqlite3_file *p_file, int i_pg, int pgsz, int is_write, void volatile **pp) {
  esp_block_file *p = (esp_block_file *)p_file;
  shm_node *p_node;
  const char *db_name = sqlite3_filename_database(p->base.pMethods->pAppData);

  /* Find the existing SHM region */
  for (p_node = shm_list; p_node; p_node = p_node->p_next) {
    if (strcmp(p_node->z_name, db_name) == 0) break;
  }

  /* If not found, create a new one */
  if (!p_node) {
    int n_size = pgsz * i_pg;
    if (n_size > MAX_SHM_SIZE) return SQLITE_NOMEM; /* Enforce SHM limit */

    p_node = sqlite3_malloc(sizeof(shm_node));
    if (!p_node) return SQLITE_NOMEM;
    memset(p_node, 0, sizeof(shm_node));

    p_node->z_name = sqlite3_mprintf("%s", db_name);
    if (!p_node->z_name) {
        sqlite3_free(p_node);
        return SQLITE_NOMEM;
    }

    p_node->p_mem = sqlite3_malloc(n_size);
    if (!p_node->p_mem) {
      sqlite3_free(p_node->z_name);
      sqlite3_free(p_node);
      return SQLITE_NOMEM;
    }
    memset(p_node->p_mem, 0, n_size);

    for (int i = 0; i < SQLITE_SHM_NLOCK; i++) {
      p_node->a_mutex[i] = xSemaphoreCreateMutex();
    }

    p_node->p_next = shm_list;
    shm_list = p_node;
  }

  p_node->n_ref++;
  *pp = p_node->p_mem;
  return SQLITE_OK;
}

static int esp_block_shm_unmap(sqlite3_file *p_file, int delete_flag) {
  esp_block_file *p = (esp_block_file *)p_file;
  shm_node *p_node, **pp_node;
  const char *db_name = sqlite3_filename_database(p->base.pMethods->pAppData);

  for (pp_node = &shm_list; (p_node = *pp_node); pp_node = &p_node->p_next) {
    if (strcmp(p_node->z_name, db_name) == 0) break;
  }

  if (!p_node) return SQLITE_OK; /* Should not happen */

  p_node->n_ref--;
  if (p_node->n_ref == 0) {
    for (int i = 0; i < SQLITE_SHM_NLOCK; i++) {
      vSemaphoreDelete(p_node->a_mutex[i]);
    }
    sqlite3_free(p_node->p_mem);
    sqlite3_free(p_node->z_name);
    *pp_node = p_node->p_next;
    sqlite3_free(p_node);
  }

  return SQLITE_OK;
}

static int esp_block_shm_lock(sqlite3_file *p_file, int offset, int n, int flags) {
  esp_block_file *p = (esp_block_file *)p_file;
  shm_node *p_node;
  const char *db_name = sqlite3_filename_database(p->base.pMethods->pAppData);
  
  for (p_node = shm_list; p_node; p_node = p_node->p_next) {
    if (strcmp(p_node->z_name, db_name) == 0) break;
  }

  if (!p_node) return SQLITE_IOERR; /* Should not happen */

  for (int i = offset; i < offset + n; i++) {
    if (flags & SQLITE_SHM_UNLOCK) {
      xSemaphoreGive(p_node->a_mutex[i]);
    } else {
      if (xSemaphoreTake(p_node->a_mutex[i], portMAX_DELAY) != pdTRUE) {
        return SQLITE_BUSY;
      }
    }
  }
  return SQLITE_OK;
}

static void esp_block_shm_barrier(sqlite3_file *p_file) {
  __sync_synchronize();
}

/******************************************************************************
** VFS Routines
******************************************************************************/

static int esp_block_open(sqlite3_vfs *p_vfs, const char *z_name, sqlite3_file *p_file, int flags, int *p_out_flags) {
  esp_block_file *p = (esp_block_file *)p_file;
  memset(p, 0, sizeof(esp_block_file));
  p->p_card = (sdmmc_card_t *)p_vfs->pAppData;

  if (flags & SQLITE_OPEN_MAIN_DB) {
    p->file_type = DB_FILE;
    p->start_block = DB_REGION_START_BLOCK;
    p->max_blocks = DB_REGION_MAX_SIZE_BLOCKS;
  } else if (flags & SQLITE_OPEN_WAL) {
    p->file_type = WAL_FILE;
    p->start_block = WAL_REGION_START_BLOCK;
    p->max_blocks = WAL_REGION_MAX_SIZE_BLOCKS;
  } else {
    /* This VFS only supports the main DB and WAL file. Others are errors. */
    return SQLITE_CANTOPEN;
  }

  p->base.pMethods = &esp_block_io_methods;
  p->base.pMethods->pAppData = (void*)z_name;
  if (p_out_flags) *p_out_flags = flags;

  /* For a new database, the size is 0. For existing, we don't know yet.
     SQLite will find out via xFileSize, which we can leave at 0 for now.
     A more robust implementation might read a metadata block. */
  p->current_size = 0;

  return SQLITE_OK;
}

static int esp_block_delete(sqlite3_vfs *p_vfs, const char *z_name, int sync_dir) {
  /* Since files are fixed regions, "deleting" just means we can maybe zero
     out the region. For WAL, this means truncating the WAL file. */
  return SQLITE_OK;
}

static int esp_block_access(sqlite3_vfs *p_vfs, const char *z_name, int flags, int *p_res_out) {
  *p_res_out = 0; /* Assume nothing exists until opened */
  return SQLITE_OK;
}

static int esp_block_full_pathname(sqlite3_vfs *p_vfs, const char *z_path, int n_out, char *z_out) {
  sqlite3_snprintf(n_out, z_out, "%s", z_path);
  return SQLITE_OK;
}

/* No-op implementations for unused VFS methods */
static void *esp_block_dl_open(sqlite3_vfs *p_vfs, const char *z_path){ return 0; }
static void esp_block_dl_error(sqlite3_vfs *p_vfs, int n_byte, char *z_err_msg){
  sqlite3_snprintf(n_byte, z_err_msg, "Dynamic linking not supported");
}
static void (*esp_block_dl_sym(sqlite3_vfs *p_vfs, void *p_h, const char *z_sym))(void){ return 0; }
static void esp_block_dl_close(sqlite3_vfs *p_vfs, void *p_handle){ return; }

static int esp_block_randomness(sqlite3_vfs *p_vfs, int n_byte, char *z_buf_out){
    uint32_t r = 0;
    for(int i=0; i<n_byte; i+=4){
        r = esp_random();
        memcpy(&z_buf_out[i], &r, MIN(4, n_byte-i));
    }
    return n_byte;
}

static int esp_block_sleep(sqlite3_vfs *p_vfs, int microseconds){
  vTaskDelay(pdMS_TO_TICKS(microseconds/1000));
  return microseconds;
}

static int esp_block_current_time(sqlite3_vfs *p_vfs, double *p_time_out){
  struct timeval tv;
  gettimeofday(&tv, NULL);
  *p_time_out = tv.tv_sec + tv.tv_usec / 1000000.0;
  return SQLITE_OK;
}

static int esp_block_current_time_int64(sqlite3_vfs *p_vfs, sqlite3_int64 *p_time_out){
  struct timeval tv;
  gettimeofday(&tv, NULL);
  *p_time_out = (sqlite3_int64)tv.tv_sec * 1000 + tv.tv_usec / 1000;
  return SQLITE_OK;
}

/*
** Register the VFS with SQLite.
*/
int sqlite3_esp_block_vfs_register(sdmmc_card_t *p_card, int make_default){
  if (!p_card) return SQLITE_MISUSE;
  esp_block_vfs.pAppData = p_card;
  return sqlite3_vfs_register(&esp_block_vfs, make_default);
}
