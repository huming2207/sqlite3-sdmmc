#include <string.h>
#include <sys/time.h>

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"

#include <sdkconfig.h>

// Not sure why this is needed, but without these esp_log won't compile
#ifdef ESP_STATIC_ASSERT
#undef ESP_STATIC_ASSERT
#endif
#define ESP_STATIC_ASSERT(cond, msg) _Static_assert(cond, msg)
#include <esp_log.h>

#include "esp_random.h"
#include "sdmmc_cmd.h"

#include "sqlite_port.h"
#include "esp_sqlite_sdmmc_vfs.h"
#include "sqlite3.h"

#define TAG "sqlite_port"

/*
** Maximum pathname length supported by the fs backend.
*/
#define BLOCKSIZE SQLITE_DEFAULT_PAGE_SIZE

/*
** Name used to identify this VFS.
*/
#define FS_VFS_NAME "sdmmc"

typedef struct sdmmc_vfs_data {
    int64_t nDatabase;     /* Current size of database region */
    int64_t nJournal;      /* Current size of journal region */
    int64_t nBlob;         /* Total size of allocated blob */
    int nRef;              /* Number of open file handles */
    const char *zName;     /* Name of database file */
    int open;              /* True if database file is open */
    sdmmc_card_t *pCard;
} sdmmc_vfs_data;

typedef struct fs_file fs_file;

struct fs_file
{
  sqlite3_file base;
  sqlite3_vfs *pVfs;
  int eType;
};

typedef struct tmp_file tmp_file;

struct tmp_file
{
  sqlite3_file base;
  int nSize;
  int nAlloc;
  char* zAlloc;
};

/* Values for fs_file.eType. */
#define DATABASE_FILE   1
#define JOURNAL_FILE    2

/*
** Method declarations for fs_file.
*/
static int fsClose(sqlite3_file*);

static int fsRead(sqlite3_file*, void*, int iAmt, sqlite3_int64 iOfst);

static int fsWrite(sqlite3_file*, const void*, int iAmt, sqlite3_int64 iOfst);

static int fsTruncate(sqlite3_file*, sqlite3_int64 size);

static int fsSync(sqlite3_file*, int flags);

static int fsFileSize(sqlite3_file*, sqlite3_int64* pSize);

static int fsLock(sqlite3_file*, int);

static int fsUnlock(sqlite3_file*, int);

static int fsCheckReservedLock(sqlite3_file*, int* pResOut);

static int fsFileControl(sqlite3_file*, int op, void* pArg);

static int fsSectorSize(sqlite3_file*);

static int fsDeviceCharacteristics(sqlite3_file*);

/*
** Method declarations for tmp_file.
*/
static int tmpClose(sqlite3_file*);

static int tmpRead(sqlite3_file*, void*, int iAmt, sqlite3_int64 iOfst);

static int tmpWrite(sqlite3_file*, const void*, int iAmt, sqlite3_int64 iOfst);

static int tmpTruncate(sqlite3_file*, sqlite3_int64 size);

static int tmpSync(sqlite3_file*, int flags);

static int tmpFileSize(sqlite3_file*, sqlite3_int64* pSize);

static int tmpLock(sqlite3_file*, int);

static int tmpUnlock(sqlite3_file*, int);

static int tmpCheckReservedLock(sqlite3_file*, int* pResOut);

static int tmpFileControl(sqlite3_file*, int op, void* pArg);

static int tmpSectorSize(sqlite3_file*);

static int tmpDeviceCharacteristics(sqlite3_file*);

/*
** Method declarations for fs_vfs.
*/
static int fsOpen(sqlite3_vfs*, const char*, sqlite3_file*, int, int*);

static int fsDelete(sqlite3_vfs*, const char* zName, int syncDir);

static int fsAccess(sqlite3_vfs*, const char* zName, int flags, int*);

static int fsFullPathname(sqlite3_vfs*, const char* zName, int nOut, char* zOut);

static void* fsDlOpen(sqlite3_vfs*, const char* zFilename);

static void fsDlError(sqlite3_vfs*, int nByte, char* zErrMsg);

static void (* fsDlSym(sqlite3_vfs*, void*, const char* zSymbol))(void);

static void fsDlClose(sqlite3_vfs*, void*);

static int fsRandomness(sqlite3_vfs*, int nByte, char* zOut);

static int fsSleep(sqlite3_vfs*, int microseconds);

static int fsCurrentTime(sqlite3_vfs*, double*);


typedef struct fs_vfs_t fs_vfs_t;

struct fs_vfs_t
{
  sqlite3_vfs base;
};

static fs_vfs_t fs_vfs = {
  {
    1, /* iVersion */
    0, /* szOsFile */
    0, /* mxPathname */
    0, /* pNext */
    FS_VFS_NAME, /* zName */
    0, /* pAppData */
    fsOpen, /* xOpen */
    fsDelete, /* xDelete */
    fsAccess, /* xAccess */
    fsFullPathname, /* xFullPathname */
    fsDlOpen, /* xDlOpen */
    fsDlError, /* xDlError */
    fsDlSym, /* xDlSym */
    fsDlClose, /* xDlClose */
    fsRandomness, /* xRandomness */
    fsSleep, /* xSleep */
    fsCurrentTime, /* xCurrentTime */
    0 /* xCurrentTimeInt64 */
  }
};

static sqlite3_io_methods fs_io_methods = {
  1, /* iVersion */
  fsClose, /* xClose */
  fsRead, /* xRead */
  fsWrite, /* xWrite */
  fsTruncate, /* xTruncate */
  fsSync, /* xSync */
  fsFileSize, /* xFileSize */
  fsLock, /* xLock */
  fsUnlock, /* xUnlock */
  fsCheckReservedLock, /* xCheckReservedLock */
  fsFileControl, /* xFileControl */
  fsSectorSize, /* xSectorSize */
  fsDeviceCharacteristics, /* xDeviceCharacteristics */
  0, /* xShmMap */
  0, /* xShmLock */
  0, /* xShmBarrier */
  0 /* xShmUnmap */
};


static sqlite3_io_methods tmp_io_methods = {
  1, /* iVersion */
  tmpClose, /* xClose */
  tmpRead, /* xRead */
  tmpWrite, /* xWrite */
  tmpTruncate, /* xTruncate */
  tmpSync, /* xSync */
  tmpFileSize, /* xFileSize */
  tmpLock, /* xLock */
  tmpUnlock, /* xUnlock */
  tmpCheckReservedLock, /* xCheckReservedLock */
  tmpFileControl, /* xFileControl */
  tmpSectorSize, /* xSectorSize */
  tmpDeviceCharacteristics, /* xDeviceCharacteristics */
  0, /* xShmMap */
  0, /* xShmLock */
  0, /* xShmBarrier */
  0 /* xShmUnmap */
};

/* Useful macros used in several places */
#define FS_MIN(x,y) ((x)<(y)?(x):(y))
#define FS_MAX(x,y) ((x)>(y)?(x):(y))


/*
** Close a tmp-file.
*/
static int tmpClose(sqlite3_file* pFile)
{
  tmp_file* pTmp = (tmp_file *) pFile;
  sqlite3_free(pTmp->zAlloc);
  return SQLITE_OK;
}

/*
** Read data from a tmp-file.
*/
static int tmpRead(
  sqlite3_file* pFile,
  void* zBuf,
  int iAmt,
  sqlite3_int64 iOfst
)
{
  tmp_file* pTmp = (tmp_file *) pFile;
  if ((iAmt + iOfst) > pTmp->nSize) {
    return SQLITE_IOERR_SHORT_READ;
  }
  memcpy(zBuf, &pTmp->zAlloc[iOfst], iAmt);
  return SQLITE_OK;
}

/*
** Write data to a tmp-file.
*/
static int tmpWrite(
  sqlite3_file* pFile,
  const void* zBuf,
  int iAmt,
  sqlite3_int64 iOfst
)
{
  tmp_file* pTmp = (tmp_file *) pFile;
  if ((iAmt + iOfst) > pTmp->nAlloc) {
    int nNew = (int) (2 * (iAmt + iOfst + pTmp->nAlloc));
    char* zNew = sqlite3_realloc(pTmp->zAlloc, nNew);
    if (!zNew) {
      return SQLITE_NOMEM;
    }
    pTmp->zAlloc = zNew;
    pTmp->nAlloc = nNew;
  }
  memcpy(&pTmp->zAlloc[iOfst], zBuf, iAmt);
  pTmp->nSize = (int) FS_MAX(pTmp->nSize, iOfst+iAmt);
  return SQLITE_OK;
}

/*
** Truncate a tmp-file.
*/
static int tmpTruncate(sqlite3_file* pFile, sqlite3_int64 size)
{
  tmp_file* pTmp = (tmp_file *) pFile;
  pTmp->nSize = (int) FS_MIN(pTmp->nSize, size);
  return SQLITE_OK;
}

/*
** Sync a tmp-file.
*/
static int tmpSync(sqlite3_file* pFile, int flags)
{
  return SQLITE_OK;
}

/*
** Return the current file-size of a tmp-file.
*/
static int tmpFileSize(sqlite3_file* pFile, sqlite3_int64* pSize)
{
  tmp_file* pTmp = (tmp_file *) pFile;
  *pSize = pTmp->nSize;
  return SQLITE_OK;
}

/*
** Lock a tmp-file.
*/
static int tmpLock(sqlite3_file* pFile, int eLock)
{
  return SQLITE_OK;
}

/*
** Unlock a tmp-file.
*/
static int tmpUnlock(sqlite3_file* pFile, int eLock)
{
  return SQLITE_OK;
}

/*
** Check if another file-handle holds a RESERVED lock on a tmp-file.
*/
static int tmpCheckReservedLock(sqlite3_file* pFile, int* pResOut)
{
  *pResOut = 0;
  return SQLITE_OK;
}

/*
** File control method. For custom operations on a tmp-file.
*/
static int tmpFileControl(sqlite3_file* pFile, int op, void* pArg)
{
  return SQLITE_OK;
}

/*
** Return the sector-size in bytes for a tmp-file.
*/
static int tmpSectorSize(sqlite3_file* pFile)
{
  return 0;
}

/*
** Return the device characteristic flags supported by a tmp-file.
*/
static int tmpDeviceCharacteristics(sqlite3_file* pFile)
{
  return 0;
}

/*
** Close an fs-file.
*/
static int fsClose(sqlite3_file* pFile)
{
  fs_file* p = (fs_file*) pFile;
  sdmmc_vfs_data *p_vfs_data = (sdmmc_vfs_data *)p->pVfs->pAppData;
  p_vfs_data->nRef--;
  if (p_vfs_data->nRef == 0) {
    p_vfs_data->open = 0;
    p_vfs_data->zName = NULL;
    p_vfs_data->nDatabase = 0;
    p_vfs_data->nJournal = 0;
  }
  return SQLITE_OK;
}

/*
** Read data from an fs-file.
*/
static int fsRead(
  sqlite3_file* pFile,
  void* zBuf,
  int iAmt,
  sqlite3_int64 iOfst
)
{
  fs_file* p = (fs_file *) pFile;
  sdmmc_vfs_data *p_vfs_data = (sdmmc_vfs_data *)p->pVfs->pAppData;
  esp_err_t rc = ESP_OK;
  int sector_size = p_vfs_data->pCard->csd.sector_size;
  int64_t available_size;
  int64_t read_offset_on_disk;

  ESP_LOGD(TAG, "fsRead - iAmt %d, iOfst %lld", iAmt, iOfst);

  if (p->eType == DATABASE_FILE) {
    available_size = p_vfs_data->nDatabase;
  } else { // JOURNAL_FILE
    available_size = p_vfs_data->nJournal;
  }

  if (iOfst >= available_size) {
    memset(zBuf, 0, iAmt);
    return SQLITE_IOERR_SHORT_READ;
  }

  int to_read = iAmt;
  if (iOfst + to_read > available_size) {
    to_read = available_size - iOfst;
    memset((char*)zBuf + to_read, 0, iAmt - to_read);
  }

  if (to_read == 0) {
      return iAmt > 0 ? SQLITE_IOERR_SHORT_READ : SQLITE_OK;
  }

  if (p->eType == DATABASE_FILE) {
    read_offset_on_disk = BLOCKSIZE + iOfst;
  } else { // JOURNAL_FILE
    read_offset_on_disk = p_vfs_data->nBlob - iOfst - to_read;
  }

  uint32_t start_sector = read_offset_on_disk / sector_size;
  int64_t end_byte_on_disk = read_offset_on_disk + to_read;
  uint32_t end_sector = (end_byte_on_disk - 1) / sector_size;
  uint32_t num_sectors = end_sector - start_sector + 1;

  bool is_aligned = (read_offset_on_disk % sector_size == 0) && (to_read % sector_size == 0);

  if (is_aligned) {
    rc = sdmmc_read_sectors(p_vfs_data->pCard, zBuf, start_sector, num_sectors);
  } else {
#ifdef CONFIG_SPIRAM
    uint8_t* temp_buf = heap_caps_malloc(num_sectors * sector_size, MALLOC_CAP_SPIRAM);
#else
    uint8_t* temp_buf = malloc(num_sectors * sector_size);
#endif

    if (!temp_buf) {
      return SQLITE_NOMEM;
    }
    rc = sdmmc_read_sectors(p_vfs_data->pCard, temp_buf, start_sector, num_sectors);
    if (rc == ESP_OK) {
      uint32_t offset_in_buffer = read_offset_on_disk - (start_sector * sector_size);
      memcpy(zBuf, temp_buf + offset_in_buffer, to_read);
    }
    free(temp_buf);
  }

  if (rc != ESP_OK) {
    ESP_LOGE(TAG, "fsRead - read fail: %d", rc);
    return SQLITE_IOERR_READ;
  }

  if (to_read < iAmt) {
    ESP_LOGE(TAG, "fsRead - short read: %d %d", to_read, iAmt);
    return SQLITE_IOERR_SHORT_READ;
  }

  return SQLITE_OK;
}

/*
** Write data to an fs-file.
*/
static int fsWrite(
  sqlite3_file* pFile,
  const void* zBuf,
  int iAmt,
  sqlite3_int64 iOfst
)
{
  fs_file* p = (fs_file *) pFile;
  sdmmc_vfs_data *p_vfs_data = (sdmmc_vfs_data *)p->pVfs->pAppData;
  esp_err_t rc = ESP_OK;
  int sector_size = p_vfs_data->pCard->csd.sector_size;

  ESP_LOGD(TAG, "fsWrite - iAmt %d, iOfst %lld ", iAmt, iOfst);

  int64_t start_byte_on_disk;
  if (p->eType == DATABASE_FILE) {
    if ((iOfst + iAmt + BLOCKSIZE) > (p_vfs_data->nBlob - p_vfs_data->nJournal)) {
      return SQLITE_FULL;
    }
    start_byte_on_disk = BLOCKSIZE + iOfst;
  } else { // JOURNAL_FILE
    start_byte_on_disk = p_vfs_data->nBlob - iOfst - iAmt;
    if (start_byte_on_disk < (p_vfs_data->nDatabase + BLOCKSIZE)) {
        return SQLITE_FULL;
    }
  }

  uint32_t start_sector = start_byte_on_disk / sector_size;
  int64_t end_byte_on_disk = start_byte_on_disk + iAmt;
  uint32_t end_sector = (end_byte_on_disk - 1) / sector_size;
  uint32_t num_sectors = end_sector - start_sector + 1;

  bool is_aligned = (start_byte_on_disk % sector_size == 0) && (iAmt % sector_size == 0);

  if (is_aligned) {
    rc = sdmmc_write_sectors(p_vfs_data->pCard, zBuf, start_sector, num_sectors);
  } else {
#ifdef CONFIG_SPIRAM
    uint8_t* temp_buf = heap_caps_malloc(num_sectors * sector_size, MALLOC_CAP_SPIRAM);
#else
    uint8_t* temp_buf = malloc(num_sectors * sector_size);
#endif

    if (!temp_buf) {
      return SQLITE_NOMEM;
    }

    rc = sdmmc_read_sectors(p_vfs_data->pCard, temp_buf, start_sector, num_sectors);
    if (rc != ESP_OK) {
      free(temp_buf);
      return SQLITE_IOERR_READ;
    }

    uint32_t offset_in_buffer = start_byte_on_disk - (start_sector * sector_size);
    memcpy(temp_buf + offset_in_buffer, zBuf, iAmt);

    rc = sdmmc_write_sectors(p_vfs_data->pCard, temp_buf, start_sector, num_sectors);
    free(temp_buf);
  }


  if (rc != ESP_OK) {
    return SQLITE_IOERR_WRITE;
  }

  if (p->eType == DATABASE_FILE) {
    p_vfs_data->nDatabase = (int64_t) FS_MAX(p_vfs_data->nDatabase, iAmt + iOfst);
  } else { // JOURNAL_FILE
    p_vfs_data->nJournal = (int64_t) FS_MAX(p_vfs_data->nJournal, iAmt + iOfst);
  }

  return SQLITE_OK;
}

/*
** Truncate an fs-file.
*/
static int fsTruncate(sqlite3_file* pFile, sqlite3_int64 size)
{
  fs_file* p = (fs_file *) pFile;
  sdmmc_vfs_data *p_vfs_data = (sdmmc_vfs_data *)p->pVfs->pAppData;
  if (p->eType == DATABASE_FILE) {
    p_vfs_data->nDatabase = (int) FS_MIN(p_vfs_data->nDatabase, size);
  } else {
    p_vfs_data->nJournal = (int) FS_MIN(p_vfs_data->nJournal, size);
  }
  return SQLITE_OK;
}

/*
** Sync an fs-file.
*/
static int fsSync(sqlite3_file* pFile, int flags)
{
  fs_file* p = (fs_file *) pFile;
  sdmmc_vfs_data *p_vfs_data = (sdmmc_vfs_data *)p->pVfs->pAppData;
  esp_err_t rc = ESP_OK;

  if (p->eType == DATABASE_FILE) {
    uint8_t zSize[BLOCKSIZE] = {0};
    zSize[0] = (p_vfs_data->nDatabase & 0xFF000000) >> 24;
    zSize[1] = (unsigned char) ((p_vfs_data->nDatabase & 0x00FF0000) >> 16);
    zSize[2] = (p_vfs_data->nDatabase & 0x0000FF00) >> 8;
    zSize[3] = (p_vfs_data->nDatabase & 0x000000FF);
    rc = sdmmc_write_sectors(p_vfs_data->pCard, zSize, 0, 1);
  }

  if (rc != ESP_OK) {
    return SQLITE_IOERR_FSYNC;
  }

  return SQLITE_OK;
}

/*
** Return the current file-size of an fs-file.
*/
static int fsFileSize(sqlite3_file* pFile, sqlite3_int64* pSize)
{
  fs_file* p = (fs_file *) pFile;
  sdmmc_vfs_data *p_vfs_data = (sdmmc_vfs_data *)p->pVfs->pAppData;
  if (p->eType == DATABASE_FILE) {
    *pSize = p_vfs_data->nDatabase;
  } else {
    *pSize = p_vfs_data->nJournal;
  }
  return SQLITE_OK;
}

/*
** Lock an fs-file.
*/
static int fsLock(sqlite3_file* pFile, int eLock)
{
  return SQLITE_OK;
}

/*
** Unlock an fs-file.
*/
static int fsUnlock(sqlite3_file* pFile, int eLock)
{
  return SQLITE_OK;
}

/*
** Check if another file-handle holds a RESERVED lock on an fs-file.
*/
static int fsCheckReservedLock(sqlite3_file* pFile, int* pResOut)
{
  *pResOut = 0;
  return SQLITE_OK;
}

/*
** File control method. For custom operations on an fs-file.
*/
static int fsFileControl(sqlite3_file* pFile, int op, void* pArg)
{
  if (op == SQLITE_FCNTL_PRAGMA) return SQLITE_NOTFOUND;
  return SQLITE_OK;
}

/*
** Return the sector-size in bytes for an fs-file.
*/
static int fsSectorSize(sqlite3_file* pFile)
{
  return BLOCKSIZE;
}

/*
** Return the device characteristic flags supported by an fs-file.
*/
static int fsDeviceCharacteristics(sqlite3_file* pFile)
{
  return SQLITE_IOCAP_ATOMIC512 | SQLITE_IOCAP_SEQUENTIAL;
}

/*
** Open an fs file handle.
*/
static int fsOpen(
  sqlite3_vfs* pVfs,
  const char* zName,
  sqlite3_file* pFile,
  int flags,
  int* pOutFlags
)
{
  fs_file* p = (fs_file *) pFile;
  sdmmc_vfs_data *p_vfs_data = (sdmmc_vfs_data *)pVfs->pAppData;
  int eType;
  esp_err_t rc = ESP_OK;

  if (0 == (flags & (SQLITE_OPEN_MAIN_DB | SQLITE_OPEN_MAIN_JOURNAL))) {
    tmp_file* p2 = (tmp_file *) pFile;
    memset(p2, 0, sizeof(*p2));
    p2->base.pMethods = &tmp_io_methods;
    return SQLITE_OK;
  }

  eType = ((flags & SQLITE_OPEN_MAIN_DB)) ? DATABASE_FILE : JOURNAL_FILE;
  p->pVfs = pVfs;
  p->eType = eType;

  if (eType == DATABASE_FILE) {
    if (p_vfs_data->open) {
      if (strcmp(p_vfs_data->zName, zName) != 0) {
        return SQLITE_BUSY;
      }
    } else {
      p_vfs_data->nBlob = (int64_t)p_vfs_data->pCard->csd.capacity * p_vfs_data->pCard->csd.sector_size;
      p_vfs_data->zName = zName;
      int sector_size = p_vfs_data->pCard->csd.sector_size;

      uint8_t block0[BLOCKSIZE] = {0};
      rc = sdmmc_read_sectors(p_vfs_data->pCard, block0, 0, 1);
      if (rc != ESP_OK) {
        ESP_LOGI(TAG, "Read blk0 fail");
        return SQLITE_IOERR_READ;
      }
      p_vfs_data->nDatabase = (block0[0] << 24) + (block0[1] << 16) + (block0[2] << 8) + block0[3];

#ifdef CONFIG_SPIRAM
      uint8_t* last_block = heap_caps_malloc(sector_size, MALLOC_CAP_SPIRAM);
#else
      uint8_t* last_block = malloc(sector_size);
#endif

      if (!last_block) {
          return SQLITE_NOMEM;
      }
      rc = sdmmc_read_sectors(p_vfs_data->pCard, last_block, (p_vfs_data->nBlob / sector_size) - 1, 1);
      if (rc != ESP_OK) {
        ESP_LOGE(TAG, "Read last blk fail, nblob=%llu", p_vfs_data->nBlob);
        free(last_block);
        return SQLITE_IOERR_READ;
      }

      if (last_block[0] || last_block[1] || last_block[2] || last_block[3]) {
          p_vfs_data->nJournal = p_vfs_data->nBlob;
      }
      free(last_block);
      p_vfs_data->open = 1;
    }
    p_vfs_data->nRef++;
  } else {
    if (!p_vfs_data->open) {
      return SQLITE_CANTOPEN;
    }
    p_vfs_data->nRef++;
  }

  p->base.pMethods = &fs_io_methods;
  
  return SQLITE_OK;
}

/*
** Delete the file located at zPath. If the dirSync argument is true,
** ensure the file-system modifications are synced to disk before
** returning.
*/
static int fsDelete(sqlite3_vfs* pVfs, const char* zPath, int dirSync)
{
  sdmmc_vfs_data *p_vfs_data = (sdmmc_vfs_data *)pVfs->pAppData;
  esp_err_t rc = ESP_OK;
  int nName = (int) strlen(zPath) - 8;

  assert(strcmp("-journal", &zPath[nName])==0);

  if (p_vfs_data->open && strncmp(p_vfs_data->zName, zPath, nName) == 0) {
    uint8_t block[p_vfs_data->pCard->csd.sector_size];
    memset(block, 0, sizeof(block));
    uint32_t sector = (p_vfs_data->nBlob / p_vfs_data->pCard->csd.sector_size - 1);
    rc = sdmmc_write_sectors(p_vfs_data->pCard, block, sector, 1);
    if (rc == ESP_OK) {
      p_vfs_data->nJournal = 0;
    }
  }
  return rc == ESP_OK ? SQLITE_OK : SQLITE_IOERR_DELETE;
}

/*
** Test for access permissions. Return true if the requested permission
** is available, or false otherwise.
*/
static int fsAccess(
  sqlite3_vfs* pVfs,
  const char* zPath,
  int flags,
  int* pResOut
)
{
  sdmmc_vfs_data *p_vfs_data = (sdmmc_vfs_data *)pVfs->pAppData;
  int isJournal = 0;
  int nName = (int) strlen(zPath);

  if (flags != SQLITE_ACCESS_EXISTS) {
    *pResOut = 0;
    return SQLITE_OK;
  }

  if (nName > 8 && strcmp("-journal", &zPath[nName - 8]) == 0) {
    nName -= 8;
    isJournal = 1;
  }

  if (p_vfs_data->open && strncmp(p_vfs_data->zName, zPath, nName) == 0) {
    *pResOut = (!isJournal || p_vfs_data->nJournal > 0);
  } else {
    *pResOut = 0;
  }

  return SQLITE_OK;
}

/*
** Populate buffer zOut with the full canonical pathname corresponding
** to the pathname in zPath. zOut is guaranteed to point to a buffer
** of at least (FS_MAX_PATHNAME+1) bytes.
*/
static int fsFullPathname(
  sqlite3_vfs* pVfs, /* Pointer to vfs object */
  const char* zPath, /* Possibly relative input path */
  int nOut, /* Size of output buffer in bytes */
  char* zOut /* Output buffer */
)
{
  strncpy(zOut, zPath, nOut);
  zOut[nOut-1] = '\0';
  return SQLITE_OK;
}

/*
** Open the dynamic library located at zPath and return a handle.
*/
static void* fsDlOpen(sqlite3_vfs* pVfs, const char* zPath)
{
  return 0;
}

/*
** Populate the buffer zErrMsg (size nByte bytes) with a human readable
** utf-8 string describing the most recent error encountered associated
** with dynamic libraries.
*/
static void fsDlError(sqlite3_vfs* pVfs, int nByte, char* zErrMsg)
{
  sqlite3_snprintf(nByte, zErrMsg, "Dynamic libraries not supported");
  zErrMsg[nByte-1] = '\0';
}

/*
** Return a pointer to the symbol zSymbol in the dynamic library pHandle.
*/
static void (* fsDlSym(sqlite3_vfs* pVfs, void* pH, const char* zSym))(void)
{
  return 0;
}

/*
** Close the dynamic library handle pHandle.
*/
static void fsDlClose(sqlite3_vfs* pVfs, void* pHandle)
{
  return;
}

/*
** Populate the buffer pointed to by zBufOut with nByte bytes of
** random data.
*/
static int fsRandomness(sqlite3_vfs* pVfs, int nByte, char* zBufOut)
{
  esp_fill_random(zBufOut, nByte);
  return nByte;
}

/*
** Sleep for nMicro microseconds. Return the number of microseconds
** actually slept.
*/
static int fsSleep(sqlite3_vfs* pVfs, int nMicro)
{
  vTaskDelay(nMicro / 1000 / portTICK_PERIOD_MS);
  return nMicro;
}

/*
** Return the current time as a Julian Day number in *pTimeOut.
*/
static int fsCurrentTime(sqlite3_vfs* pVfs, double* pTimeOut)
{
  struct timeval sNow;
  gettimeofday(&sNow, 0);
  *pTimeOut = 2440587.5 + sNow.tv_sec/86400.0 + sNow.tv_usec/86400000000.0;
  return SQLITE_OK;
}

/*
** This procedure registers the fs vfs with SQLite. If the argument is
** true, the fs vfs becomes the new default vfs. It is the only publicly
** available function in this file.
*/
int sqlite3_esp_sqlite_sdmmc_vfs_register(sdmmc_card_t *p_card, int make_default)
{
  if (fs_vfs.base.pAppData) {
      sqlite3_free(fs_vfs.base.pAppData);
  }

  sdmmc_vfs_data *p_data = malloc(sizeof(sdmmc_vfs_data));
  if(!p_data) {
      fs_vfs.base.pAppData = NULL;
      return SQLITE_NOMEM;
  }
  memset(p_data, 0, sizeof(sdmmc_vfs_data));
  p_data->pCard = p_card;

  fs_vfs.base.pAppData = p_data;
  fs_vfs.base.mxPathname = 256;
  fs_vfs.base.szOsFile = FS_MAX(sizeof(tmp_file), sizeof(fs_file));
  return sqlite3_vfs_register(&fs_vfs.base, make_default);
}

#ifdef SQLITE_TEST
int SqlitetestOnefile_Init() { return fs_register(); }
#endif

int sqlite3_os_init()
{
  return 0;
}

int sqlite3_os_end()
{
  return 0;
}