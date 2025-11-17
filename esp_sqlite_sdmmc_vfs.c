#include <string.h>
#include <stddef.h>
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
    int64_t nWal;          /* Current size of wal region */
    int64_t nBlob;         /* Total size of allocated blob */
    int nRef;              /* Number of open file handles */
    const char *zName;     /* Name of database file */
    int open;              /* True if database file is open */
    sdmmc_card_t *pCard;
    uint8_t *pShm;         /* In-RAM shm backing store */
    int nShm;              /* Bytes allocated for shm */
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
#define WAL_FILE        3

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

static int fsShmMap(sqlite3_file*, int, int, int, void volatile**);

static int fsShmLock(sqlite3_file*, int, int, int);

static void fsShmBarrier(sqlite3_file*);

static int fsShmUnmap(sqlite3_file*, int);

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
  2, /* iVersion */
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
  fsShmMap, /* xShmMap */
  fsShmLock, /* xShmLock */
  fsShmBarrier, /* xShmBarrier */
  fsShmUnmap /* xShmUnmap */
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

#define SDMMC_META_MAGIC 0x53514c4954455344ULL /* "SQLITESD" */
#define SDMMC_META_VERSION 1

typedef struct __attribute__((packed)) {
  uint64_t magic;
  uint32_t version;
  uint32_t checksum;
  int64_t nDatabase;
  int64_t nJournal;
  int64_t nWal;
} sdmmc_meta_t;

_Static_assert(sizeof(sdmmc_meta_t) <= BLOCKSIZE, "metadata must fit inside block");

static const uint32_t fsCrc32Table[256] = {
  0x00000000u, 0x77073096u, 0xEE0E612Cu, 0x990951BAu, 0x076DC419u, 0x706AF48Fu, 0xE963A535u, 0x9E6495A3u,
  0x0EDB8832u, 0x79DCB8A4u, 0xE0D5E91Eu, 0x97D2D988u, 0x09B64C2Bu, 0x7EB17CBDu, 0xE7B82D07u, 0x90BF1D91u,
  0x1DB71064u, 0x6AB020F2u, 0xF3B97148u, 0x84BE41DEu, 0x1ADAD47Du, 0x6DDDE4EBu, 0xF4D4B551u, 0x83D385C7u,
  0x136C9856u, 0x646BA8C0u, 0xFD62F97Au, 0x8A65C9ECu, 0x14015C4Fu, 0x63066CD9u, 0xFA0F3D63u, 0x8D080DF5u,
  0x3B6E20C8u, 0x4C69105Eu, 0xD56041E4u, 0xA2677172u, 0x3C03E4D1u, 0x4B04D447u, 0xD20D85FDu, 0xA50AB56Bu,
  0x35B5A8FAu, 0x42B2986Cu, 0xDBBBC9D6u, 0xACBCF940u, 0x32D86CE3u, 0x45DF5C75u, 0xDCD60DCFu, 0xABD13D59u,
  0x26D930ACu, 0x51DE003Au, 0xC8D75180u, 0xBFD06116u, 0x21B4F4B5u, 0x56B3C423u, 0xCFBA9599u, 0xB8BDA50Fu,
  0x2802B89Eu, 0x5F058808u, 0xC60CD9B2u, 0xB10BE924u, 0x2F6F7C87u, 0x58684C11u, 0xC1611DABu, 0xB6662D3Du,
  0x76DC4190u, 0x01DB7106u, 0x98D220BCu, 0xEFD5102Au, 0x71B18589u, 0x06B6B51Fu, 0x9FBFE4A5u, 0xE8B8D433u,
  0x7807C9A2u, 0x0F00F934u, 0x9609A88Eu, 0xE10E9818u, 0x7F6A0DBBu, 0x086D3D2Du, 0x91646C97u, 0xE6635C01u,
  0x6B6B51F4u, 0x1C6C6162u, 0x856530D8u, 0xF262004Eu, 0x6C0695EDu, 0x1B01A57Bu, 0x8208F4C1u, 0xF50FC457u,
  0x65B0D9C6u, 0x12B7E950u, 0x8BBEB8EAu, 0xFCB9887Cu, 0x62DD1DDFu, 0x15DA2D49u, 0x8CD37CF3u, 0xFBD44C65u,
  0x4DB26158u, 0x3AB551CEu, 0xA3BC0074u, 0xD4BB30E2u, 0x4ADFA541u, 0x3DD895D7u, 0xA4D1C46Du, 0xD3D6F4FBu,
  0x4369E96Au, 0x346ED9FCu, 0xAD678846u, 0xDA60B8D0u, 0x44042D73u, 0x33031DE5u, 0xAA0A4C5Fu, 0xDD0D7CC9u,
  0x5005713Cu, 0x270241AAu, 0xBE0B1010u, 0xC90C2086u, 0x5768B525u, 0x206F85B3u, 0xB966D409u, 0xCE61E49Fu,
  0x5EDEF90Eu, 0x29D9C998u, 0xB0D09822u, 0xC7D7A8B4u, 0x59B33D17u, 0x2EB40D81u, 0xB7BD5C3Bu, 0xC0BA6CADu,
  0xEDB88320u, 0x9ABFB3B6u, 0x03B6E20Cu, 0x74B1D29Au, 0xEAD54739u, 0x9DD277AFu, 0x04DB2615u, 0x73DC1683u,
  0xE3630B12u, 0x94643B84u, 0x0D6D6A3Eu, 0x7A6A5AA8u, 0xE40ECF0Bu, 0x9309FF9Du, 0x0A00AE27u, 0x7D079EB1u,
  0xF00F9344u, 0x8708A3D2u, 0x1E01F268u, 0x6906C2FEu, 0xF762575Du, 0x806567CBu, 0x196C3671u, 0x6E6B06E7u,
  0xFED41B76u, 0x89D32BE0u, 0x10DA7A5Au, 0x67DD4ACCu, 0xF9B9DF6Fu, 0x8EBEEFF9u, 0x17B7BE43u, 0x60B08ED5u,
  0xD6D6A3E8u, 0xA1D1937Eu, 0x38D8C2C4u, 0x4FDFF252u, 0xD1BB67F1u, 0xA6BC5767u, 0x3FB506DDu, 0x48B2364Bu,
  0xD80D2BDAu, 0xAF0A1B4Cu, 0x36034AF6u, 0x41047A60u, 0xDF60EFC3u, 0xA867DF55u, 0x316E8EEFu, 0x4669BE79u,
  0xCB61B38Cu, 0xBC66831Au, 0x256FD2A0u, 0x5268E236u, 0xCC0C7795u, 0xBB0B4703u, 0x220216B9u, 0x5505262Fu,
  0xC5BA3BBEu, 0xB2BD0B28u, 0x2BB45A92u, 0x5CB36A04u, 0xC2D7FFA7u, 0xB5D0CF31u, 0x2CD99E8Bu, 0x5BDEAE1Du,
  0x9B64C2B0u, 0xEC63F226u, 0x756AA39Cu, 0x026D930Au, 0x9C0906A9u, 0xEB0E363Fu, 0x72076785u, 0x05005713u,
  0x95BF4A82u, 0xE2B87A14u, 0x7BB12BAEu, 0x0CB61B38u, 0x92D28E9Bu, 0xE5D5BE0Du, 0x7CDCEFB7u, 0x0BDBDF21u,
  0x86D3D2D4u, 0xF1D4E242u, 0x68DDB3F8u, 0x1FDA836Eu, 0x81BE16CDu, 0xF6B9265Bu, 0x6FB077E1u, 0x18B74777u,
  0x88085AE6u, 0xFF0F6A70u, 0x66063BCAu, 0x11010B5Cu, 0x8F659EFFu, 0xF862AE69u, 0x616BFFD3u, 0x166CCF45u,
  0xA00AE278u, 0xD70DD2EEu, 0x4E048354u, 0x3903B3C2u, 0xA7672661u, 0xD06016F7u, 0x4969474Du, 0x3E6E77DBu,
  0xAED16A4Au, 0xD9D65ADCu, 0x40DF0B66u, 0x37D83BF0u, 0xA9BCAE53u, 0xDEBB9EC5u, 0x47B2CF7Fu, 0x30B5FFE9u,
  0xBDBDF21Cu, 0xCABAC28Au, 0x53B39330u, 0x24B4A3A6u, 0xBAD03605u, 0xCDD70693u, 0x54DE5729u, 0x23D967BFu,
  0xB3667A2Eu, 0xC4614AB8u, 0x5D681B02u, 0x2A6F2B94u, 0xB40BBE37u, 0xC30C8EA1u, 0x5A05DF1Bu, 0x2D02EF8Du
};

static uint32_t fsCrc32(const uint8_t *data, size_t n)
{
  uint32_t crc = 0xFFFFFFFFu;
  size_t i;
  for (i = 0; i < n; ++i) {
    uint8_t idx = (uint8_t)((crc ^ data[i]) & 0xFFu);
    crc = (crc >> 8) ^ fsCrc32Table[idx];
  }
  return ~crc;
}

static int fsPersistMetadata(sdmmc_vfs_data *p_vfs_data)
{
  uint8_t block[BLOCKSIZE] = {0};
  sdmmc_meta_t meta = {
    .magic = SDMMC_META_MAGIC,
    .version = SDMMC_META_VERSION,
    .checksum = 0,
    .nDatabase = p_vfs_data->nDatabase,
    .nJournal = p_vfs_data->nJournal,
    .nWal = p_vfs_data->nWal
  };
  meta.checksum = fsCrc32((const uint8_t *)&meta, sizeof(meta));
  memcpy(block, &meta, sizeof(meta));
  esp_err_t rc = sdmmc_write_sectors(p_vfs_data->pCard, block, 0, 1);
  if (rc != ESP_OK) {
    ESP_LOGE(TAG, "Failed to persist metadata: %d", rc);
    return SQLITE_IOERR_WRITE;
  }
  return SQLITE_OK;
}


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
    p_vfs_data->nWal = 0;
    if (p_vfs_data->pShm) {
      sqlite3_free(p_vfs_data->pShm);
      p_vfs_data->pShm = NULL;
      p_vfs_data->nShm = 0;
    }
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
  int64_t base_end = p_vfs_data->nBlob;

  ESP_LOGD(TAG, "fsRead - iAmt %d, iOfst %lld", iAmt, iOfst);

  if (p->eType == DATABASE_FILE) {
    available_size = p_vfs_data->nDatabase;
  } else if (p->eType == JOURNAL_FILE) {
    available_size = p_vfs_data->nJournal;
  } else {
    available_size = p_vfs_data->nWal;
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
  } else if (p->eType == JOURNAL_FILE) {
    read_offset_on_disk = p_vfs_data->nBlob - iOfst - to_read;
  } else {
    base_end = p_vfs_data->nBlob - p_vfs_data->nJournal;
    read_offset_on_disk = base_end - iOfst - to_read;
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
    int64_t tail_available = p_vfs_data->nBlob - (p_vfs_data->nJournal + p_vfs_data->nWal);
    if ((iOfst + iAmt + BLOCKSIZE) > tail_available) {
      return SQLITE_FULL;
    }
    start_byte_on_disk = BLOCKSIZE + iOfst;
  } else if (p->eType == JOURNAL_FILE) {
    start_byte_on_disk = p_vfs_data->nBlob - iOfst - iAmt;
    if (start_byte_on_disk < (p_vfs_data->nDatabase + BLOCKSIZE)) {
        return SQLITE_FULL;
    }
  } else { // WAL_FILE
    int64_t wal_end = p_vfs_data->nBlob - p_vfs_data->nJournal;
    start_byte_on_disk = wal_end - iOfst - iAmt;
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
  } else if (p->eType == JOURNAL_FILE) {
    p_vfs_data->nJournal = (int64_t) FS_MAX(p_vfs_data->nJournal, iAmt + iOfst);
  } else {
    p_vfs_data->nWal = (int64_t) FS_MAX(p_vfs_data->nWal, iAmt + iOfst);
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
  } else if (p->eType == JOURNAL_FILE) {
    p_vfs_data->nJournal = (int) FS_MIN(p_vfs_data->nJournal, size);
  } else {
    p_vfs_data->nWal = (int) FS_MIN(p_vfs_data->nWal, size);
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
  if (p->eType == DATABASE_FILE || p->eType == JOURNAL_FILE || p->eType == WAL_FILE) {
    int rc = fsPersistMetadata(p_vfs_data);
    return rc == SQLITE_OK ? SQLITE_OK : SQLITE_IOERR_FSYNC;
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
  } else if (p->eType == JOURNAL_FILE) {
    *pSize = p_vfs_data->nJournal;
  } else {
    *pSize = p_vfs_data->nWal;
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

static int fsShmMap(sqlite3_file* pFile, int iPg, int pgsz, int isWrite, void volatile** pp)
{
  fs_file* p = (fs_file *) pFile;
  sdmmc_vfs_data *p_vfs_data = (sdmmc_vfs_data *)p->pVfs->pAppData;
  int required;
  uint8_t *pNew;

  if (pgsz <= 0 || iPg < 0) {
    return SQLITE_IOERR_SHMMAP;
  }

  required = (iPg + 1) * pgsz;
  if (required > p_vfs_data->nShm) {
    pNew = sqlite3_realloc(p_vfs_data->pShm, required);
    if (!pNew) {
      return SQLITE_NOMEM;
    }
    if (required > p_vfs_data->nShm) {
      memset(pNew + p_vfs_data->nShm, 0, required - p_vfs_data->nShm);
    }
    p_vfs_data->pShm = pNew;
    p_vfs_data->nShm = required;
  }

  *pp = (void volatile*) (p_vfs_data->pShm + (iPg * pgsz));
  return SQLITE_OK;
}

static int fsShmLock(sqlite3_file* pFile, int offset, int n, int flags)
{
  (void)pFile;
  (void)offset;
  (void)n;
  (void)flags;
  return SQLITE_OK;
}

static void fsShmBarrier(sqlite3_file* pFile)
{
  (void)pFile;
#if defined(__GNUC__)
  __sync_synchronize();
#endif
}

static int fsShmUnmap(sqlite3_file* pFile, int deleteFlag)
{
  fs_file* p = (fs_file *) pFile;
  sdmmc_vfs_data *p_vfs_data = (sdmmc_vfs_data *)p->pVfs->pAppData;
  if (deleteFlag && p_vfs_data->pShm) {
    sqlite3_free(p_vfs_data->pShm);
    p_vfs_data->pShm = NULL;
    p_vfs_data->nShm = 0;
  }
  return SQLITE_OK;
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

  if (0 == (flags & (SQLITE_OPEN_MAIN_DB | SQLITE_OPEN_MAIN_JOURNAL | SQLITE_OPEN_WAL))) {
    tmp_file* p2 = (tmp_file *) pFile;
    memset(p2, 0, sizeof(*p2));
    p2->base.pMethods = &tmp_io_methods;
    return SQLITE_OK;
  }

  if (flags & SQLITE_OPEN_MAIN_DB) {
    eType = DATABASE_FILE;
  } else if (flags & SQLITE_OPEN_MAIN_JOURNAL) {
    eType = JOURNAL_FILE;
  } else {
    eType = WAL_FILE;
  }
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

      uint8_t block0[BLOCKSIZE] = {0};
      sdmmc_meta_t meta = {};
      rc = sdmmc_read_sectors(p_vfs_data->pCard, block0, 0, 1);
      if (rc != ESP_OK) {
        ESP_LOGI(TAG, "Read blk0 fail");
        return SQLITE_IOERR_READ;
      }
      memcpy(&meta, block0, sizeof(meta));
      int meta_valid = 0;
      if (meta.magic == SDMMC_META_MAGIC && meta.version == SDMMC_META_VERSION) {
        uint32_t stored = meta.checksum;
        meta.checksum = 0;
        if (stored == fsCrc32((const uint8_t *)&meta, sizeof(meta))) {
          meta_valid = 1;
        }
      }
      if (meta_valid) {
        p_vfs_data->nDatabase = meta.nDatabase;
        p_vfs_data->nJournal = meta.nJournal;
        p_vfs_data->nWal = meta.nWal;
      } else {
        p_vfs_data->nDatabase = 0;
        p_vfs_data->nJournal = 0;
        p_vfs_data->nWal = 0;
      }

      int64_t maxDb = p_vfs_data->nBlob > BLOCKSIZE ? (p_vfs_data->nBlob - BLOCKSIZE) : 0;
      if (p_vfs_data->nDatabase < 0 || p_vfs_data->nDatabase > maxDb) {
        p_vfs_data->nDatabase = 0;
      }
      if (p_vfs_data->nJournal < 0) {
        p_vfs_data->nJournal = 0;
      }
      if (p_vfs_data->nWal < 0) {
        p_vfs_data->nWal = 0;
      }

      int64_t maxTail = p_vfs_data->nBlob - (BLOCKSIZE + p_vfs_data->nDatabase);
      if (maxTail < 0) {
        p_vfs_data->nJournal = 0;
        p_vfs_data->nWal = 0;
      } else {
        if (p_vfs_data->nWal > maxTail) {
          p_vfs_data->nWal = maxTail;
        }
        if (p_vfs_data->nJournal > (maxTail - p_vfs_data->nWal)) {
          p_vfs_data->nJournal = maxTail - p_vfs_data->nWal;
        }
      }
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
  int nName = (int) strlen(zPath);
  int isJournal = 0;
  int isWal = 0;
  int isShm = 0;
  (void)dirSync;

  if (nName > 8 && strcmp("-journal", &zPath[nName - 8]) == 0) {
    nName -= 8;
    isJournal = 1;
  } else if (nName > 4 && strcmp("-wal", &zPath[nName - 4]) == 0) {
    nName -= 4;
    isWal = 1;
  } else if (nName > 4 && strcmp("-shm", &zPath[nName - 4]) == 0) {
    nName -= 4;
    isShm = 1;
  } else {
    return SQLITE_OK;
  }

  if (!p_vfs_data->open || strncmp(p_vfs_data->zName, zPath, nName) != 0) {
    return SQLITE_OK;
  }

  if (isShm) {
    if (p_vfs_data->pShm) {
      sqlite3_free(p_vfs_data->pShm);
      p_vfs_data->pShm = NULL;
      p_vfs_data->nShm = 0;
    }
    return SQLITE_OK;
  }

  uint32_t sector_size = p_vfs_data->pCard->csd.sector_size;
  uint8_t block[sector_size];
  memset(block, 0, sizeof(block));

  if (isJournal && p_vfs_data->nJournal > 0) {
    uint32_t sector = (uint32_t)((p_vfs_data->nBlob / sector_size) - 1);
    rc = sdmmc_write_sectors(p_vfs_data->pCard, block, sector, 1);
    if (rc != ESP_OK) {
      return SQLITE_IOERR_DELETE;
    }
    p_vfs_data->nJournal = 0;
  } else if (isWal && p_vfs_data->nWal > 0) {
    int64_t wal_start = p_vfs_data->nBlob - p_vfs_data->nJournal - p_vfs_data->nWal;
    uint32_t sector = (uint32_t)(wal_start / sector_size);
    rc = sdmmc_write_sectors(p_vfs_data->pCard, block, sector, 1);
    if (rc != ESP_OK) {
      return SQLITE_IOERR_DELETE;
    }
    p_vfs_data->nWal = 0;
  }

  {
    int rcMeta = fsPersistMetadata(p_vfs_data);
    return rcMeta == SQLITE_OK ? SQLITE_OK : SQLITE_IOERR_DELETE;
  }
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
  int isWal = 0;
  int isShm = 0;
  int nName = (int) strlen(zPath);

  if (flags != SQLITE_ACCESS_EXISTS) {
    *pResOut = 0;
    return SQLITE_OK;
  }

  if (nName > 8 && strcmp("-journal", &zPath[nName - 8]) == 0) {
    nName -= 8;
    isJournal = 1;
  } else if (nName > 4 && strcmp("-wal", &zPath[nName - 4]) == 0) {
    nName -= 4;
    isWal = 1;
  } else if (nName > 4 && strcmp("-shm", &zPath[nName - 4]) == 0) {
    nName -= 4;
    isShm = 1;
  }

  if (p_vfs_data->open && strncmp(p_vfs_data->zName, zPath, nName) == 0) {
    if (isJournal) {
      *pResOut = (p_vfs_data->nJournal > 0);
    } else if (isWal) {
      *pResOut = (p_vfs_data->nWal > 0);
    } else if (isShm) {
      *pResOut = (p_vfs_data->pShm != NULL);
    } else {
      *pResOut = 1;
    }
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

#ifdef CONFIG_SPIRAM
  sdmmc_vfs_data *p_data = heap_caps_malloc(sizeof(sdmmc_vfs_data), MALLOC_CAP_SPIRAM);
#else
  sdmmc_vfs_data *p_data = malloc(sizeof(sdmmc_vfs_data));
#endif

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

static void *sqlite3_psram_malloc(int len)
{
  return heap_caps_malloc(len, MALLOC_CAP_SPIRAM);
}

static void *sqlite3_psram_realloc(void *ptr, int len)
{
  return heap_caps_realloc(ptr, len, MALLOC_CAP_SPIRAM);
}

static int sqlite3_psram_size(void *ptr)
{
  return heap_caps_get_allocated_size(ptr);
}

static int sqlite3_psram_roundup(int size)
{
  return size;
}

static int sqlite3_psram_init(void *ctx)
{
  return 0;
}

static void sqlite3_psram_deinit()
{
}

int sqlite3_os_init()
{
  static sqlite3_mem_methods mem_methods = {
    .xFree = free,
    .xMalloc = sqlite3_psram_malloc,
    .xRealloc = sqlite3_psram_realloc,
    .xSize = sqlite3_psram_size,
    .xRoundup = sqlite3_psram_roundup,
    .xInit = sqlite3_psram_init,
    .xShutdown = sqlite3_psram_deinit,
  };

  sqlite3_config(SQLITE_CONFIG_MALLOC, &mem_methods);
  return 0;
}

int sqlite3_os_end()
{
  return 0;
}
