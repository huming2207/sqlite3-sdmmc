#pragma once

#include <sd_protocol_types.h>

#ifdef __cplusplus
extern "C" {
#endif

#define ESP_SQLITE_SDMMC_VFS_NAME "sdmmc"

int sqlite3_esp_sqlite_sdmmc_vfs_register(sdmmc_card_t *p_card, int make_default);

#ifdef __cplusplus
}
#endif