idf_component_register(
        SRCS "sqlite3.c" "esp_sqlite_sdmmc_vfs.c"
        INCLUDE_DIRS "."
        REQUIRES "sdmmc"
)