idf_component_register(
        SRCS "sqlite3.c" "esp_sqlite_sdmmc_vfs.c"
        INCLUDE_DIRS "."
        REQUIRES "sdmmc"
)

target_compile_options(${COMPONENT_LIB} PRIVATE -std=gnu99 -g3
        -fno-stack-protector -ffunction-sections -fdata-sections -fstrict-volatile-bitfields -nostdlib -Wpointer-arith
        -Wno-error=misleading-indentation -Wno-error=unused-value -Wno-error=unused-label -Wno-error=unused-function
        -Wno-error=unused-but-set-variable -Wno-error=unused-variable -Wno-error=deprecated-declarations -Wno-error=char-subscripts
        -Wno-error=maybe-uninitialized -Wno-unused-parameter -Wno-sign-compare -Wno-old-style-declaration -MMD -c -DNDEBUG)