cmake_minimum_required(VERSION 3.20)
project(crypto_data_downloader)

set(CMAKE_CXX_STANDARD 20)

if (MSVC)
    add_definitions(-D_WIN32_WINNT=0x0A00 /utf-8)
endif ()

if (POLICY CMP0167)
    cmake_policy(SET CMP0167 NEW)
endif ()

find_package(spdlog CONFIG REQUIRED)
find_package(cxxopts CONFIG REQUIRED)
find_package(PostgreSQL REQUIRED)

add_subdirectory(binance_cpp_api)
add_subdirectory(bybit_cpp_api)
add_subdirectory(okx_cpp_api)

if (NOT TARGET vk_common)
    add_subdirectory(vk_cpp_common)
endif ()

include_directories(include vk_cpp_common/include binance_cpp_api/include bybit_cpp_api/include okx_cpp_api/include magic_enum)

set(HEADERS
        include/vk/downloader.h
        include/vk/binance/binance_futures_downloader.h
        include/vk/binance/binance_spot_downloader.h
        include/vk/binance/binance_common.h
        include/vk/bybit/bybit_downloader.h
        include/vk/okx/okx_downloader.h
)

set(SOURCES
        src/binance_futures_downloader.cpp
        src/binance_spot_downloader.cpp
        src/bybit_downloader.cpp
        src/postgres_connector.cpp
        src/okx_downloader.cpp
        src/binance_common.cpp
)

add_executable(crypto_data_downloader main.cpp ${SOURCES} ${HEADERS})
target_link_libraries(crypto_data_downloader PRIVATE spdlog::spdlog_header_only PostgreSQL::PostgreSQL binance_api bybit_api okx_api)
