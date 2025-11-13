# Crypto Data Downloader

Utility for **downloading** historical market data and funding rates from crypto exchanges, currently only Binance, Bybit and OKX exchange is supported

## Compilation:

### Windows

This is a **CMake** Project, you can download **CMake** here: https://cmake.org/download/ 

Then you need a C++ compiler with C++20 support, so download and install Visual Studio C++ 2022 Community:
https://visualstudio.microsoft.com/downloads/ and install with the option: **Desktop development with C++** enabled.

Then you must install all the dependencies. The easiest way on a Windows platform is to use the **vcpkg** package manager.
This can be installed using the instructions here: https://vcpkg.io/en/getting-started, the prerequisite is to have **Git** installed.

Once you are done, add a vcpkg.exe to your PATH and you can start installing the necessary packages:

```console
- vcpkg install cxxopts:x64-windows
- vcpkg install libpqxx:x64-windows
- vcpkg install spdlog:x64-windows
- vcpkg install openssl:x64-windows
- vcpkg install boost:x64-windows
- vcpkg install nlohmann-json:x64-windows
```

Finally, build the Data Downloader:

```console
git clone git@github.com:vitakot/crypto_data_downloader.git
cd .\crypto_data_downloader\
git submodule update --init --recursive
mkdir build
cd .\build\
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=C:\vcpkg\scripts\buildsystems\vcpkg.cmake ..
cmake --build . -j12
```

### Linux

Install CMake:

```console
sudo apt install cmake
```

Install C++ build tools:

```console
sudo apt-get install build-essential
```

Linux has a native package manager so just install the dependencies using it e.g. on Ubuntu:

```console
- sudo apt install libfmt-dev
- sudo apt install libssl-dev
...
```

Build is the same except there is no need to specify the path to the package manager (vcpkg).

## Usage:

data_downloader [OPTION...]

* -e, --exchange arg     Exchange name, either Binance (bnb), OKX (okx) or Bybit (bybit), example: -e bnb (default: bnb)
* -t, --data_type arg    Data type for download, either candles 'c' or funding rate 'fr', example -t c, default is candles)",
* -o, --output arg       Output directory path, example: -o "C:\Users\UserName\BNBData"
* -s, --symbols arg      Symbols of assets to download, example: -s "BTCUSDT,ETHUSDT", "all" means All symbols, mutually exclusive with parameter -a (default: all)
* -a, --assets_file arg  Path to Zorro Assets file, mutually exclusive with parameter -s (default: "")
* -j, --jobs arg         Maximum number of jobs to run in parallel, example -j 8 (default: depends on machine power)
* -b, --bar_size arg     Bar size in minutes, example -b 5, default is 1 (default: 1)
* -h, --help             Print usage
