# Crypto Data Downloader

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![C++20](https://img.shields.io/badge/C%2B%2B-20-blue.svg)](https://en.cppreference.com/w/cpp/20)
[![CMake](https://img.shields.io/badge/CMake-3.20+-green.svg)](https://cmake.org/)

A high-performance command-line utility for downloading historical market data (OHLCV candles) and funding rates from major cryptocurrency exchanges.

## Features

- **Multi-Exchange Support**: Binance, Bybit, OKX, and MEXC
- **Multiple Data Types**: OHLCV candles and funding rate history
- **Parallel Downloads**: Configurable concurrent job processing
- **Flexible Output**: CSV format with optional T6 (Zorro) conversion
- **Incremental Updates**: Only downloads new data since last run
- **Symbol Filtering**: Download specific symbols or all available pairs
- **Multiple Timeframes**: Support for various bar sizes (1m, 5m, 15m, 1h, etc.)
- **Market Categories**: Supports both Spot and Futures markets

## Supported Exchanges

| Exchange | Futures | Spot | Candles | Funding Rates |
|----------|:-------:|:----:|:-------:|:-------------:|
| Binance  | ✅ | ✅ | ✅ | ✅ |
| Bybit    | ✅ | ✅ | ✅ | ✅ |
| OKX      | ✅ | ✅ | ✅ | ✅ |
| MEXC     | ✅ | ✅ | ✅ | ✅ |

### Exchange-Specific Notes

#### MEXC Historical Data Limits

MEXC API has **undocumented limits** for historical candlestick data:

| Interval | Spot Available | Futures Available |
|----------|----------------|-------------------|
| 1m       | ~30 days       | ~30 days          |
| 5m       | ~270 days      | ~360 days         |
| 15m      | ~270 days      | ~180-365 days     |
| 30m      | ~270 days      | 5+ years          |
| **1h+**  | **Complete**   | **Complete**      |

> **Recommendation:** Use **1h (hourly)** or larger intervals for complete MEXC historical data.

## Bybit Delisted Symbol Downloader (Python)

The C++ downloader only fetches data for **active symbols** via exchange REST APIs.
Historical data for delisted symbols is not available through the standard API endpoints,
but Bybit publishes complete tick-by-tick trade history on a public server:

- **Futures:** `https://public.bybit.com/trading/`
- **Spot:** `https://public.bybit.com/spot/`

The Python script `python/bybit_history_downloader.py` handles this use case.

### What it does

1. Queries the Bybit REST API (`GET /v5/market/instruments-info?status=Closed`) to obtain
   the full list of delisted symbols (USDT-margined linear perpetuals or spot).
2. Cross-references the result with symbols available on `public.bybit.com`.
3. Downloads the missing daily tick data files (`.csv.gz`) and aggregates them to OHLCV
   candles at **1m** and **1h** resolution.
4. Saves the output in the same directory structure as the C++ downloader
   (`csvFut/1m/SYMBOL.csv`, `csvFut/1h/SYMBOL.csv` for futures;
   `csvSpot/1m/SYMBOL.csv`, `csvSpot/1h/SYMBOL.csv` for spot).
5. On subsequent runs, only newly published files are downloaded (incremental updates).

### Output format

```csv
open_time,open,high,low,close,volume,turnover
1704067200000,42285.0,42300.0,42280.0,42290.0,1.5,63427.5
```

- `open_time` — milliseconds since UTC epoch (compatible with C++ CSV reader)
- `volume` — base asset quantity
- `turnover` — USDT value

### Setup

```bash
cd python
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### Usage

```bash
# All delisted futures (1m + 1h), 4 parallel threads
python python/bybit_history_downloader.py --type futures -o /data/crypto

# All delisted spot
python python/bybit_history_downloader.py --type spot -o /data/crypto

# Specific symbols only
python python/bybit_history_downloader.py --type futures -s LUNA2USDT FTMUSDT -o /data/crypto

# More parallel threads for faster download
python python/bybit_history_downloader.py --type futures -o /data/crypto -j 8
```

### Options

| Option | Description | Default |
|--------|-------------|---------|
| `-o`, `--outdir` | Output base directory (same as C++ downloader) | *required* |
| `--type` | Market type: `futures` or `spot` | `futures` |
| `-s`, `--symbol` | Specific symbol(s); bypasses API lookup | all delisted |
| `-j`, `--jobs` | Number of parallel download threads | `4` |
| `--include-today` | Include today's (potentially incomplete) data file | off |

### Requirements

- Python 3.10+
- `requests`, `pandas` (see `python/requirements.txt`)

---

## Requirements

- C++20 compatible compiler (GCC 11+, Clang 14+, MSVC 2022)
- CMake 3.20 or later
- Git (for submodules)

### Dependencies

- [OpenSSL](https://www.openssl.org/) - TLS/SSL support
- [Boost](https://www.boost.org/) - Networking (Beast, Asio)
- [spdlog](https://github.com/gabime/spdlog) - Logging
- [nlohmann/json](https://github.com/nlohmann/json) - JSON parsing
- [cxxopts](https://github.com/jarro2783/cxxopts) - Command-line parsing
- [libpqxx](https://github.com/jtv/libpqxx) - PostgreSQL client (optional)

## Installation

### Windows

1. **Install CMake**: Download from [cmake.org](https://cmake.org/download/)

2. **Install Visual Studio 2022**: Download [Visual Studio Community](https://visualstudio.microsoft.com/downloads/) and install with **Desktop development with C++** workload.

3. **Install vcpkg** (package manager):
   ```powershell
   git clone https://github.com/Microsoft/vcpkg.git C:\vcpkg
   cd C:\vcpkg
   .\bootstrap-vcpkg.bat
   .\vcpkg integrate install
   ```

4. **Install dependencies**:
   ```powershell
   vcpkg install cxxopts:x64-windows
   vcpkg install libpqxx:x64-windows
   vcpkg install spdlog:x64-windows
   vcpkg install openssl:x64-windows
   vcpkg install boost:x64-windows
   vcpkg install nlohmann-json:x64-windows
   ```

5. **Build the project**:
   ```powershell
   git clone https://github.com/vitakot/crypto_data_downloader.git
   cd crypto_data_downloader
   git submodule update --init --recursive
   mkdir build && cd build
   cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=C:\vcpkg\scripts\buildsystems\vcpkg.cmake ..
   cmake --build . --config Release -j
   ```

### Linux (Ubuntu/Debian)

1. **Install build tools**:
   ```bash
   sudo apt update
   sudo apt install -y cmake build-essential git
   ```

2. **Install dependencies**:
   ```bash
   sudo apt install -y \
       libssl-dev \
       libboost-all-dev \
       libspdlog-dev \
       nlohmann-json3-dev \
       libcxxopts-dev \
       libpq-dev \
       libpqxx-dev
   ```

3. **Build the project**:
   ```bash
   git clone https://github.com/vitakot/crypto_data_downloader.git
   cd crypto_data_downloader
   git submodule update --init --recursive
   mkdir build && cd build
   cmake -DCMAKE_BUILD_TYPE=Release ..
   cmake --build . -j$(nproc)
   ```

## Usage

```bash
crypto_data_downloader [OPTIONS]
```

### Command-Line Options

| Option | Long Form | Description | Default |
|--------|-----------|-------------|---------|
| `-e` | `--exchange` | Exchange: `bnb` (Binance), `bybit`, `okx`, `mexc` | `bnb` |
| `-t` | `--data_type` | Data type: `c` (candles), `fr` (funding rates) | `c` |
| `-o` | `--output` | Output directory path | *required* |
| `-s` | `--symbols` | Symbols to download (comma-separated) or `all` | `all` |
| `-a` | `--assets_file` | Path to Zorro Assets file (alternative to `-s`) | - |
| `-j` | `--jobs` | Maximum parallel download jobs | auto |
| `-b` | `--bar_size` | Bar size in minutes (1, 5, 15, 30, 60, etc.) | `1` |
| `-c` | `--category` | Market category: `f` (futures), `s` (spot) | `f` |
| `-d` | `--delete_delisted` | Delete delisted symbols data files | - |
| `-z` | `--t6_conversion` | Convert CSV data to T6 format (Zorro Trader format) | - |
| `-v` | `--version` | Print version and exit | - |
| `-h` | `--help` | Print help and exit | - |

### Examples

**Download all Binance futures 1-minute candles:**
```bash
./crypto_data_downloader -e bnb -t c -o /data/binance -c f
```

**Download specific symbols from Bybit:**
```bash
./crypto_data_downloader -e bybit -s "BTCUSDT,ETHUSDT,SOLUSDT" -o /data/bybit
```

**Download 5-minute candles:**
```bash
./crypto_data_downloader -e bnb -b 5 -o /data/binance_5m
```

**Download funding rate history from OKX:**
```bash
./crypto_data_downloader -e okx -t fr -o /data/okx
```

**Download MEXC futures candles (hourly recommended):**
```bash
./crypto_data_downloader -e mexc -c f -b 60 -o /data/mexc
```

**Download MEXC spot data:**
```bash
./crypto_data_downloader -e mexc -c s -b 60 -o /data/mexc_spot
```

**Download MEXC funding rate history:**
```bash
./crypto_data_downloader -e mexc -t fr -o /data/mexc
```

**Download Binance spot data:**
```bash
./crypto_data_downloader -e bnb -c s -o /data/binance_spot
```

**Delete data for delisted symbols:**
```bash
./crypto_data_downloader -e bnb -d -o /data/binance
```

**Download and convert to T6 format (Zorro):**
```bash
./crypto_data_downloader -e bnb -z -o /data/binance
```

**Combined options (delete delisted + T6 conversion):**
```bash
./crypto_data_downloader -e bnb -d -z -o /data/binance
```

## Output Format

### Candle Data (CSV)

Files are saved to `<output_dir>/csvFut/<timeframe>/<SYMBOL>.csv` (or `csvSpot` for spot):

```csv
open_time,open,high,low,close,volume
1704067200000,42000.50,42150.00,41980.25,42100.75,1234.56
...
```

### Funding Rate Data (CSV)

Files are saved to `<output_dir>/csvFr/<SYMBOL>_fr.csv`:

```csv
funding_time,funding_rate
1704067200000,0.0001
...
```

### T6 Format (Zorro)

When the `-z` (or `--t6_conversion`) option is specified, the tool generates binary T6 files compatible with the [Zorro](https://zorro-project.com/) trading platform in the `t6Fut` or `t6Spot` directories.

## Project Structure

```
crypto_data_downloader/
├── include/vk/           # Header files
│   ├── binance/          # Binance-specific downloader
│   ├── bybit/            # Bybit-specific downloader
│   ├── okx/              # OKX-specific downloader
│   ├── mexc/             # MEXC-specific downloader
│   └── downloader.h      # Common utilities
├── src/                  # Implementation files
├── binance_cpp_api/      # Binance API wrapper (submodule)
├── bybit_cpp_api/        # Bybit API wrapper (submodule)
├── okx_cpp_api/          # OKX API wrapper (submodule)
├── mexc_cpp_api/         # MEXC API wrapper (submodule)
├── vk_cpp_common/        # Common utilities (submodule)
├── CMakeLists.txt        # Build configuration
└── main.cpp              # Entry point
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Author

**Vítězslav Kot** - [vitakot](https://github.com/vitakot)

## Acknowledgments

- Exchange API wrappers are maintained as separate submodules
- Thanks to all contributors and users of this project

## Disclaimer

This software is for educational and research purposes only. Use at your own risk. The author is not responsible for any financial losses incurred through the use of this software or the data it downloads.
