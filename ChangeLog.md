# Changelog
All notable changes to this project will be documented in this file. This project adheres to [Semantic Versioning](http://semver.org/).

## [2.1.0](https://github.com/vitakot/crypto_data_downloader/releases/tag/v2.1.0) (2025-07-24)

- Initial release

## [2.1.1](https://github.com/vitakot/crypto_data_downloader/releases/tag/v2.1.1) (2025-09-28)

- [#1] Fix error while downloading binance spot data

## [2.1.2](https://github.com/vitakot/crypto_data_downloader/releases/tag/v2.1.2) (2025-10-28)

- Fix error while downloading binance spot data (old API version)
- Add automatic deletion of delisted symbols data

## [2.1.3](https://github.com/vitakot/crypto_data_downloader/releases/tag/v2.1.3) (2025-11-13)
- Improve automatic deletion of delisted symbols data

## [2.1.4](https://github.com/vitakot/crypto_data_downloader/releases/tag/v2.1.4) (2025-11-16)
- Add pagination to bybit getInstrumentsInfo, it downloaded data for max 500 symbols without it

## [2.1.5](https://github.com/vitakot/crypto_data_downloader/releases/tag/v2.1.5) (2025-11-17)
- Add continuous download for Bybit

## [2.1.6](https://github.com/vitakot/crypto_data_downloader/releases/tag/v2.1.5) (2026-01-18)
- Add option for handling delisted symbols data (keep or delete)

## [2.1.7](https://github.com/vitakot/crypto_data_downloader/releases/tag/v2.1.5) (2026-01-24)
- Change -k option for handling delisted symbols data (keep by default)
- Add -z option for t6 conversion (no conversion by default)
- Fix Bybit spot error

## [2.1.8](https://github.com/vitakot/crypto_data_downloader/releases/tag/v2.1.5) (2026-01-28)
- Add OKX Spot support
- Fix in Bybit candles
- Add/Improve rate limiters

## [2.1.9](https://github.com/vitakot/crypto_data_downloader/releases/tag/v2.1.5) (2026-01-31)
- Add MEXC support
- Many fixes I don't remember