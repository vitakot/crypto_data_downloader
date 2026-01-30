/**
MEXC Futures Market Data Downloader

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2026 Vitezslav Kot <vitezslav.kot@gmail.com>.
*/

#include "vk/mexc/mexc_futures_downloader.h"
#include "vk/mexc/mexc_futures_rest_client.h"
#include "vk/mexc/mexc.h"
#include "vk/downloader.h"
#include "vk/utils/semaphore.h"
#include "vk/utils/utils.h"
#include "vk/interface/exchange_enums.h"
#include <filesystem>
#include <fstream>
#include <spdlog/spdlog.h>
#include <future>
#include <spdlog/fmt/ranges.h>

using namespace vk::mexc;
using namespace vk::mexc::futures;

namespace vk {
struct MEXCFuturesDownloader::P {
    std::unique_ptr<RESTClient> mexcFuturesClient;
    mutable Semaphore maxConcurrentConvertJobs;
    Semaphore maxConcurrentDownloadJobs{3};
    bool deleteDelistedData = false;

    explicit P(const std::uint32_t maxJobs, const bool deleteDelistedData)
        : mexcFuturesClient(std::make_unique<RESTClient>("", "")),
          maxConcurrentConvertJobs(maxJobs),
          deleteDelistedData(deleteDelistedData) {
    }

    static int64_t checkSymbolCSVFile(const std::string &path);

    static bool writeCandlesToCSVFile(const std::vector<Candle> &candles, const std::string &path);

    static int64_t checkFundingRatesCSVFile(const std::string &path);

    static bool writeFundingRatesToCSVFile(const std::vector<FundingRate> &fr, const std::string &path);

    static mexc::CandleInterval vkIntervalToMexcInterval(vk::CandleInterval interval);
};

mexc::CandleInterval MEXCFuturesDownloader::P::vkIntervalToMexcInterval(const vk::CandleInterval interval) {
    switch (interval) {
    case vk::CandleInterval::_1m:
        return mexc::CandleInterval::_1m;
    case vk::CandleInterval::_5m:
        return mexc::CandleInterval::_5m;
    case vk::CandleInterval::_15m:
        return mexc::CandleInterval::_15m;
    case vk::CandleInterval::_30m:
        return mexc::CandleInterval::_30m;
    case vk::CandleInterval::_1h:
        return mexc::CandleInterval::_60m;
    case vk::CandleInterval::_4h:
        return mexc::CandleInterval::_4h;
    case vk::CandleInterval::_8h:
        return mexc::CandleInterval::_8h;
    case vk::CandleInterval::_1d:
        return mexc::CandleInterval::_1d;
    case vk::CandleInterval::_1w:
        return mexc::CandleInterval::_1W;
    case vk::CandleInterval::_1M:
        return mexc::CandleInterval::_1M;
    default:
        throw std::invalid_argument("Unsupported candle interval for MEXC");
    }
}

int64_t MEXCFuturesDownloader::P::checkSymbolCSVFile(const std::string &path) {
    constexpr int64_t oldestDate = 1577836800000; // Wednesday 1. January 2020 0:00:00

    std::ifstream ifs;
    ifs.open(path, std::ios::ate);

    if (!ifs.is_open()) {
        if (std::filesystem::exists(path)) {
            spdlog::error(fmt::format("Couldn't open file: {}", path));
        }
        return oldestDate;
    }

    const std::streampos size = ifs.tellg();
    char c;
    std::string row;
    int endLines = 0;

    for (int i = 1; i <= size; i++) {
        ifs.seekg(-i, std::ios::end);
        ifs.get(c);

        if (c == '\n') {
            endLines++;
            if (endLines >= 1 && !row.empty()) {
                std::ranges::reverse(row);
                const auto records = splitString(row, ',');

                if (records.size() < 7) {
                    spdlog::error(fmt::format("Wrong records number in the CSV file: {}", path));
                    ifs.close();
                    return oldestDate;
                }
                ifs.close();
                // Return timestamp + interval duration to get next expected candle
                return std::stoll(records[0]) + 60000; // Assuming 1m candles, adjust if needed
            }
        } else {
            row.push_back(c);
        }
    }
    ifs.close();
    return oldestDate;
}

bool MEXCFuturesDownloader::P::writeCandlesToCSVFile(const std::vector<Candle> &candles, const std::string &path) {
    const std::filesystem::path pathToCSVFile{path};

    std::ofstream ofs;
    ofs.open(pathToCSVFile.string(), std::ios::app);

    if (!ofs.is_open()) {
        spdlog::error(fmt::format("Couldn't open file: {}", path));
        return false;
    }

    uint64_t fileSize;

    try {
        fileSize = std::filesystem::file_size(pathToCSVFile.string());
    } catch (const std::filesystem::filesystem_error &) {
        fileSize = 0;
    }

    if (fileSize == 0) {
        ofs << "open_time,open,high,low,close,volume,amount" << std::endl;
    }

    for (const auto &candle: candles) {
        ofs << candle.openTime << ",";
        ofs << candle.open.str(8, std::ios_base::fixed) << ",";
        ofs << candle.high.str(8, std::ios_base::fixed) << ",";
        ofs << candle.low.str(8, std::ios_base::fixed) << ",";
        ofs << candle.close.str(8, std::ios_base::fixed) << ",";
        ofs << candle.volume.str(10, std::ios_base::fixed) << ",";
        ofs << candle.amount.str(10, std::ios_base::fixed) << std::endl;
    }

    ofs.close();
    return true;
}

int64_t MEXCFuturesDownloader::P::checkFundingRatesCSVFile(const std::string &path) {
    constexpr int64_t oldestDate = 1577836800000; // Wednesday 1. January 2020 0:00:00

    std::ifstream ifs;
    ifs.open(path, std::ios::ate);

    if (!ifs.is_open()) {
        if (std::filesystem::exists(path)) {
            spdlog::error(fmt::format("Couldn't open file: {}", path));
        }
        return oldestDate;
    }

    const std::streampos size = ifs.tellg();
    char c;
    std::string row;
    int endLines = 0;

    for (int i = 1; i <= size; i++) {
        ifs.seekg(-i, std::ios::end);
        ifs.get(c);

        if (c == '\n') {
            endLines++;
            if (endLines >= 1 && !row.empty()) {
                std::ranges::reverse(row);
                const auto records = splitString(row, ',');

                if (records.size() != 2) {
                    spdlog::error(fmt::format("Wrong records number in the CSV file: {}", path));
                    ifs.close();
                    return oldestDate;
                }
                ifs.close();
                return std::stoll(records[0]);
            }
        } else {
            row.push_back(c);
        }
    }
    ifs.close();
    return oldestDate;
}

bool MEXCFuturesDownloader::P::writeFundingRatesToCSVFile(const std::vector<FundingRate> &fr, const std::string &path) {
    const std::filesystem::path pathToCSVFile{path};

    std::ofstream ofs;
    ofs.open(pathToCSVFile.string(), std::ios::app);

    if (!ofs.is_open()) {
        spdlog::error(fmt::format("Couldn't open file: {}", path));
        return false;
    }

    uint64_t fileSize;

    try {
        fileSize = std::filesystem::file_size(pathToCSVFile.string());
    } catch (const std::filesystem::filesystem_error &) {
        fileSize = 0;
    }

    if (fileSize == 0) {
        ofs << "funding_time,funding_rate" << std::endl;
    }

    for (const auto &record: fr) {
        ofs << record.timestamp << ",";
        ofs << record.fundingRate << std::endl;
    }

    ofs.close();
    return true;
}

MEXCFuturesDownloader::MEXCFuturesDownloader(std::uint32_t maxJobs, bool deleteDelistedData)
    : m_p(std::make_unique<P>(maxJobs, deleteDelistedData)) {
}

MEXCFuturesDownloader::~MEXCFuturesDownloader() = default;

void MEXCFuturesDownloader::updateMarketData(const std::string &dirPath, const std::vector<std::string> &symbols,
                                              CandleInterval candleInterval,
                                              const onSymbolsToUpdate &onSymbolsToUpdateCB,
                                              const onSymbolCompleted &onSymbolCompletedCB,
                                              const bool convertToT6) const {
    const auto mexcCandleInterval = P::vkIntervalToMexcInterval(candleInterval);
    const auto barSizeInMinutes = static_cast<std::underlying_type_t<CandleInterval>>(candleInterval) / 60;

    std::vector<std::future<std::filesystem::path>> futures;
    const std::filesystem::path finalPath(dirPath);
    std::vector<std::string> symbolsToUpdate = symbols;
    std::vector<std::filesystem::path> csvFilePaths;

    spdlog::info(fmt::format("Symbols directory: {}", finalPath.string()));

    if (symbolsToUpdate.empty()) {
        spdlog::info("Updating all symbols");
        // Get all symbols from funding rates endpoint
        const auto fundingRates = m_p->mexcFuturesClient->getContractFundingRates();
        for (const auto &fr: fundingRates) {
            if (fr.symbol.find("USDT") != std::string::npos) {
                symbolsToUpdate.push_back(fr.symbol);
            }
        }
    } else {
        spdlog::info(fmt::format("Updating symbols: {}", fmt::join(symbols, ", ")));
    }

    if (onSymbolsToUpdateCB) {
        onSymbolsToUpdateCB(symbolsToUpdate);
    }

    for (const auto &s: symbolsToUpdate) {
        futures.push_back(
            std::async(std::launch::async,
                       [finalPath, this, &mexcCandleInterval, &barSizeInMinutes, &onSymbolCompletedCB](
                   const std::string &symbol,
                   Semaphore &maxJobs) -> std::filesystem::path {
                           std::scoped_lock w(maxJobs);
                           std::filesystem::path symbolFilePathCsv = finalPath;

                           symbolFilePathCsv.append(CSV_FUT_DIR);
                           symbolFilePathCsv.append(Downloader::minutesToString(barSizeInMinutes));
                           symbolFilePathCsv = symbolFilePathCsv.lexically_normal();

                           if (const auto err = createDirectoryRecursively(symbolFilePathCsv.string())) {
                               throw std::runtime_error(fmt::format("Failed to create {}, err: {}",
                                                                    symbolFilePathCsv.string(),
                                                                    err.message().c_str()));
                           }

                           symbolFilePathCsv.append(symbol + ".csv");

                           // MEXC futures API uses timestamps in SECONDS
                           auto nowTimestamp = std::chrono::seconds(std::time(nullptr)).count();
                           nowTimestamp = nowTimestamp - 60; // Don't download incomplete candle

                           spdlog::info(fmt::format("Updating candles for symbol: {}...", symbol));

                           const int64_t fromTimeStamp = P::checkSymbolCSVFile(symbolFilePathCsv.string());
                           // Convert from ms to seconds for MEXC API
                           const int64_t fromTimeSec = fromTimeStamp / 1000;

                           try {
                               const auto candles = m_p->mexcFuturesClient->getHistoricalPrices(
                                   symbol, mexcCandleInterval, fromTimeSec, nowTimestamp);

                               if (!candles.empty()) {
                                   if (P::writeCandlesToCSVFile(candles, symbolFilePathCsv.string())) {
                                       spdlog::info(fmt::format("CSV file for symbol: {} updated", symbol));
                                   }
                               }

                               if (onSymbolCompletedCB) {
                                   onSymbolCompletedCB(symbol);
                               }

                               if (std::filesystem::exists(symbolFilePathCsv)) {
                                   return symbolFilePathCsv;
                               }
                           } catch (const std::exception &e) {
                               spdlog::warn(fmt::format("Updating candles for symbol: {} failed, reason: {}",
                                                        symbol, e.what()));
                           }
                           return "";
                       }, s, std::ref(m_p->maxConcurrentDownloadJobs)));
    }

    do {
        for (auto &future: futures) {
            if (isReady(future)) {
                csvFilePaths.push_back(future.get());
            }
        }
    } while (csvFilePaths.size() < futures.size());

    if (convertToT6) {
        spdlog::warn("T6 conversion is not supported for MEXC yet");
    }
}

void MEXCFuturesDownloader::updateMarketData(const std::string &connectionString,
                                              const onSymbolsToUpdate &onSymbolsToUpdateCB,
                                              const onSymbolCompleted &onSymbolCompletedCB) const {
    throw std::runtime_error("Unimplemented: MEXCFuturesDownloader::updateMarketData()");
}

void MEXCFuturesDownloader::updateFundingRateData(const std::string &dirPath,
                                                   const std::vector<std::string> &symbols,
                                                   const onSymbolsToUpdate &onSymbolsToUpdateCB,
                                                   const onSymbolCompleted &onSymbolCompletedCB) const {
    const std::filesystem::path finalPath(dirPath);
    std::vector<std::string> symbolsToUpdate = symbols;

    spdlog::info(fmt::format("Symbols directory: {}", finalPath.string()));

    // Get current funding rates - this also serves as our symbol list
    const auto fundingRates = m_p->mexcFuturesClient->getContractFundingRates();

    if (symbolsToUpdate.empty()) {
        spdlog::info("Updating all symbols");
        for (const auto &fr: fundingRates) {
            if (fr.symbol.find("USDT") != std::string::npos) {
                symbolsToUpdate.push_back(fr.symbol);
            }
        }
    } else {
        spdlog::info(fmt::format("Updating symbols: {}", fmt::join(symbols, ", ")));
    }

    if (onSymbolsToUpdateCB) {
        onSymbolsToUpdateCB(symbolsToUpdate);
    }

    // Create funding rate directory
    std::filesystem::path frDir = finalPath;
    frDir.append(CSV_FUT_FR_DIR);

    if (const auto err = createDirectoryRecursively(frDir.string()); err.value() != 0) {
        throw std::runtime_error(fmt::format("Failed to create directory: {}, error: {}",
                                             frDir.string(), err.value()));
    }

    // MEXC returns current funding rates for all symbols in one call
    // We'll save each symbol's current funding rate to its file
    for (const auto &fr: fundingRates) {
        if (std::ranges::find(symbolsToUpdate, fr.symbol) == symbolsToUpdate.end()) {
            continue;
        }

        std::filesystem::path symbolFilePathCsv = frDir;
        symbolFilePathCsv.append(fr.symbol + "_fr.csv");

        const int64_t lastTimestamp = P::checkFundingRatesCSVFile(symbolFilePathCsv.string());

        // Only append if this is a new funding rate
        if (fr.timestamp > lastTimestamp) {
            std::vector<FundingRate> frVec{fr};
            if (P::writeFundingRatesToCSVFile(frVec, symbolFilePathCsv.string())) {
                spdlog::info(fmt::format("Funding rate for symbol: {} updated", fr.symbol));
            }
        }

        if (onSymbolCompletedCB) {
            onSymbolCompletedCB(fr.symbol);
        }
    }
}
}
