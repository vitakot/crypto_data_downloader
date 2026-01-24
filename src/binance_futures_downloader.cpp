/**
Binance Futures Market Data Downloader

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2025 Vitezslav Kot <vitezslav.kot@gmail.com>.
*/

#include "vk/binance/binance_futures_downloader.h"
#include "vk/binance/binance_futures_rest_client.h"
#include "vk/binance/binance_common.h"
#include "vk/downloader.h"
#include "vk/postgres_connector.h"
#include "vk/utils/semaphore.h"
#include "vk/utils/utils.h"
#include "vk/binance/binance.h"
#include "vk/interface/exchange_enums.h"
#include <filesystem>
#include <fstream>
#include <spdlog/spdlog.h>
#include <regex>
#include <future>
#include <spdlog/fmt/ranges.h>

#include "vk/utils/magic_enum_wrapper.hpp"

using namespace vk::binance;

namespace vk {
struct BinanceFuturesDownloader::P {
    std::unique_ptr<futures::RESTClient> m_bnbFuturesClient;
    std::unique_ptr<BinanceCommon> m_binanceCommon;
    mutable Semaphore m_maxConcurrentConvertJobs;
    std::unique_ptr<PostgresConnector> m_postgresConnector;
    Semaphore m_maxConcurrentDownloadJobs{3};
    bool m_deleteDelistedData = false;

    explicit P(const std::uint32_t maxJobs, const bool deleteDelistedData) : m_bnbFuturesClient(std::make_unique<futures::RESTClient>("", "")),
                                              m_binanceCommon(std::make_unique<BinanceCommon>(maxJobs)),
                                              m_maxConcurrentConvertJobs(maxJobs),
                                              m_deleteDelistedData(deleteDelistedData) {
    }

    void writeCandlesToDB(const std::vector<Candle>& candles, int64_t indicatorId) const;

    void
    getAndStoreHistoricalPrices(int64_t indicatorId, const std::string& symbol, binance::CandleInterval interval,
                                std::int64_t startTime, std::int64_t endTime, std::int32_t limit) const;

    static int64_t checkFundingRatesCSVFile(const std::string& path);

    static bool writeFundingRatesToCSVFile(const std::vector<futures::FundingRate>& fr, const std::string& path);

};

BinanceFuturesDownloader::BinanceFuturesDownloader(std::uint32_t maxJobs, bool deleteDelistedData) : m_p(std::make_unique<P>(maxJobs, deleteDelistedData)) {

}

BinanceFuturesDownloader::~BinanceFuturesDownloader() = default;


void BinanceFuturesDownloader::P::writeCandlesToDB(const std::vector<Candle>& candles, int64_t indicatorId) const {
    std::vector<PostgresConnector::Candle> pCandles;

    for (const auto& bnbCandle : candles) {
        PostgresConnector::Candle pCandle;
        pCandle.m_time = bnbCandle.m_closeTime;
        pCandle.m_open = bnbCandle.m_open;
        pCandle.m_high = bnbCandle.m_high;
        pCandle.m_low = bnbCandle.m_low;
        pCandle.m_close = bnbCandle.m_close;
        pCandle.m_volume = bnbCandle.m_volume;
        pCandles.push_back(pCandle);
    }

    int numAttempt = 0;

    while (numAttempt < 3) {
        try {
            m_postgresConnector->writeCandlesToDB(indicatorId, pCandles);
            return;
        }
        catch (std::exception& e) {
            numAttempt++;
            spdlog::error(fmt::format(
                "Failed to write candles into DB, attempt: {}, indicator Id: {}, reason: {}, repeating...",
                numAttempt, indicatorId, e.what()));
        }
    }
}

void BinanceFuturesDownloader::P::getAndStoreHistoricalPrices(const int64_t indicatorId, const std::string& symbol,
                                                              const binance::CandleInterval interval,
                                                              const std::int64_t startTime, const std::int64_t endTime,
                                                              const std::int32_t limit) const {
    std::int64_t lastFromTime = startTime;
    std::vector<Candle> candles;

    if (lastFromTime < endTime) {
        candles = m_bnbFuturesClient->getHistoricalPricesSingle(symbol, interval, lastFromTime, endTime, limit);
    }

    while (!candles.empty()) {
        writeCandlesToDB(candles, indicatorId);

        lastFromTime = candles.back().m_closeTime;
        candles.clear();

        if (lastFromTime < endTime) {
            candles = m_bnbFuturesClient->getHistoricalPricesSingle(symbol, interval, lastFromTime, endTime, limit);
        }
    }
}

int64_t BinanceFuturesDownloader::P::checkFundingRatesCSVFile(const std::string& path) {
    int64_t oldestBNBDate = 1420070400000; /// Thursday 1. January 2015 0:00:00

    std::ifstream ifs;
    ifs.open(path, std::ios::ate);

    if (!ifs.is_open()) {
        if (std::filesystem::exists(path)) {
            spdlog::error(fmt::format("Couldn't open file: {}", path));
        }
        return oldestBNBDate;
    }

    /// Read last row
    std::streampos size = ifs.tellg();
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

                auto records = splitString(row, ',');

                if (records.size() != 2) {
                    spdlog::error(fmt::format("Wrong records number in the CSV file: {}", path));
                    ifs.close();
                    return oldestBNBDate;
                }
                ifs.close();
                return std::stoll(records[0]);
            }
        }
        else {
            row.push_back(c);
        }
    }
    ifs.close();
    return oldestBNBDate;
}

bool BinanceFuturesDownloader::P::writeFundingRatesToCSVFile(const std::vector<futures::FundingRate>& fr, const std::string& path) {
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
    }
    catch (const std::filesystem::filesystem_error&) {
        fileSize = 0;
    }

    if (fileSize == 0) {
        ofs << "funding_time,funding_rate"
            << std::endl;
    }

    for (const auto& record : fr) {
        ofs << record.m_fundingTime << ",";
        ofs << record.m_fundingRate << std::endl;
    }

    ofs.close();

    return true;
}

void BinanceFuturesDownloader::updateMarketData(const std::string& dirPath, const std::vector<std::string>& symbols,
                                                          CandleInterval candleInterval,
                                                          const onSymbolsToUpdate& onSymbolsToUpdateCB,
                                                          const onSymbolCompleted& onSymbolCompletedCB,
                                                          const bool convertToT6) const {
    auto bnbCandleInterval = binance::CandleInterval::_1m;
    const auto barSizeInMinutes = static_cast<std::underlying_type_t<CandleInterval>>(candleInterval) / 60;

    if (const auto isOk = binance::Binance::isValidCandleResolution(barSizeInMinutes, bnbCandleInterval); !isOk) {
        throw std::invalid_argument("invalid Binance candle resolution: " + std::to_string(barSizeInMinutes) + " m");
    }

    std::vector<std::future<std::filesystem::path>> futures;
    const std::filesystem::path finalPath(dirPath);
    std::vector<std::string> symbolsToUpdate = symbols;
    std::vector<std::filesystem::path> csvFilePaths;
    std::vector<std::string> symbolsToDelete;

    spdlog::info(fmt::format("Symbols directory: {}", finalPath.string()));

    if (symbolsToUpdate.empty()) {
        spdlog::info(fmt::format("Updating all symbols"));
    }
    else {
        spdlog::info(fmt::format("Updating symbols: {}", fmt::join(symbols, ", ")));
    }

    const auto exchangeInfo = m_p->m_bnbFuturesClient->getExchangeInfo();

    for (const auto& limit : exchangeInfo.m_rateLimits) {
        if (limit.m_rateLimitType == RateLimitType::REQUEST_WEIGHT && limit.m_intervalNum == 1 &&
            limit.m_interval == RateLimitInterval::MINUTE) {
            spdlog::info(fmt::format("Weight limit: {}", limit.m_limit));
            m_p->m_bnbFuturesClient->setAPIWeightLimit(limit.m_limit);
        }
    }

    if (symbolsToUpdate.empty()) {
        constexpr auto symbolContract = futures::ContractType::PERPETUAL;
        const auto symbolType = std::string(magic_enum::enum_name(symbolContract));

        for (const auto& el : exchangeInfo.m_symbols) {
            if (el.m_contractType == symbolType && el.m_quoteAsset == "USDT") {
                if (el.m_status == ContractStatus::TRADING) {
                    symbolsToUpdate.push_back(el.m_symbol);
                }
                else {
                    symbolsToDelete.push_back(el.m_symbol);
                }
            }
        }
    } else {
        std::vector<std::string> tempSymbols;

        for (const auto &symbol : symbolsToUpdate) {
            auto it = std::ranges::find_if(exchangeInfo.m_symbols,[symbol](const futures::Symbol &s) {
                return s.m_symbol == symbol;
            });

            if (it == exchangeInfo.m_symbols.end() || it->m_status != ContractStatus::TRADING) {
                symbolsToDelete.push_back(symbol);
                spdlog::info(fmt::format("Symbol: {} not found on Exchange, probably delisted", symbol));
            } else {
                tempSymbols.push_back(it->m_symbol);
            }
        }

        symbolsToUpdate = tempSymbols;
    }

    for (const auto& s : symbolsToUpdate) {
        futures.push_back(
            std::async(std::launch::async,
                       [finalPath, this, &bnbCandleInterval, &barSizeInMinutes, convertToT6](const std::string& symbol,
                       Semaphore& maxJobs) ->
                       std::filesystem::path {
                           std::scoped_lock w(maxJobs);
                           std::filesystem::path symbolFilePathCsv = finalPath;
                           std::filesystem::path symbolFilePathT6 = finalPath;

                           symbolFilePathCsv.append(CSV_FUT_DIR);
                           symbolFilePathT6.append(T6_FUT_DIR);

                           symbolFilePathCsv.append(Downloader::minutesToString(barSizeInMinutes));
                           symbolFilePathT6.append(Downloader::minutesToString(barSizeInMinutes));

                           symbolFilePathCsv = symbolFilePathCsv.lexically_normal();
                           symbolFilePathT6 = symbolFilePathT6.lexically_normal();

                           {
                               if (const auto err = createDirectoryRecursively(symbolFilePathCsv.string())) {
                                   throw std::runtime_error(fmt::format("Failed to create {}, err: {}", symbolFilePathCsv.string(),err.message().c_str()));
                               }
                           }

                           if (convertToT6) {
                               if (const auto err = createDirectoryRecursively(symbolFilePathT6.string())) {
                                   throw std::runtime_error(fmt::format("Failed to create {}, err: {}", symbolFilePathCsv.string(),err.message().c_str()));
                               }
                           }

                           symbolFilePathCsv.append(symbol + ".csv");
                           symbolFilePathT6.append(symbol + ".t6");

                           auto nowTimestamp = std::chrono::seconds(std::time(nullptr)).count() * 1000;

                           // Do not download the last minute - it is not complete yet.
                           nowTimestamp = nowTimestamp - 60000;

                           spdlog::info(fmt::format("Updating candles for symbol: {}...", symbol));

                           const int64_t fromTimeStamp = BinanceCommon::checkSymbolCSVFile(symbolFilePathCsv.string());

                           try {
                               const auto candles = m_p->m_bnbFuturesClient->getHistoricalPrices(symbol,
                                   bnbCandleInterval,
                                   fromTimeStamp,
                                   nowTimestamp, 1500);

                               if (!candles.empty()) {
                                   if (BinanceCommon::writeCandlesToCSVFile(candles, symbolFilePathCsv.string())) {
                                       spdlog::info(fmt::format("CSV file for symbol: {} updated", symbol));
                                   }
                               }
                               // Return the path if the CSV file exists (for T6 conversion)
                               if (std::filesystem::exists(symbolFilePathCsv)) {
                                   return symbolFilePathCsv;
                               }
                           }
                           catch (const std::exception& e) {
                                  spdlog::warn(fmt::format("Updating candles for symbol: {} failed, reason: {}",
                                                           symbol, e.what()));
                              }
                           return "";
                       }, s, std::ref(m_p->m_maxConcurrentDownloadJobs)));
    }

    do {
        for (auto& future : futures) {
            if (isReady(future)) {
                csvFilePaths.push_back(future.get());
            }
        }
    }
    while (csvFilePaths.size() < futures.size());

    std::filesystem::path T6Directory = finalPath;

    T6Directory.append(T6_FUT_DIR);
    T6Directory.append(Downloader::minutesToString(barSizeInMinutes));

    if (convertToT6 && !csvFilePaths.empty()) {
        if (const auto err = createDirectoryRecursively(T6Directory.string())) {
            throw std::runtime_error(fmt::format("Failed to create {}, err: {}", T6Directory.string(), err.message().c_str()));
        }
        spdlog::info(fmt::format("Converting from csv to t6..."));
        m_p->m_binanceCommon->convertFromCSVToT6(csvFilePaths, T6Directory.string());
    }

    if (m_p->m_deleteDelistedData) {
        for (const auto& symbol : symbolsToDelete) {
            std::filesystem::path symbolFilePathCsv = finalPath;
            std::filesystem::path symbolFilePathT6 = finalPath;

            symbolFilePathCsv.append(CSV_FUT_DIR);
            symbolFilePathT6.append(T6_FUT_DIR);

            symbolFilePathCsv.append(Downloader::minutesToString(barSizeInMinutes));
            symbolFilePathT6.append(Downloader::minutesToString(barSizeInMinutes));

            symbolFilePathCsv = symbolFilePathCsv.lexically_normal();
            symbolFilePathT6 = symbolFilePathT6.lexically_normal();

            symbolFilePathCsv.append(symbol + ".csv");
            symbolFilePathT6.append(symbol + ".t6");

            if (std::filesystem::exists(symbolFilePathCsv)) {
                std::filesystem::remove(symbolFilePathCsv);
                spdlog::info("Removing csv file for delisted symbol: {}, file: {}...", symbol, symbolFilePathCsv.string());
            }

            if (std::filesystem::exists(symbolFilePathT6)) {
                std::filesystem::remove(symbolFilePathT6);
                spdlog::info("Removing t6 file for delisted symbol: {}, file: {}...", symbol, symbolFilePathT6.string());
            }
        }
    }
}

void BinanceFuturesDownloader::updateMarketData(const std::string& connectionString,
                                                const onSymbolsToUpdate& onSymbolsToUpdateCB,
                                                const onSymbolCompleted& onSymbolCompletedCB) const {
    m_p->m_postgresConnector = std::make_unique<PostgresConnector>(connectionString);
    const auto assets = m_p->m_postgresConnector->loadAssetsToUpdate(
        std::string(magic_enum::enum_name(ExchangeId::BinanceFutures)));

    std::vector<std::future<void>> futures;
    std::vector<std::string> symbolsToUpdateFutures;
    std::vector<std::string> symbolsToUpdateSpot;
    std::vector<std::string> symbolsToUpdateResolution;

    for (const auto& [ticker, interval, assetType] : assets) {
        if (assetType == PostgresConnector::AssetType::Futures && interval == "m1") {
            symbolsToUpdateFutures.push_back(ticker);
        }
        else if (assetType == PostgresConnector::AssetType::Spot && interval == "m1") {
            symbolsToUpdateSpot.push_back(ticker);
        }

        if (interval != "m1") {
            symbolsToUpdateResolution.push_back(ticker);
        }
    }

    if (!symbolsToUpdateSpot.empty()) {
        spdlog::info(fmt::format("Exchange: {}, unsupported symbols (SPOT): {}",
                                 magic_enum::enum_name(ExchangeId::BinanceFutures),
                                 fmt::join(symbolsToUpdateSpot, ", ")));
    }

    if (!symbolsToUpdateResolution.empty()) {
        spdlog::info(fmt::format("Exchange: {}, symbols with unsupported bar size: {}",
                                 magic_enum::enum_name(ExchangeId::BinanceFutures),
                                 fmt::join(symbolsToUpdateResolution, ", ")));
    }

    if (symbolsToUpdateFutures.empty()) {
        spdlog::info(fmt::format("Exchange: {}, no symbols to update",
                                 magic_enum::enum_name(ExchangeId::BinanceFutures)));
    }
    else {
        spdlog::info(fmt::format("Exchange: {}, updating symbols: {}",
                                 magic_enum::enum_name(ExchangeId::BinanceFutures),
                                 fmt::join(symbolsToUpdateFutures, ", ")));
    }

    for (const auto exchangeInfo = m_p->m_bnbFuturesClient->getExchangeInfo(); const auto& limit : exchangeInfo.m_rateLimits) {
        if (limit.m_rateLimitType == RateLimitType::REQUEST_WEIGHT && limit.m_intervalNum == 1 &&
            limit.m_interval == RateLimitInterval::MINUTE) {
            spdlog::info(fmt::format("Weight limit: {}", limit.m_limit));
            m_p->m_bnbFuturesClient->setAPIWeightLimit(limit.m_limit);
        }
    }

    for (const auto& asset : assets) {
        futures.push_back(
            std::async(std::launch::async, [this](const PostgresConnector::Asset& pgAsset, Semaphore& maxJobs) {
                std::scoped_lock w(maxJobs);
                try {
                    constexpr auto candleInterval = binance::CandleInterval::_1m;

                    const auto nowTimestamp = std::chrono::seconds(std::time(nullptr)).count() * 1000;
                    spdlog::info(fmt::format("Updating candles for symbol: {}...", pgAsset.ticker));

                    const auto indicatorId = m_p->m_postgresConnector->checkIndicatorId(
                        std::string(magic_enum::enum_name(ExchangeId::BinanceFutures)),
                        pgAsset);
                    const auto fromTimeStamp = m_p->m_postgresConnector->getNewestTimeOfIndicatorData(
                        indicatorId) * 1000;

                    m_p->getAndStoreHistoricalPrices(indicatorId, pgAsset.ticker, candleInterval, fromTimeStamp,
                                                     nowTimestamp, 1500);

                    spdlog::info(fmt::format("DB for symbol: {}, exchange: {} updated", pgAsset.ticker,
                                             magic_enum::enum_name(ExchangeId::BinanceFutures)));
                }
                catch (const std::exception& e) {
                    spdlog::info(fmt::format("Updating candles for symbol: {} failed, reason: {}",
                                             pgAsset.ticker, e.what()));
                }
            }, asset, std::ref(m_p->m_maxConcurrentDownloadJobs)));
    }

    for (auto& future : futures) {
        future.get();
    }

    spdlog::info(fmt::format("All symbols in DB updated"));
}

void BinanceFuturesDownloader::updateFundingRateData(const std::string& dirPath,
                                                     const std::vector<std::string>& symbols,
                                                     const onSymbolsToUpdate& onSymbolsToUpdateCB,
                                                     const onSymbolCompleted& onSymbolCompletedCB) const {
    std::vector<std::future<std::filesystem::path>> futures;
    const std::filesystem::path finalPath(dirPath);
    std::vector<std::string> symbolsToUpdate = symbols;
    std::vector<std::filesystem::path> csvFilePaths;
    std::vector<std::string> symbolsToDelete;

    spdlog::info(fmt::format("Symbols directory: {}", finalPath.string()));

    if (symbolsToUpdate.empty()) {
        spdlog::info(fmt::format("Updating all symbols"));
    }
    else {
        spdlog::info(fmt::format("Updating symbols: {}", fmt::join(symbols, ", ")));
    }

    const auto exchangeInfo = m_p->m_bnbFuturesClient->getExchangeInfo();

    for (const auto& limit : exchangeInfo.m_rateLimits) {
        if (limit.m_rateLimitType == RateLimitType::REQUEST_WEIGHT && limit.m_intervalNum == 1 &&
            limit.m_interval == RateLimitInterval::MINUTE) {
            spdlog::info(fmt::format("Wight limit: {}", limit.m_limit));
            m_p->m_bnbFuturesClient->setAPIWeightLimit(limit.m_limit);
        }
    }

    if (symbolsToUpdate.empty()) {
        constexpr auto symbolContract = futures::ContractType::PERPETUAL;
        const auto symbolType = std::string(magic_enum::enum_name(symbolContract));

        for (const auto& el : exchangeInfo.m_symbols) {
            if (el.m_contractType == symbolType && el.m_quoteAsset == "USDT") {
                if (el.m_status == ContractStatus::TRADING) {
                    symbolsToUpdate.push_back(el.m_symbol);
                } else {
                    symbolsToDelete.push_back(el.m_symbol);
                }
            }
        }
    } else {
        std::vector<std::string> tempSymbols;

        for (const auto &symbol: symbolsToUpdate) {
            auto it = std::ranges::find_if(exchangeInfo.m_symbols, [symbol](const futures::Symbol &s) {
                return s.m_symbol == symbol;
            });

            if (it == exchangeInfo.m_symbols.end() || it->m_status != ContractStatus::TRADING) {
                symbolsToDelete.push_back(symbol);
                spdlog::info(fmt::format(
                    "Symbol: {} not found on Exchange, probably delisted", symbol));
            } else {
                tempSymbols.push_back(it->m_symbol);
            }
        }

        symbolsToUpdate = tempSymbols;
    }

    for (const auto& s : symbolsToUpdate) {
        futures.push_back(
            std::async(std::launch::async,
                       [finalPath, this](const std::string& symbol,
                                          Semaphore& maxJobs) -> std::filesystem::path {
                           std::scoped_lock w(maxJobs);
                           std::filesystem::path symbolFilePathCsv = finalPath;


                           symbolFilePathCsv.append(CSV_FUT_FR_DIR);

                           if (const auto err = createDirectoryRecursively(symbolFilePathCsv.string()); err.value() != 0) {
                               throw std::runtime_error(fmt::format("Failed to create directory: {}, error: {}", symbolFilePathCsv.string(), err.value()));
                           }

                           symbolFilePathCsv.append(symbol + "_fr.csv");

                           const auto nowTimestamp = std::chrono::seconds(std::time(nullptr)).count() * 1000;

                           spdlog::info(fmt::format("Updating FR for symbol: {}...", symbol));

                           const int64_t fromTimeStamp = P::checkFundingRatesCSVFile(symbolFilePathCsv.string());

                           try {
                               const auto fr = m_p->m_bnbFuturesClient->getFundingRates(
                                   symbol, fromTimeStamp, nowTimestamp,
                                   1000);
                               if (!fr.empty()) {
                                   if (fr.size() == 1) {
                                       if (fromTimeStamp == fr.front().m_fundingTime) {
                                           spdlog::info(fmt::format("CSV file for symbol: {} updated", symbol));
                                           return symbolFilePathCsv;
                                       }
                                   }

                                   if (P::writeFundingRatesToCSVFile(fr, symbolFilePathCsv.string())) {
                                       spdlog::info(fmt::format("CSV file for symbol: {} updated", symbol));
                                       return symbolFilePathCsv;
                                   }
                               }
                           } catch (const std::exception &e) {
                               spdlog::warn(fmt::format("Updating symbol: {} failed, reason: {}",
                                                        symbol, e.what()));
                           }
                           return "";
                       }, s, std::ref(m_p->m_maxConcurrentDownloadJobs)));
    }

    do {
        for (auto& future : futures) {
            if (isReady(future)) {
                csvFilePaths.push_back(future.get());
            }
        }
    }
    while (csvFilePaths.size() < futures.size());

    if (m_p->m_deleteDelistedData) {
        for (const auto& symbol : symbolsToDelete) {
            std::filesystem::path symbolFilePathCsv = finalPath;
            symbolFilePathCsv.append(CSV_FUT_DIR);
            symbolFilePathCsv = symbolFilePathCsv.lexically_normal();
            symbolFilePathCsv.append(symbol + "_fr.csv");

            if (std::filesystem::exists(symbolFilePathCsv)) {
                std::filesystem::remove(symbolFilePathCsv);
                spdlog::info("Removing csv file for delisted symbol: {}, file: {}...", symbol, symbolFilePathCsv.string());
            }
        }
    }
}

}
