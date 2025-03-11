/**
Binance Spot Market Data Downloader

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2025 Vitezslav Kot <vitezslav.kot@gmail.com>.
*/

#include "vk/binance/binance_spot_downloader.h"
#include "vk/binance/binance_spot_rest_client.h"
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
#include "csv.h"

using namespace vk::binance;

namespace vk {
struct BinanceSpotDownloader::P {
    std::unique_ptr<spot::RESTClient> m_bnbSpotClient;
    std::unique_ptr<BinanceCommon> m_binanceCommon;
    mutable Semaphore m_maxConcurrentConvertJobs;
    Semaphore m_maxConcurrentDownloadJobs{3};

    explicit P(const std::uint32_t maxJobs) : m_bnbSpotClient(std::make_unique<spot::RESTClient>("", "")),
                                              m_binanceCommon(std::make_unique<BinanceCommon>(maxJobs)),
                                              m_maxConcurrentConvertJobs(maxJobs) {
    }
};

BinanceSpotDownloader::BinanceSpotDownloader(std::uint32_t maxJobs) : m_p(std::make_unique<P>(maxJobs)) {

}

BinanceSpotDownloader::~BinanceSpotDownloader() = default;

void BinanceSpotDownloader::updateMarketData(const std::string& dirPath, const std::vector<std::string>& symbols,
                                                          CandleInterval candleInterval,
                                                          const onSymbolsToUpdate& onSymbolsToUpdateCB,
                                                          const onSymbolCompleted& onSymbolCompletedCB) const {
    auto bnbCandleInterval = binance::CandleInterval::_1m;
    const auto barSizeInMinutes = static_cast<std::underlying_type_t<CandleInterval>>(candleInterval) / 60;

    if (const auto isOk = binance::Binance::isValidCandleResolution(barSizeInMinutes, bnbCandleInterval); !isOk) {
        throw std::invalid_argument("invalid Binance candle resolution: " + std::to_string(barSizeInMinutes) + " m");
    }

    std::vector<std::future<std::filesystem::path>> futures;
    std::filesystem::path finalPath(dirPath);
    std::vector<std::string> symbolsToUpdate = symbols;
    std::vector<std::filesystem::path> csvFilePaths;

    spdlog::info(fmt::format("Symbols directory: {}", finalPath.string()));

    if (symbolsToUpdate.empty()) {
        spdlog::info(fmt::format("Updating all symbols"));
    }
    else {
        spdlog::info(fmt::format("Updating symbols: {}", fmt::join(symbols, ", ")));
    }

    const auto exchangeInfo = m_p->m_bnbSpotClient->getExchangeInfo();

    for (const auto& limit : exchangeInfo.m_rateLimits) {
        if (limit.m_rateLimitType == RateLimitType::REQUEST_WEIGHT && limit.m_intervalNum == 1 &&
            limit.m_interval == RateLimitInterval::MINUTE) {
            spdlog::info(fmt::format("Weight limit: {}", limit.m_limit));
            m_p->m_bnbSpotClient->setAPIWeightLimit(limit.m_limit);
        }
    }

    if (symbolsToUpdate.empty()) {
        for (const auto& el : exchangeInfo.m_symbols) {
            if (el.m_status == ContractStatus::TRADING && el.m_quoteAsset == "USDT") {
                symbolsToUpdate.push_back(el.m_symbol);
            }
        }
    }

    for (const auto& s : symbolsToUpdate) {
        futures.push_back(
            std::async(std::launch::async,
                       [&finalPath, this, &bnbCandleInterval, &barSizeInMinutes](const std::string& symbol,
                       Semaphore& maxJobs) ->
                       std::filesystem::path {
                           std::scoped_lock w(maxJobs);
                           std::filesystem::path symbolFilePathCsv = finalPath;
                           std::filesystem::path symbolFilePathT6 = finalPath;

                           symbolFilePathCsv.append(CSV_SPOT_DIR);
                           symbolFilePathT6.append(T6_SPOT_DIR);

                           symbolFilePathCsv.append(Downloader::minutesToString(barSizeInMinutes));
                           symbolFilePathT6.append(Downloader::minutesToString(barSizeInMinutes));

                           {
                               if (const auto err = createDirectoryRecursively(symbolFilePathCsv.string())) {
                                   throw std::runtime_error(fmt::format("Failed to create {}, err: {}", symbolFilePathCsv.string(),err.message().c_str()));
                               }
                           }

                           {
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

                           const auto candles = m_p->m_bnbSpotClient->getHistoricalPrices(symbol,
                               bnbCandleInterval,
                               fromTimeStamp,
                               nowTimestamp, 1500);

                           if (!candles.empty()) {
                               if (BinanceCommon::writeCandlesToCSVFile(candles, symbolFilePathCsv.string())) {
                                   spdlog::info(fmt::format("CSV file for symbol: {} updated", symbol));
                                   return symbolFilePathCsv;
                               }
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

    T6Directory.append(T6_SPOT_DIR);
    T6Directory.append(Downloader::minutesToString(barSizeInMinutes));

    spdlog::info(fmt::format("Converting from csv to t6..."));
    m_p->m_binanceCommon->convertFromCSVToT6(csvFilePaths, T6Directory.string());
}

void BinanceSpotDownloader::updateMarketData(const std::string &connectionString,
                                             const onSymbolsToUpdate &onSymbolsToUpdateCB,
                                             const onSymbolCompleted &onSymbolCompletedCB) const {

    throw std::runtime_error("Unimplemented: BinanceSpotDownloader::updateMarketData()");
}

void BinanceSpotDownloader::updateFundingRateData(const std::string &dirPath, const std::vector<std::string> &symbols,
                                                  const onSymbolsToUpdate &onSymbolsToUpdateCB,
                                                  const onSymbolCompleted &onSymbolCompletedCB) const {

    throw std::runtime_error("Unimplemented: BinanceSpotDownloader::updateFundingRateData()");
}
}
