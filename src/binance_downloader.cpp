/**
Binance Market Data Downloader

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2025 Vitezslav Kot <vitezslav.kot@gmail.com>.
*/

#include "vk/binance/binance_downloader.h"
#include "vk/binance/binance_futures_rest_client.h"
#include "vk/downloader.h"
#include "vk/postgres_connector.h"
#include "vk/utils/semaphore.h"
#include "vk/utils/utils.h"
#include "vk/binance/binance.h"
#include "vk/interface/exchange_enums.h"
#include <filesystem>
#include <fstream>
#include <spdlog/spdlog.h>
#include <ranges>
#include <regex>
#include <future>
#include <magic_enum/magic_enum.hpp>
#include "csv.h"

using namespace vk::binance::futures;

namespace vk {
struct BinanceDownloader::P {
    std::unique_ptr<RESTClient> m_bnbClient;
    std::unique_ptr<PostgresConnector> m_postgresConnector;
    mutable Semaphore m_maxConcurrentConvertJobs;
    Semaphore m_maxConcurrentDownloadJobs{3};
    MarketCategory m_marketCategory = MarketCategory::Futures;

    explicit P(const std::uint32_t maxJobs) : m_bnbClient(std::make_unique<RESTClient>("", "")),
                                              m_maxConcurrentConvertJobs(maxJobs) {
    }

    static bool writeCSVCandlesToZorroT6File(const std::string& csvPath, const std::string& t6Path);

    static bool writeCandlesToCSVFile(const std::vector<Candle>& candles, const std::string& path);

    static bool readCandlesFromCSVFile(const std::string& path, std::vector<Candle>& candles);

    static int64_t checkSymbolCSVFile(const std::string& path);

    void convertFromCSVToT6(const std::vector<std::filesystem::path>& filePaths, const std::string& outDirPath) const;

    void writeCandlesToDB(const std::vector<Candle>& candles, int64_t indicatorId) const;

    void
    getAndStoreHistoricalPrices(int64_t indicatorId, const std::string& symbol, binance::CandleInterval interval,
                                std::int64_t startTime, std::int64_t endTime, std::int32_t limit) const;

    static int64_t checkFundingRatesCSVFile(const std::string& path);

    static bool writeFundingRatesToCSVFile(const std::vector<FundingRate>& fr, const std::string& path);

    void updateFuturesMarketData(const std::string& dirPath, const std::vector<std::string>& symbols,
                                                    CandleInterval candleInterval,
                                                    const onSymbolsToUpdate& onSymbolsToUpdateCB,
                                                    const onSymbolCompleted& onSymbolCompletedCB);

    void updateSpotMarketData(const std::string& dirPath, const std::vector<std::string>& symbols,
                                 CandleInterval candleInterval,
                                 const onSymbolsToUpdate& onSymbolsToUpdateCB,
                                 const onSymbolCompleted& onSymbolCompletedCB);
};

BinanceDownloader::BinanceDownloader(std::uint32_t maxJobs, MarketCategory marketCategory) : m_p(std::make_unique<P>(maxJobs)) {
    m_p->m_marketCategory = marketCategory;
}

BinanceDownloader::~BinanceDownloader() = default;

bool BinanceDownloader::P::writeCSVCandlesToZorroT6File(const std::string& csvPath, const std::string& t6Path) {
    std::filesystem::path pathToT6File{t6Path};

    std::ofstream ofs;
    ofs.open(pathToT6File.string(), std::ios::trunc | std::ios::binary);

    if (!ofs.is_open()) {
        spdlog::error(fmt::format("Couldn't open file: {}", t6Path));
        return false;
    }

    std::vector<Candle> candles;
    if (!readCandlesFromCSVFile(csvPath, candles)) {
        spdlog::error(fmt::format("Couldn't read candles from csv file: {}", csvPath));
        return false;
    }

    for (auto& candle : std::ranges::reverse_view(candles)) {
        T6 t6;
        t6.fOpen = static_cast<float>(candle.m_open);
        t6.fHigh = static_cast<float>(candle.m_high);
        t6.fLow = static_cast<float>(candle.m_low);
        t6.fClose = static_cast<float>(candle.m_close);
        t6.fVal = 0.0;
        t6.fVol = static_cast<float>(candle.m_volume);
        t6.time = convertTimeMs(candle.m_closeTime);
        ofs.write(reinterpret_cast<char*>(&t6), sizeof(T6));
    }

    ofs.close();
    return true;
}

bool BinanceDownloader::P::readCandlesFromCSVFile(const std::string& path, std::vector<Candle>& candles) {
    try {
        io::CSVReader<7> in(path);
        in.read_header(io::ignore_extra_column, "close_time", "open", "high", "low", "close", "volume", "timestamp");

        Candle candle;
        while (in.read_row(candle.m_closeTime, candle.m_open, candle.m_high, candle.m_low, candle.m_close,
                           candle.m_volume, candle.m_openTime)) {
            candles.push_back(candle);
        }
    }
    catch (std::exception& e) {
        spdlog::warn(fmt::format("Could not parse CSV asset file: {}, reason: {}", path, e.what()));
        return false;
    }

    return true;
}

bool BinanceDownloader::P::writeCandlesToCSVFile(const std::vector<Candle>& candles, const std::string& path) {
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
        ofs << "close_time,open,high,low,close,volume,timestamp,quote_av,trades,tb_base_av,tb_quote_av,ignore"
            << std::endl;
    }

    for (const auto& candle : candles) {
        ofs << candle.m_closeTime << ",";
        ofs << candle.m_open << ",";
        ofs << candle.m_high << ",";
        ofs << candle.m_low << ",";
        ofs << candle.m_close << ",";
        ofs << candle.m_volume << ",";
        ofs << candle.m_openTime << ",";
        ofs << candle.m_quoteVolume << ",";
        ofs << candle.m_numberOfTrades << ",";
        ofs << candle.m_takerBuyVolume << ",";
        ofs << candle.m_takerQuoteVolume << ",";
        ofs << candle.m_ignore << std::endl;
    }

    ofs.close();
    return true;
}

int64_t BinanceDownloader::P::checkSymbolCSVFile(const std::string& path) {
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

                if (records.size() != 12) {
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

void BinanceDownloader::P::convertFromCSVToT6(const std::vector<std::filesystem::path>& filePaths,
                                              const std::string& outDirPath) const {
    std::vector<std::future<std::pair<std::string, bool>>> futures;
    std::vector<std::pair<std::string, bool>> readyFutures;

    for (const auto& path : filePaths) {
        if (path.empty()) {
            continue;
        }
        std::filesystem::path t6FilePath = outDirPath;
        const auto fileName = path.filename().replace_extension("t6");
        t6FilePath.append(fileName.string());

        spdlog::info(fmt::format("Converting symbol: {}...", path.filename().replace_extension("").string()));

        futures.push_back(
            std::async(std::launch::async,
                       [](const std::filesystem::path& csvPath, const std::filesystem::path& t6Path,
                          Semaphore& maxJobs) -> std::pair<std::string, bool> {
                           std::scoped_lock w(maxJobs);
                           std::pair<std::string, bool> retVal;
                           retVal.first = csvPath.filename().replace_extension("").string();
                           retVal.second = writeCSVCandlesToZorroT6File(csvPath.string(), t6Path.string());
                           return retVal;
                       }, path, t6FilePath, std::ref(m_maxConcurrentConvertJobs)));
    }

    do {
        for (auto& future : futures) {
            if (isReady(future)) {
                readyFutures.push_back(future.get());
                if (readyFutures.back().second) {
                    spdlog::info(fmt::format("Symbol: {} converted", readyFutures.back().first));
                }
                else {
                    spdlog::error(fmt::format("Symbol: {} conversion failed", readyFutures.back().first));
                }
            }
        }
    }
    while (readyFutures.size() < futures.size());
}

void BinanceDownloader::P::writeCandlesToDB(const std::vector<Candle>& candles, int64_t indicatorId) const {
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

void BinanceDownloader::P::getAndStoreHistoricalPrices(const int64_t indicatorId, const std::string& symbol,
                                                       const binance::CandleInterval interval,
                                                       const std::int64_t startTime, const std::int64_t endTime,
                                                       const std::int32_t limit) const {
    std::int64_t lastFromTime = startTime;
    std::vector<Candle> candles;

    if (lastFromTime < endTime) {
        candles = m_bnbClient->getHistoricalPricesSingle(symbol, interval, lastFromTime, endTime, limit);
    }

    while (!candles.empty()) {
        writeCandlesToDB(candles, indicatorId);

        lastFromTime = candles.back().m_closeTime;
        candles.clear();

        if (lastFromTime < endTime) {
            candles = m_bnbClient->getHistoricalPricesSingle(symbol, interval, lastFromTime, endTime, limit);
        }
    }
}

int64_t BinanceDownloader::P::checkFundingRatesCSVFile(const std::string& path) {
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

bool BinanceDownloader::P::writeFundingRatesToCSVFile(const std::vector<FundingRate>& fr, const std::string& path) {
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

void BinanceDownloader::P::updateFuturesMarketData(const std::string& dirPath, const std::vector<std::string>& symbols,
                                         CandleInterval candleInterval,
                                         const onSymbolsToUpdate& onSymbolsToUpdateCB,
                                         const onSymbolCompleted& onSymbolCompletedCB) {
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

    const auto exchangeInfo = m_bnbClient->getExchangeInfo();

    for (const auto& limit : exchangeInfo.m_rateLimits) {
        if (limit.m_rateLimitType == RateLimitType::REQUEST_WEIGHT && limit.m_intervalNum == 1 &&
            limit.m_interval == RateLimitInterval::MINUTE) {
            spdlog::info(fmt::format("Weight limit: {}", limit.m_limit));
            m_bnbClient->setAPIWeightLimit(limit.m_limit);
        }
    }

    if (symbolsToUpdate.empty()) {
        constexpr auto symbolContract = ContractType::PERPETUAL;
        const auto symbolType = std::string(magic_enum::enum_name(symbolContract));

        for (const auto& el : exchangeInfo.m_symbols) {
            if (el.m_contractType == symbolType && el.m_quoteAsset == "USDT") {
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

                           symbolFilePathCsv.append(CSV_FUT_DIR);
                           symbolFilePathT6.append(T6_FUT_DIR);

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

                           const int64_t fromTimeStamp = P::checkSymbolCSVFile(symbolFilePathCsv.string());

                           const auto candles = m_bnbClient->getHistoricalPrices(symbol,
                               bnbCandleInterval,
                               fromTimeStamp,
                               nowTimestamp, 1500);

                           if (!candles.empty()) {
                               if (P::writeCandlesToCSVFile(candles, symbolFilePathCsv.string())) {
                                   spdlog::info(fmt::format("CSV file for symbol: {} updated", symbol));
                                   return symbolFilePathCsv;
                               }
                           }
                           return "";
                       }, s, std::ref(m_maxConcurrentDownloadJobs)));
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

    spdlog::info(fmt::format("Converting from csv to t6..."));
    convertFromCSVToT6(csvFilePaths, T6Directory.string());
}

void BinanceDownloader::P::updateSpotMarketData(const std::string &dirPath, const std::vector<std::string> &symbols,
                                                CandleInterval candleInterval,
                                                const onSymbolsToUpdate &onSymbolsToUpdateCB,
                                                const onSymbolCompleted &onSymbolCompletedCB) {

}

void BinanceDownloader::updateMarketData(const std::string& connectionString,
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

    for (const auto exchangeInfo = m_p->m_bnbClient->getExchangeInfo(); const auto& limit : exchangeInfo.m_rateLimits) {
        if (limit.m_rateLimitType == RateLimitType::REQUEST_WEIGHT && limit.m_intervalNum == 1 &&
            limit.m_interval == RateLimitInterval::MINUTE) {
            spdlog::info(fmt::format("Weight limit: {}", limit.m_limit));
            m_p->m_bnbClient->setAPIWeightLimit(limit.m_limit);
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

void BinanceDownloader::updateFundingRateData(const std::string& dirPath,
                                              const std::vector<std::string>& symbols,
                                              const onSymbolsToUpdate& onSymbolsToUpdateCB,
                                              const onSymbolCompleted& onSymbolCompletedCB) const {
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

    const auto exchangeInfo = m_p->m_bnbClient->getExchangeInfo();

    for (const auto& limit : exchangeInfo.m_rateLimits) {
        if (limit.m_rateLimitType == RateLimitType::REQUEST_WEIGHT && limit.m_intervalNum == 1 &&
            limit.m_interval == RateLimitInterval::MINUTE) {
            spdlog::info(fmt::format("Wight limit: {}", limit.m_limit));
            m_p->m_bnbClient->setAPIWeightLimit(limit.m_limit);
        }
    }

    if (symbolsToUpdate.empty()) {
        constexpr auto symbolContract = ContractType::PERPETUAL;
        const auto symbolType = std::string(magic_enum::enum_name(symbolContract));

        for (const auto& el : exchangeInfo.m_symbols) {
            if (el.m_contractType == symbolType && el.m_quoteAsset == "USDT") {
                symbolsToUpdate.push_back(el.m_symbol);
            }
        }
    }

    for (const auto& s : symbolsToUpdate) {
        futures.push_back(
            std::async(std::launch::async,
                       [&finalPath, this](const std::string& symbol,
                                          Semaphore& maxJobs) -> std::filesystem::path {
                           std::scoped_lock w(maxJobs);
                           std::filesystem::path symbolFilePathCsv = finalPath;


                           symbolFilePathCsv.append(CSV_FUT_DIR);

                           if (const auto err = createDirectoryRecursively(symbolFilePathCsv.string()); err.value() != 0) {
                               throw std::runtime_error(fmt::format("Failed to create directory: {}, error: {}", symbolFilePathCsv.string(), err.value()));
                           }

                           symbolFilePathCsv.append(symbol + ".csv");

                           const auto nowTimestamp = std::chrono::seconds(std::time(nullptr)).count() * 1000;

                           spdlog::info(fmt::format("Updating FR for symbol: {}...", symbol));

                           const int64_t fromTimeStamp = P::checkSymbolCSVFile(symbolFilePathCsv.string());

                           const auto fr = m_p->m_bnbClient->getFundingRates(symbol, fromTimeStamp, nowTimestamp,
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
}

void BinanceDownloader::updateMarketData(const std::string &dirPath,
                                         const std::vector<std::string> &symbols,
                                         CandleInterval candleInterval,
                                         const onSymbolsToUpdate &onSymbolsToUpdateCB,
                                         const onSymbolCompleted &onSymbolCompletedCB) const {

    if (m_p->m_marketCategory == MarketCategory::Futures) {
        m_p->updateFuturesMarketData(dirPath, symbols, candleInterval, onSymbolsToUpdateCB, onSymbolCompletedCB);
    } else {
        m_p->updateSpotMarketData(dirPath, symbols, candleInterval, onSymbolsToUpdateCB, onSymbolCompletedCB);
    }
}

}
