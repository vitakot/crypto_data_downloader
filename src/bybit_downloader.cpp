/**
Bybit Market Data Downloader

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2025 Vitezslav Kot <vitezslav.kot@gmail.com>.
*/

#include "vk/bybit/bybit_downloader.h"
#include "vk/downloader.h"
#include "vk/bybit/bybit_rest_client.h"
#include "vk/bybit/bybit.h"
#include "vk/utils/utils.h"
#include "vk/utils/semaphore.h"
#include "csv.h"
#include <filesystem>
#include <fstream>
#include <spdlog/spdlog.h>
#include <ranges>
#include <regex>
#include <future>
#include <fmt/ranges.h>

using namespace vk::bybit;

namespace vk {
struct BybitDownloader::P {
    std::unique_ptr<RESTClient> m_bybitClient;
    mutable Semaphore maxConcurrentConvertJobs;
    mutable std::recursive_mutex m_locker;
    Semaphore m_maxConcurrentDownloadJobs{3};
    MarketCategory m_marketCategory = MarketCategory::Futures;

    static bool writeCSVCandlesToZorroT6File(const std::string& csvPath, const std::string& t6Path);

    static int64_t checkSymbolCSVFile(const std::string& path);

    static bool writeCandlesToCSVFile(const std::vector<Candle>& candles, const std::string& path);

    static bool readCandlesFromCSVFile(const std::string& path, std::vector<Candle>& candles);

    void convertFromCSVToT6(const std::vector<std::filesystem::path>& filePaths, const std::string& outDirPath) const;

    static int64_t checkFundingRatesCSVFile(const std::string& path);

    static bool writeFundingRatesToCSVFile(const std::vector<FundingRate>& fr, const std::string& path);

    explicit P(const std::uint32_t maxJobs) : m_bybitClient(std::make_unique<RESTClient>("", "")),
                                              maxConcurrentConvertJobs(maxJobs) {
    }
};

BybitDownloader::BybitDownloader(std::uint32_t maxJobs, const MarketCategory marketCategory) : m_p(
    std::make_unique<P>(maxJobs)) {
    m_p->m_marketCategory = marketCategory;
}

BybitDownloader::~BybitDownloader() = default;

bool BybitDownloader::P::readCandlesFromCSVFile(const std::string& path, std::vector<Candle>& candles) {
    try {
        io::CSVReader<6> in(path);
        in.read_header(io::ignore_extra_column, "open_time", "open", "high", "low", "close", "volume");

        Candle candle;
        while (in.read_row(candle.m_startTime, candle.m_open, candle.m_high, candle.m_low, candle.m_close,
                           candle.m_volume)) {
            candles.push_back(candle);
        }
    }
    catch (std::exception& e) {
        spdlog::warn(fmt::format("Could not parse CSV asset file: {}, reason: {}", path, e.what()));
        return false;
    }

    return true;
}

bool BybitDownloader::P::writeCSVCandlesToZorroT6File(const std::string& csvPath, const std::string& t6Path) {
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

    auto numMSecondsForInterval = Bybit::numberOfMsForCandleInterval(bybit::CandleInterval::_1);

    for (const auto& candle : std::ranges::reverse_view(candles)) {
        T6 t6;
        t6.fOpen = static_cast<float>(candle.m_open);
        t6.fHigh = static_cast<float>(candle.m_high);
        t6.fLow = static_cast<float>(candle.m_low);
        t6.fClose = static_cast<float>(candle.m_close);
        t6.fVal = 0.0;
        t6.fVol = static_cast<float>(candle.m_volume);
        t6.time = convertTimeMs(candle.m_startTime + numMSecondsForInterval);
        ofs.write(reinterpret_cast<char*>(&t6), sizeof(T6));
    }

    ofs.close();
    return true;
}

void BybitDownloader::P::convertFromCSVToT6(const std::vector<std::filesystem::path>& filePaths,
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
                       }, path, t6FilePath, std::ref(maxConcurrentConvertJobs)));
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

bool BybitDownloader::P::writeCandlesToCSVFile(const std::vector<Candle>& candles, const std::string& path) {
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
        ofs << "open_time,open,high,low,close,volume"
            << std::endl;
    }

    for (const auto& candle : candles) {
        ofs << candle.m_startTime << ",";
        ofs << candle.m_open << ",";
        ofs << candle.m_high << ",";
        ofs << candle.m_low << ",";
        ofs << candle.m_close << ",";
        ofs << candle.m_volume << std::endl;
    }

    ofs.close();
    return true;
}

int64_t BybitDownloader::P::checkSymbolCSVFile(const std::string& path) {
    int64_t oldestBybitDate = 1420070400000; /// Thursday 1. January 2015 0:00:00

    std::ifstream ifs;
    ifs.open(path, std::ios::ate);

    if (!ifs.is_open()) {
        if (std::filesystem::exists(path)) {
            spdlog::error(fmt::format("Couldn't open file: {}", path));
        }
        return oldestBybitDate;
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

                if (records.size() != 6) {
                    spdlog::error(fmt::format("Wrong records number in the CSV file: {}", path));
                    ifs.close();
                    return oldestBybitDate;
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
    return oldestBybitDate;
}

int64_t BybitDownloader::P::checkFundingRatesCSVFile(const std::string& path) {
    int64_t oldestBybitDate = 1420070400000; /// Thursday 1. January 2015 0:00:00

    std::ifstream ifs;
    ifs.open(path, std::ios::ate);

    if (!ifs.is_open()) {
        if (std::filesystem::exists(path)) {
            spdlog::error(fmt::format("Couldn't open file: {}", path));
        }
        return oldestBybitDate;
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
                    return oldestBybitDate;
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
    return oldestBybitDate;
}

bool BybitDownloader::P::writeFundingRatesToCSVFile(const std::vector<FundingRate>& fr, const std::string& path) {
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
        ofs << record.m_fundingRateTimestamp << ",";
        ofs << record.m_fundingRate << std::endl;
    }

    ofs.close();

    return true;
}

void BybitDownloader::updateMarketData(const std::string& dirPath,
                                       const std::vector<std::string>& symbols,
                                       CandleInterval candleInterval,
                                       const onSymbolsToUpdate& onSymbolsToUpdateCB,
                                       const onSymbolCompleted& onSymbolCompletedCB) const {
    auto category = Category::linear;
    std::string csvDirName;
    std::string t6DirName;

    switch (m_p->m_marketCategory) {
    case MarketCategory::Spot:
        category = Category::spot;
        csvDirName = CSV_SPOT_DIR;
        t6DirName = T6_SPOT_DIR;
        break;
    case MarketCategory::Futures:
        category = Category::linear;
        csvDirName = CSV_FUT_DIR;
        t6DirName = T6_FUT_DIR;
        break;
    }
    const auto barSizeInMinutes = static_cast<std::underlying_type_t<CandleInterval>>(candleInterval) / 60;
    auto bybitCandleInterval = bybit::CandleInterval::_1;

    if (const auto isOk = Bybit::isValidCandleResolution(barSizeInMinutes, bybitCandleInterval); !isOk) {
        throw std::invalid_argument("invalid Bybit candle resolution: " + std::to_string(barSizeInMinutes) + " m");
    }

    std::vector<std::future<std::filesystem::path>> futures;
    const std::filesystem::path finalPath(dirPath);
    std::vector<std::string> symbolsToUpdate = symbols;
    std::vector<std::filesystem::path> csvFilePaths;

    spdlog::info(fmt::format("Symbols directory: {}", finalPath.string()));

    if (symbolsToUpdate.empty()) {
        spdlog::info(fmt::format("Updating all symbols"));
    }
    else {
        spdlog::info(fmt::format("Updating symbols: {}", fmt::join(symbols, ", ")));
    }

    std::vector<Instrument> exchangeSymbols = m_p->m_bybitClient->getInstrumentsInfo(category);

    if (symbolsToUpdate.empty()) {
        for (const auto& el : exchangeSymbols) {
            if (el.m_quoteCoin == "USDT") {
                symbolsToUpdate.push_back(el.m_symbol);
            }
        }
    }

    for (const auto& s : symbolsToUpdate) {
        futures.push_back(
            std::async(std::launch::async,
                       [finalPath, this, &bybitCandleInterval, &barSizeInMinutes, &category, &csvDirName, &t6DirName](
                       const std::string& symbol,
                       Semaphore& maxJobs) -> std::filesystem::path {
                           std::scoped_lock w(maxJobs);
                           std::filesystem::path symbolFilePathCsv = finalPath;
                           std::filesystem::path symbolFilePathT6 = finalPath;

                           symbolFilePathCsv.append(csvDirName);
                           symbolFilePathT6.append(t6DirName);

                           symbolFilePathCsv.append(Downloader::minutesToString(barSizeInMinutes));
                           symbolFilePathT6.append(Downloader::minutesToString(barSizeInMinutes));

                           {
                               if (const auto err = createDirectoryRecursively(symbolFilePathCsv.string())) {
                                   throw std::runtime_error(fmt::format("Failed to create {}, err: {}",
                                                                        symbolFilePathCsv.string(),
                                                                        err.message().c_str()));
                               }
                           }

                           {
                               if (const auto err = createDirectoryRecursively(symbolFilePathT6.string())) {
                                   throw std::runtime_error(fmt::format("Failed to create {}, err: {}",
                                                                        symbolFilePathCsv.string(),
                                                                        err.message().c_str()));
                               }
                           }

                           symbolFilePathCsv.append(symbol + ".csv");
                           symbolFilePathT6.append(symbol + ".t6");

                           const auto nowTimestamp = std::chrono::seconds(std::time(nullptr)).count() * 1000;

                           spdlog::info(fmt::format("Updating candles for symbol: {}...", symbol));

                           const int64_t fromTimeStamp = P::checkSymbolCSVFile(symbolFilePathCsv.string());

                           if (const std::string fromTimeStampStr = std::to_string(fromTimeStamp); fromTimeStampStr.
                               length() < 13) {
                               spdlog::warn(
                                   fmt::format("Old data format for symbol: {}, delete file: {}...", symbol,
                                               symbolFilePathCsv.string()));
                               return "";
                           }

                           /// Add 1000 ms to fromTimeStamp so that we start with the next candle (1ms - 999ms does not work)
                           const auto candles = m_p->m_bybitClient->getHistoricalPrices(category,
                               symbol,
                               bybitCandleInterval,
                               fromTimeStamp + 1000,
                               nowTimestamp, 200);

                           if (!candles.empty()) {
                               if (P::writeCandlesToCSVFile(candles, symbolFilePathCsv.string())) {
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

    T6Directory.append(t6DirName);
    T6Directory.append(Downloader::minutesToString(barSizeInMinutes));

    if (!csvFilePaths.empty()) {
        spdlog::info(fmt::format("Converting from csv to t6..."));
        m_p->convertFromCSVToT6(csvFilePaths, T6Directory.string());
    }
}

void BybitDownloader::updateMarketData(const std::string& connectionString,
                                       const onSymbolsToUpdate& onSymbolsToUpdateCB,
                                       const onSymbolCompleted& onSymbolCompletedCB) const {
    throw std::runtime_error("Unimplemented: BybitDownloader::updateMarketData");
}

void BybitDownloader::updateFundingRateData(const std::string& dirPath,
                                            const std::vector<std::string>& symbols,
                                            const onSymbolsToUpdate& onSymbolsToUpdateCB,
                                            const onSymbolCompleted& onSymbolCompletedCB) const {
    std::vector<std::future<std::filesystem::path>> futures;
    const std::filesystem::path finalPath(dirPath);
    std::vector<std::string> symbolsToUpdate = symbols;
    std::vector<std::filesystem::path> csvFilePaths;

    spdlog::info(fmt::format("Symbols directory: {}", finalPath.string()));

    if (symbolsToUpdate.empty()) {
        spdlog::info(fmt::format("Updating all symbols"));
    }
    else {
        spdlog::info(fmt::format("Updating symbols: {}", fmt::join(symbols, ", ")));
    }

    if (symbolsToUpdate.empty()) {
        const auto instrumentsInfo = m_p->m_bybitClient->getInstrumentsInfo(Category::linear);

        constexpr auto symbolContract = ContractType::LinearPerpetual;

        for (const auto& el : instrumentsInfo) {
            if (el.m_contractType == symbolContract && el.m_quoteCoin == "USDT") {
                symbolsToUpdate.push_back(el.m_symbol);
            }
        }
    }

    for (const auto& s : symbolsToUpdate) {
        futures.push_back(
            std::async(std::launch::async,
                       [finalPath, this](const std::string& symbol,
                                          Semaphore& maxJobs) -> std::filesystem::path {
                           std::scoped_lock w(maxJobs);
                           std::filesystem::path symbolFilePathCsv = finalPath;


                           symbolFilePathCsv.append(CSV_FUT_DIR);

                           if (const auto err = createDirectoryRecursively(symbolFilePathCsv.string()); err.value() !=0) {
                               throw std::runtime_error(fmt::format("Failed to create directory: {}, error: {}",
                                                                    symbolFilePathCsv.string(), err.value()));
                           }

                           symbolFilePathCsv.append(symbol + ".csv");

                           const auto nowTimestamp = std::chrono::seconds(std::time(nullptr)).count() * 1000;

                           spdlog::info(fmt::format("Updating FR for symbol: {}...", symbol));

                           const int64_t fromTimeStamp = P::checkSymbolCSVFile(symbolFilePathCsv.string());

                           if (const auto fr = m_p->m_bybitClient->getFundingRates(
                               Category::linear, symbol, fromTimeStamp + 1000, nowTimestamp); !fr.empty()) {
                               if (fr.size() == 1) {
                                   if (fromTimeStamp == fr.front().m_fundingRateTimestamp) {
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
}
