/**
OKX Market Data Downloader

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2025 Vitezslav Kot <vitezslav.kot@gmail.com>.
*/

#include "vk/okx/okx_downloader.h"
#include "vk/okx/okx.h"
#include "vk/okx/okx_rest_client.h"
#include "vk/downloader.h"
#include "vk/utils/utils.h"
#include "vk/utils/semaphore.h"
#include "csv.h"
#include <filesystem>
#include <fstream>
#include <spdlog/spdlog.h>
#include <spdlog/fmt/ranges.h>

using namespace vk::okx;

namespace vk {
struct OKXDownloader::P {
    std::unique_ptr<RESTClient> okxClient;
    mutable Semaphore maxConcurrentConvertJobs;
    mutable std::recursive_mutex locker;
    Semaphore maxConcurrentDownloadJobs{3};
    bool deleteDelistedData = false;
    MarketCategory marketCategory = MarketCategory::Futures;

    static bool writeCSVCandlesToZorroT6File(const std::string &csvPath, const std::string &t6Path);

    static int64_t checkSymbolCSVFile(const std::string &path);

    static bool writeCandlesToCSVFile(const std::vector<Candle> &candles, const std::string &path, bool rewrite);

    static bool reverseCandlesInCSVFile(const std::string &path);

    static bool readCandlesFromCSVFile(const std::string &path, std::vector<Candle> &candles);

    void convertFromCSVToT6(const std::vector<std::filesystem::path> &filePaths, const std::string &outDirPath) const;

    static int64_t checkFundingRatesCSVFile(const std::string &path);

    static bool writeFundingRatesToCSVFile(const std::vector<FundingRate> &fr, const std::string &path);

    explicit P(const std::uint32_t maxJobs, const bool deleteDelistedData) : okxClient(
                                                                                 std::make_unique<RESTClient>(
                                                                                     "", "", "")),
                                                                             maxConcurrentConvertJobs(maxJobs),
                                                                             deleteDelistedData(deleteDelistedData) {
    }
};

OKXDownloader::OKXDownloader(std::uint32_t maxJobs, const MarketCategory marketCategory, bool deleteDelistedData) : m_p(
    std::make_unique<P>(maxJobs, deleteDelistedData)) {
    m_p->marketCategory = marketCategory;
}

OKXDownloader::~OKXDownloader() = default;

bool OKXDownloader::P::readCandlesFromCSVFile(const std::string &path, std::vector<Candle> &candles) {
    try {
        io::CSVReader<6> in(path);
        in.read_header(io::ignore_extra_column, "open_time", "open", "high", "low", "close", "volume");

        Candle candle;
        double o, h, l, c, vol = 0.0;
        while (in.read_row(candle.ts, o, h, l, c, vol)) {
            candle.o = o;
            candle.h = h;
            candle.l = l;
            candle.c = c;
            candle.vol = vol;
            candles.push_back(candle);
        }
    } catch (std::exception &e) {
        spdlog::warn(fmt::format("Could not parse CSV asset file: {}, reason: {}", path, e.what()));
        return false;
    }

    return true;
}

bool OKXDownloader::P::writeCSVCandlesToZorroT6File(const std::string &csvPath, const std::string &t6Path) {
    const std::filesystem::path pathToT6File{t6Path};

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

    const auto numMsForInterval = OKX::numberOfMsForBarSize(BarSize::_1m) / 1000;

    for (auto &candle: std::ranges::reverse_view(candles)) {
        T6 t6;
        t6.fOpen = candle.o.convert_to<float>();
        t6.fHigh = candle.h.convert_to<float>();
        t6.fLow = candle.l.convert_to<float>();
        t6.fClose = candle.c.convert_to<float>();
        t6.fVal = 0.0;
        t6.fVol = candle.vol.convert_to<float>();
        t6.time = convertTimeMs(candle.ts + numMsForInterval);
        ofs.write(reinterpret_cast<char *>(&t6), sizeof(T6));
    }

    ofs.close();
    return true;
}

void OKXDownloader::P::convertFromCSVToT6(const std::vector<std::filesystem::path> &filePaths,
                                          const std::string &outDirPath) const {
    std::vector<std::future<std::pair<std::string, bool> > > futures;
    std::vector<std::pair<std::string, bool> > readyFutures;

    for (const auto &path: filePaths) {
        if (path.empty()) {
            continue;
        }
        std::filesystem::path t6FilePath = outDirPath;
        const auto fileName = path.filename().replace_extension("t6");
        t6FilePath.append(fileName.string());

        spdlog::info(fmt::format("Converting symbol: {}...", path.filename().replace_extension("").string()));

        futures.push_back(
            std::async(std::launch::async,
                       [](const std::filesystem::path &csvPath, const std::filesystem::path &t6Path,
                          Semaphore &maxJobs) -> std::pair<std::string, bool> {
                           std::scoped_lock w(maxJobs);
                           std::pair<std::string, bool> retVal;
                           retVal.first = csvPath.filename().replace_extension("").string();
                           retVal.second = writeCSVCandlesToZorroT6File(csvPath.string(), t6Path.string());
                           return retVal;
                       }, path, t6FilePath, std::ref(maxConcurrentConvertJobs)));
    }

    do {
        for (auto &future: futures) {
            if (isReady(future)) {
                readyFutures.push_back(future.get());
                if (readyFutures.back().second) {
                    spdlog::info(fmt::format("Symbol: {} converted", readyFutures.back().first));
                } else {
                    spdlog::error(fmt::format("Symbol: {} conversion failed", readyFutures.back().first));
                }
            }
        }
    } while (readyFutures.size() < futures.size());
}

bool OKXDownloader::P::writeCandlesToCSVFile(const std::vector<Candle> &candles, const std::string &path,
                                             const bool rewrite) {
    const std::filesystem::path pathToCSVFile{path};

    std::ofstream ofs;

    if (!rewrite) {
        ofs.open(pathToCSVFile.string(), std::ios::app);
    } else {
        ofs.open(pathToCSVFile.string(), std::ios::trunc);
    }

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
        ofs << "open_time,open,high,low,close,volume,vol_ccy,vol_ccy_quote" << std::endl;
    }

    for (const auto &candle: candles) {
        ofs << candle.ts << ",";
        ofs << candle.o << ",";
        ofs << candle.h << ",";
        ofs << candle.l << ",";
        ofs << candle.c << ",";
        ofs << candle.vol << ",";
        ofs << candle.volCcy << ",";
        ofs << candle.volCcyQuote << std::endl;
    }

    ofs.close();
    return true;
}

bool OKXDownloader::P::reverseCandlesInCSVFile(const std::string &path) {
    if (std::vector<Candle> candles; readCandlesFromCSVFile(path, candles)) {
        std::ranges::reverse(candles);
        return writeCandlesToCSVFile(candles, path, true);
    }

    return false;
}

int64_t OKXDownloader::P::checkSymbolCSVFile(const std::string &path) {
    constexpr int64_t oldestBybitDate = 1420070400000; /// Thursday 1. January 2015 0:00:00

    std::ifstream ifs;
    ifs.open(path, std::ios::ate);

    if (!ifs.is_open()) {
        if (std::filesystem::exists(path)) {
            spdlog::error(fmt::format("Couldn't open file: {}", path));
        }
        return oldestBybitDate;
    }

    /// Read last row
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

                if (records.size() != 8) {
                    spdlog::error(fmt::format("Wrong records number in the CSV file: {}", path));
                    ifs.close();
                    return oldestBybitDate;
                }
                ifs.close();
                return std::stoll(records[0]);
            }
        } else {
            row.push_back(c);
        }
    }
    ifs.close();
    return oldestBybitDate;
}

int64_t OKXDownloader::P::checkFundingRatesCSVFile(const std::string &path) {
    constexpr int64_t oldestDate = 1420070400000; /// Thursday 1. January 2015 0:00:00

    std::ifstream ifs;
    ifs.open(path, std::ios::ate);

    if (!ifs.is_open()) {
        if (std::filesystem::exists(path)) {
            spdlog::error(fmt::format("Couldn't open file: {}", path));
        }
        return oldestDate;
    }

    /// Read last row
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

bool OKXDownloader::P::writeFundingRatesToCSVFile(const std::vector<FundingRate> &fr, const std::string &path) {
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
        ofs << "funding_time,funding_rate"
                << std::endl;
    }

    for (const auto &record: fr) {
        ofs << record.fundingTime << ",";
        ofs << record.fundingRate << std::endl;
    }

    ofs.close();

    return true;
}

void OKXDownloader::updateMarketData(const std::string &dirPath,
                                     const std::vector<std::string> &symbols,
                                     CandleInterval candleInterval,
                                     const onSymbolsToUpdate &onSymbolsToUpdateCB,
                                     const onSymbolCompleted &onSymbolCompletedCB,
                                     const bool convertToT6) const {
    auto okxBarSize = BarSize::_1m;


    const auto barSizeInMinutes = static_cast<std::underlying_type_t<CandleInterval>>(candleInterval) / 60;

    if (const auto isOk = OKX::isValidBarSize(barSizeInMinutes, okxBarSize); !isOk) {
        throw std::invalid_argument("invalid OKX bar size: " + std::to_string(barSizeInMinutes) + " m");
    }

    std::vector<std::future<std::filesystem::path> > futures;
    const std::filesystem::path finalPath(dirPath);
    std::vector<std::string> symbolsToUpdate = symbols;
    std::vector<std::filesystem::path> csvFilePaths;
    std::vector<std::string> symbolsToDelete;

    std::string csvDirName;
    std::string t6DirName;
    InstrumentType instrumentType;

    switch (m_p->marketCategory) {
        case MarketCategory::Spot:
            instrumentType = InstrumentType::SPOT;
            csvDirName = CSV_SPOT_DIR;
            t6DirName = T6_SPOT_DIR;
            break;
        case MarketCategory::Futures:
            instrumentType = InstrumentType::SWAP;
            csvDirName = CSV_FUT_DIR;
            t6DirName = T6_FUT_DIR;
            break;
    }

    spdlog::info(fmt::format("Symbols directory: {}", finalPath.string()));

    if (symbolsToUpdate.empty()) {
        spdlog::info(fmt::format("Updating all symbols"));
    } else {
        spdlog::info(fmt::format("Updating symbols: {}", fmt::join(symbols, ", ")));
    }

    const auto exchangeInstruments = m_p->okxClient->getInstruments(instrumentType);

    if (symbolsToUpdate.empty()) {
        for (const auto &el: exchangeInstruments) {
            if (el.settleCcy == "USDT" || el.quoteCcy == "USD") {
                if (el.state == InstrumentStatus::live) {
                    symbolsToUpdate.push_back(el.instId);
                } else {
                    symbolsToDelete.push_back(el.instId);
                }
            }
        }
    } else {
        std::vector<std::string> tempSymbols;

        for (const auto &symbol: symbolsToUpdate) {
            auto it = std::ranges::find_if(exchangeInstruments, [symbol](const Instrument &i) {
                return i.instId == symbol;
            });

            if (it == exchangeInstruments.end() || it->state != InstrumentStatus::live) {
                symbolsToDelete.push_back(symbol);
                spdlog::info(fmt::format("Symbol: {} not found on Exchange, probably delisted", symbol));
            } else {
                tempSymbols.push_back(it->instId);
            }
        }

        symbolsToUpdate = tempSymbols;
    }

    for (const auto &s: symbolsToUpdate) {
        futures.push_back(
            std::async(std::launch::async,
                       [finalPath, this, &okxBarSize, &barSizeInMinutes, &csvDirName, &t6DirName, convertToT6](
                   const std::string &symbol,
                   Semaphore &maxJobs) -> std::filesystem::path {
                           std::scoped_lock w(maxJobs);
                           std::filesystem::path symbolFilePathCsv = finalPath;
                           std::filesystem::path symbolFilePathT6 = finalPath;

                           symbolFilePathCsv.append(csvDirName);
                           symbolFilePathT6.append(t6DirName);

                           symbolFilePathCsv.append(Downloader::minutesToString(barSizeInMinutes));
                           symbolFilePathT6.append(Downloader::minutesToString(barSizeInMinutes)); {
                               if (const auto err = createDirectoryRecursively(symbolFilePathCsv.string())) {
                                   throw std::runtime_error(fmt::format("Failed to create {}, err: {}",
                                                                        symbolFilePathCsv.string(),
                                                                        err.message().c_str()));
                               }
                           }

                           if (convertToT6) {
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

                           try {
                               const auto candles = m_p->okxClient->getHistoricalPrices(symbol,
                                   okxBarSize,
                                   fromTimeStamp,
                                   nowTimestamp, 100, [symbolFilePathCsv, symbol](const std::vector<Candle> &cnd) {
                                       if (!cnd.empty()) {
                                           if (!P::writeCandlesToCSVFile(cnd, symbolFilePathCsv.string(), false)) {
                                               spdlog::warn(
                                                   fmt::format("CSV file for symbol: {} update failed", symbol));
                                           }
                                       }
                                   });

                               if (!P::reverseCandlesInCSVFile(symbolFilePathCsv.string())) {
                                   spdlog::warn(fmt::format(
                                       "CSV file for symbol: {} reversion failed, removing data...", symbol));
                                   std::filesystem::remove(symbolFilePathCsv);
                                   return "";
                               }

                               // Return the path if the CSV file exists (for T6 conversion)
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

    std::filesystem::path T6Directory = finalPath;

    T6Directory.append(t6DirName);
    T6Directory.append(Downloader::minutesToString(barSizeInMinutes));

    if (convertToT6 && !csvFilePaths.empty()) {
        if (const auto err = createDirectoryRecursively(T6Directory.string())) {
            throw std::runtime_error(fmt::format("Failed to create {}, err: {}", T6Directory.string(),
                                                 err.message().c_str()));
        }
        spdlog::info(fmt::format("Converting from csv to t6..."));
        m_p->convertFromCSVToT6(csvFilePaths, T6Directory.string());
    }

    if (m_p->deleteDelistedData) {
        for (const auto &symbol: symbolsToDelete) {
            std::filesystem::path symbolFilePathCsv = finalPath;
            std::filesystem::path symbolFilePathT6 = finalPath;

            symbolFilePathCsv.append(csvDirName);
            symbolFilePathT6.append(t6DirName);

            symbolFilePathCsv.append(Downloader::minutesToString(barSizeInMinutes));
            symbolFilePathT6.append(Downloader::minutesToString(barSizeInMinutes));

            symbolFilePathCsv = symbolFilePathCsv.lexically_normal();
            symbolFilePathT6 = symbolFilePathT6.lexically_normal();

            symbolFilePathCsv.append(symbol + ".csv");
            symbolFilePathT6.append(symbol + ".t6");

            if (std::filesystem::exists(symbolFilePathCsv)) {
                std::filesystem::remove(symbolFilePathCsv);
                spdlog::info("Removing csv file for delisted symbol: {}, file: {}...", symbol,
                             symbolFilePathCsv.string());
            }

            if (std::filesystem::exists(symbolFilePathT6)) {
                std::filesystem::remove(symbolFilePathT6);
                spdlog::info("Removing t6 file for delisted symbol: {}, file: {}...", symbol,
                             symbolFilePathT6.string());
            }
        }
    }
}

void OKXDownloader::updateMarketData(const std::string &connectionString,
                                     const onSymbolsToUpdate &onSymbolsToUpdateCB,
                                     const onSymbolCompleted &onSymbolCompletedCB) const {
    throw std::runtime_error("Unimplemented: OKXDownloader::updateMarketData");
}

void OKXDownloader::updateFundingRateData(const std::string &dirPath,
                                          const std::vector<std::string> &symbols,
                                          const onSymbolsToUpdate &onSymbolsToUpdateCB,
                                          const onSymbolCompleted &onSymbolCompletedCB) const {
    std::vector<std::future<std::filesystem::path> > futures;
    const std::filesystem::path finalPath(dirPath);
    std::vector<std::string> symbolsToUpdate = symbols;
    std::vector<std::filesystem::path> csvFilePaths;
    std::vector<std::string> symbolsToDelete;

    spdlog::info(fmt::format("Symbols directory: {}", finalPath.string()));

    if (symbolsToUpdate.empty()) {
        spdlog::info(fmt::format("Updating all symbols"));
    } else {
        spdlog::info(fmt::format("Updating symbols: {}", fmt::join(symbols, ", ")));
    }

    const auto exchangeInstruments = m_p->okxClient->getInstruments(InstrumentType::SWAP);

    if (symbolsToUpdate.empty()) {
        for (const auto &el: exchangeInstruments) {
            if (el.settleCcy == "USDT") {
                if (el.state == InstrumentStatus::live) {
                    symbolsToUpdate.push_back(el.instId);
                } else {
                    symbolsToDelete.push_back(el.instId);
                }
            }
        }
    } else {
        std::vector<std::string> tempSymbols;

        for (const auto &symbol: symbolsToUpdate) {
            auto it = std::ranges::find_if(exchangeInstruments, [symbol](const Instrument &i) {
                return i.instId == symbol;
            });

            if (it == exchangeInstruments.end() || it->state != InstrumentStatus::live) {
                symbolsToDelete.push_back(symbol);
                spdlog::info(fmt::format(
                    "Symbol: {} not found on Exchange, probably delisted", symbol));
            } else {
                tempSymbols.push_back(it->instId);
            }
        }

        symbolsToUpdate = tempSymbols;
    }

    for (const auto &s: symbolsToUpdate) {
        futures.push_back(
            std::async(std::launch::async,
                       [finalPath, this](const std::string &symbol,
                                         Semaphore &maxJobs) -> std::filesystem::path {
                           std::scoped_lock w(maxJobs);
                           std::filesystem::path symbolFilePathCsv = finalPath;


                           symbolFilePathCsv.append(CSV_FUT_FR_DIR);
                           if (const auto err = createDirectoryRecursively(symbolFilePathCsv.string());
                               err.value() != 0) {
                               throw std::runtime_error(fmt::format("Failed to create directory: {}, error: {}",
                                                                    symbolFilePathCsv.string(), err.value()));
                           }

                           symbolFilePathCsv.append(symbol + "_fr.csv");

                           const auto nowTimestamp = std::chrono::seconds(std::time(nullptr)).count() * 1000;

                           spdlog::info(fmt::format("Updating FR for symbol: {}...", symbol));

                           const int64_t fromTimeStamp = P::checkSymbolCSVFile(symbolFilePathCsv.string());

                           const auto fr = m_p->okxClient->getFundingRates(symbol, fromTimeStamp, nowTimestamp,
                                                                           1000);

                           try {
                               if (!fr.empty()) {
                                   if (fr.size() == 1) {
                                       if (fromTimeStamp == fr.front().fundingTime) {
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
                       }, s, std::ref(m_p->maxConcurrentDownloadJobs)));
    }

    do {
        for (auto &future: futures) {
            if (isReady(future)) {
                csvFilePaths.push_back(future.get());
            }
        }
    } while (csvFilePaths.size() < futures.size());

    if (m_p->deleteDelistedData) {
        for (const auto &symbol: symbolsToDelete) {
            std::filesystem::path symbolFilePathCsv = finalPath;
            symbolFilePathCsv.append(CSV_FUT_DIR);
            symbolFilePathCsv = symbolFilePathCsv.lexically_normal();
            symbolFilePathCsv.append(symbol + "_fr.csv");

            if (std::filesystem::exists(symbolFilePathCsv)) {
                std::filesystem::remove(symbolFilePathCsv);
                spdlog::info("Removing csv file for delisted symbol: {}, file: {}...", symbol,
                             symbolFilePathCsv.string());
            }
        }
    }
}
}
