/**
Binance Downloader Common

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2025 Vitezslav Kot <vitezslav.kot@gmail.com>.
*/

#include "vk/binance/binance_common.h"
#include <filesystem>
#include <fstream>
#include <spdlog/spdlog.h>
#include <ranges>
#include <regex>
#include <future>
#include <magic_enum.hpp>
#include "csv.h"
#include "vk/binance/binance_models.h"
#include "vk/utils/utils.h"
#include "vk/utils/semaphore.h"

namespace vk::binance {

struct BinanceCommon::P {

    mutable Semaphore m_maxConcurrentConvertJobs;
    Semaphore m_maxConcurrentDownloadJobs{3};

    explicit P(const std::uint32_t maxJobs) : m_maxConcurrentConvertJobs(maxJobs) {
    }
};

BinanceCommon::BinanceCommon(std::uint32_t maxJobs) : m_p(std::make_unique<P>(maxJobs)) {

}

BinanceCommon::~BinanceCommon() = default;

bool BinanceCommon::writeCSVCandlesToZorroT6File(const std::string &csvPath, const std::string &t6Path) {
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

    for (auto &candle: std::ranges::reverse_view(candles)) {
        T6 t6;
        t6.fOpen = static_cast<float>(candle.m_open);
        t6.fHigh = static_cast<float>(candle.m_high);
        t6.fLow = static_cast<float>(candle.m_low);
        t6.fClose = static_cast<float>(candle.m_close);
        t6.fVal = 0.0;
        t6.fVol = static_cast<float>(candle.m_volume);
        t6.time = convertTimeMs(candle.m_closeTime);
        ofs.write(reinterpret_cast<char *>(&t6), sizeof(T6));
    }

    ofs.close();
    return true;
}

bool BinanceCommon::readCandlesFromCSVFile(const std::string &path, std::vector<Candle> &candles) {
    try {
        io::CSVReader<7> in(path);
        in.read_header(io::ignore_extra_column, "close_time", "open", "high", "low", "close", "volume", "timestamp");

        Candle candle;
        while (in.read_row(candle.m_closeTime, candle.m_open, candle.m_high, candle.m_low, candle.m_close,
                           candle.m_volume, candle.m_openTime)) {
            candles.push_back(candle);
        }
    }
    catch (std::exception &e) {
        spdlog::warn(fmt::format("Could not parse CSV asset file: {}, reason: {}", path, e.what()));
        return false;
    }

    return true;
}

bool BinanceCommon::writeCandlesToCSVFile(const std::vector<Candle> &candles, const std::string &path) {
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
    catch (const std::filesystem::filesystem_error &) {
        fileSize = 0;
    }

    if (fileSize == 0) {
        ofs << "close_time,open,high,low,close,volume,timestamp,quote_av,trades,tb_base_av,tb_quote_av,ignore"
            << std::endl;
    }

    for (const auto &candle: candles) {
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

int64_t BinanceCommon::checkSymbolCSVFile(const std::string &path) {
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
        } else {
            row.push_back(c);
        }
    }
    ifs.close();
    return oldestBNBDate;
}

void BinanceCommon::convertFromCSVToT6(const std::vector<std::filesystem::path> &filePaths,
                                       const std::string &outDirPath) const {
    std::vector<std::future<std::pair<std::string, bool>>> futures;
    std::vector<std::pair<std::string, bool>> readyFutures;

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
                           }, path, t6FilePath, std::ref(m_p->m_maxConcurrentConvertJobs)));
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
}