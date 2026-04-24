/**
Binance Downloader Common

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2025 Vitezslav Kot <vitezslav.kot@stonky.cz>, Stonky s.r.o.
*/

#ifndef INCLUDE_STONKY_BINANCE_COMMON_H
#define INCLUDE_STONKY_BINANCE_COMMON_H

#include "stonky/binance/binance_models.h"
#include <memory>
#include <string>
#include <vector>

namespace stonky::binance {
class BinanceCommon {
    struct P;
    std::unique_ptr<P> m_p{};

public:
    explicit BinanceCommon(std::uint32_t maxJobs);

    ~BinanceCommon();

    static bool writeCSVCandlesToZorroT6File(const std::string &csvPath, const std::string &t6Path);

    static bool readCandlesFromCSVFile(const std::string &path, std::vector<Candle> &candles);

    static bool writeCandlesToCSVFile(const std::vector<Candle> &candles, const std::string &path);

    static int64_t checkSymbolCSVFile(const std::string &path);

    void convertFromCSVToT6(const std::vector<std::filesystem::path> &filePaths, const std::string &outDirPath) const;
};
}
#endif //INCLUDE_STONKY_BINANCE_COMMON_H
