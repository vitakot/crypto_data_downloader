/**
Binance Spot Market Data Downloader

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2025 Vitezslav Kot <vitezslav.kot@gmail.com>.
*/

#ifndef INCLUDE_VK_BINANCE_SPOT_DOWNLOADER_H
#define INCLUDE_VK_BINANCE_SPOT_DOWNLOADER_H

#include "vk/interface/i_exchange_downloader.h"
#include <string>
#include <vector>
#include <memory>

namespace vk {
class BinanceSpotDownloader final : public IExchangeDownloader {
    struct P;
    std::unique_ptr<P> m_p{};

public:
    explicit BinanceSpotDownloader(std::uint32_t maxJobs);

    ~BinanceSpotDownloader() override;

    void updateMarketData(const std::string& dirPath,
                          const std::vector<std::string>& symbols,
                          CandleInterval candleInterval,
                          const onSymbolsToUpdate& onSymbolsToUpdateCB,
                          const onSymbolCompleted& onSymbolCompletedCB) const override;

    void updateMarketData(const std::string& connectionString,
                          const onSymbolsToUpdate& onSymbolsToUpdateCB,
                          const onSymbolCompleted& onSymbolCompletedCB) const override;

    void updateFundingRateData(const std::string& dirPath,
                               const std::vector<std::string>& symbols,
                               const onSymbolsToUpdate& onSymbolsToUpdateCB,
                               const onSymbolCompleted& onSymbolCompletedCB) const override;
};
}

#endif //INCLUDE_VK_BINANCE_SPOT_DOWNLOADER_H
