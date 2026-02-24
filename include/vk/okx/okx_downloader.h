/**
OKX Market Data Downloader

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2025 Vitezslav Kot <vitezslav.kot@gmail.com>.
*/

#ifndef INCLUDE_VK_OKX_DOWNLOADER_H
#define INCLUDE_VK_OKX_DOWNLOADER_H

#include "vk/interface/i_exchange_downloader.h"
#include <string>
#include <vector>
#include <memory>

namespace vk {
class OKXDownloader final : public IExchangeDownloader {
    struct P;
    std::unique_ptr<P> m_p{};

public:
    explicit OKXDownloader(std::uint32_t maxJobs, MarketCategory marketCategory, bool deleteDelistedData = false);

    ~OKXDownloader() override;

    void updateMarketData(const std::string &dirPath,
                          const std::vector<std::string> &symbols,
                          CandleInterval candleInterval,
                          const onSymbolsToUpdate &onSymbolsToUpdateCB,
                          const onSymbolCompleted &onSymbolCompletedCB,
                          bool convertToT6) const override;

    void updateMarketData(const std::string &connectionString,
                          const onSymbolsToUpdate &onSymbolsToUpdateCB,
                          const onSymbolCompleted &onSymbolCompletedCB) const override;

    void updateFundingRateData(const std::string &dirPath,
                               const std::vector<std::string> &symbols,
                               const onSymbolsToUpdate &onSymbolsToUpdateCB,
                               const onSymbolCompleted &onSymbolCompletedCB) const override;

    void convertToT6(const std::string &dirPath, CandleInterval candleInterval) const override;
};
}

#endif //INCLUDE_VK_OKX_DOWNLOADER_H
