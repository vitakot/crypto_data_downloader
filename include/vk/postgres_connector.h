/**
Postgres Connector

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2025 Vitezslav Kot <vitezslav.kot@gmail.com>.
*/

#ifndef INCLUDE_VK_POSTGRES_CONNECTOR_H
#define INCLUDE_VK_POSTGRES_CONNECTOR_H

#include <vector>
#include <string>
#include <memory>

namespace vk {
class PostgresConnector {
public:
    enum AssetType : int {
        Spot,
        Futures,
        Unknown,
    };

    struct Asset {
        std::string ticker{};
        std::string interval{};
        AssetType assetType = Unknown;
    };

    struct Candle {
        std::int64_t m_time = 0;
        double m_open = 0.0;
        double m_high = 0.0;
        double m_low = 0.0;
        double m_close = 0.0;
        double m_volume = 0.0;
    };

private:
    struct P;
    std::unique_ptr<P> m_p{};

public:
    explicit PostgresConnector(const std::string& connectionString);

    ~PostgresConnector();

    [[nodiscard]] std::vector<Asset> loadAssetsToUpdate(const std::string& exchange) const;

    [[nodiscard]] std::int64_t checkIndicatorId(const std::string& exchange, const Asset& asset) const;

    [[nodiscard]] std::int64_t getNewestTimeOfIndicatorData(std::int64_t indicatorId) const;

    void writeCandlesToDB(std::int64_t indicatorId, const std::vector<Candle>& candles) const;
};
}
#endif //INCLUDE_VK_POSTGRES_CONNECTOR_H
