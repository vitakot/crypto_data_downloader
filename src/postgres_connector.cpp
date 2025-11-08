/**
Postgres Connector

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2025 Vitezslav Kot <vitezslav.kot@gmail.com>.
*/

#include "vk/postgres_connector.h"
#include "vk/utils/utils.h"
#include <mutex>
#include "libpq-fe.h"

namespace vk {
class DBConnection {
    PGconn* m_connection = nullptr;

public:
    explicit DBConnection(PGconn* connection) {
        m_connection = connection;
    }

    ~DBConnection() {
        PQfinish(m_connection);
    }

    [[nodiscard]] PGconn* get() const {
        return m_connection;
    }
};

struct PostgresConnector::P {
    std::string m_connectionString;
    mutable PGresult* m_pgResult = nullptr;
    mutable std::recursive_mutex m_dbLocker;
};

PostgresConnector::PostgresConnector(const std::string& connectionString) : m_p(std::make_unique<P>()) {
    m_p->m_connectionString = connectionString;
}

PostgresConnector::~PostgresConnector() {
    PQclear(m_p->m_pgResult);
}

std::vector<PostgresConnector::Asset> PostgresConnector::loadAssetsToUpdate(const std::string& exchange) const {
    std::lock_guard lk(m_p->m_dbLocker);
    std::vector<Asset> retVal;

    PQclear(m_p->m_pgResult);
    const DBConnection connection(PQconnectdb(m_p->m_connectionString.c_str()));

    if (PQstatus(connection.get()) != CONNECTION_OK) {
        throw std::runtime_error(fmt::format("{}", PQerrorMessage(connection.get())).c_str());
    }

    std::string command = fmt::format(
        "SELECT * FROM indicators WHERE indicator ='{}' AND exchange='{}' AND interval = '{}'", "price_ohlcv",
        exchange, "m1");
    m_p->m_pgResult = PQexec(connection.get(), command.c_str());
    std::vector<Asset> assetsToUpdate;

    if (PQresultStatus(m_p->m_pgResult) != PGRES_TUPLES_OK) {
        throw std::runtime_error(fmt::format("Select failed: {}", PQresultErrorMessage(m_p->m_pgResult)));
    }
    for (auto i = 0; i < PQntuples(m_p->m_pgResult); i++) {
        Asset asset;
        asset.ticker = PQgetvalue(m_p->m_pgResult, i, 2);
        asset.interval = PQgetvalue(m_p->m_pgResult, i, 4);
        assetsToUpdate.push_back(asset);
    }

    command = fmt::format("SELECT * FROM assets");
    m_p->m_pgResult = PQexec(connection.get(), command.c_str());
    std::vector<Asset> assetsInDb;

    if (PQresultStatus(m_p->m_pgResult) != PGRES_TUPLES_OK) {
        throw std::runtime_error(fmt::format("Select failed: {}", PQresultErrorMessage(m_p->m_pgResult)));
    }
    for (auto i = 0; i < PQntuples(m_p->m_pgResult); i++) {
        Asset asset;

        asset.ticker = PQgetvalue(m_p->m_pgResult, i, 0);

        if (const std::string typeString = PQgetvalue(m_p->m_pgResult, i, 1); typeString == "spot") {
            asset.assetType = Spot;
        }
        else if (typeString == "futures") {
            asset.assetType = Futures;
        }
        assetsInDb.push_back(asset);
    }

    // Assign Asset types and remove those with the unknown type
    for (auto& assetToUpdate : assetsToUpdate) {
        for (const auto& assetInDb : assetsInDb) {
            if (assetToUpdate.ticker == assetInDb.ticker) {
                assetToUpdate.assetType = assetInDb.assetType;
                retVal.push_back(assetToUpdate);
                break;
            }
        }
    }

    return retVal;
}

std::int64_t PostgresConnector::checkIndicatorId(const std::string& exchange, const Asset& asset) const {
    std::lock_guard lk(m_p->m_dbLocker);

    PQclear(m_p->m_pgResult);
    const DBConnection connection(PQconnectdb(m_p->m_connectionString.c_str()));

    if (PQstatus(connection.get()) != CONNECTION_OK) {
        throw std::runtime_error(fmt::format("{}", PQerrorMessage(connection.get())).c_str());
    }

    const std::string command = fmt::format(
        "SELECT * FROM indicators WHERE indicator='price_ohlcv' AND ticker='{}' AND exchange='{}' AND interval='{}'",
        asset.ticker, exchange, asset.interval);
    m_p->m_pgResult = PQexec(connection.get(), command.c_str());

    if (PQresultStatus(m_p->m_pgResult) != PGRES_TUPLES_OK) {
        throw std::runtime_error(fmt::format("Select failed: {}", PQresultErrorMessage(m_p->m_pgResult)));
    }
    const std::string value = PQgetvalue(m_p->m_pgResult, 0, 0);
    char* p_end{};
    return std::strtoll(value.c_str(), &p_end, 10);
}

std::int64_t PostgresConnector::getNewestTimeOfIndicatorData(std::int64_t indicatorId) const {
    std::lock_guard lk(m_p->m_dbLocker);

    PQclear(m_p->m_pgResult);
    const DBConnection connection(PQconnectdb(m_p->m_connectionString.c_str()));

    if (PQstatus(connection.get()) != CONNECTION_OK) {
        throw std::runtime_error(fmt::format("{}", PQerrorMessage(connection.get())).c_str());
    }

    const std::string command = fmt::format(
        "SELECT time FROM price_ohlcv_data WHERE indicator='{}' ORDER BY time DESC LIMIT 1", indicatorId);
    m_p->m_pgResult = PQexec(connection.get(), command.c_str());

    if (PQresultStatus(m_p->m_pgResult) != PGRES_TUPLES_OK) {
        throw std::runtime_error(fmt::format("Select failed: {}", PQresultErrorMessage(m_p->m_pgResult)));
    }
    if (!PQntuples(m_p->m_pgResult)) {
        constexpr int64_t oldestBNBDate = 1420070400; /// Thursday 1. January 2015 0:00:00
        return oldestBNBDate;
    }

    const std::string dateTimeString = PQgetvalue(m_p->m_pgResult, 0, 0);
    return getTimeStampFromStringWithZone(dateTimeString, "%Y-%m-%d %H:%M:%S%z");
}

void PostgresConnector::writeCandlesToDB(const std::int64_t indicatorId, const std::vector<Candle>& candles) const {
    std::lock_guard lk(m_p->m_dbLocker);

    PQclear(m_p->m_pgResult);
    const DBConnection connection(PQconnectdb(m_p->m_connectionString.c_str()));

    if (PQstatus(connection.get()) != CONNECTION_OK) {
        throw std::runtime_error(fmt::format("{}", PQerrorMessage(connection.get())).c_str());
    }

    const std::string command = fmt::format(
        "INSERT INTO price_ohlcv_data (time, indicator, open, high, low, close, volume) VALUES ($1, $2, $3, $4, $5, $6, $7)");

    const char* paramValues[7];
    int paramLengths[7];
    const int paramFormats[] = {0, 0, 0, 0, 0, 0, 0};

    for (const auto& [m_time, m_open, m_high, m_low, m_close, m_volume] : candles) {
        constexpr int resultFormat = 0;
        auto ts = getDateTimeStringFromTimeStamp(m_time / 1000, "%Y-%m-%d %H:%M:%S") + "+00";
        auto id = std::to_string(indicatorId);
        auto o = std::to_string(m_open);
        auto h = std::to_string(m_high);
        auto l = std::to_string(m_low);
        auto c = std::to_string(m_close);
        auto v = std::to_string(m_volume);

        paramValues[0] = ts.c_str();
        paramValues[1] = id.c_str();
        paramValues[2] = o.c_str();
        paramValues[3] = h.c_str();
        paramValues[4] = l.c_str();
        paramValues[5] = c.c_str();
        paramValues[6] = v.c_str();

        paramLengths[0] = static_cast<int>(strlen(paramValues[0]));
        paramLengths[1] = static_cast<int>(strlen(paramValues[1]));
        paramLengths[2] = static_cast<int>(strlen(paramValues[2]));
        paramLengths[3] = static_cast<int>(strlen(paramValues[3]));
        paramLengths[4] = static_cast<int>(strlen(paramValues[4]));
        paramLengths[5] = static_cast<int>(strlen(paramValues[5]));
        paramLengths[6] = static_cast<int>(strlen(paramValues[6]));

        m_p->m_pgResult = PQexecParams(connection.get(), command.c_str(), 7, nullptr, paramValues, paramLengths,
                                       paramFormats, resultFormat);

        if (PQresultStatus(m_p->m_pgResult) != PGRES_COMMAND_OK) {
            throw std::runtime_error(fmt::format("Select failed: {}", PQresultErrorMessage(m_p->m_pgResult)));
        }
    }
}
}
