/**
Crypto Data Downloader main

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2025 Vitezslav Kot <vitezslav.kot@gmail.com>.
*/

#include <memory>
#include "vk/binance/binance_futures_downloader.h"
#include "vk/bybit/bybit_downloader.h"
#include "vk/okx/okx_downloader.h"
#include "vk/downloader.h"
#include "vk/binance/binance_spot_downloader.h"
#include <spdlog/spdlog.h>
#include <cxxopts.hpp>
#include <filesystem>
#include <spdlog/sinks/stdout_sinks.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <algorithm>
#include "csv.h"
#include <iostream>

#include "vk/utils/magic_enum_wrapper.hpp"

#undef max

#define VERSION "2.1.7"

using namespace vk;

std::vector<std::string> parseZorroAssetList(const std::string& path) {
    std::vector<std::string> retVal;

    try {
        io::CSVReader<1> in(path);
        std::string symbolStr;
        in.read_header(io::ignore_extra_column, "Symbol");

        while (in.read_row(symbolStr)) {
            retVal.push_back(symbolStr);
        }
    }
    catch (std::exception& e) {
        spdlog::warn(fmt::format("Could not parse Zorro asset file: {}, reason: {}", path, e.what()));
    }
    return retVal;
}

int main(int argc, char** argv) {
    cxxopts::Options options("data_downloader",
                             "Utility for downloading historical data from crypto exchanges, currently only Binance (bnb), Bybit (bybit) and OKX (okx) exchange is supported");
    std::vector<std::string> symbols;
    std::string outputDirectory;
    std::string dataType;
    std::string exchange;
    int32_t barSizeInMinutes = 1;
    auto marketCategory = MarketCategory::Futures;
    bool convertToT6 = false;
    bool keepDelistedData = false;
    std::uint32_t maxJobs = static_cast<std::uint32_t>(std::max(std::floor(std::thread::hardware_concurrency() * 0.75),
                                                                1.0));

    options.add_options()
        ("e,exchange",
         R"(Exchange name, either Binance (bnb), OKX (okx) or Bybit (bybit), example: -e bnb (default: bnb))",
         cxxopts::value<std::string>()->default_value({"bnb"}))
        ("o,output", R"(Output directory path, example: -o "C:\Users\UserName\BNBData")",
         cxxopts::value<std::string>())
        ("t,data_type",
         R"(Data type for download, either candles 'c' or funding rate 'fr', example -t c, default is candles)",
         cxxopts::value<std::string>()->default_value({"c"}))
        ("s,symbols",
         R"(Symbols of assets to download, example: -s "BTCUSDT,ETHUSDT", "all" means All symbols, mutually exclusive with parameter -a)",
         cxxopts::value<std::vector<std::string>>()->default_value({"all"}))
        ("a,assets_file", R"(Path to Zorro Assets file, mutually exclusive with parameter -s)",
         cxxopts::value<std::string>()->default_value(""))
        ("j,jobs", R"(Maximum number of jobs to run in parallel, example -j 8)",
         cxxopts::value<std::uint32_t>()->default_value(std::to_string(maxJobs)))
        ("b,bar_size", R"(Bar size in minutes, example -b 5, default is 1)",
         cxxopts::value<int32_t>()->default_value("1"))
        ("c,category", R"(Market category, either Spot (s) or Futures (f), example -c f, default is Futures)",
         cxxopts::value<std::string>()->default_value("f"))
        ("k,keep_delisted", R"(Keep delisted symbols data files, if not specified delisted files will be deleted)")
        ("z,t6_conversion", R"(Convert CSV data to T6 format (Zorro Trader format) after download)")
        ("v,version", R"(Print version and quit)")
        ("h,help", R"(Print usage and quit)");
    try {
        cxxopts::ParseResult parseResult;
        parseResult = options.parse(argc, argv);

        if (parseResult["help"].as<bool>()) {
            spdlog::info(options.help());
            return 0;
        }

        if (parseResult["version"].as<bool>()) {
            spdlog::info(fmt::format("Version: {}", VERSION));
            return 0;
        }

        outputDirectory = parseResult["output"].as<std::string>();

        if (!std::filesystem::exists(outputDirectory)) {
            spdlog::critical("Output directory dost not exist!");
            return -1;
        }

        if (const auto assetFile = parseResult["assets_file"].as<std::string>(); assetFile.empty()) {
            symbols = parseResult["symbols"].as<std::vector<std::string>>();

            if (symbols.size() == 1 && symbols[0] == "all") {
                symbols.clear();
            }
        }
        else {
            symbols = parseZorroAssetList(assetFile);

            if (symbols.empty()) {
                spdlog::info("Zorro asset list is empty, updating all exchange symbols");
            }
        }

        maxJobs = parseResult["jobs"].as<std::uint32_t>();

        if (maxJobs < 1) {
            maxJobs = 1;
        }

        if (maxJobs >= std::thread::hardware_concurrency()) {
            spdlog::warn("Number of concurrent jobs is {}, which is too high, system can experience performance issues",
                         std::thread::hardware_concurrency());
        }

        dataType = parseResult["data_type"].as<std::string>();

        if (dataType != "c" && dataType != "fr") {
            spdlog::error("Wrong value of data_type parameter, must be 'c' or 'fr', is: {}", dataType);
            spdlog::info(options.help());
            return -1;
        }

        exchange = parseResult["exchange"].as<std::string>();

        if (exchange != "bnb" && exchange != "bybit" && exchange != "okx") {
            spdlog::error("Wrong value of exchange parameter, must be 'bnb', 'okx' or 'bybit', is: {}", exchange);
            spdlog::info(options.help());
            return -1;
        }

        std::string outputDirectoryLowerCase = outputDirectory;
        std::ranges::transform(outputDirectoryLowerCase,
                               outputDirectoryLowerCase.begin(),
                               [](const unsigned char c) { return std::tolower(c); });

        if (exchange == "bnb") {
            if (outputDirectoryLowerCase.find("bybit") != std::string::npos ||
                outputDirectoryLowerCase.find("okx") != std::string::npos) {
                std::string response;
                std::cout
                    << "Seems that you are trying to save Binance data into Bybit or OKX folder, are you sure? Type y (yes) or n (no)"
                    << std::endl;
                std::cin >> response;

                if (response != "y") {
                    return -1;
                }
            }
        }
        else if (exchange == "bybit") {
            if (outputDirectoryLowerCase.find("bnb") != std::string::npos ||
                outputDirectoryLowerCase.find("binance") != std::string::npos ||
                outputDirectoryLowerCase.find("okx") != std::string::npos) {
                std::string response;
                std::cout
                    << "Seems that you are trying to save Bybit data into Binance or OKX folder, are you sure? Type y (yes) or n (no)"
                    << std::endl;
                std::cin >> response;

                if (response != "y") {
                    return -1;
                }
            }
        }
        else if (exchange == "okx") {
            if (outputDirectoryLowerCase.find("bnb") != std::string::npos ||
                outputDirectoryLowerCase.find("binance") != std::string::npos ||
                outputDirectoryLowerCase.find("bybit") != std::string::npos) {
                std::string response;
                std::cout
                    << "Seems that you are trying to save OKX data into Binance or Bybit folder, are you sure? Type y (yes) or n (no)"
                    << std::endl;
                std::cin >> response;

                if (response != "y") {
                    return -1;
                }
            }
        }

        barSizeInMinutes = parseResult["bar_size"].as<int32_t>();

        if (dataType != "c" && dataType != "fr") {
            spdlog::error("Wrong value of data_type parameter, must be 'c' or 'fr', is: {}", dataType);
            spdlog::info(options.help());
            return -1;
        }

        const auto category = parseResult["category"].as<std::string>();

        if (category != "s" && category != "f") {
            spdlog::error("Wrong value of category parameter, must be 's' or 'f', is: {}", category);
            spdlog::info(options.help());
            return -1;
        }
        if (category == "s") {
            marketCategory = MarketCategory::Spot;
        }
        else {
            marketCategory = MarketCategory::Futures;
        }

        convertToT6 = parseResult["t6_conversion"].as<bool>();
        keepDelistedData = parseResult["keep_delisted"].as<bool>();
    }
    catch (const std::exception&) {
        spdlog::critical("Wrong parameters!");
        spdlog::info(options.help());
        return -1;
    }

    try {
        std::vector<spdlog::sink_ptr> sinks;
        sinks.push_back(std::make_shared<spdlog::sinks::stdout_sink_st>());
        sinks.push_back(std::make_shared<spdlog::sinks::basic_file_sink_st>("crypto_data_downloader.log"));
        auto combined_logger = std::make_shared<spdlog::logger>("crypto_data_downloader", begin(sinks), end(sinks));
        register_logger(combined_logger);
        set_default_logger(combined_logger);
        spdlog::flush_on(spdlog::level::info);

        std::unique_ptr<IExchangeDownloader> downloader;
        const auto candleInterval = Downloader::minutesToCandleInterval(barSizeInMinutes);

        const bool deleteDelistedData = !keepDelistedData;

        if (exchange == "bnb" && marketCategory == MarketCategory::Futures) {
            downloader = std::make_unique<BinanceFuturesDownloader>(maxJobs, deleteDelistedData);
        }
        else if (exchange == "bnb" && marketCategory == MarketCategory::Spot) {
            downloader = std::make_unique<BinanceSpotDownloader>(maxJobs, deleteDelistedData);
        }
        else if (exchange == "bybit") {
            spdlog::info(
                "Limiting number of parallel jobs to 1, Bybit Rate limiting does not work according to Docs "
                "(missing header values in response)");
            downloader = std::make_unique<BybitDownloader>(1, marketCategory, deleteDelistedData);
        }
        else if (exchange == "okx") {
            if (marketCategory == MarketCategory::Spot) {
                spdlog::error(
                    "Unsupported market category: {} for OKX exchange", magic_enum::enum_name(marketCategory));
                return 0;
            }
            downloader = std::make_unique<OKXDownloader>(maxJobs, deleteDelistedData);
        }

        if (dataType == "c") {
            downloader->updateMarketData(outputDirectory, symbols, candleInterval, {}, {}, convertToT6);
        }
        else if (dataType == "fr") {
            downloader->updateFundingRateData(outputDirectory, symbols, {}, {});
        }
    }
    catch (std::exception& e) {
        spdlog::critical(e.what());
    }

    return 0;
}
