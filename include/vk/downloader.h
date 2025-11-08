/**
Common Definitions for Data Downloader

Licensed under the MIT License <http://opensource.org/licenses/MIT>.
SPDX-License-Identifier: MIT
Copyright (c) 2025 Vitezslav Kot <vitezslav.kot@gmail.com>.
*/

#ifndef INCLUDE_VK_DEFINITIONS_H
#define INCLUDE_VK_DEFINITIONS_H
#include <thread>
#include <spdlog/fmt/ostr.h>

namespace vk {
static auto CSV_FUT_DIR = "csvFut";
static auto CSV_SPOT_DIR = "csvSpot";
static auto T6_FUT_DIR = "t6Fut";
static auto T6_SPOT_DIR = "t6Spot";

class Downloader {
public:
    static std::string minutesToString(const std::int32_t minutes) {
        switch (minutes) {
        case 1:
            return "1m";
        case 3:
            return "3m";
        case 5:
            return "5m";
        case 15:
            return "15m";
        case 30:
            return "30m";
        case 60:
            return "1h";
        case 120:
            return "2h";
        case 240:
            return "4h";
        case 360:
            return "6h";
        case 480:
            return "8h";
        case 720:
            return "12h";
        case 1440:
            return "1d";
        case 4320:
            return "3d";
        case 10080:
            return "1w";
        case 40320:
            return "1M";
        default:
            throw std::runtime_error(fmt::format("Invalid minutes number: {} ", minutes));
        }
    }

    static CandleInterval minutesToCandleInterval(const std::int32_t minutes) {
        switch (minutes) {
        case 1:
            return CandleInterval::_1m;
        case 3:
            return CandleInterval::_3m;
        case 5:
            return CandleInterval::_5m;
        case 15:
            return CandleInterval::_15m;
        case 30:
            return CandleInterval::_30m;
        case 60:
            return CandleInterval::_1h;
        case 120:
            return CandleInterval::_2h;
        case 240:
            return CandleInterval::_4h;
        case 360:
            return CandleInterval::_6h;
        case 480:
            return CandleInterval::_8h;
        case 720:
            return CandleInterval::_12h;
        case 1440:
            return CandleInterval::_1d;
        case 4320:
            return CandleInterval::_3d;
        case 10080:
            return CandleInterval::_1w;
        case 40320:
            return CandleInterval::_1M;
        default:
            throw std::runtime_error(fmt::format("Invalid minutes number: {} ", minutes));
        }
    }

    static std::int32_t determineMaxJobs() {
        auto jobs = static_cast<std::int32_t>(std::thread::hardware_concurrency() * 0.75);

        if (jobs < 1) {
            jobs = 1;
        }
        return jobs;
    }
};
}

#endif //INCLUDE_VK_DEFINITIONS_H
