#pragma once

#include <benchmark/benchmark.h>

#include "helpers.h"

using benchmark::ConsoleReporter;

namespace benchmark {

#ifndef _GLIBCXX_DEBUG
class [[nodiscard]] Reporter : public ConsoleReporter {
protected:
	void PrintHeader(const Run& run) {
		std::string str = FormatString("%-*s %13s %13s", static_cast<int>(name_field_width_), "Benchmark", "Time", "RPS");
		if (!run.counters.empty()) {
			if (output_options_ & OO_Tabular) {
				for (const auto& c : run.counters) {
					if (std::string_view(c.first) == std::string_view("bytes_per_second") ||
						std::string_view(c.first) == std::string_view("items_per_second")) {
						continue;
					}
					str += FormatString(" %10s", c.first.c_str());
				}
			} else {
				str += " UserCounters...";
			}
		}
		str += "\n";
		std::string line = std::string(str.length(), '-');
		GetOutputStream() << line << "\n" << str << line << "\n";
	}

	void PrintRunData(const Run& result) {
		auto& Out = GetOutputStream();

		// CHECK ERROR
		if (result.error_occurred) {
			IgnoreColorPrint(Out, "ERROR OCCURRED: \'%s\'\n", result.error_message.c_str());
			return;
		}

		// print benchmark name
		IgnoreColorPrint(Out, "%-*s", name_field_width_, result.benchmark_name().c_str());

		// print REAL time and time unit
		const double real_time = result.GetAdjustedRealTime();
		const char* timeLabel = GetTimeUnitString(result.time_unit);
		IgnoreColorPrint(Out, "%10.0f %s ", real_time, timeLabel);

		// print Requests per second
		double rps = result.iterations / result.real_accumulated_time;
		IgnoreColorPrint(Out, "%13.0f ", rps);

		// print user-defined counters
		for (auto& c : result.counters) {
			if (std::string_view(c.first) == std::string_view("bytes_per_second") ||
				std::string_view(c.first) == std::string_view("items_per_second")) {
				continue;
			}
			const std::size_t cNameLen = std::max(std::string::size_type(10), c.first.length());
			const auto& s = HumanReadableNumber(c.second.value, true);
			if (output_options_ & OO_Tabular) {
				if (c.second.flags & Counter::kIsRate) {
					IgnoreColorPrint(Out, " %*s/s", cNameLen - 2, s.c_str());
				} else {
					IgnoreColorPrint(Out, " %*s", cNameLen, s.c_str());
				}
			} else {
				const char* unit = (c.second.flags & Counter::kIsRate) ? "/s" : "";
				IgnoreColorPrint(Out, " %s=%s%s", c.first.c_str(), s.c_str(), unit);
			}
		}

		// print rates, items/s and label
		std::string rate;
		if (result.counters.find("bytes_per_second") != result.counters.end()) {
			rate = " " + HumanReadableNumber(static_cast<int64_t>(result.counters.at("bytes_per_second")), false) + " B/s";
		}

		// Format items per second
		std::string items;
		if (result.counters.find("items_per_second") != result.counters.end()) {
			items = " " + HumanReadableNumber(static_cast<int64_t>(result.counters.at("items_per_second")), false) + " items/s";
		}

		if (!rate.empty()) {
			IgnoreColorPrint(Out, " %*s", 13, rate.c_str());
		}

		if (!items.empty()) {
			IgnoreColorPrint(Out, " %*s", 18, items.c_str());
		}

		if (!result.report_label.empty()) {
			IgnoreColorPrint(Out, " %s", result.report_label.c_str());
		}

		IgnoreColorPrint(Out, "\n");
	}

private:
	void IgnoreColorPrint(std::ostream& out, const char* fmt, ...) {
		va_list args;
		va_start(args, fmt);
		out << FormatString(fmt, args);
		va_end(args);
	}

	inline const char* GetTimeUnitString(TimeUnit unit) {
		switch (unit) {
			case kMillisecond:
				return "ms";
			case kMicrosecond:
				return "us";
			case kNanosecond:
				return "ns";
			case kSecond:
				return "s";
			default:
				abort();
		}
	}
};
#endif	// #ifndef _GLIBCXX_DEBUG

}  // namespace benchmark
