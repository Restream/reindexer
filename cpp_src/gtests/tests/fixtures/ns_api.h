#pragma once

#include <gtest/gtest.h>
#include "reindexer_api.h"
#include "tools/timetools.h"

class NsApi : public ReindexerApi {
protected:
	const string updatedTimeSecFieldName = "updated_time_sec";
	const string updatedTimeMSecFieldName = "updated_time_msec";
	const string updatedTimeUSecFieldName = "updated_time_usec";
	const string updatedTimeNSecFieldName = "updated_time_nsec";
	const string serialFieldName = "serial_field_int";
	const string manualFieldName = "manual_field_int";
	const int idNum = 1;
	const uint8_t upsertTimes = 3;
};
