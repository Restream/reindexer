#pragma once

#include <chrono>
#include <string>
#include "loggerwrapper.h"
#include "net/ev/ev.h"

namespace reindexer_server {
class DBManager;

}

extern "C" {
void* start_reindexer_grpc(reindexer_server::DBManager& dbMgr, std::chrono::seconds txIdleTimeout, reindexer::net::ev::dynamic_loop& loop,
						   const std::string& address, reindexer_server::LoggerWrapper logger);
void stop_reindexer_grpc(void*);
}
typedef void* (*p_start_reindexer_grpc)(reindexer_server::DBManager& dbMgr, std::chrono::seconds txIdleTimeout,
										reindexer::net::ev::dynamic_loop& loop, const std::string& address,
										reindexer_server::LoggerWrapper logger);

typedef void (*p_stop_reindexer_grpc)(void*);
