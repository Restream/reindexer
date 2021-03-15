#pragma once

#include <chrono>
#include <string>
#include "net/ev/ev.h"

namespace reindexer_server {
class DBManager;

}

extern "C" {
void start_reindexer_grpc(reindexer_server::DBManager& dbMgr, std::chrono::seconds txIdleTimeout, reindexer::net::ev::dynamic_loop& loop,
						  const string& address);
void stop_reindexer_grpc();
}
typedef void* (*p_start_reindexer_grpc)(reindexer_server::DBManager& dbMgr, std::chrono::seconds txIdleTimeout,
										reindexer::net::ev::dynamic_loop& loop, const string& address);

typedef void (*p_stop_reindexer_grpc)(void*);
