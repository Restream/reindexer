#pragma once

#if REINDEX_WITH_GPERFTOOLS

namespace reindexer_server {
namespace pprof {

void ProfilerRegisterThread();
int ProfilerStart(const char* fname);
void ProfilerStop();
char* GetHeapProfile();
bool GperfProfilerIsAvailable();

}  // namespace pprof
}  // namespace reindexer_server

#endif	// REINDEX_WITH_GPERFTOOLS
