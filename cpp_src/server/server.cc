#include "server.h"
#include "serverimpl.h"

namespace reindexer_server {

Server::Server(ServerMode mode) : impl_(new ServerImpl(mode)) {}
Server::~Server() = default;
Error Server::InitFromCLI(int argc, char* argv[]) { return impl_->InitFromCLI(argc, argv); }
Error Server::InitFromFile(const char* filePath) { return impl_->InitFromFile(filePath); }
Error Server::InitFromYAML(const std::string& yaml) { return impl_->InitFromYAML(yaml); }
int Server::Start() { return impl_->Start(); }
void Server::Stop() { return impl_->Stop(); }
void Server::EnableHandleSignals(bool enable) noexcept { impl_->EnableHandleSignals(enable); }
DBManager& Server::GetDBManager() noexcept { return impl_->GetDBManager(); }
bool Server::IsReady() const noexcept { return impl_->IsReady(); }
bool Server::IsRunning() const noexcept { return impl_->IsRunning(); }
void Server::ReopenLogFiles() { impl_->ReopenLogFiles(); }
std::string Server::GetCoreLogPath() const { return impl_->GetCoreLogPath(); }

}  // namespace reindexer_server
