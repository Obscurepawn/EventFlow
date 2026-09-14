#include "eventflow/http_message_server.hpp"
#include <charconv>
#include <chrono>
#include <csignal>
#include <iostream>
#include <string_view>
#include <thread>

namespace {

volatile std::sig_atomic_t stop_requested = 0;

void request_stop(int) noexcept {
  stop_requested = 1;
}

eventflow::Status run(int argc, char** argv) noexcept {
  using namespace eventflow;
  int port = 0;
  if (argc != 3 || std::string_view(argv[1]) != "--port")
    return Status(ErrorCode::InvalidArgument, "usage: eventflow_message_server --port PORT");
  const std::string_view value(argv[2]);
  const auto parsed = std::from_chars(value.data(), value.data() + value.size(), port);
  if (parsed.ec != std::errc{} || parsed.ptr != value.data() + value.size() || port < 0 ||
      port > 65535)
    return Status(ErrorCode::InvalidArgument, "port must be an integer in [0, 65535]");
  EF_ASSIGN_OR_RETURN(auto server,
                      HttpMessageServer::create(
                          {.partitions = {{.topic = "trades", .partition = 0, .next_offset = 0},
                                          {.topic = "trades", .partition = 1, .next_offset = 0}},
                           .max_messages = 1000,
                           .max_log_bytes = 4 * 1024 * 1024,
                           .max_message_bytes = 262144}));
  EF_ASSIGN_OR_RETURN(auto bound_port, server->start("127.0.0.1", port));
  std::cout << "HTTP_MESSAGE_SOURCE_READY " << bound_port << '\n' << std::flush;
  while (!stop_requested && server->is_running())
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
  const auto stopped_by_signal = stop_requested != 0;
  server->stop();
  return stopped_by_signal ? Status{}
                           : Status(ErrorCode::Unavailable, "HTTP message listener stopped");
}

} // namespace

int main(int argc, char** argv) noexcept {
  std::signal(SIGINT, request_stop);
  std::signal(SIGTERM, request_stop);
  const auto result = run(argc, argv);
  if (!result.ok()) {
    std::cerr << "Message server failed: " << result.message() << '\n';
    return 1;
  }
  return 0;
}
