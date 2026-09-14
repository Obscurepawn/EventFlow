#pragma once
#include "status_test_support.hpp"
#include <charconv>
#include <chrono>
#include <csignal>
#include <filesystem>
#include <poll.h>
#include <string>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>

namespace eventflow::test {
class TempDirectory {
public:
  TempDirectory() noexcept {
    auto pattern = (std::filesystem::temp_directory_path() / "eventflow-test-XXXXXX").string();
    if (!::mkdtemp(pattern.data())) {
      ADD_FAILURE() << "mkdtemp failed";
      std::abort();
    }
    path = std::move(pattern);
  }

  ~TempDirectory() noexcept {
    std::error_code error;
    std::filesystem::remove_all(path, error);
  }

  std::string path;
};

class KvProcess {
public:
  static StatusOr<std::unique_ptr<KvProcess>> open(const std::string& backend,
                                                   const std::string& directory) noexcept {
    int descriptors[2];
    if (::pipe(descriptors) != 0)
      return Status(ErrorCode::Unavailable, "pipe failed");
    auto result = std::unique_ptr<KvProcess>(new KvProcess());
    result->pid_ = ::fork();
    if (result->pid_ == 0) {
      ::close(descriptors[0]);
      ::dup2(descriptors[1], STDOUT_FILENO);
      ::close(descriptors[1]);
      ::execl(EVENTFLOW_BINARY_PATH, EVENTFLOW_BINARY_PATH, "kv", "--backend", backend.c_str(),
              "--data-dir", directory.c_str(), "--port", "0", "--shards", "2", nullptr);
      ::_exit(127);
    }
    ::close(descriptors[1]);
    if (result->pid_ < 0) {
      ::close(descriptors[0]);
      return Status(ErrorCode::Unavailable, "fork failed");
    }
    const auto status = result->read_ready(descriptors[0]);
    ::close(descriptors[0]);
    if (!status.ok())
      return status;
    return result;
  }

  ~KvProcess() noexcept {
    stop();
  }

  KvProcess(const KvProcess&) = delete;
  KvProcess& operator=(const KvProcess&) = delete;

  void stop() noexcept {
    if (pid_ <= 0)
      return;
    ::kill(pid_, SIGTERM);
    int status;
    for (int attempt = 0; attempt < 100; ++attempt) {
      if (::waitpid(pid_, &status, WNOHANG) == pid_) {
        pid_ = -1;
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ::kill(pid_, SIGKILL);
    ::waitpid(pid_, &status, 0);
    pid_ = -1;
  }

  std::string endpoint() const noexcept {
    return "http://127.0.0.1:" + std::to_string(port_);
  }

private:
  KvProcess() noexcept = default;

  Status read_ready(int descriptor) noexcept {
    std::string line;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (std::chrono::steady_clock::now() < deadline && line.size() < 512) {
      pollfd input{descriptor, POLLIN, 0};
      const int polled = ::poll(&input, 1, 100);
      if (polled < 0)
        return Status(ErrorCode::Unavailable, "readiness poll failed");
      if (polled == 0)
        continue;
      char character;
      if (::read(descriptor, &character, 1) != 1)
        return Status(ErrorCode::Unavailable, "KV exited before ready");
      if (character == '\n')
        break;
      line.push_back(character);
    }
    constexpr std::string_view prefix = "EVENTFLOW_KV_READY ";
    if (!line.starts_with(prefix))
      return Status(ErrorCode::Unavailable, "KV readiness timeout");
    auto [end, error] =
        std::from_chars(line.data() + prefix.size(), line.data() + line.size(), port_);
    if (error != std::errc{} || end != line.data() + line.size() || port_ <= 0)
      return Status(ErrorCode::Unavailable, "invalid KV readiness");
    return {};
  }

  pid_t pid_ = -1;
  int port_ = 0;
};
} // namespace eventflow::test
