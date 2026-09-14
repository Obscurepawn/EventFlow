#pragma once

#include "eventflow/status.hpp"
#include <array>
#include <cerrno>
#include <charconv>
#include <chrono>
#include <csignal>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <string>
#include <string_view>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace eventflow::test {

// Real child processes inherit the test's environment, including sanitizer settings.
class CoreProcess {
public:
  CoreProcess() noexcept = default;
  CoreProcess(const CoreProcess&) = delete;
  CoreProcess& operator=(const CoreProcess&) = delete;

  ~CoreProcess() noexcept {
    const auto result = stop();
    if (!result.ok())
      ADD_FAILURE() << result.message() << '\n' << output();
  }

  Status start(const std::string& binary, const std::vector<std::string>& args,
               const std::string& ready_marker, const std::string& log_path) noexcept {
    if (pid_ > 0)
      return Status(ErrorCode::FailedPrecondition, "child process is already running");
    if (!failure_.ok())
      return failure_;
    if (binary.empty() || binary.find('\0') != std::string::npos || log_path.empty() ||
        log_path.find('\0') != std::string::npos || ready_marker.empty() ||
        ready_marker.find_first_of(" \t\r\n") != std::string::npos ||
        ready_marker.find('\0') != std::string::npos)
      return Status(ErrorCode::InvalidArgument, "invalid child binary, log path or READY marker");
    std::vector<char*> argv;
    argv.reserve(args.size() + 2);
    argv.push_back(const_cast<char*>(binary.c_str()));
    for (const auto& argument : args) {
      if (argument.find('\0') != std::string::npos)
        return Status(ErrorCode::InvalidArgument, "child argument contains a NUL byte");
      argv.push_back(const_cast<char*>(argument.c_str()));
    }
    argv.push_back(nullptr);
    const auto prefix = ready_marker + " ";
    log_path_ = log_path;
    port_ = 0;
    const int log_fd = ::open(log_path_.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
    if (log_fd < 0)
      return error("cannot open child log", errno);
    pid_ = ::fork();
    if (pid_ == 0) {
      if (::dup2(log_fd, STDOUT_FILENO) < 0 || ::dup2(log_fd, STDERR_FILENO) < 0)
        ::_exit(126);
      if (::fcntl(STDOUT_FILENO, F_SETFD, 0) < 0 || ::fcntl(STDERR_FILENO, F_SETFD, 0) < 0)
        ::_exit(126);
      if (log_fd != STDOUT_FILENO && log_fd != STDERR_FILENO)
        ::close(log_fd);
      ::execv(binary.c_str(), argv.data());
      constexpr char message[] = "CoreProcess: execv failed\n";
      (void)::write(STDERR_FILENO, message, sizeof(message) - 1);
      ::_exit(127);
    }
    const auto fork_errno = errno;
    ::close(log_fd);
    if (pid_ < 0)
      return error("fork failed", fork_errno);

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (std::chrono::steady_clock::now() < deadline) {
      if (poll_exit(false))
        return failure_;
      auto contents = read_log();
      if (!contents.ok())
        return abort_start(contents.status());
      std::size_t begin = 0;
      while (begin < contents->size()) {
        const auto end = contents->find('\n', begin);
        if (end == std::string::npos)
          break;
        const std::string_view line(contents->data() + begin, end - begin);
        if (line.starts_with(prefix)) {
          int port = 0;
          const auto first = line.data() + prefix.size();
          const auto last = line.data() + line.size();
          const auto parsed = std::from_chars(first, last, port);
          if (parsed.ec != std::errc{} || parsed.ptr != last || port <= 0 || port > 65535)
            return abort_start(error("invalid READY port"));
          if (poll_exit(false))
            return failure_;
          port_ = port;
          return {};
        }
        begin = end + 1;
      }
      if (contents->size() >= max_log_bytes)
        return abort_start(error("child log limit reached before READY"));
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return abort_start(error("child READY timeout after 30 seconds"));
  }

  Status stop() noexcept {
    if (pid_ <= 0)
      return failure_;
    // An already dead child is an unexpected service exit, even with exit code zero.
    if (poll_exit(false))
      return failure_;
    if (::kill(pid_, SIGTERM) != 0 && errno != ESRCH)
      remember(error("cannot send SIGTERM", errno));
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (std::chrono::steady_clock::now() < deadline) {
      if (poll_exit(true))
        return failure_;
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    remember(error("child stop timeout after 30 seconds; SIGKILL required"));
    if (::kill(pid_, SIGKILL) != 0 && errno != ESRCH)
      remember(error("cannot send SIGKILL", errno));
    const auto reap_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while (std::chrono::steady_clock::now() < reap_deadline) {
      if (poll_exit(true))
        return failure_;
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    // Keep the PID owned so a subsequent stop can still reap a delayed killed child.
    return failure_;
  }

  std::string endpoint() const noexcept {
    return port_ > 0 ? "http://127.0.0.1:" + std::to_string(port_) : std::string();
  }

  pid_t pid() const noexcept {
    return pid_;
  }

  const std::string& log_path() const noexcept {
    return log_path_;
  }

  std::string output() const noexcept {
    const auto heading = "child log: " + log_path_ + "\n";
    auto contents = read_log();
    if (!contents.ok())
      return heading + "[log read failed: " + std::string(contents.status().message()) + "]\n";
    if (contents->size() == max_log_bytes)
      contents->append("\n[child log output limited to first 1 MiB]\n");
    return heading + *contents;
  }

private:
  static constexpr std::size_t max_log_bytes = 1024 * 1024;

  Status error(std::string_view message, int error_number = 0) const noexcept {
    auto detail = std::string(message);
    if (error_number != 0)
      detail += " (errno " + std::to_string(error_number) + ")";
    detail += "; log: " + log_path_;
    return Status(ErrorCode::Unavailable, detail);
  }

  void remember(Status failure) noexcept {
    if (failure_.ok())
      failure_ = failure;
  }

  Status abort_start(Status failure) noexcept {
    remember(failure);
    (void)stop();
    return failure_;
  }

  bool poll_exit(bool shutdown_requested) noexcept {
    int status = 0;
    const auto result = ::waitpid(pid_, &status, WNOHANG);
    if (result == 0 || (result < 0 && errno == EINTR))
      return false;
    if (result < 0) {
      const auto wait_errno = errno;
      remember(error("cannot reap child", wait_errno));
      if (wait_errno == ECHILD)
        pid_ = -1;
      return pid_ <= 0;
    }
    pid_ = -1;
    if (WIFEXITED(status)) {
      const auto code = WEXITSTATUS(status);
      if (!shutdown_requested || code != 0)
        remember(error((shutdown_requested ? "child exited with code "
                                           : "child exited unexpectedly with code ") +
                       std::to_string(code)));
    } else if (WIFSIGNALED(status)) {
      remember(error("child terminated by signal " + std::to_string(WTERMSIG(status))));
    } else {
      remember(error("child reported an unexpected wait status"));
    }
    return true;
  }

  StatusOr<std::string> read_log() const noexcept {
    const int descriptor = ::open(log_path_.c_str(), O_RDONLY | O_CLOEXEC);
    if (descriptor < 0)
      return error("cannot read child log", errno);
    std::string contents;
    std::array<char, 8192> bytes{};
    while (contents.size() < max_log_bytes) {
      const auto remaining = max_log_bytes - contents.size();
      const auto count =
          ::read(descriptor, bytes.data(), remaining < bytes.size() ? remaining : bytes.size());
      if (count < 0) {
        if (errno == EINTR)
          continue;
        const auto read_errno = errno;
        ::close(descriptor);
        return error("child log read failed", read_errno);
      }
      if (count == 0)
        break;
      contents.append(bytes.data(), static_cast<std::size_t>(count));
    }
    ::close(descriptor);
    return contents;
  }

  pid_t pid_ = -1;
  int port_ = 0;
  std::string log_path_;
  Status failure_;
};
} // namespace eventflow::test
