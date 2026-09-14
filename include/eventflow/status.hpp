#pragma once
#include <array>
#include <cstdlib>
#include <cstring>
#include <string_view>
#include <type_traits>
#include <utility>
#include <variant>

namespace eventflow {
enum class ErrorCode {
  Ok,
  InvalidArgument,
  NotFound,
  Conflict,
  Unavailable,
  ResourceExhausted,
  FailedPrecondition,
  Internal,
  UnknownCommit
};

// Reporting an allocation failure must not allocate another string.
class [[nodiscard]] Status {
public:
  Status() noexcept = default;

  Status(ErrorCode code, std::string_view message) noexcept : code_(code) {
    size_ = message.size() < message_.size() ? message.size() : message_.size();
    // Do not split a UTF-8 sequence when a valid diagnostic exceeds the buffer.
    if (size_ < message.size()) {
      while (size_ > 0 && (static_cast<unsigned char>(message[size_]) & 0xc0) == 0x80)
        --size_;
    }
    if (size_)
      std::memcpy(message_.data(), message.data(), size_);
  }

  bool ok() const noexcept {
    return code_ == ErrorCode::Ok;
  }

  ErrorCode code() const noexcept {
    return code_;
  }

  std::string_view message() const noexcept {
    return {message_.data(), size_};
  }

  explicit operator bool() const noexcept {
    return ok();
  }

private:
  ErrorCode code_ = ErrorCode::Ok;
  std::array<char, 384> message_{};
  std::size_t size_ = 0;
};

template <typename T> class [[nodiscard]] StatusOr {
  static_assert(std::is_nothrow_move_constructible_v<T>);

public:
  StatusOr(Status status) noexcept
      : result_(status.ok() ? Status(ErrorCode::Internal, "OK without a value") : status) {}

  StatusOr(T&& value) noexcept : result_(std::in_place_type<T>, std::move(value)) {}

  StatusOr(const StatusOr&) noexcept = delete;
  StatusOr& operator=(const StatusOr&) noexcept = delete;
  StatusOr(StatusOr&&) noexcept = default;
  StatusOr& operator=(StatusOr&&) noexcept = default;

  bool ok() const noexcept {
    return std::holds_alternative<T>(result_);
  }

  explicit operator bool() const noexcept {
    return ok();
  }

  Status status() const noexcept {
    return ok() ? Status{} : std::get<Status>(result_);
  }

  T* get() noexcept {
    return std::get_if<T>(&result_);
  }

  const T* get() const noexcept {
    return std::get_if<T>(&result_);
  }

  // Calling value() without checking ok() is a programmer invariant violation.
  T& value() & noexcept {
    auto* p = get();
    if (!p)
      std::abort();
    return *p;
  }

  const T& value() const& noexcept {
    auto* p = get();
    if (!p)
      std::abort();
    return *p;
  }

  T&& value() && noexcept {
    return std::move(value());
  }

  T& operator*() noexcept {
    return value();
  }

  const T& operator*() const noexcept {
    return value();
  }

  T* operator->() noexcept {
    return &value();
  }

  const T* operator->() const noexcept {
    return &value();
  }

private:
  std::variant<Status, T> result_;
};

inline Status check(bool condition, std::string_view message) noexcept {
  return condition ? Status{} : Status(ErrorCode::InvalidArgument, message);
}
} // namespace eventflow

#define EF_RETURN_IF_ERROR(expression)                                                             \
  do {                                                                                             \
    const auto ef_status = (expression);                                                           \
    if (!ef_status.ok())                                                                           \
      return ef_status;                                                                            \
  } while (false)
#define EF_CONCAT_INNER(a, b) a##b
#define EF_CONCAT(a, b) EF_CONCAT_INNER(a, b)
#define EF_ASSIGN_IMPL(lhs, expression, id)                                                        \
  auto EF_CONCAT(ef_result_, id) = (expression);                                                   \
  if (!EF_CONCAT(ef_result_, id).ok())                                                             \
    return EF_CONCAT(ef_result_, id).status();                                                     \
  lhs = std::move(EF_CONCAT(ef_result_, id)).value()
#define EF_ASSIGN_OR_RETURN(lhs, expression) EF_ASSIGN_IMPL(lhs, expression, __COUNTER__)
