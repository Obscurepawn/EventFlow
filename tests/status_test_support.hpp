#pragma once
#include "eventflow/status.hpp"
#include <cstdlib>
#include <gtest/gtest.h>

namespace eventflow::test {
template <typename T> T take(StatusOr<T> value) noexcept {
  if (!value.ok()) {
    ADD_FAILURE() << value.status().message();
    std::abort();
  }
  return std::move(value).value();
}
} // namespace eventflow::test
