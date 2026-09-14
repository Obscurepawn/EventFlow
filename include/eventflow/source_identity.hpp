#pragma once
#include <cstdint>
#include <string>

namespace eventflow {
struct SourceIdentity {
  std::string source_id;
  std::string topic;
  std::int32_t partition = 0;
  std::int64_t offset = 0;
  bool operator==(const SourceIdentity&) const noexcept = default;
};
} // namespace eventflow
