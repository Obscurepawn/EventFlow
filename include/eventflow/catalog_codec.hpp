#pragma once
#include "eventflow/catalog.hpp"

namespace eventflow {
struct CatalogCodecConfig {
  std::size_t max_bytes = 16 * 1024 * 1024;
  std::size_t max_relations = 1024;
  std::size_t max_fields_per_relation = 4096;
  // Plans retain exact earlier Catalog prefixes; cap their cumulative expansion.
  std::size_t max_provenance_bytes = 32 * 1024 * 1024;
  std::size_t max_expression_nodes = 65536;
  std::size_t max_expression_depth = 128;
};

// Portable canonical definitions only. Callback implementations are registered separately.
StatusOr<std::string> encode_catalog(const CatalogSnapshot& catalog,
                                     const CatalogCodecConfig& config = {}) noexcept;
StatusOr<CatalogSnapshot> decode_catalog(std::string_view bytes,
                                         const CatalogCodecConfig& config = {}) noexcept;
} // namespace eventflow
