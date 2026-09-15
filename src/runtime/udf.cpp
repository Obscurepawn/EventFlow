#include "eventflow/udf.hpp"
#include "eventflow/udf_build_identity.hpp"
#include <array>
#include <bit>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <dlfcn.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace eventflow {
namespace {

struct FileDescriptor {
  int value = -1;

  ~FileDescriptor() noexcept {
    if (value >= 0)
      ::close(value);
  }
};

} // namespace

std::string udf_digest(std::string_view bytes) noexcept {
  constexpr std::array<std::uint32_t, 64> constants{
      0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
      0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
      0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
      0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
      0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
      0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
      0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
      0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
      0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
      0xc67178f2};
  std::array<std::uint32_t, 8> hash{0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a,
                                    0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19};
  std::string padded(bytes);
  padded.push_back(static_cast<char>(0x80));
  while (padded.size() % 64 != 56)
    padded.push_back('\0');
  const auto bit_count = static_cast<std::uint64_t>(bytes.size()) * 8;
  for (int shift = 56; shift >= 0; shift -= 8)
    padded.push_back(static_cast<char>(bit_count >> shift));
  for (std::size_t offset = 0; offset < padded.size(); offset += 64) {
    std::array<std::uint32_t, 64> words{};
    for (std::size_t i = 0; i < 16; ++i)
      for (std::size_t j = 0; j < 4; ++j)
        words[i] = (words[i] << 8) | static_cast<unsigned char>(padded[offset + 4 * i + j]);
    for (std::size_t i = 16; i < 64; ++i) {
      const auto x = words[i - 15], y = words[i - 2];
      words[i] = words[i - 16] + (std::rotr(x, 7) ^ std::rotr(x, 18) ^ (x >> 3)) + words[i - 7] +
                 (std::rotr(y, 17) ^ std::rotr(y, 19) ^ (y >> 10));
    }
    auto [a, b, c, d, e, f, g, h] = hash;
    for (std::size_t i = 0; i < 64; ++i) {
      const auto first = h + (std::rotr(e, 6) ^ std::rotr(e, 11) ^ std::rotr(e, 25)) +
                         ((e & f) ^ (~e & g)) + constants[i] + words[i];
      const auto second =
          (std::rotr(a, 2) ^ std::rotr(a, 13) ^ std::rotr(a, 22)) + ((a & b) ^ (a & c) ^ (b & c));
      h = g;
      g = f;
      f = e;
      e = d + first;
      d = c;
      c = b;
      b = a;
      a = first + second;
    }
    const std::array<std::uint32_t, 8> state{a, b, c, d, e, f, g, h};
    for (std::size_t i = 0; i < hash.size(); ++i)
      hash[i] += state[i];
  }
  std::array<char, 65> result{};
  for (std::size_t i = 0; i < hash.size(); ++i)
    std::snprintf(result.data() + i * 8, 9, "%08x", static_cast<unsigned>(hash[i]));
  return std::string(result.data(), 64);
}

StatusOr<std::string> read_udf_file(const std::string& path) noexcept {
  if (path.empty() || path.find('\0') != std::string::npos)
    return Status(ErrorCode::InvalidArgument, "invalid UDF file path");
  FileDescriptor fd{.value = ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NONBLOCK)};
  struct stat info{};
  if (fd.value < 0 || ::fstat(fd.value, &info) != 0)
    return Status(ErrorCode::NotFound, "cannot read UDF file");
  if (!S_ISREG(info.st_mode) || info.st_size <= 0 ||
      static_cast<std::uint64_t>(info.st_size) > max_udf_bytes)
    return Status(ErrorCode::InvalidArgument,
                  "UDF must be a nonempty regular file within byte limit");
  std::string bytes(static_cast<std::size_t>(info.st_size), '\0');
  std::size_t offset = 0;
  while (offset < bytes.size()) {
    const auto count = ::read(fd.value, bytes.data() + offset, bytes.size() - offset);
    if (count < 0 && errno == EINTR)
      continue;
    if (count <= 0)
      return Status(ErrorCode::Unavailable, "UDF file changed or could not be read");
    offset += static_cast<std::size_t>(count);
  }
  char extra = 0;
  if (::read(fd.value, &extra, 1) != 0)
    return Status(ErrorCode::Unavailable, "UDF file grew while reading");
  return bytes;
}

struct UdfLibrary::Impl {
  FileDescriptor fd;
  void* handle = nullptr;
  const UdfDefinition* definition = nullptr;
  std::string digest;

  ~Impl() noexcept {
    if (handle)
      ::dlclose(handle);
  }
};

UdfLibrary::UdfLibrary(std::unique_ptr<Impl> impl) noexcept : impl_(std::move(impl)) {}

UdfLibrary::~UdfLibrary() noexcept = default;

StatusOr<std::shared_ptr<UdfLibrary>> UdfLibrary::open(std::string_view bytes) noexcept {
  if (bytes.size() < 4 || bytes.size() > max_udf_bytes ||
      bytes.substr(0, 4) != "\x7f"
                            "ELF")
    return Status(ErrorCode::InvalidArgument, "UDF must contain a bounded ELF shared library");
  auto impl = std::make_unique<Impl>();
  impl->fd.value = ::memfd_create("eventflow-udf", MFD_CLOEXEC | MFD_ALLOW_SEALING);
  if (impl->fd.value < 0)
    return Status(ErrorCode::Unavailable, "cannot create UDF memory file");
  std::size_t offset = 0;
  while (offset < bytes.size()) {
    const auto count = ::write(impl->fd.value, bytes.data() + offset, bytes.size() - offset);
    if (count < 0 && errno == EINTR)
      continue;
    if (count <= 0)
      return Status(ErrorCode::Unavailable, "cannot write UDF memory file");
    offset += static_cast<std::size_t>(count);
  }
  if (::fcntl(impl->fd.value, F_ADD_SEALS,
              F_SEAL_WRITE | F_SEAL_GROW | F_SEAL_SHRINK | F_SEAL_SEAL) < 0)
    return Status(ErrorCode::Unavailable, "cannot seal UDF memory file");
  const auto path = "/proc/self/fd/" + std::to_string(impl->fd.value);
  impl->handle = ::dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);
  if (!impl->handle)
    return Status(ErrorCode::InvalidArgument, ::dlerror());
  const auto identify =
      reinterpret_cast<UdfBuildId>(::dlsym(impl->handle, "eventflow_udf_build_id"));
  const auto* identity = identify ? identify() : nullptr;
  if (!identity || std::strcmp(identity, udf_detail::build_id) != 0)
    return Status(ErrorCode::Conflict, "UDF build identity differs from this server");
  const auto entry = reinterpret_cast<UdfEntry>(::dlsym(impl->handle, "eventflow_udf_entry"));
  if (!entry || !(impl->definition = entry()) || !impl->definition->invoke)
    return Status(ErrorCode::InvalidArgument, "UDF has no callable entry");
  impl->digest = udf_digest(bytes);
  auto library = std::shared_ptr<UdfLibrary>(new UdfLibrary(std::move(impl)));
  EF_ASSIGN_OR_RETURN(auto registry, FunctionRegistry::create());
  EF_RETURN_IF_ERROR(register_udf(*registry, library));
  return std::move(library);
}

const UdfDefinition& UdfLibrary::definition() const noexcept {
  return *impl_->definition;
}

const std::string& UdfLibrary::digest() const noexcept {
  return impl_->digest;
}

Status register_udf(FunctionRegistry& registry, std::shared_ptr<UdfLibrary> library) noexcept {
  if (!library)
    return Status(ErrorCode::InvalidArgument, "UDF library is required");
  auto signature = library->definition().signature;
  signature.implementation_identity += ":sha256:" + library->digest();
  return registry.add({.signature = std::move(signature),
                       .callback = [library = std::move(library)](
                                       const FunctionInputs& inputs) noexcept -> StatusOr<Row> {
                         return library->definition().invoke(inputs);
                       }});
}

} // namespace eventflow
