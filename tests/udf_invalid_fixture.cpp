#include "eventflow/udf_build_identity.hpp"

extern "C" const char* eventflow_udf_build_id() noexcept {
#if defined(EVENTFLOW_TEST_BAD_UDF_BUILD)
  return "incompatible-udf-build";
#else
  return eventflow::udf_detail::build_id;
#endif
}
