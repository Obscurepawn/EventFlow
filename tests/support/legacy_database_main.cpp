#include "eventflow/database_main.hpp"
#include "legacy_feature_fixture.hpp"

// Existing cluster regressions explicitly select their legacy business graph.
int main(int argc, char** argv) noexcept {
  return eventflow::database_main(argc, argv, eventflow::test_support::legacy_feature_module());
}
