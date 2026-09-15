#include "definition.hpp"
#include "eventflow/database_main.hpp"

int main(int argc, char** argv) noexcept {
  return eventflow::database_main(argc, argv, eventflow::streaming_example::computation_module());
}
