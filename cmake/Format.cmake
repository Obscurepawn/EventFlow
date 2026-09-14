if(NOT CLANG_FORMAT)
  find_program(CLANG_FORMAT NAMES clang-format clang-format-21 clang-format-20 clang-format-19 clang-format-18)
endif()
if(NOT CLANG_FORMAT)
  message(FATAL_ERROR "clang-format is an optional developer tool. Supply -DCLANG_FORMAT=/path/to/clang-format")
endif()
file(GLOB_RECURSE sources
  "${CMAKE_CURRENT_LIST_DIR}/../src/*.cpp"
  "${CMAKE_CURRENT_LIST_DIR}/../src/*.h"
  "${CMAKE_CURRENT_LIST_DIR}/../src/*.hpp"
  "${CMAKE_CURRENT_LIST_DIR}/../examples/*.cpp"
  "${CMAKE_CURRENT_LIST_DIR}/../examples/*.h"
  "${CMAKE_CURRENT_LIST_DIR}/../examples/*.hpp"
  "${CMAKE_CURRENT_LIST_DIR}/../include/*.h"
  "${CMAKE_CURRENT_LIST_DIR}/../include/*.hpp"
  "${CMAKE_CURRENT_LIST_DIR}/../tests/*.cpp"
  "${CMAKE_CURRENT_LIST_DIR}/../tests/*.h"
  "${CMAKE_CURRENT_LIST_DIR}/../tests/*.hpp")
if(FORMAT_CHECK)
  set(args --dry-run --Werror)
else()
  set(args -i)
endif()
execute_process(COMMAND "${CLANG_FORMAT}" ${args} ${sources} RESULT_VARIABLE result)
if(NOT result EQUAL 0)
  message(FATAL_ERROR "clang-format returned ${result}")
endif()
