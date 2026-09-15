cmake_minimum_required(VERSION 3.20)
if(NOT DEFINED CMAKE_CXX_COMPILER)
  find_program(EVENTFLOW_CLANGXX NAMES clang++ clang++-18 REQUIRED)
  set(CMAKE_CXX_COMPILER "${EVENTFLOW_CLANGXX}" CACHE FILEPATH "Clang C++ compiler")
endif()
project(EventFlow VERSION 0.1.0 LANGUAGES CXX)
if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  message(FATAL_ERROR "EventFlow requires Clang. Configure a fresh build directory with -DCMAKE_CXX_COMPILER=clang++")
endif()
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug CACHE STRING "Build configuration" FORCE)
endif()
if(CMAKE_BUILD_TYPE STREQUAL "Release")
  set(EVENTFLOW_SANITIZERS OFF CACHE BOOL "Production build has no runtime instrumentation" FORCE)
  if(DEFINED BUILD_TESTING AND BUILD_TESTING)
    message(FATAL_ERROR "Run instrumented tests in Debug; Release requires -DBUILD_TESTING=OFF")
  endif()
  set(BUILD_TESTING OFF CACHE BOOL "Build tests" FORCE)
  set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG" CACHE STRING "Release optimization" FORCE)
  include(CheckIPOSupported)
  check_ipo_supported(RESULT EVENTFLOW_IPO_SUPPORTED OUTPUT EVENTFLOW_IPO_REASON LANGUAGES CXX)
  if(EVENTFLOW_IPO_SUPPORTED)
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
  else()
    message(STATUS "LTO unavailable: ${EVENTFLOW_IPO_REASON}")
  endif()
else()
  set(EVENTFLOW_SANITIZERS ON CACHE BOOL "Development/test quality instrumentation" FORCE)
endif()
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
include(CTest)
find_package(Threads REQUIRED)
set(EF_THIRD "${CMAKE_CURRENT_SOURCE_DIR}/cpp3rdlib")
foreach(required include/rocksdb/db.h include/httplib.h include/nlohmann/json.hpp lib/librocksdb.so)
  if(NOT EXISTS "${EF_THIRD}/${required}")
    message(FATAL_ERROR "Missing packaged dependency: cpp3rdlib/${required}. See cpp3rdlib/README.md")
  endif()
endforeach()
add_library(ef_options INTERFACE)
target_compile_features(ef_options INTERFACE cxx_std_20)
target_compile_options(ef_options INTERFACE -Wall -Wextra -Wpedantic)
target_include_directories(ef_options INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}/include")
target_include_directories(ef_options SYSTEM INTERFACE "${EF_THIRD}/include")
target_link_libraries(ef_options INTERFACE Threads::Threads)
if(EVENTFLOW_SANITIZERS)
  target_compile_options(ef_options INTERFACE -fsanitize=address,undefined,leak -fno-sanitize-recover=all -fno-omit-frame-pointer)
  target_link_options(ef_options INTERFACE -fsanitize=address,undefined,leak -fno-sanitize-recover=all)
endif()
add_library(ef_rocks SHARED IMPORTED)
set_target_properties(ef_rocks PROPERTIES IMPORTED_LOCATION "${EF_THIRD}/lib/librocksdb.so")
function(ef_runtime target)
  target_link_options(${target} PRIVATE "LINKER:--disable-new-dtags")
  set_target_properties(${target} PROPERTIES
    BUILD_RPATH "\$ORIGIN/../../cpp3rdlib/lib"
    INSTALL_RPATH "\$ORIGIN/../cpp3rdlib/lib")
endfunction()
add_library(ef_core STATIC src/core/types.cpp src/core/key_codec.cpp)
target_link_libraries(ef_core PUBLIC ef_options)
add_library(ef_exact_sum STATIC src/runtime/exact_sum.cpp)
target_link_libraries(ef_exact_sum PUBLIC ef_options)
add_library(ef_time STATIC src/runtime/time.cpp)
target_link_libraries(ef_time PUBLIC ef_core)
add_library(ef_fsm STATIC src/runtime/fsm.cpp)
target_link_libraries(ef_fsm PUBLIC ef_core)
add_library(ef_cep STATIC src/runtime/cep.cpp)
target_link_libraries(ef_cep PUBLIC ef_core)
add_library(ef_observation_credit STATIC src/runtime/observation_credit.cpp)
target_link_libraries(ef_observation_credit PUBLIC ef_core)
add_library(ef_kafka SHARED IMPORTED)
set_target_properties(ef_kafka PROPERTIES IMPORTED_LOCATION "${EF_THIRD}/lib/librdkafka.so")
add_library(ef_message_source STATIC src/ingest/message_source.cpp)
target_link_libraries(ef_message_source PUBLIC ef_options PRIVATE ef_kafka)
target_link_libraries(ef_message_source PRIVATE ef_core)
add_library(ef_source_poller STATIC src/ingest/source_poller.cpp)
target_link_libraries(ef_source_poller PUBLIC ef_message_source)
add_library(ef_http_message_server STATIC src/ingest/http_message_server.cpp)
target_link_libraries(ef_http_message_server PUBLIC ef_core)
add_library(ef_storage STATIC src/storage/state_store.cpp)
target_link_libraries(ef_storage PUBLIC ef_core PRIVATE ef_rocks)
add_library(ef_cache STATIC src/storage/cached_store.cpp)
target_link_libraries(ef_cache PUBLIC ef_storage)
add_library(ef_transport STATIC src/storage/remote_store.cpp src/service/kv_server.cpp)
target_link_libraries(ef_transport PUBLIC ef_storage)
add_library(ef_antlr SHARED IMPORTED)
set_target_properties(ef_antlr PROPERTIES IMPORTED_LOCATION "${EF_THIRD}/lib/libantlr4-runtime.so")
add_library(ef_sql STATIC src/sql/sql_parser.cpp src/sql/catalog.cpp src/sql/binder.cpp src/sql/expression.cpp
  src/sql/generated/EventFlowSqlLexer.cpp src/sql/generated/EventFlowSqlParser.cpp)
target_include_directories(ef_sql SYSTEM PRIVATE "${EF_THIRD}/include/antlr4-runtime")
target_include_directories(ef_sql PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src/sql/generated")
target_link_libraries(ef_sql PUBLIC ef_core PRIVATE ef_antlr)
add_library(ef_catalog_codec STATIC src/sql/catalog_codec.cpp)
target_link_libraries(ef_catalog_codec PUBLIC ef_sql)
add_library(ef_trigger_expression STATIC src/runtime/trigger_expression.cpp src/runtime/typed_schema.cpp)
target_link_libraries(ef_trigger_expression PUBLIC ef_sql ef_cep)
add_library(ef_trigger_pattern STATIC src/runtime/trigger_pattern.cpp src/runtime/typed_trigger.cpp)
target_link_libraries(ef_trigger_pattern PUBLIC ef_trigger_expression)
add_library(ef_observation_program STATIC src/runtime/observation_program.cpp)
target_link_libraries(ef_observation_program PUBLIC ef_sql ef_fsm ef_cep)
add_library(ef_engine STATIC src/runtime/engine.cpp)
target_link_libraries(ef_engine PUBLIC ef_sql ef_storage ef_exact_sum PRIVATE ef_function_executor)
add_library(ef_event_processor STATIC src/runtime/event_processor.cpp src/runtime/trigger_audit.cpp)
target_link_libraries(ef_event_processor PUBLIC ef_engine ef_fsm ef_time ef_cep ef_request_runtime)
add_library(ef_computation STATIC src/runtime/trigger.cpp src/runtime/computation.cpp)
target_link_libraries(ef_computation PUBLIC ef_trigger_pattern ef_event_processor)
add_library(ef_computation_runtime STATIC src/runtime/computation_runtime.cpp)
target_link_libraries(ef_computation_runtime PUBLIC ef_computation)
add_library(ef_function_executor STATIC src/runtime/function_executor.cpp)
target_link_libraries(ef_function_executor PUBLIC ef_core ef_time)
add_library(ef_udf STATIC src/runtime/udf.cpp)
target_link_libraries(ef_udf PUBLIC ef_function_executor PRIVATE ${CMAKE_DL_LIBS})
add_library(ef_submitted_computation STATIC src/runtime/submitted_computation.cpp)
target_link_libraries(ef_submitted_computation PUBLIC ef_computation ef_udf)
add_library(ef_request_machine STATIC src/runtime/request_machine.cpp)
target_link_libraries(ef_request_machine PUBLIC ef_sql ef_function_executor)
add_library(ef_request_runtime STATIC src/runtime/request_runtime.cpp)
target_link_libraries(ef_request_runtime PUBLIC ef_engine ef_request_machine)
add_library(ef_compute_service STATIC src/service/compute_service.cpp)
target_link_libraries(ef_compute_service PUBLIC ef_computation_runtime ef_transport ef_cache)
target_link_libraries(ef_compute_service PUBLIC ef_submitted_computation)
add_library(ef_coordinator_service STATIC src/service/coordinator_service.cpp)
target_link_libraries(ef_coordinator_service PUBLIC ef_computation_runtime ef_catalog_codec ef_transport ef_source_poller)
target_link_libraries(ef_coordinator_service PUBLIC ef_submitted_computation)
add_library(ef_cluster_client STATIC src/client/cluster_client.cpp)
target_link_libraries(ef_cluster_client PUBLIC ef_core)
target_link_libraries(ef_cluster_client PUBLIC ef_submitted_computation)
add_library(ef_database_main STATIC src/service/database_main.cpp)
target_link_libraries(ef_database_main PUBLIC ef_transport ef_compute_service ef_coordinator_service ef_message_source)
add_executable(eventflow src/main.cpp)
target_link_libraries(eventflow PRIVATE ef_database_main)
ef_runtime(eventflow)

# Application-owned definitions are linked into this example's database processes.
# Core services never depend on an example implementation.
add_library(ef_static_streaming_definition STATIC examples/static_streaming/definition.cpp)
target_link_libraries(ef_static_streaming_definition PUBLIC ef_computation_runtime)
target_include_directories(ef_static_streaming_definition PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/examples/static_streaming")
add_executable(eventflow_static_streaming_database examples/static_streaming/main.cpp)
target_link_libraries(eventflow_static_streaming_database PRIVATE ef_database_main ef_static_streaming_definition)
ef_runtime(eventflow_static_streaming_database)
add_executable(eventflow_message_server examples/streaming_database/message_server.cpp)
target_link_libraries(eventflow_message_server PRIVATE ef_http_message_server)
ef_runtime(eventflow_message_server)
add_executable(eventflow_static_streaming_verify examples/static_streaming/verify.cpp)
target_link_libraries(eventflow_static_streaming_verify PRIVATE ef_cluster_client ef_static_streaming_definition)
ef_runtime(eventflow_static_streaming_verify)

# Production loader and plugin modules share one SDK build identity.
set(ef_udf_identity "${CMAKE_CXX_COMPILER}|${CMAKE_CXX_COMPILER_VERSION}|${CMAKE_SYSTEM_NAME}|${CMAKE_SYSTEM_PROCESSOR}|${CMAKE_BUILD_TYPE}|${CMAKE_CXX_FLAGS}|${CMAKE_CXX_FLAGS_DEBUG}|${CMAKE_CXX_FLAGS_RELEASE}|${EVENTFLOW_SANITIZERS}")
foreach(ef_udf_header include/eventflow/udf.hpp include/eventflow/status.hpp
    include/eventflow/types.hpp include/eventflow/function_executor.hpp cpp3rdlib/include/nlohmann/json.hpp)
  file(SHA256 "${CMAKE_CURRENT_SOURCE_DIR}/${ef_udf_header}" ef_udf_header_hash)
  string(APPEND ef_udf_identity "|${ef_udf_header_hash}")
  set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS "${ef_udf_header}")
endforeach()
string(SHA256 EVENTFLOW_CPP_UDF_BUILD_ID "${ef_udf_identity}")
configure_file(include/eventflow/udf_build_identity.hpp.in generated/eventflow/udf_build_identity.hpp @ONLY)
target_include_directories(ef_options INTERFACE "${CMAKE_CURRENT_BINARY_DIR}/generated")
add_library(ef_cpp_udf_options INTERFACE)
target_link_libraries(ef_cpp_udf_options INTERFACE ef_options)

add_library(eventflow_news_udf MODULE examples/cpp_udf/news_impact.cpp)
target_link_libraries(eventflow_news_udf PRIVATE ef_cpp_udf_options)
set_target_properties(eventflow_news_udf PROPERTIES LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")
add_executable(eventflow_cpp_udf_demo examples/cpp_udf/main.cpp)
target_link_libraries(eventflow_cpp_udf_demo PRIVATE ef_cpp_udf_options ef_udf)
add_dependencies(eventflow_cpp_udf_demo eventflow_news_udf)
add_executable(eventflow_native_udf_verify examples/streaming_database/verify.cpp examples/streaming_database/definition.cpp)
target_link_libraries(eventflow_native_udf_verify PRIVATE ef_cluster_client ef_catalog_codec)
ef_runtime(eventflow_native_udf_verify)
install(TARGETS eventflow_native_udf_verify RUNTIME DESTINATION bin)
install(DIRECTORY examples/native_udf DESTINATION examples USE_SOURCE_PERMISSIONS)
install(TARGETS eventflow_cpp_udf_demo RUNTIME DESTINATION bin)
install(TARGETS eventflow_news_udf LIBRARY DESTINATION lib)
install(DIRECTORY examples/cpp_udf DESTINATION examples USE_SOURCE_PERMISSIONS)

install(TARGETS eventflow eventflow_static_streaming_database eventflow_message_server eventflow_static_streaming_verify
  RUNTIME DESTINATION bin)
install(DIRECTORY examples/streaming_database examples/static_streaming DESTINATION examples USE_SOURCE_PERMISSIONS)
install(DIRECTORY cpp3rdlib/lib DESTINATION cpp3rdlib)
install(DIRECTORY cpp3rdlib/licenses DESTINATION cpp3rdlib)
if(BUILD_TESTING)
  add_test(NAME streaming_database_example
    COMMAND "${CMAKE_COMMAND}" -E env "EVENTFLOW_BUILD_DIR=${CMAKE_BINARY_DIR}" "EVENTFLOW_SKIP_BUILD=1"
      bash "${CMAKE_CURRENT_SOURCE_DIR}/examples/streaming_database/run.sh")
  set_tests_properties(streaming_database_example PROPERTIES TIMEOUT 240
    ENVIRONMENT "ASAN_OPTIONS=detect_leaks=1:halt_on_error=1;UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1;LSAN_OPTIONS=exitcode=23")
  add_executable(submitted_computation_test tests/submitted_computation_test.cpp)
  ef_runtime(submitted_computation_test)
  target_link_libraries(submitted_computation_test PRIVATE ef_submitted_computation ef_computation_runtime ef_coordinator_service ef_compute_service ef_cluster_client GTest::gtest_main)
  target_compile_definitions(submitted_computation_test PRIVATE EVENTFLOW_TEST_UDF_PATH="$<TARGET_FILE:eventflow_news_udf>")
  add_dependencies(submitted_computation_test eventflow_news_udf)
  add_test(NAME submitted_computation_test COMMAND submitted_computation_test)
  set_tests_properties(submitted_computation_test PROPERTIES TIMEOUT 30
    ENVIRONMENT "ASAN_OPTIONS=detect_leaks=1:halt_on_error=1;UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1;LSAN_OPTIONS=exitcode=23")
  add_executable(udf_test tests/udf_test.cpp)
  add_library(udf_missing_entry MODULE tests/udf_invalid_fixture.cpp)
  target_link_libraries(udf_missing_entry PRIVATE ef_options)
  add_library(udf_bad_build MODULE tests/udf_invalid_fixture.cpp)
  target_link_libraries(udf_bad_build PRIVATE ef_options)
  target_compile_definitions(udf_bad_build PRIVATE EVENTFLOW_TEST_BAD_UDF_BUILD=1)
  target_link_libraries(udf_test PRIVATE ef_udf GTest::gtest_main)
  target_compile_definitions(udf_test PRIVATE EVENTFLOW_TEST_UDF_PATH="$<TARGET_FILE:eventflow_news_udf>")
  target_compile_definitions(udf_test PRIVATE EVENTFLOW_TEST_MISSING_UDF_ENTRY="$<TARGET_FILE:udf_missing_entry>"
    EVENTFLOW_TEST_BAD_UDF_BUILD_PATH="$<TARGET_FILE:udf_bad_build>")
  add_dependencies(udf_test udf_missing_entry udf_bad_build)
  add_dependencies(udf_test eventflow_news_udf)
  add_test(NAME udf_test COMMAND udf_test)
  set_tests_properties(udf_test PROPERTIES TIMEOUT 30
    ENVIRONMENT "ASAN_OPTIONS=detect_leaks=1:halt_on_error=1;UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1;LSAN_OPTIONS=exitcode=23")
  add_test(NAME cpp_udf_example COMMAND eventflow_cpp_udf_demo $<TARGET_FILE:eventflow_news_udf>)
  set_tests_properties(cpp_udf_example PROPERTIES TIMEOUT 30
    ENVIRONMENT "ASAN_OPTIONS=detect_leaks=1:halt_on_error=1;UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1;LSAN_OPTIONS=exitcode=23")
  add_library(ef_legacy_feature_fixture STATIC tests/support/legacy_feature_fixture.cpp)
  target_link_libraries(ef_legacy_feature_fixture PUBLIC ef_computation)
  target_include_directories(ef_legacy_feature_fixture PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/tests/support")
  add_executable(eventflow_legacy_test_server tests/support/legacy_database_main.cpp)
  target_link_libraries(eventflow_legacy_test_server PRIVATE ef_database_main ef_legacy_feature_fixture)
  ef_runtime(eventflow_legacy_test_server)
  add_test(NAME static_streaming_example
    COMMAND "${CMAKE_COMMAND}" -E env "EVENTFLOW_BUILD_DIR=${CMAKE_BINARY_DIR}" "EVENTFLOW_SKIP_BUILD=1"
      bash "${CMAKE_CURRENT_SOURCE_DIR}/examples/static_streaming/run.sh")
  set_tests_properties(static_streaming_example PROPERTIES TIMEOUT 180
    ENVIRONMENT "ASAN_OPTIONS=detect_leaks=1:halt_on_error=1;UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1;LSAN_OPTIONS=exitcode=23")
  add_library(GTest::gtest STATIC IMPORTED)
  set_target_properties(GTest::gtest PROPERTIES IMPORTED_LOCATION "${EF_THIRD}/lib/libgtest.a" INTERFACE_LINK_LIBRARIES Threads::Threads INTERFACE_INCLUDE_DIRECTORIES "${EF_THIRD}/include")
  add_library(GTest::gtest_main STATIC IMPORTED)
  set_target_properties(GTest::gtest_main PROPERTIES IMPORTED_LOCATION "${EF_THIRD}/lib/libgtest_main.a" INTERFACE_LINK_LIBRARIES GTest::gtest)
  foreach(pair
      "types_test|ef_core"
      "store_contract_test|ef_transport"
      "store_limits_test|ef_transport"
      "store_snapshot_test|ef_transport"
      "cached_store_test|ef_cache"
      "remote_store_test|ef_transport"
      "exact_sum_test|ef_exact_sum"
      "time_test|ef_time"
      "fsm_test|ef_fsm"
      "cep_test|ef_cep"
      "message_source_test|ef_message_source"
      "source_poller_test|ef_source_poller"
      "http_message_source_test|ef_message_source"
      "sql_parser_test|ef_sql"
      "catalog_test|ef_sql"
      "catalog_codec_test|ef_catalog_codec"
      "trigger_audit_catalog_test|ef_catalog_codec"
      "trigger_audit_test|ef_computation"
      "trigger_audit_request_test|ef_computation"
      "trigger_audit_engine_test|ef_engine"
      "catalog_observation_test|ef_catalog_codec"
      "observation_credit_test|ef_observation_credit"
      "observation_program_test|ef_observation_program"
      "request_observation_test|ef_request_runtime"
      "compute_service_test|ef_compute_service"
      "coordinator_service_test|ef_coordinator_service"
      "sql_module_service_test|ef_coordinator_service"
      "core_cluster_test|ef_cluster_client"
      "cluster_client_test|ef_cluster_client"
      "trigger_expression_test|ef_trigger_expression"
      "trigger_pattern_test|ef_trigger_pattern"
      "typed_schema_test|ef_trigger_expression"
      "static_function_test|ef_trigger_expression"
      "typed_trigger_test|ef_trigger_pattern"
      "typed_input_domain_test|ef_event_processor"
      "computation_test|ef_computation"
      "relation_envelope_scope_test|ef_computation"
      "trigger_test|ef_computation"
      "function_catalog_test|ef_sql"
      "latest_catalog_test|ef_request_machine"
      "function_executor_test|ef_function_executor"
      "request_machine_test|ef_request_machine"
      "request_runtime_test|ef_request_runtime"
      "request_latest_test|ef_request_runtime"
      "engine_runtime_test|ef_engine"
      "engine_lifecycle_test|ef_engine"
      "engine_latest_gate_test|ef_engine"
      "engine_latest_test|ef_engine"
      "expression_test|ef_sql"
      "engine_test|ef_engine"
      "engine_limits_test|ef_engine"
      "engine_observation_test|ef_engine"
      "engine_query_cache_test|ef_engine"
      "engine_cache_test|ef_engine"
      "engine_contract_test|ef_engine"
      "engine_review_test|ef_engine"
      "event_processor_cep_test|ef_event_processor"
      "event_request_test|ef_event_processor"
      "legacy_feature_fixture_test|ef_legacy_feature_fixture"
      "event_processor_test|ef_event_processor")
    string(REPLACE "|" ";" parts "${pair}")
    list(GET parts 0 name)
    list(GET parts 1 lib)
    list(LENGTH parts part_count)
    if(part_count GREATER 2)
      list(GET parts 2 test_source)
    else()
      set(test_source "tests/${name}.cpp")
    endif()
    add_executable(${name} "${test_source}")
    target_link_libraries(${name} PRIVATE ${lib} GTest::gtest_main)
    target_include_directories(${name} PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/tests")
    if(name STREQUAL "core_cluster_test")
      target_compile_definitions(${name} PRIVATE EVENTFLOW_BINARY_PATH="$<TARGET_FILE:eventflow_legacy_test_server>")
    else()
      target_compile_definitions(${name} PRIVATE EVENTFLOW_BINARY_PATH="$<TARGET_FILE:eventflow>")
    endif()
    ef_runtime(${name})
    add_test(NAME ${name} COMMAND ${name})
    set_tests_properties(${name} PROPERTIES TIMEOUT 180
      ENVIRONMENT "ASAN_OPTIONS=detect_leaks=1:halt_on_error=1;UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1;LSAN_OPTIONS=exitcode=23")
  endforeach()
  target_link_libraries(store_limits_test PRIVATE ef_cache)
  target_link_libraries(http_message_source_test PRIVATE ef_http_message_server)
  target_link_libraries(static_function_test PRIVATE ef_function_executor)
  target_link_libraries(engine_contract_test PRIVATE ef_transport)
  target_link_libraries(engine_limits_test PRIVATE ef_transport)
  target_link_libraries(engine_observation_test PRIVATE ef_transport ef_fsm)
  target_link_libraries(engine_review_test PRIVATE ef_transport)
  target_link_libraries(engine_cache_test PRIVATE ef_transport ef_cache)
  target_link_libraries(trigger_audit_engine_test PRIVATE ef_transport)
  target_include_directories(trigger_audit_engine_test PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src/runtime")
  target_link_libraries(engine_runtime_test PRIVATE ef_transport)
  target_link_libraries(request_runtime_test PRIVATE ef_transport)
  target_link_libraries(request_observation_test PRIVATE ef_transport)
  target_link_libraries(coordinator_service_test PRIVATE ef_compute_service ef_cluster_client ef_legacy_feature_fixture)
  target_compile_definitions(coordinator_service_test PRIVATE EVENTFLOW_TEST_UDF_PATH="$<TARGET_FILE:eventflow_news_udf>")
  add_dependencies(coordinator_service_test eventflow_news_udf)
  target_link_libraries(sql_module_service_test PRIVATE ef_compute_service ef_cluster_client)
  target_link_libraries(compute_service_test PRIVATE ef_legacy_feature_fixture)
  add_dependencies(core_cluster_test eventflow_legacy_test_server)
  set_tests_properties(core_cluster_test PROPERTIES TIMEOUT 300)
  # This contract repeatedly binds full computation graphs under three sanitizers.
  set_tests_properties(compute_service_test coordinator_service_test PROPERTIES TIMEOUT 600)
  target_link_libraries(request_latest_test PRIVATE ef_transport)
  target_link_libraries(engine_latest_test PRIVATE ef_transport)
  target_include_directories(engine_runtime_test PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src/runtime")
  target_link_libraries(event_processor_test PRIVATE ef_transport ef_message_source)
  target_link_libraries(event_processor_cep_test PRIVATE ef_transport)
  target_link_libraries(event_request_test PRIVATE ef_transport)
  target_link_libraries(legacy_feature_fixture_test PRIVATE ef_transport)
  set_tests_properties(legacy_feature_fixture_test PROPERTIES TIMEOUT 600)
  target_link_options(store_snapshot_test PRIVATE "LINKER:--wrap=fsync")
  add_dependencies(remote_store_test eventflow)
endif()
