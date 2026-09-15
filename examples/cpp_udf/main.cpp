#include "eventflow/udf.hpp"
#include <chrono>
#include <iostream>

namespace eventflow::udf_example {
namespace {

Status run_example(const char* path, bool threaded) noexcept {
  EF_ASSIGN_OR_RETURN(auto registry, FunctionRegistry::create());
  EF_ASSIGN_OR_RETURN(auto bytes, read_udf_file(path));
  EF_ASSIGN_OR_RETURN(auto library, UdfLibrary::open(bytes));
  EF_RETURN_IF_ERROR(register_udf(*registry, library));
  const std::weak_ptr<UdfLibrary> lifetime = library;
  library.reset();
  EF_RETURN_IF_ERROR(check(!lifetime.expired(), "registry must retain the loaded library"));

  std::unique_ptr<IFunctionExecutor> executor;
  SimpleFunctionExecutor* simple = nullptr;
  if (threaded) {
    EF_ASSIGN_OR_RETURN(auto created, ThreadPoolFunctionExecutor::create(registry));
    executor = std::move(created);
  } else {
    EF_ASSIGN_OR_RETURN(auto created, SimpleFunctionExecutor::create(registry));
    simple = created.get();
    executor = std::move(created);
  }

  // This is a local executor task identity, not a durable database request ID.
  FunctionTask task{.task_id = "news-impact:ACME:valid",
                    .function_id = "news_impact",
                    .function_version = 1,
                    .implementation_identity = "news-impact.mean.v1:sha256:" + udf_digest(bytes),
                    .inputs = {Row{std::string("ACME"), std::int64_t{2}, 103.0}},
                    .ready_at_us = 0};
  auto invalid = task;
  invalid.task_id = "news-impact:ACME:wrong-type";
  invalid.inputs = {Row{std::string("ACME"), 2.0, 103.0}};
  EF_RETURN_IF_ERROR(check(executor->submit(invalid).code() == ErrorCode::InvalidArgument,
                           "executor must reject an incorrect input type"));
  invalid = task;
  invalid.function_version = 99;
  EF_RETURN_IF_ERROR(check(executor->submit(invalid).code() == ErrorCode::NotFound,
                           "executor must reject an unknown function version"));

  EF_RETURN_IF_ERROR(executor->submit(task));
  // submit owns a copy; later caller changes must not affect this computation.
  task.inputs = {Row{std::string("ACME"), std::int64_t{2}, 999.0}};
  invalid = task;
  invalid.task_id = "news-impact:ACME:zero-count";
  invalid.inputs = {Row{std::string("ACME"), std::int64_t{0}, 103.0}};
  EF_RETURN_IF_ERROR(executor->submit(invalid));
  if (simple) {
    EF_ASSIGN_OR_RETURN(auto executed, simple->run_ready(0));
    EF_RETURN_IF_ERROR(check(executed == 2, "expected two UDF calls"));
  }

  bool success_seen = false;
  bool error_seen = false;
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while ((!success_seen || !error_seen) && std::chrono::steady_clock::now() < deadline) {
    EF_ASSIGN_OR_RETURN(auto batch, executor->poll(2, threaded ? 20 : 0));
    for (const auto& completion : batch) {
      if (completion.task_id == task.task_id) {
        EF_RETURN_IF_ERROR(completion.status);
        EF_RETURN_IF_ERROR(
            check(!success_seen && completion.output == Row{std::string("ACME"), 51.5},
                  "expected frozen input result ACME 51.5"));
        success_seen = true;
      } else if (completion.task_id == invalid.task_id) {
        EF_RETURN_IF_ERROR(
            check(!error_seen && completion.status.code() == ErrorCode::InvalidArgument,
                  "expected the UDF positive-count validation error"));
        error_seen = true;
      } else {
        return Status(ErrorCode::Internal, "unexpected function completion");
      }
    }
  }
  EF_RETURN_IF_ERROR(check(success_seen && error_seen, "UDF completions did not arrive"));
  EF_RETURN_IF_ERROR(executor->close());
  executor.reset();
  registry.reset();
  EF_RETURN_IF_ERROR(
      check(lifetime.expired(), "library ownership must end after executor cleanup"));
  std::cout << (threaded ? "thread_pool" : "simple")
            << ": ticker=ACME impact_score=51.5; invalid_count=rejected; frozen_input=verified\n";
  return {};
}

} // namespace
} // namespace eventflow::udf_example

int main(int argc, char** argv) noexcept {
  if (argc != 2) {
    std::cerr << "Usage: eventflow_cpp_udf_demo /absolute/path/to/libeventflow_news_udf.so\n";
    return 2;
  }
  for (bool threaded : {false, true}) {
    const auto status = eventflow::udf_example::run_example(argv[1], threaded);
    if (!status.ok()) {
      std::cerr << "UDF example failed: " << status.message() << '\n';
      return 1;
    }
  }
  std::cout << "C++ UDF example passed.\n";
  return 0;
}
