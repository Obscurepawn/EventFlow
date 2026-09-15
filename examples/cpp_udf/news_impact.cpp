#include "eventflow/udf.hpp"
#include "eventflow/udf_build_identity.hpp"
#include <cmath>

namespace eventflow::udf_example {
namespace {

StatusOr<Row> calculate_news_impact(const FunctionInputs& inputs) noexcept {
  if (inputs.size() != 1 || !inputs[0] || inputs[0]->size() != 3)
    return Status(ErrorCode::InvalidArgument, "news impact requires one three-column input");

  const auto& row = *inputs[0];
  const auto* ticker = std::get_if<std::string>(&row[0]);
  const auto* count = std::get_if<std::int64_t>(&row[1]);
  const auto* weighted_sum = std::get_if<double>(&row[2]);
  if (!ticker || !count || !weighted_sum || ticker->empty() || *count <= 0 ||
      !std::isfinite(*weighted_sum))
    return Status(ErrorCode::InvalidArgument,
                  "news impact requires valid fields and positive count");

  const double score = *weighted_sum / static_cast<double>(*count);
  if (!std::isfinite(score))
    return Status(ErrorCode::InvalidArgument, "news impact is not finite");

  // The returned Row owns its string and numeric values; no borrowed plugin pointers escape.
  return Row{*ticker, score};
}

} // namespace
} // namespace eventflow::udf_example

extern "C" const char* eventflow_udf_build_id() noexcept {
  return eventflow::udf_detail::build_id;
}

extern "C" const eventflow::UdfDefinition* eventflow_udf_entry() noexcept {
  using namespace eventflow;
  using namespace eventflow::udf_example;
  static const UdfDefinition definition{
      .signature = {.function_id = "news_impact",
                    .version = 1,
                    .implementation_identity = "news-impact.mean.v1",
                    .inputs = {{.alias = "news_features",
                                .required = true,
                                .fields = {{.id = 1, .name = "ticker", .type = DataType::String},
                                           {.id = 2, .name = "news_count", .type = DataType::Int64},
                                           {.id = 3,
                                            .name = "sentiment_price_sum",
                                            .type = DataType::Float64}}}},
                    .output_fields = {{.id = 1, .name = "scored_ticker", .type = DataType::String},
                                      {.id = 2, .name = "impact_score", .type = DataType::Float64}},
                    .pure = true},
      .invoke = &calculate_news_impact};
  return &definition;
}
