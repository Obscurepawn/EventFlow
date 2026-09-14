#include "eventflow/source_poller.hpp"
#include <limits>

namespace eventflow {
namespace {

bool retryable(const Status& status) noexcept {
  return status.code() == ErrorCode::Unavailable || status.code() == ErrorCode::UnknownCommit ||
         status.code() == ErrorCode::ResourceExhausted;
}

} // namespace

StatusOr<std::unique_ptr<SourcePoller>>
SourcePoller::open(std::shared_ptr<IMessageSource> source, SourceApply apply,
                   const SourcePollerConfig& config) noexcept {
  if (!source || !apply || config.max_message_bytes == 0)
    return Status(ErrorCode::InvalidArgument, "invalid source poller configuration");
  auto result = std::unique_ptr<SourcePoller>(new SourcePoller);
  result->source_ = std::move(source);
  result->apply_ = std::move(apply);
  result->config_ = config;
  return result;
}

SourcePoller::~SourcePoller() noexcept {
  const auto ignored = close();
  (void)ignored;
}

StatusOr<SourcePollerProgress> SourcePoller::step(std::int64_t now_us) noexcept {
  if (closed_ || now_us < 0)
    return Status(ErrorCode::FailedPrecondition, "source poller is closed or time is invalid");
  if (progress_.failed || (progress_.paused && !progress_.input_committed))
    return SourcePollerProgress(progress_);
  progress_.operation_status = {};
  if (!pending_) {
    if (progress_.polled == std::numeric_limits<std::uint64_t>::max())
      return Status(ErrorCode::ResourceExhausted, "source poll counter is exhausted");
    auto batch = source_->poll(1, config_.max_message_bytes, 0);
    if (!batch.ok()) {
      progress_.operation_status = batch.status();
      progress_.failed = !retryable(batch.status());
      return SourcePollerProgress(progress_);
    }
    if (batch->empty())
      return SourcePollerProgress(progress_);
    if (batch->size() != 1)
      return Status(ErrorCode::Internal, "source exceeded the requested delivery count");
    pending_ = std::move(batch->front());
    pending_time_us_ = now_us;
    progress_.pending_time_us = now_us;
    progress_.pending = true;
    ++progress_.polled;
  }
  if (!progress_.input_committed) {
    auto applied = apply_(*pending_, pending_time_us_);
    if (!applied.ok()) {
      progress_.operation_status = applied.status();
      progress_.failed = !retryable(applied.status());
      return SourcePollerProgress(progress_);
    }
    progress_.input_committed = applied->input_committed;
    progress_.operation_status = applied->operation_status;
    if (!progress_.input_committed) {
      if (progress_.operation_status.ok())
        progress_.operation_status =
            Status(ErrorCode::ResourceExhausted, "source input awaits durable confirmation");
      progress_.failed = !retryable(progress_.operation_status);
      return SourcePollerProgress(progress_);
    }
  }
  // Later function backpressure cannot revoke an already confirmed source input.
  // An uncertain ACK retries this token without applying the database input again.
  const auto acknowledged = source_->ack(pending_->token);
  if (!acknowledged.ok()) {
    progress_.operation_status = acknowledged;
    progress_.failed = !retryable(acknowledged);
    return SourcePollerProgress(progress_);
  }
  ++progress_.acknowledged;
  pending_.reset();
  progress_.pending = false;
  progress_.input_committed = false;
  progress_.pending_time_us.reset();
  return SourcePollerProgress(progress_);
}

void SourcePoller::pause() noexcept {
  progress_.paused = true;
}

void SourcePoller::resume() noexcept {
  progress_.paused = false;
}

const SourcePollerProgress& SourcePoller::progress() const noexcept {
  return progress_;
}

Status SourcePoller::close() noexcept {
  if (closed_)
    return {};
  closed_ = true;
  return source_->close();
}

} // namespace eventflow
