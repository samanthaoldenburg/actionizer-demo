class EnsureFileDeletionWorker
  include Sidekiq::Worker

  class DeletionFailure < StandardError
    attr_reader :context

    def initialize(message, context: {})
      @context = context
      super(message)
    end
  end

  # We'll be manually retrying
  sidekiq_options retry: false

  def perform(args)
    result = EnsureFileDeletion.call(
      key: args["key"],
      content_hash: args["content_hash"],
      bucket: args["bucket"]
    )

    return Log.info("File is deleted") if result.success?

    attempt_retry!(result, args) if result.error == :timeout

    raise_deletion_failure!(result)
  end

  private

  def attempt_retry!(failure, args)
    args["retry_count"] ||= 0

    raise_deletion_failure!(failure) if retry_count >= 5

    Logger.warn(
      message: "Deletion failed, retrying",
      retry_count: retry_count,
      error: failure
    )

    args["retry_count"] += 1

    self.class.perform_async(args)
  end

  def raise_deletion_failure!(failure)
    context = failure.to_h
    error = context.delete(:error)

    raise DeletionFailure.new(
      "Unable to delete object - #{error}",
      context: context
    )
  end
end
