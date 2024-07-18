# Simple S3 Actions - Little to know business logic
class S3Atoms
  inputs_for(:get) {
    required :key, type: String
    required :bucket, type: Aws::S3::Bucket
  }

  def get
    fail_with_context! error: :does_not_exist unless s3_object.exist?

    s3_object.get.body.read
  end

  inputs_for(:write) {
    required :key, type: String
    required :value, type: String
    required :bucket, type: Aws::S3::Bucket
    optional :allow_overwrite
  }

  def write
    if s3_object.exist? && !allow_overwrite?
      fail_with_context! error: :cannot_overwrite_existing_file
    end

    s3_object.put(input.value)
  end

  inputs_for(:delete) {
    required :key, type: String
    required :content_hash, type: String
    required :bucket, type: Aws::S3::Bucket
  }

  def delete
    contents = get

    unless input.content_hash == Digest::SHA256.digest(contents)
      fail_with_context! error: :stale_s3_object
    end

    s3_object.delete
  rescue Aws::S3::PermissionError => e
    fail_with_context! error: :s3_permission_error, source: e
  rescue Aws::S3::Timeout => e
    fail_with_context! error: :s3_timeout, source: e
  end

  private

  def s3_object
    @s3_object ||= input.bucket.object(input.key)
  end

  def allow_overwrite?
    return @allow_overwrite if defined? @allow_overwrite

    @allow_overwrite = input.allow_overwrite || false
  end

  def fail_with_context!(**args)
    fail! key: input.key, bucket_name: input.bucket.name, **args
  end
end
