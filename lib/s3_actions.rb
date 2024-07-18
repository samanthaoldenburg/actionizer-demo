class CopyFile
  include Actionizer

  inputs_for(:call) {
    required :key, type: String
    required :src_bucket, type: Aws::S3::Bucket
    required :dest_bucket, type: Aws::S3::Bucket
    optional :allow_overwrite
  }

  def call
    contents = S3Atoms.get!(key: input.key, bucket: input.src_bucket)

    S3Atoms.write!(
      key: input.key,
      value: contents,
      bucket: input.dest_bucket,
      allow_overwrite: input.allow_overwrite
    )

    Digest::SHA256.digest(contents)
  end
end

class CutFile
  include Actionizer

  inputs_for(:call) {
    required :key, type: String
    required :src_bucket, type: Aws::S3::Bucket
    required :dest_bucket, type: Aws::S3::Bucket
    optional :allow_overwrite
  }

  def call
    sha = CopyFile.call!(**input.to_h)

    EnsureFileDeletionWorker.new.perform(
      {
        key: input.key,
        content_hash: sha,
        bucket: input.dest_bucket
      }.stringify_keys
    )
  end
end

class EnsureFileDeletion
  include Actionizer

  inputs_for(:call) {
    required :key, type: String
    required :content_hash, type: String
    required :bucket, type: Aws::S3::Bucket
  }

  def call
    result = S3Atom.delete(**input.to_h)

    return true if result.success?

    fail! result.to_h unless result.error == :does_not_exist

    Logger.info(
      message: "File Already Deleted",
      key: input.key,
      content_hash: input.content_hash
    )

    true
  end
end
