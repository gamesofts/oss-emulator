require 'builder'
require 'securerandom'
require 'digest'
require 'yaml'
require 'find'
require 'fileutils'
require 'time'
require "rexml/document"  
require 'emulator/config'
require 'emulator/util'
require 'emulator/response'
include REXML
include Comparable

module OssEmulator
  module Multipart

    def self.multipart_uploads_root(bucket, object)
      File.join(Config.store, bucket, object, Store::MULTIPART_UPLOAD_DIR)
    end

    def self.multipart_upload_dir(bucket, object, upload_id)
      File.join(multipart_uploads_root(bucket, object), upload_id.to_s)
    end

    def self.multipart_metadata_filename(bucket, object, upload_id)
      File.join(multipart_upload_dir(bucket, object, upload_id), Store::MULTIPART_UPLOAD_METADATA)
    end
    
    # InitiateMultipartUpload
    def self.initiate_multipart_upload(bucket, object, request, response)
      # NoSuchBucket
      return if OssResponse.response_no_such_bucket(response, bucket)

      upload_id = SecureRandom.hex
      upload_dir = multipart_upload_dir(bucket, object, upload_id)
      FileUtils.mkdir_p(upload_dir)
      content_type = request.header["content-type"]&.first
      metadata = {
        bucket: bucket,
        object: object,
        upload_id: upload_id,
        initiated: Time.now.utc.iso8601(HttpMsg::SUBSECOND_PRECISION),
        storage_class: "Standard"
      }
      metadata[:content_type] = content_type if content_type && !content_type.empty?
      File.open(File.join(upload_dir, Store::MULTIPART_UPLOAD_METADATA), 'w') do |file|
        file << YAML.dump(metadata)
      end
      dataset = {
        cmd: Request::POST_INIT_MULTIPART_UPLOAD, 
        bucket: bucket, 
        object: object, 
        upload_id: upload_id
      }

      OssResponse.response_ok(response, dataset)
    end

    # UploadPart
    def self.upload_part(req, query, request, response) 
      part_number = query['partNumber']&.first
      upload_id = query['uploadId']&.first
      if part_number.nil? || upload_id.nil?
        OssResponse.response_error(response, ErrorCode::BAD_REQUEST)
        return
      end

      # NoSuchBucket
      return if OssResponse.response_no_such_bucket(response, req.bucket)

      # InvalidObjectName
      return if OssResponse.response_invalid_object_name(response, req.object)

      upload_dir = multipart_upload_dir(req.bucket, req.object, upload_id)
      unless File.exist?(upload_dir)
        dataset = { bucket: req.bucket, object: req.object }.merge(ErrorCode::NO_SUCH_UPLOAD)
        OssResponse.response_error(response, dataset)
        return
      end
      metadata_file = multipart_metadata_filename(req.bucket, req.object, upload_id)
      if File.exist?(metadata_file)
        metadata = File.open(metadata_file) { |file| YAML.load(file) }
        if !metadata.key?(:content_type)
          content_type = request.header["content-type"]&.first
          if content_type && !content_type.empty?
            metadata[:content_type] = content_type
            File.open(metadata_file, 'w') { |file| file << YAML.dump(metadata) }
          end
        end
      end

      part_filename = File.join(upload_dir, "#{Store::OBJECT_CONTENT_PREFIX}#{part_number}")
      check_chunked_filesize = false
      if request.header.include?('content-length')
        content_length = request.header['content-length'].first.to_i
        if content_length > Object::MAX_OBJECT_FILE_SIZE
          OssResponse.response_error(response, ErrorCode::INVALID_ARGUMENT)
          return
        end
      else
        if request.header.include?('transfer-encoding')
          if request.header['transfer-encoding'].first != 'chunked'
            OssResponse.response_error(response, ErrorCode::MISSING_CONTENT_LENGTH)
            return
          end
          check_chunked_filesize = true
        else
          OssResponse.response_error(response, ErrorCode::MISSING_CONTENT_LENGTH)
          return
        end
      end

      FileUtils.mkdir_p(upload_dir) unless File.exist?(upload_dir)
      File.open(part_filename, 'wb') do |file|
        total_size = 0
        body = request.body
        if body.respond_to?(:read)
          while (chunk = body.read(Object::STREAM_CHUNK_SIZE))
            file.syswrite(chunk)
            total_size += chunk.bytesize
            if check_chunked_filesize && total_size > Object::MAX_OBJECT_FILE_SIZE
              FileUtils.rm_f(part_filename)
              OssResponse.response_error(response, ErrorCode::INVALID_ARGUMENT)
              return
            end
          end
        else
          chunk = body.to_s
          file.syswrite(chunk)
          total_size = chunk.bytesize
          if check_chunked_filesize && total_size > Object::MAX_OBJECT_FILE_SIZE
            FileUtils.rm_f(part_filename)
            OssResponse.response_error(response, ErrorCode::INVALID_ARGUMENT)
            return
          end
        end
      end

      dataset = {
        cmd: Request::PUT_UPLOAD_PART,
        md5: Digest::MD5.file(part_filename).hexdigest
      }
      OssResponse.response_ok(response, dataset)
    end

    # UploadPartCopy
    def self.upload_part_copy(req, query, request, response)
      part_number = query['partNumber']&.first
      upload_id = query['uploadId']&.first
      if part_number.nil? || upload_id.nil?
        OssResponse.response_error(response, ErrorCode::BAD_REQUEST)
        return
      end

      return if OssResponse.response_no_such_bucket(response, req.bucket)
      return if OssResponse.response_invalid_object_name(response, req.object)

      upload_dir = multipart_upload_dir(req.bucket, req.object, upload_id)
      unless File.exist?(upload_dir)
        dataset = { bucket: req.bucket, object: req.object }.merge(ErrorCode::NO_SUCH_UPLOAD)
        OssResponse.response_error(response, dataset)
        return
      end

      src_bucket = req.src_bucket
      src_object = req.src_object
      return if OssResponse.response_no_such_object(response, src_bucket, src_object)

      src_object_dir = File.join(Config.store, src_bucket, src_object)
      src_content_filename = File.join(src_object_dir, Store::OBJECT_CONTENT)
      unless File.exist?(src_content_filename)
        OssResponse.response_error(response, ErrorCode::NOT_FOUND)
        return
      end

      start_pos = 0
      end_pos = File.size(src_content_filename) - 1
      range = request.header['x-oss-copy-source-range']&.first
      if range && range =~ /bytes=(\d+)-(\d+)/
        start_pos = $1.to_i
        end_pos = $2.to_i
      end

      part_filename = File.join(upload_dir, "#{Store::OBJECT_CONTENT_PREFIX}#{part_number}")
      File.open(part_filename, 'wb') do |file|
        File.open(src_content_filename, 'rb') do |src|
          src.seek(start_pos)
          remaining = end_pos - start_pos + 1
          while remaining > 0
            chunk = src.read([Object::STREAM_CHUNK_SIZE, remaining].min)
            break unless chunk
            file.write(chunk)
            remaining -= chunk.bytesize
          end
        end
      end

      dataset = { cmd: Request::PUT_UPLOAD_PART_COPY, md5: Digest::MD5.file(part_filename).hexdigest }
      OssResponse.response_ok(response, dataset)
    end

    # CompleteMultipartUpload
    def self.complete_multipart_upload(req, request, response)
      upload_id = req.query_parser['uploadId']&.first
      if upload_id.nil?
        OssResponse.response_error(response, ErrorCode::BAD_REQUEST)
        return
      end

      upload_dir = multipart_upload_dir(req.bucket, req.object, upload_id)
      unless File.exist?(upload_dir)
        dataset = { bucket: req.bucket, object: req.object }.merge(ErrorCode::NO_SUCH_UPLOAD)
        OssResponse.response_error(response, dataset)
        return
      end

      parts = []
      xml = Document.new(request.body)
      xml.elements.each("*/Part") do |e| 
        part = {}
        part[:number] = e.elements["PartNumber"].text
        etag = e.elements["ETag"].text
        part[:etag] = (etag.include?("&#34;")) ? etag[/&#34;(.+)&#34;/, 1] : etag
        parts << part
      end
      
      object_dir = File.join(Config.store, req.bucket, req.object)
      complete_file_size = 0
      part_files = []
      parts.each do |part|
        part_filename = File.join(upload_dir, "#{Store::OBJECT_CONTENT_PREFIX}#{part[:number]}")
        unless File.exist?(part_filename)
          OssResponse.response_error(response, ErrorCode::FILE_PART_NO_EXIST)
          return
        end
        complete_file_size += File.size(part_filename)
        part_files << { number: part[:number], filename: part_filename }
      end

      if File.exist?(object_dir)
        Dir[File.join(object_dir, "#{Store::OBJECT_CONTENT_PREFIX}*")].each do |filename|
          next if filename.include?(Store::MULTIPART_UPLOAD_DIR)
          FileUtils.rm_f(filename)
        end
        FileUtils.rm_f(File.join(object_dir, Store::OBJECT_METADATA))
      else
        FileUtils.mkdir_p(object_dir)
      end
      ordered_parts = part_files.sort_by { |part| part[:number].to_i }
      final_content_filename = File.join(object_dir, Store::OBJECT_CONTENT)
      md5 = Digest::MD5.new
      File.open(final_content_filename, 'wb') do |merged|
        ordered_parts.each do |part|
          File.open(part[:filename], 'rb') do |src|
            while (chunk = src.read(Object::STREAM_CHUNK_SIZE))
              merged.write(chunk)
              md5.update(chunk)
            end
          end
        end
      end

      part_size = ordered_parts.empty? ? 0 : File.size(ordered_parts.first[:filename])
      crc64 = OssUtil.crc64_ecma_file(final_content_filename)
      multipart_metadata = {}
      metadata_file = multipart_metadata_filename(req.bucket, req.object, upload_id)
      if File.exist?(metadata_file)
        multipart_metadata = File.open(metadata_file) { |file| YAML.load(file) } || {}
      end
      options = { size: complete_file_size, part_size: part_size, md5: md5.hexdigest, crc64: crc64 }
      if multipart_metadata[:content_type] && !multipart_metadata[:content_type].to_s.empty?
        options[:content_type] = multipart_metadata[:content_type]
      end
      dataset = OssUtil.put_object_metadata(req.bucket, req.object, request, options)

      dataset[:cmd] = Request::POST_COMPLETE_MULTIPART_UPLOAD
      dataset[:object] = req.object
      FileUtils.rm_rf(upload_dir)
      uploads_root = multipart_uploads_root(req.bucket, req.object)
      if File.exist?(uploads_root) && Dir.glob(File.join(uploads_root, '*')).empty?
        FileUtils.rm_rf(uploads_root)
      end
      OssResponse.response_ok(response, dataset)
    end #function

    # ListMultipartUploads
    def self.list_multipart_uploads(bucket, req, response)
      return if OssResponse.response_no_such_bucket(response, bucket)

      prefix = req.query["prefix"] ? req.query["prefix"].to_s : nil
      delimiter = req.query["delimiter"] ? req.query["delimiter"].to_s : nil
      key_marker = req.query["key-marker"] ? req.query["key-marker"].to_s : ""
      upload_id_marker = req.query["upload-id-marker"] ? req.query["upload-id-marker"].to_s : ""
      max_uploads = req.query["max-uploads"] ? req.query["max-uploads"].to_i : 1000
      max_uploads = max_uploads > 1000 ? 1000 : max_uploads

      uploads = []
      Find.find(File.join(Config.store, bucket)) do |filename|
        next unless File.basename(filename) == Store::MULTIPART_UPLOAD_METADATA
        metadata = File.open(filename) { |file| YAML::load(file) }
        next if prefix && !metadata[:object].start_with?(prefix)
        uploads << metadata
      end

      uploads.sort_by! { |item| [item[:object], item[:upload_id]] }

      filtered_uploads = []
      is_truncated = false
      common_prefixes = []
      uploads.each do |upload|
        next if upload[:object] < key_marker
        if upload[:object] == key_marker && upload[:upload_id] <= upload_id_marker
          next
        end

        if delimiter
          delimiter_index = upload[:object].index(delimiter, prefix ? prefix.length : 0)
          if delimiter_index
            common_prefix = upload[:object][0..delimiter_index]
            common_prefixes << common_prefix unless common_prefixes.include?(common_prefix)
            next
          end
        end

        filtered_uploads << upload
        if filtered_uploads.length >= max_uploads
          is_truncated = true
          break
        end
      end

      next_key_marker = ""
      next_upload_id_marker = ""
      if is_truncated
        last = filtered_uploads.last
        next_key_marker = last[:object]
        next_upload_id_marker = last[:upload_id]
      end

      dataset = {
        cmd: Request::GET_LIST_MULTIPART_UPLOADS,
        bucket: bucket,
        prefix: prefix,
        delimiter: delimiter,
        key_marker: key_marker,
        upload_id_marker: upload_id_marker,
        next_key_marker: next_key_marker,
        next_upload_id_marker: next_upload_id_marker,
        max_uploads: max_uploads,
        is_truncated: is_truncated,
        uploads: filtered_uploads,
        common_prefixes: common_prefixes
      }
      OssResponse.response_ok(response, dataset)
    end

    # ListParts
    def self.list_parts(req, response)
      upload_id = req.query["uploadId"]&.first
      if upload_id.nil?
        OssResponse.response_error(response, ErrorCode::BAD_REQUEST)
        return
      end
      return if OssResponse.response_no_such_bucket(response, req.bucket)
      return if OssResponse.response_invalid_object_name(response, req.object)

      upload_dir = multipart_upload_dir(req.bucket, req.object, upload_id)
      unless File.exist?(upload_dir)
        dataset = { bucket: req.bucket, object: req.object }.merge(ErrorCode::NO_SUCH_UPLOAD)
        OssResponse.response_error(response, dataset)
        return
      end

      part_number_marker = req.query["part-number-marker"] ? req.query["part-number-marker"].to_i : 0
      max_parts = req.query["max-parts"] ? req.query["max-parts"].to_i : 1000
      max_parts = max_parts > 1000 ? 1000 : max_parts

      parts = Dir[File.join(upload_dir, "#{Store::OBJECT_CONTENT_PREFIX}*")].map do |filename|
        number = File.basename(filename).sub(Store::OBJECT_CONTENT_PREFIX, '').to_i
        next if number <= part_number_marker
        {
          number: number,
          filename: filename,
          size: File.size(filename),
          etag: Digest::MD5.file(filename).hexdigest,
          modified_date: File.mtime(filename).utc.iso8601(HttpMsg::SUBSECOND_PRECISION)
        }
      end.compact

      parts.sort_by! { |part| part[:number] }
      is_truncated = parts.length > max_parts
      parts = parts.first(max_parts)
      next_part_number_marker = is_truncated ? parts.last[:number] : 0

      dataset = {
        cmd: Request::GET_LIST_PARTS,
        bucket: req.bucket,
        object: req.object,
        upload_id: upload_id,
        part_number_marker: part_number_marker,
        next_part_number_marker: next_part_number_marker,
        max_parts: max_parts,
        is_truncated: is_truncated,
        parts: parts
      }
      OssResponse.response_ok(response, dataset)
    end

    # AbortMultipartUpload
    def self.abort_multipart_upload(req, response)
      upload_id = req.query["uploadId"]&.first
      if upload_id.nil?
        OssResponse.response_error(response, ErrorCode::BAD_REQUEST)
        return
      end
      return if OssResponse.response_no_such_bucket(response, req.bucket)

      upload_dir = multipart_upload_dir(req.bucket, req.object, upload_id)
      unless File.exist?(upload_dir)
        dataset = { bucket: req.bucket, object: req.object }.merge(ErrorCode::NO_SUCH_UPLOAD)
        OssResponse.response_error(response, dataset)
        return
      end

      FileUtils.rm_rf(upload_dir)
      dataset = { cmd: Request::DELETE_ABORT_MULTIPART_UPLOAD }
      OssResponse.response_ok(response, dataset)
    end

  end # class
end # module
