require 'builder'
require "rexml/document"  
require 'digest'
require 'webrick'
require 'emulator/config'
require 'emulator/util'
require 'emulator/response'
include REXML
include Comparable

module OssEmulator
  module Object

    # PutObject
    def self.put_object(bucket, object, request, response, part_number=nil)
      # NoSuchBucket
      return if OssResponse.response_no_such_bucket(response, bucket)

      # InvalidObjectName
      return if OssResponse.response_invalid_object_name(response, object)

      check_chunked_filesize = false
      if request.header.include?('content-length')
        content_length = request.header['content-length'].first.to_i
        Log.debug("put_object : content_length=#{content_length}", 'blue')

        # InvalidArgument : Filesize <= 5G
        if content_length>Object::MAX_OBJECT_FILE_SIZE
          OssResponse.response_error(response, ErrorCode::INVALID_ARGUMENT)
          return  
        end
      else
        if request.header.include?('transfer-encoding')
          if request.header['transfer-encoding'].first!='chunked'
            # MissingContentLength
            OssResponse.response_error(response, ErrorCode::MISSING_CONTENT_LENGTH)
            return
          end
          check_chunked_filesize = true
          Log.debug("put_object : check_chunked_filesize=#{check_chunked_filesize}", 'blue')
        else
          # MissingContentLength
          OssResponse.response_error(response, ErrorCode::MISSING_CONTENT_LENGTH)
          return
        end
      end

      obj_dir = File.join(Config.store, bucket, object)
      if part_number
        object_content_filename = File.join(obj_dir, "#{Store::OBJECT_CONTENT_PREFIX}#{part_number}")
      else
        OssUtil.delete_object_file_and_dir(bucket, object)
        object_content_filename = File.join(obj_dir, Store::OBJECT_CONTENT)
      end
      FileUtils.mkdir_p(obj_dir) unless File.exist?(obj_dir)
      f_object_content = File.new(object_content_filename, 'a')  
      f_object_content.binmode

      content_type = request.content_type || ""
      match = content_type.match(/^multipart\/form-data; boundary=(.+)/)
      boundary = match[1] if match
      if boundary
        boundary = WEBrick::HTTPUtils::dequote(boundary)
        form_data = WEBrick::HTTPUtils::parse_form_data(request.body, boundary)

        if form_data['file'] == nil || form_data['file'] == ""
          OssResponse.response_error(response, ErrorCode::BAD_REQUEST)
          return
        end

        f_object_content.syswrite(form_data['file'])
      else
        total_size = 0
        body = request.body
        if body.respond_to?(:read)
          while (chunk = body.read(Object::STREAM_CHUNK_SIZE))
            f_object_content.syswrite(chunk)
            total_size += chunk.bytesize
            if check_chunked_filesize && total_size>Object::MAX_OBJECT_FILE_SIZE
              OssUtil.delete_object_file_and_dir(bucket, object)
              OssResponse.response_error(response, ErrorCode::INVALID_ARGUMENT)
              return
            end
          end
        else
          chunk = body.to_s
          f_object_content.syswrite(chunk)
          total_size = chunk.bytesize
          if check_chunked_filesize && total_size>Object::MAX_OBJECT_FILE_SIZE
            OssUtil.delete_object_file_and_dir(bucket, object)
            OssResponse.response_error(response, ErrorCode::INVALID_ARGUMENT)
            return
          end
        end
      end
      f_object_content.close()

      dataset = {}
      # put object metadata if not multipart upload
      dataset = OssUtil.put_object_metadata(bucket, object, request) unless part_number

      dataset[:cmd] = Request::PUT_OBJECT
      OssResponse.response_ok(response, dataset)
    end

    # CopyObject
    def self.copy_object(src_bucket, src_object, dst_bucket, dst_object, request, response)
      src_object_dir = File.join(Config.store, src_bucket, src_object)
      src_metadata_filename = File.join(src_object_dir, Store::OBJECT_METADATA)

      dst_object_dir = File.join(Config.store, dst_bucket, dst_object)
      dst_metadata_filename = File.join(dst_object_dir, Store::OBJECT_METADATA)

      # NoSuchBucket : SrcBucket
      return if OssResponse.response_no_such_bucket(response, src_bucket)

      # NoSuchObject : SrcObject
      return if OssResponse.response_no_such_object(response, src_bucket, src_object)

      # Only update metadata if the src_object is the same as the dst_object
      if src_bucket==dst_bucket && src_object==dst_object
        metadata = OssUtil.put_object_metadata(dst_bucket, dst_object, request)
        metadata[:cmd] = Request::PUT_COPY_OBJECT
        OssResponse.response_ok(response, metadata)
        return
      end

      # Create New Bucket if the dst_bucket not exist
      dst_bucket_metadata_file = File.join(Config.store, dst_bucket, Store::BUCKET_METADATA)
      if !File.exist?(dst_bucket_metadata_file)
        dst_bucket_dir = File.join(Config.store, dst_bucket)
        FileUtils.mkdir_p(dst_bucket_dir)

        src_bucket_metadata_file = File.join(Config.store, src_bucket, Store::BUCKET_METADATA)
        metadata = File.open(src_bucket_metadata_file) { |file| YAML::load(file) }
        metadata[:bucket] = dst_bucket
        metadata[:creation_date] = Time.now.utc.iso8601(HttpMsg::SUBSECOND_PRECISION)
        File.open(dst_bucket_metadata_file,'w') do |f|
          f << YAML::dump(metadata)
        end
      end

      # Create dst_object folder if not exists.
      if !File.exist?(dst_object_dir)
        FileUtils.mkdir_p(dst_object_dir)
      end

      # Copy file content
      src_content_filename_base = File.join(src_object_dir, Store::OBJECT_CONTENT_PREFIX)
      dst_content_filename_base = File.join(dst_object_dir, Store::OBJECT_CONTENT_PREFIX)
      file_number = 1
      loop do
        current_src_filename = "#{src_content_filename_base}#{file_number}"
        break unless File.exist?(current_src_filename)
        current_dst_filename = "#{dst_content_filename_base}#{file_number}"
        File.open(current_dst_filename, 'wb') do |f|
          File.open(current_src_filename, 'rb') do |input|
            f << input.read
          end
        end
        file_number += 1
      end

      # Copy or Replace metadata
      metadata = {}
      metadata_directive = request.header["x-oss-metadata-directive"]&.first
      if metadata_directive == "REPLACE"
        metadata = OssUtil.put_object_metadata(dst_bucket, dst_object, request)
      else
        File.open(dst_metadata_filename, 'w') do |f|
          File.open(src_metadata_filename, 'r') do |input|
            f << input.read
          end
        end
        metadata = YAML.load(File.open(dst_metadata_filename, 'rb').read)
        metadata[:bucket] = dst_bucket
        metadata[:object] = dst_object
        metadata[:creation_date] = Time.now.utc.iso8601(HttpMsg::SUBSECOND_PRECISION)
        metadata[:modified_date] = Time.now.utc.iso8601(HttpMsg::SUBSECOND_PRECISION)
        File.open(dst_metadata_filename, 'w') do |f|
          f << YAML::dump(metadata)
        end
      end

      metadata[:cmd] = Request::PUT_COPY_OBJECT
      OssResponse.response_ok(response, metadata)
    end

    # PutSymlink
    def self.put_symlink(bucket, object, request, response)
      return if OssResponse.response_no_such_bucket(response, bucket)
      return if OssResponse.response_invalid_object_name(response, object)

      target = request.header['x-oss-symlink-target']&.first
      if target.nil? || target.strip.empty?
        OssResponse.response_error(response, ErrorCode::BAD_REQUEST)
        return
      end

      obj_dir = File.join(Config.store, bucket, object)
      FileUtils.mkdir_p(obj_dir)
      content_filename = File.join(obj_dir, Store::OBJECT_CONTENT)
      File.open(content_filename, 'wb') {}
      options = { size: 0, md5: Digest::MD5.file(content_filename).hexdigest }
      metadata = OssUtil.put_object_metadata(bucket, object, request, options)
      metadata[:symlink] = true
      metadata[:symlink_target] = target
      File.open(File.join(obj_dir, Store::OBJECT_METADATA), 'w') do |file|
        file << YAML::dump(metadata)
      end

      dataset = { cmd: Request::PUT_SYMLINK, md5: metadata[:md5] }
      OssResponse.response_ok(response, dataset)
    end

    # GetSymlink
    def self.get_symlink(bucket, object, response)
      return if OssResponse.response_no_such_object(response, bucket, object)

      object_metadata_filename = File.join(Config.store, bucket, object, Store::OBJECT_METADATA)
      metadata = File.open(object_metadata_filename) { |file| YAML::load(file) }
      target = metadata[:symlink_target]
      if target.nil? || target.to_s.empty?
        OssResponse.response_error(response, ErrorCode::BAD_REQUEST)
        return
      end

      dataset = { cmd: Request::GET_SYMLINK, target: target }
      OssResponse.response_ok(response, dataset)
    end

    # GetObject
    def self.get_object(req, request, response)
      # NoSuchObject
      object_name = req.object
      if OssResponse.response_no_such_object(response, req.bucket, object_name)
        if !object_name.end_with?('/')
          dir_object = "#{object_name}/"
          if !OssResponse.response_no_such_object(response, req.bucket, dir_object)
            object_name = dir_object
          else
            return
          end
        else
          return
        end
      end

      object_multipart_content_tag = File.join(Config.store, req.bucket, object_name, Store::OBJECT_CONTENT_TWO)
      object_metadata_filename = File.join(Config.store, req.bucket, object_name, Store::OBJECT_METADATA)
      metadata = File.open(object_metadata_filename) { |file| YAML::load(file) }
      dataset = {}
      dataset[:cmd] = Request::GET_OBJECT
      dataset[:bucket] = req.bucket
      dataset[:object] = object_name
      dataset[:md5] = metadata[:md5]
      dataset[:crc64] = metadata[:crc64]
      dataset[:multipart] = File.exist?(object_multipart_content_tag) ? true : false 
      dataset[:content_type] = request.query['response-content-type'] || metadata.fetch(:content_type) { "application/octet-stream" }
      dataset[:content_disposition] = request.query['response-content-disposition'] || metadata[:content_disposition]
      dataset[:content_encoding] = metadata[:content_encoding]
      dataset[:size] = metadata.fetch(:size) { 0 }
      dataset[:part_size] = metadata.fetch(:part_size) { 0 }
      dataset[:creation_date] = metadata.fetch(:creation_date) { Time.now.utc.iso8601(HttpMsg::SUBSECOND_PRECISION) }
      dataset[:modified_date] = metadata.fetch(:modified_date) { Time.now.utc.iso8601(HttpMsg::SUBSECOND_PRECISION) }
      dataset[:custom_metadata] = metadata.fetch(:custom_metadata) { {} }

      # Range support
      range = request.header["range"].first
      if range
        Log.debug("get_object : request.header['range'].first=#{request.header['range'].first}", 'yellow')
        content_length = dataset[:size]
        if range =~ /bytes=(\d*)-(\d*)/
          start = $1.to_i
          finish = $2.to_i
          finish_str = ""
          if finish == 0
            finish = content_length - 1
            finish_str = "#{finish}"
          else
            finish_str = finish.to_s
          end

          dataset[:pos] = start
          dataset[:bytes_to_read] = finish - start + 1
          dataset['Content-Range'] = "bytes #{start}-#{finish_str}/#{content_length}"
        end 
      else
        dataset['Content-Length'] = dataset[:size]
      end #if range

      OssResponse.response_get_object_by_chunk(response, dataset)
    end #function

    # RestoreObject
    def self.restore_object(bucket, object, request, response)
      return if OssResponse.response_no_such_object(response, bucket, object)

      object_metadata_filename = File.join(Config.store, bucket, object, Store::OBJECT_METADATA)
      metadata = File.open(object_metadata_filename) { |file| YAML::load(file) }
      metadata[:restore] = true
      metadata[:restore_date] = Time.now.utc.iso8601(HttpMsg::SUBSECOND_PRECISION)
      File.open(object_metadata_filename, 'w') do |file|
        file << YAML::dump(metadata)
      end
      OssResponse.response_ok(response, Request::POST_RESTORE_OBJECT)
    end

    # PostObject
    def self.post_object(bucket, request, response)
      return if OssResponse.response_no_such_bucket(response, bucket)

      content_type = request.content_type || ""
      match = content_type.match(/^multipart\/form-data; boundary=(.+)/)
      boundary = match[1] if match
      boundary = WEBrick::HTTPUtils::dequote(boundary) if boundary
      unless boundary
        OssResponse.response_error(response, ErrorCode::BAD_REQUEST)
        return
      end

      form_data = WEBrick::HTTPUtils::parse_form_data(request.body, boundary)
      key = form_data['key'] || form_data['Key']
      file_data = form_data['file']
      if key.nil? || key.to_s.empty? || file_data.nil?
        OssResponse.response_error(response, ErrorCode::BAD_REQUEST)
        return
      end

      if key.include?('${filename}')
        key = key.gsub('${filename}', form_data['filename'].to_s)
      end

      return if OssResponse.response_invalid_object_name(response, key)

      obj_dir = File.join(Config.store, bucket, key)
      FileUtils.mkdir_p(obj_dir)
      content_filename = File.join(obj_dir, Store::OBJECT_CONTENT)
      File.open(content_filename, 'wb') do |file|
        file.write(file_data)
      end
      options = { size: File.size(content_filename), md5: Digest::MD5.file(content_filename).hexdigest }
      OssUtil.put_object_metadata(bucket, key, request, options)

      dataset = {
        cmd: Request::POST_OBJECT,
        Etag: options[:md5],
        bucket: bucket,
        key: key,
        success_action_redirect: form_data['success_action_redirect'],
        success_action_status: form_data['success_action_status']
      }
      dataset[:Location] = form_data['success_action_redirect'] if form_data['success_action_redirect']
      OssResponse.response_ok(response, dataset)
    end

    # AppendObject
    def self.append_object(bucket, object, request, position, response)
      # NoSuchBucket
      return if OssResponse.response_no_such_bucket(response, bucket)

      obj_dir = File.join(Config.store, bucket, object)
      metadata_filename = File.join(obj_dir, Store::OBJECT_METADATA)

      if File.exist?(metadata_filename)
        dataset = YAML.load(File.open(metadata_filename, 'rb').read)
        if !dataset.include?(:appendable) || (dataset.include?(:appendable) && dataset[:appendable]!=true)
          OssResponse.response_error(response, ErrorCode::OBJECT_NOT_APPENDABLE)
          return  
        end
      end

      FileUtils.mkdir_p(obj_dir)

      content_filename = File.join(obj_dir, Store::OBJECT_CONTENT)
      File.open(content_filename, 'a+')  do |f| 
        f.binmode
        f.pos = (position.to_i==-1) ? File.size(content_filename) : position.to_i
        f.syswrite(request.body)
      end

      options = { appendable: true }
      metadata = OssUtil.put_object_metadata(bucket, object, request, options)

      dataset = { cmd: Request::POST_APPEND_OBJECT }
      dataset['x-oss-next-append-position'] = (metadata[:size].to_i + 1).to_s
      OssResponse.response_ok(response, dataset)
    end

    # DeleteObject
    def self.delete_object(bucket, object, response)
      # NoSuchBucket
      return if OssResponse.response_no_such_bucket(response, bucket)

      # delete object
      OssUtil.delete_object_file_and_dir(bucket, object)

      OssResponse.response_ok(response, Request::DELETE_OBJECT)
    end

    # DeleteMultipleObjects
    def self.delete_multiple_objects(bucket, request, response) 
      body = request.body
      body = body.read if body.respond_to?(:read)
      xml = Document.new(body.to_s)
      quiet_node = xml.root&.elements["/Delete/Quiet"]
      quiet = quiet_node ? quiet_node.text.to_s : ""
      encoding_type = request.header["encoding-type"]&.first
      object_list = []
      xml.elements.each("*/Object/Key") do |e|
        object = e.text.to_s
        if encoding_type && encoding_type.downcase.start_with?("url")
          object = CGI.unescape(object)
        end
        object_list << object
        Object.delete_object(bucket, object, response)
      end
      
      dataset = { cmd: Request::DELETE_MULTIPLE_OBJECTS }
      if quiet.downcase=="false"
        dataset[:object_list] = object_list      
      end

      OssResponse.response_ok(response, dataset)
    end

    # HeadObject
    def self.head_object(bucket, object, response) 
      # NoSuchObject
      object_name = object
      if OssResponse.response_no_such_object(response, bucket, object_name)
        if !object_name.end_with?('/')
          dir_object = "#{object_name}/"
          if !OssResponse.response_no_such_object(response, bucket, dir_object)
            object_name = dir_object
          else
            return
          end
        else
          return
        end
      end

      object_metadata_filename = File.join(Config.store, bucket, object_name, Store::OBJECT_METADATA)
      dataset = File.open(object_metadata_filename) { |file| YAML::load(file) }
      dataset[:cmd] = Request::HEAD_OBJECT

      OssResponse.response_ok(response, dataset)
    end

    # GetObjectMeta
    def self.get_object_meta(bucket, object, request, response) 
      # NoSuchObject
      return if OssResponse.response_no_such_object(response, bucket, object)

      object_metadata_filename = File.join(Config.store, bucket, object, Store::OBJECT_METADATA)
      dataset = File.open(object_metadata_filename) { |file| YAML::load(file) }
      dataset[:cmd] = Request::GET_OBJECT_META

      OssResponse.response_ok(response, dataset)
    end

    # PutObjectACL
    def self.put_object_acl(bucket, object, request, response) 
      # NoSuchObject
      return if OssResponse.response_no_such_object(response, bucket, object)
      
      object_metadata_filename = File.join(Config.store, bucket, object, Store::OBJECT_METADATA)
      dataset = File.open(object_metadata_filename) { |file| YAML::load(file) }
      acl_old = dataset[:acl]
      dataset[:acl] = request.header["x-oss-object-acl"].first || acl_old
      File.open(object_metadata_filename,'w') do |f|
        f << YAML::dump(dataset)
      end
      dataset[:cmd] = Request::PUT_OBJECT_ACL

      OssResponse.response_ok(response, dataset)
    end

    # GetObjectACL
    def self.get_object_acl(bucket, object, response) 
      # NoSuchObject
      return if OssResponse.response_no_such_object(response, bucket, object)

      object_metadata_filename = File.join(Config.store, bucket, object, Store::OBJECT_METADATA)
      dataset = File.open(object_metadata_filename) { |file| YAML::load(file) }
      dataset[:cmd] = Request::GET_OBJECT_ACL
      dataset[:acl] = "default"

      OssResponse.response_ok(response, dataset)
    end

  end # class
end # module
