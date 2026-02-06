require 'yaml'
require 'find'
require 'time'
require 'fileutils'
require 'builder'
require 'emulator/config'
require 'emulator/request'
require 'emulator/response'

module OssEmulator
  module Bucket
    def self.bucket_config_path(bucket, filename)
      File.join(Config.store, bucket, filename)
    end

    def self.read_bucket_config(bucket, filename, default_body)
      config_file = bucket_config_path(bucket, filename)
      return default_body unless File.exist?(config_file)
      File.open(config_file, 'rb').read
    end

    def self.write_bucket_config(bucket, filename, body)
      config_file = bucket_config_path(bucket, filename)
      File.open(config_file, 'w') do |file|
        file << body.to_s
      end
    end

    # GetService=ListBuckets
    def self.get_service(response) 
      Bucket.list_buckets(response)
    end

    # ListBuckets=GetService
    def self.list_buckets(response) 
      body = ""
      xml = Builder::XmlMarkup.new(:target => body)
      xml.instruct! :xml, :version=>"1.0", :encoding=>"UTF-8"
      xml.ListAllMyBucketsResult(:xmlns => HttpMsg::XMLNS) { |lam|
        lam.Owner { |owner|
          owner.ID("00220120222")
          owner.DisplayName("1390402650033793")
        }
        lam.Buckets { |node|
          Dir[File.join(Config.store, "*")].each do |bucket|
            bucket_metadata_file = File.join(bucket, Store::BUCKET_METADATA)
            if File.exist?(bucket_metadata_file)
              node.Bucket do |sub_node|
                bucket_name = File.basename(bucket)
                sub_node.Name(bucket_name)
                tz = File.ctime(File.join(Config.store, bucket_name)).utc.iso8601(HttpMsg::SUBSECOND_PRECISION)
                sub_node.CreationDate(Time.parse(tz).strftime('%Y-%m-%dT%H:%M:%S.000Z'))
              end
            end
          end
        }
      }

      dataset = {
        cmd: Request::LIST_BUCKETS, 
        body: body
      }
      OssResponse.response_ok(response, dataset)
    end

    # PutBucket=CreateBucket
    def self.create_bucket(bucket, request, response)
      # InvalidBucketName
      return if OssResponse.response_invalid_bucket_name(response, bucket)

      # TooManyBuckets
      return if OssResponse.response_too_many_buckets(response)

      bucket_folder = File.join(Config.store, bucket)
      bucket_metadata_file = File.join(bucket_folder, Store::BUCKET_METADATA)
      if not ( File.exist?(bucket_folder) && File.exist?(bucket_metadata_file) )
        FileUtils.mkdir_p(bucket_folder)
        metadata = {}
        metadata[:bucket] = bucket
        metadata[:creation_date] = File.mtime(bucket_folder).utc.iso8601(HttpMsg::SUBSECOND_PRECISION)
        metadata[:acl] = request.header['x-oss-acl']
        File.open(bucket_metadata_file,'w') do |f|
          f << YAML::dump(metadata)
        end
      end
      
      OssResponse.response_ok(response, Request::PUT_BUCKET)
    end

    # PutBucketACL
    def self.put_bucket_acl(response) 
      OssResponse.response_ok(response)
    end

    def self.put_bucket_logging(bucket, request, response)
      return if OssResponse.response_no_such_bucket(response, bucket)
      write_bucket_config(bucket, Store::BUCKET_LOGGING, request.body)
      OssResponse.response_ok(response, Request::PUT_BUCKET_LOGGING)
    end

    def self.get_bucket_logging(bucket, response)
      return if OssResponse.response_no_such_bucket(response, bucket)
      default_body = <<-eos.strip
        <?xml version="1.0" encoding="UTF-8"?>
        <BucketLoggingStatus xmlns="#{HttpMsg::XMLNS}"></BucketLoggingStatus>
      eos
      dataset = {
        cmd: Request::GET_BUCKET_LOGGING,
        body: read_bucket_config(bucket, Store::BUCKET_LOGGING, default_body)
      }
      OssResponse.response_ok(response, dataset)
    end

    def self.delete_bucket_logging(bucket, response)
      return if OssResponse.response_no_such_bucket(response, bucket)
      FileUtils.rm_f(bucket_config_path(bucket, Store::BUCKET_LOGGING))
      OssResponse.response_ok(response, Request::DELETE_BUCKET_LOGGING)
    end

    def self.put_bucket_website(bucket, request, response)
      return if OssResponse.response_no_such_bucket(response, bucket)
      write_bucket_config(bucket, Store::BUCKET_WEBSITE, request.body)
      OssResponse.response_ok(response, Request::PUT_BUCKET_WEBSITE)
    end

    def self.get_bucket_website(bucket, response)
      return if OssResponse.response_no_such_bucket(response, bucket)
      default_body = <<-eos.strip
        <?xml version="1.0" encoding="UTF-8"?>
        <WebsiteConfiguration xmlns="#{HttpMsg::XMLNS}"></WebsiteConfiguration>
      eos
      dataset = {
        cmd: Request::GET_BUCKET_WEBSITE,
        body: read_bucket_config(bucket, Store::BUCKET_WEBSITE, default_body)
      }
      OssResponse.response_ok(response, dataset)
    end

    def self.delete_bucket_website(bucket, response)
      return if OssResponse.response_no_such_bucket(response, bucket)
      FileUtils.rm_f(bucket_config_path(bucket, Store::BUCKET_WEBSITE))
      OssResponse.response_ok(response, Request::DELETE_BUCKET_WEBSITE)
    end

    def self.put_bucket_referer(bucket, request, response)
      return if OssResponse.response_no_such_bucket(response, bucket)
      write_bucket_config(bucket, Store::BUCKET_REFERER, request.body)
      OssResponse.response_ok(response, Request::PUT_BUCKET_REFERER)
    end

    def self.get_bucket_referer(bucket, response)
      return if OssResponse.response_no_such_bucket(response, bucket)
      default_body = <<-eos.strip
        <?xml version="1.0" encoding="UTF-8"?>
        <RefererConfiguration xmlns="#{HttpMsg::XMLNS}">
          <AllowEmptyReferer>true</AllowEmptyReferer>
          <RefererList></RefererList>
        </RefererConfiguration>
      eos
      dataset = {
        cmd: Request::GET_BUCKET_REFERER,
        body: read_bucket_config(bucket, Store::BUCKET_REFERER, default_body)
      }
      OssResponse.response_ok(response, dataset)
    end

    def self.delete_bucket_referer(bucket, response)
      return if OssResponse.response_no_such_bucket(response, bucket)
      FileUtils.rm_f(bucket_config_path(bucket, Store::BUCKET_REFERER))
      OssResponse.response_ok(response, Request::DELETE_BUCKET_REFERER)
    end

    def self.put_bucket_lifecycle(bucket, request, response)
      return if OssResponse.response_no_such_bucket(response, bucket)
      write_bucket_config(bucket, Store::BUCKET_LIFECYCLE, request.body)
      OssResponse.response_ok(response, Request::PUT_BUCKET_LIFECYCLE)
    end

    def self.get_bucket_lifecycle(bucket, response)
      return if OssResponse.response_no_such_bucket(response, bucket)
      default_body = <<-eos.strip
        <?xml version="1.0" encoding="UTF-8"?>
        <LifecycleConfiguration xmlns="#{HttpMsg::XMLNS}"></LifecycleConfiguration>
      eos
      dataset = {
        cmd: Request::GET_BUCKET_LIFECYCLE,
        body: read_bucket_config(bucket, Store::BUCKET_LIFECYCLE, default_body)
      }
      OssResponse.response_ok(response, dataset)
    end

    def self.delete_bucket_lifecycle(bucket, response)
      return if OssResponse.response_no_such_bucket(response, bucket)
      FileUtils.rm_f(bucket_config_path(bucket, Store::BUCKET_LIFECYCLE))
      OssResponse.response_ok(response, Request::DELETE_BUCKET_LIFECYCLE)
    end

    # GetBucket=ListObjects
    def self.get_bucket(bucket, req, response)
      # NoSuchBucket
      return if OssResponse.response_no_such_bucket(response, bucket)

      filter = {
        :marker => req.query["marker"] ? req.query["marker"].to_s : nil,
        :prefix => req.query["prefix"] ? req.query["prefix"].to_s : nil,
        :max_keys => req.query["max-keys"] ? req.query["max-keys"].to_i : nil,
        :delimiter => req.query["delimiter"] ? req.query["delimiter"].to_s : nil
      }

      body = ""
      xml = Builder::XmlMarkup.new(:target => body)
      xml.instruct! :xml, :version=>"1.0", :encoding=>"UTF-8"

      if req.query["list-type"].to_s == "2"
        list_data = OssUtil.get_bucket_list_objects_v2(req)
        xml.ListBucketResult(:xmlns => HttpMsg::XMLNS) { |lbr|
          lbr.Name(bucket)
          lbr.Prefix(filter[:prefix])
          lbr.ContinuationToken(req.query["continuation-token"])
          lbr.StartAfter(req.query["start-after"])
          lbr.MaxKeys(filter[:max_keys] || "1000")
          lbr.Delimiter(filter[:delimiter])
          lbr.KeyCount(list_data[:key_count])
          lbr.IsTruncated(list_data[:is_truncated])
          if list_data[:next_continuation_token] && list_data[:next_continuation_token] != ""
            lbr.NextContinuationToken(list_data[:next_continuation_token])
          end
          list_data[:objects].each do |obj_hash|
            lbr.Contents { |contents|
              contents.Key(obj_hash[:key])
              contents.LastModified(Time.parse(obj_hash[:modified_date]).strftime('%Y-%m-%dT%H:%M:%S.000Z'))
              contents.ETag(obj_hash[:md5])
              contents.Type("Multipart")
              contents.Size(obj_hash[:size])
              contents.StorageClass(obj_hash[:storageclass])
              contents.Owner { |node| 
                node.ID("00220120222")
                node.DisplayName("1390402650033793")
              }
            }
          end
          list_data[:common_prefixes].each do |item|
            lbr.CommonPrefixes { |node|
              node.Prefix(item)
            }
          end
        }
      else
        xml.ListBucketResult(:xmlns => HttpMsg::XMLNS) { |lbr|
          lbr.Name(bucket)
          lbr.Prefix(filter[:prefix])
          lbr.Marker(filter[:marker])
          lbr.MaxKeys("1000")
          lbr.Delimiter(filter[:delimiter])
          lbr.EncodingType("url")
          OssUtil.get_bucket_list_objects(lbr, req)
        }
      end

      dataset = {
        :cmd => Request::GET_BUCKET, 
        :body => body
      }
      OssResponse.response_ok(response, dataset)
    end

    # GetBucketACL
    def self.get_bucetk_acl(bucket, response)
      # NoSuchBucket
      return if OssResponse.response_no_such_bucket(response, bucket)

      OssResponse.response_ok(response, Request::GET_BUCKET_ACL)
    end

    # GetBucketLocation:  
    def self.get_bucket_location(response)
      OssResponse.response_ok(response, Request::GET_BUCKET_LOCATION)
    end

    # GetBucketInfo
    def self.get_bucetk_info(bucket, response)
      # NoSuchBucket
      return if OssResponse.response_no_such_bucket(response, bucket)

      bucket_metadata_filename = File.join(Config.store, bucket, Store::BUCKET_METADATA)
      dataset = YAML.load(File.open(bucket_metadata_filename, 'rb').read)
      dataset[:bucket_name] = bucket
      dataset[:cmd] = Request::GET_BUCKET_INFO
      dataset[:acl] = "private"

      OssResponse.response_ok(response, dataset)
    end

    # DeleteBucket
    def self.delete_bucket(bucket, response)
      # NoSuchBucket
      return if OssResponse.response_no_such_bucket_when_delete_bucket(response, bucket)

      # BucketNotEmpty
      return if OssResponse.response_bucket_not_empty(response, bucket)

      # DeleteBucketFolder
      FileUtils.rm_rf(File.join(Config.store, bucket))
      OssResponse.response_ok(response)
    end

  end #class
end # module
