require 'time'
require 'emulator/base'

module OssEmulator

  class ChunkFile < File
    attr_accessor :options

    def self.open(filename, options = {})
      file_handle = super(filename, 'rb')
      file_handle.options = options
      file_handle.options[:cur_pos] = 0
      file_handle.options[:bytes_left_to_read] = options[:read_length]
      file_handle.options[:abs_pos] = options[:start_pos] if options[:type] == 'multipart_range'
      file_handle.options[:base_part_filename] = options[:base_part_filename]
      file_handle.options[:part_number] = 1
      file_handle.options[:f_part] = nil
      file_handle.options[:request_id] = options[:request_id]
      if options[:type] == 'multipart_whole' || options[:type] == 'multipart_range'
        base = file_handle.options[:base_part_filename]
        file_handle.options[:part_files] = Dir["#{base}*"]
          .select { |f| File.file?(f) }
          .sort_by { |f| File.basename(f).split('-').last.to_i }
        file_handle.options[:part_sizes] = file_handle.options[:part_files].map { |f| File.size(f) }
        file_handle.options[:part_index] = 0
      end

      Log.debug("ChunkFile.open :#{file_handle.options[:base_part_filename]}#{file_handle.options[:part_number]}, #{file_handle.options[:type]}, #{file_handle.options}", 'blue')
      return file_handle
    end

    def read(args)
      case self.options[:type]
      when 'single_whole'
        return super(Object::STREAM_CHUNK_SIZE)

      when 'single_range'
        return nil if self.options[:bytes_left_to_read] <= 0
        self.pos = self.options[:start_pos] if self.options[:cur_pos] == 0

        bytes_cur_to_read = (self.options[:bytes_left_to_read] <= Object::STREAM_CHUNK_SIZE) ? self.options[:bytes_left_to_read] : Object::STREAM_CHUNK_SIZE
        self.options[:bytes_left_to_read] -= bytes_cur_to_read
        return super(bytes_cur_to_read)

      when 'multipart_whole'
        return nil if self.options[:part_files].nil? || self.options[:part_files].empty?

        if self.options[:f_part].nil?
          part_filename = self.options[:part_files][self.options[:part_index]]
          return nil unless part_filename
          self.options[:f_part] = File.open(part_filename, 'rb')
        end

        read_buf = self.options[:f_part].read(Object::STREAM_CHUNK_SIZE)
        if self.options[:f_part].eof?
          self.options[:f_part].close
          self.options[:f_part] = nil
          self.options[:part_index] += 1
        end

        return read_buf

      when 'multipart_range'
        return nil if self.options[:bytes_left_to_read] <= 0
        return nil if self.options[:part_files].nil? || self.options[:part_files].empty?

        bytes_cur_to_read = (self.options[:bytes_left_to_read] <= Object::STREAM_CHUNK_SIZE) ? self.options[:bytes_left_to_read] : Object::STREAM_CHUNK_SIZE
        bytes_to_fetch = bytes_cur_to_read
        output = ''.b

        # Locate the current absolute offset in the concrete part list first.
        part_index = 0
        offset_in_part = self.options[:abs_pos]
        while part_index < self.options[:part_sizes].length
          part_size = self.options[:part_sizes][part_index]
          break if offset_in_part < part_size
          offset_in_part -= part_size
          part_index += 1
        end

        while bytes_to_fetch > 0 && part_index < self.options[:part_files].length
          part_filename = self.options[:part_files][part_index]
          part_size = self.options[:part_sizes][part_index]
          break if offset_in_part > part_size

          File.open(part_filename, 'rb') do |part_file|
            part_file.pos = offset_in_part
            read_buf = part_file.read(bytes_to_fetch)
            output << read_buf if read_buf && !read_buf.empty?
          end

          bytes_to_fetch = bytes_cur_to_read - output.bytesize
          break if bytes_to_fetch <= 0

          part_index += 1
          offset_in_part = 0
        end

        return nil if output.empty?

        self.options[:abs_pos] += output.bytesize
        self.options[:bytes_left_to_read] -= output.bytesize
        output
      else
        return nil
      end # when

      return nil
    end # func read

  end # class ChunkFile

end # OssEmulator
