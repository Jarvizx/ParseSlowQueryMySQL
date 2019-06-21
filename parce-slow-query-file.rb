require 'csv'
require 'open-uri'
require 'json'
require 'time'

class LogSlowQueryToCsv

  def initialize(name_file_in, name_file_out)
    @name_file_in = name_file_in
    @name_file_out = name_file_out ||= "output-slow-query.csv"
    # vars
    @line            = Hash.new
    @lines           = Array.new
    @new_line        = false
    @last_row        = false
    @line_split      = ""
    @count_rows      = 0
    @concat_query    = 0
    @limit_rows_file = 0
  end
  
  def fit_log
    if File.exist?(@name_file_in) == false
      puts "The file `#{@name_file_in}` didn't found it."
      return false
    end
    File.open(@name_file_in, "r") do |fr|
      fr.each_line do |liner|
        # get Time
        if liner.include? "# Time:"
          @line_split = liner.split(/ /)
          @line[:Time] = @line_split[2].strip
          @line[:_fullTime] = Time.parse(@line[:Time])
        end
        if @concat_query == 1
          # continue concat query
          @line[:query] << " " << liner.strip
        end        
        # get User@Host
        if liner.include? "# User@Host:"
          @line_split = liner.split(/ /)
          @line[:UserHost] = @line_split[2].strip + "@" + @line_split[5].strip
          @line[:Id] = @line_split[8].strip
        end
        # get Query_time
        if liner.include? "# Query_time:"
          @line_split = liner.split(/ /)
          @line[:Query_time] = @line_split[2].strip
          @line[:Lock_time] = @line_split[5].strip
          @line[:Rows_sent] = @line_split[7].strip
          @line[:Rows_examined] = @line_split[10].strip
        end
        # delimite end concat and new line
        if liner.include? "# Time:"
          @concat_query = 0
          if @line[:Query_time]
            @new_line = true
          end
        end
        if liner.include?("SELECT") || liner.include?("select")
          @line[:query] = liner.strip
          @concat_query = 1
        end
        @count_rows += 1
        @limit_rows_file = get_max_lines_file(@name_file_in)
        puts "#{@count_rows}/#{@limit_rows_file}"
        # last row
        @last_row = @limit_rows_file == @count_rows
        if @new_line == true || @last_row
          # lines csv
          @lines.push([@line[:_fullTime].strftime('%m/%d/%Y %H:%M'), @line[:Query_time], @line[:Rows_examined],  @line[:UserHost], @line[:query]])
          @new_line = false
          @line[:query] = "< Isn't query >"
        end
      end
    end
    # write
    CSV.open(@name_file_out, "wb", { :col_sep => ";" }) do |csv|
      # title csv
      csv << [ "Date", "Query time", "Rows examineds", "Host", "Query" ]
      # list queries
      @lines.each { |row| csv << row }
    end
  end
  
  def get_max_lines_file(file)
    count = 0
    file = File.open(file, "r") { |file| file.each_line { |line| count += 1 }}
    return count
  end
  
end

file_in = ARGV[0]
file_out = ARGV[1]
if file_in == nil
  puts "File name input or output isn't defined"
  exit
end
ParseLog = LogSlowQueryToCsv.new(file_in, file_out)
ParseLog.fit_log