describe 'database' do
  def run_script(commands)
    raw_output = nil
    IO.popen("./bin/build/db mydb.db", "r+") do |pipe|
      commands.each do |command|
        pipe.puts command
      end

      pipe.close_write

      # Read entire output
      raw_output = pipe.gets(nil)
    end
    raw_output.split("\n")
  end

  it 'inserts and retreives a row' do
    result = run_script([
      "insert stb2 thehobbit warnerbros 2014-04-02 8.00 2:45",
      "select",
      ".exit",
    ])
    expect(result).to match_array([
      "db > Executed.",
      "db > (stb2, thehobbit, warnerbros, 2014-04-02, 8.000000, 2:45)",
      "Executed.",
      "db > ",
    ])
  end

  it 'prints error message when table is full' do
      script = (1..701).map do |i|
          "insert stb#{i} title#{i} provider#{i} date#{i} #{i} #{i}"
      end
      script << ".exit"
      result = run_script(script)
      expect(result[-2]).to eq('db > Error: Table full.')
  end

  it 'allows inserting strings that are the maximum length' do
      long_stb = "a"*32
      long_title = "a"*255
      long_provider = "a"*255
      long_date = "a"*10
      long_time = "a"*4
      script = [
          "insert #{long_stb} #{long_title} #{long_provider} #{long_date} 4.00 #{long_time}",
          "select",
          ".exit"
      ]
      result = run_script(script)
      expect(result).to match_array([
        "db > Executed.",
        "db > (#{long_stb}, #{long_title}, #{long_provider}, #{long_date}, 4.000000, #{long_time})",
        "Executed.",
        "db > ",
      ])
  end

  it 'prints error messages if strings are too long' do
      long_stb = "a"*33
      long_title = "a"*256
      long_provider = "a"*256
      long_date = "a"*11
      long_time = "a"*5
      script = [
          "insert #{long_stb} #{long_title} #{long_provider} #{long_date} 4.00 #{long_time}",
          "select",
          ".exit"
      ]
      result = run_script(script)
      expect(result).to match_array([
          "db > String is too long.",
          "db > Executed.",
          "db > ",
      ])
  end

  it 'printes error message if rev is negative' do
    result = run_script([
      "insert stb2 thehobbit warnerbros 2014-04-02 -1.00 2:45",
      "select",
      ".exit",
    ])
    expect(result).to match_array([
        "db > REV must be positive.",
        "db > Executed.",
        "db > ",
    ])
  end

  it 'keeps data after closing connection' do
      result1 = run_script([
        "insert stb2 thehobbit warnerbros 2014-04-02 8.00 2:45",
        ".exit",
      ])
      expect(result1).to match_array([
          "db > Executed.",
          "db > ",
      ])
      result2 = run_script([
          "select",
          ".exit",
      ])
      expect(result2).to match_array([
          "db > (stb2, thehobbit, warnerbros, 2014-04-02, 8.000000, 2:45)",
          "Executed.",
          "db > ",
      ])
  end
end
