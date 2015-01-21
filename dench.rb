require 'yaml'
require 'tempfile'
require 'fileutils'
require 'socket'

config_path = ARGV.shift()
script_path = ARGV.shift()
parameter_path = ARGV.shift()
parameters = []
unless config_path && script_path
  puts('usage: ruby dench.rb config_path script_path')
  exit(1)
end
unless File.exist?(config_path)
  puts("#{config_path} does not exist")
  exit(1)
end
unless File.exist?(script_path)
  puts("#{script_path} does not exist")
  exit(1)
end
if parameter_path.instance_of?(String) && File.exist?(parameter_path)
  parameters = File.read(parameter_path).split("\n")
end

class Server
  attr_reader :host, :process

  def initialize(host, process)
    @host = host
    @process = process
  end

  public
  def push(src, dst)
    pushcmd = "scp -r #{src} #{@host}:#{dst}"
    puts(pushcmd)
    system(pushcmd)
  end

  def pull(src, dst)
    pullcmd = "scp -r #{@host}:#{src} #{dst}"
    puts(pullcmd)
    system(pullcmd)
    rmcmd = "ssh #{@host} rm -rf #{src}"
    system(rmcmd)
  end

  def to_s()
    "-#{@host}: process = #{@process}"
  end
end

class Preparation
  attr_accessor :dench, :server

  def initialize()
    @dench = nil
    @server = nil
  end

  public
  def to_s()
    [
      "-dench: #{@dench}",
      "-server: #{@server}",
    ].join("\n")
  end
end

class DenchConfig
  attr_reader :servers, :preparation

  def initialize(config_hash)
    @servers = parse_server_config(config_hash)
    @preparation = parse_preparation_config(config_hash)
  end

  public
  def self.parse(config_hash)
    self.new(config_hash)
  end

  private
  def parse_server_config(config_hash)
    unless config_hash['servers']
      raise("invalid config: servers are not specified")
    end
    config_hash['servers'].map{|s|
      host = s['host']
      unless host 
        raise("invalid config: host is not specified")
      end
      process = s['process']
      unless process
        raise("invalid config: process is not specified")
      end
      Server.new(host, process)
    }
  end

  def parse_preparation_config(config_hash)
    preparation = Preparation.new()
    preparation_config = config_hash['preparation']
    return preparation unless preparation_config
    if preparation_config['dench']
      preparation.dench = preparation_config['dench']
    end
    if preparation_config['server']
      preparation.server = preparation_config['server']
    end
    preparation
  end

  def to_s()
    [
      "---------- servers ----------",
      @servers,
      "---------- preparation ----------",
      @preparation,
    ].flatten.join("\n")
  end
end

class Package
  attr_reader :script

  def initialize(script, tmpdir)
    @script = script
    @tmpdir = tmpdir
  end

  public
  def self.create(script_path, runner)
    tmpdir = Dir.mktmpdir(nil, Dir.getwd())
    script = File.basename(script_path)
    package = Package.new(script, tmpdir)
    FileUtils.copy(script_path, File.join(tmpdir, 'command.sh'))
    File.write(File.join(tmpdir, 'runner.sh'), runner)
    package
  end

  def destroy()
    FileUtils.remove_entry_secure(@tmpdir)
  end

  def to_s()
    @tmpdir.to_s()
  end
end

class DenchProcess
  attr_reader :id, :server, :params

  def initialize(id, timestamp, server, script_path, params)
    @id = id
    @timestamp = timestamp
    @server = server
    @script_path = script_path
    @params = params
    @remote_tmpdir = "/tmp/dench.#{@server.host}.#{@id}.#{@timestamp}" # FIXME
    @local_tmpdir = File.basename(@remote_tmpdir)
  end

  public
  def exec(dstdir)
    begin
      package = create_package()
      server.push(package, @remote_tmpdir)
      ssh(@remote_tmpdir)
      server.pull(@remote_tmpdir, "#{dstdir}/.")
    ensure
      delete_package(package)
    end
  end

  def to_s()
    "#{@id}: host = #{@server.host}, params = #{@params}"
  end

  private
  def runner()
    [
      '#!/bin/sh',
      @params.map{|param| "sh command.sh #{param}"}.join("\n")
    ].join("\n")
  end

  def ssh(wd)
    sshcmd = "ssh #{@server.host} 'cd #{wd}; sh runner.sh > stdout.log 2> stderr.log'"
    puts(sshcmd)
    system(sshcmd)
  end

  def create_package()
    Package.create(@script_path, runner())
  end

  def delete_package(package)
    package.destroy() if package
  end
end

class Dench
  def initialize(config)
    @config = config
  end

  public
  def run(script_path, parameters)
    if @config.preparation.dench
      @config.preparation.dench.each do |p|
        puts "##### preparation.dench: #{p} #####"
        puts `#{p}`
      end
    end

    timestamp = Time.now.to_i()
    dstdir = "dench.result.#{timestamp}"
    Dir.mkdir(dstdir)
    processes = gen_processes(timestamp, @config.servers, script_path, parameters)
    processes.each do |process|
      process.exec(dstdir)
    end
  end

  private
  def gen_processes(timestamp, servers, script_path, parameters)
    numprocs = servers.inject(0){|i, s| i + s.process}
    process_params = Array.new(numprocs).map{[]}
    parameters.each_with_index{|param, idx|
      process_params[idx % process_params.size].push(param)
    }
    processes = []
    servers.map{|s| Array.new(s.process).map{s}}.flatten.each_with_index{|server, id|
      processes.push(DenchProcess.new(id, timestamp, server, script_path, process_params[id]))
    }
    processes
  end
end

config_hash = YAML.load(File.read(config_path))
config = DenchConfig.parse(config_hash)
dench = Dench.new(config)
dench.run(script_path, parameters)
