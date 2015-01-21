require 'yaml'
require 'tempfile'
require 'fileutils'
require 'socket'

config_path = ARGV.shift()
script_path = ARGV.shift()
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

class Server
  attr_reader :host, :process

  def initialize(host, process)
    @host = host
    @process = process
  end

  public
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

class Dench
  def initialize(config)
    @config = config
  end

  public
  def run(script_path)
    if @config.preparation.dench
      @config.preparation.dench.each do |p|
        puts "##### preparation.dench: #{p} #####"
        puts `#{p}`
      end
    end

    timestamp = Time.now.to_i()
    local_dstdir = "dench.result.#{timestamp}"
    Dir.mkdir(local_dstdir)
    @config.servers.each_with_index do |server, idx|
      package = nil
      begin
        package = create_package(script_path, server)
        remote_tmpdir = "/tmp/dench.#{server.host}.#{idx}.#{timestamp}" # FIXME
        local_tmpdir = File.basename(remote_tmpdir)
        push(package, server, remote_tmpdir)
        ssh(server, remote_tmpdir)
        pull(server, remote_tmpdir, local_dstdir)
      ensure
        delete_package(package)
      end
    end
  end

  private
  def create_package(script_path, server)
    Package.create(script_path, runner(server))
  end

  def delete_package(package)
    package.destroy() if package
  end

  def runner(server)
    return <<-EOS
#!/bin/sh
sh command.sh
    EOS
  end

  def push(package, server, remote_tmpdir)
    pushcmd = "scp -r #{package} #{server.host}:#{remote_tmpdir}"
    puts(pushcmd)
    system(pushcmd)
  end

  def ssh(server, remote_tmpdir)
    sshcmd = "ssh #{server.host} 'cd #{remote_tmpdir}; sh runner.sh > stdout.log 2> stderr.log'"
    puts(sshcmd)
    system(sshcmd)
  end

  def pull(server, remote_tmpdir, local_dstdir)
    pullcmd = "scp -r #{server.host}:#{remote_tmpdir} #{local_dstdir}/."
    puts(pullcmd)
    system(pullcmd)
    rmcmd = "ssh #{server.host} rm -rf #{remote_tmpdir}"
    system(rmcmd)
  end
end

config_hash = YAML.load(File.read(config_path))
config = DenchConfig.parse(config_hash)
dench = Dench.new(config)
dench.run(script_path)
