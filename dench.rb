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

class DenchNode
  attr_reader :id, :host, :process, :wd

  def initialize(id, host, process)
    @id = id
    @host = host
    @process = process
    @wd = "/tmp/dench.#{@host}.#{@id}" # FIXME
  end

  public
  def prepare()
    mkdir = "ssh #{@host} 'mkdir -p #{@wd}'"
    puts(mkdir)
    system(mkdir)
  end

  def finalize()
    rm = "ssh #{@host} rm -rf #{@wd}"
    puts(rm)
    system(rm)
  end

  def push(src, dst)
    scp = "scp -r #{src} #{@host}:#{@wd}/#{dst}"
    puts(scp)
    system(scp)
  end

  def pullall(dst)
    scp = "scp -r #{@host}:#{@wd} #{dst}"
    puts(scp)
    system(scp)
  end

  def exec(cmd)
    ssh = "ssh #{@host} 'cd #{@wd}; #{cmd}'"
    puts(ssh)
    system(ssh)
  end

  def to_s()
    "-#{@host}: process = #{@process}"
  end
end

class DenchPreparation
  attr_accessor :dench, :node

  def initialize()
    @dench = nil
    @node = nil
  end

  public
  def to_s()
    [
      "-dench: #{@dench}",
      "-node: #{@node}",
    ].join("\n")
  end
end

class DenchConfig
  attr_reader :nodes, :preprocess, :name

  def initialize(config_hash)
    @name = "timestamp#{Time.now.to_i}"
    @nodes = parse_node_config(config_hash)
    @preprocess = parse_preprocess_config(config_hash)
  end

  public
  def self.parse(config_hash)
    self.new(config_hash)
  end

  private
  def parse_node_config(config_hash)
    unless config_hash['nodes']
      raise("invalid config: nodes are not specified")
    end
    id = 0
    config_hash['nodes'].map{|s|
      host = s['host']
      unless host 
        raise("invalid config: host is not specified")
      end
      process = s['process']
      unless process
        raise("invalid config: process is not specified")
      end
      DenchNode.new("#{@name}_#{id += 1}", host, process)
    }
  end

  def parse_preprocess_config(config_hash)
    preprocess = DenchPreparation.new()
    preprocess_config = config_hash['preprocess']
    return preprocess unless preprocess_config
    if preprocess_config['dench']
      preprocess.dench = preprocess_config['dench']
    end
    if preprocess_config['node']
      preprocess.node = preprocess_config['node']
    end
    preprocess
  end

  def to_s()
    [
      "---------- nodes ----------",
      @nodes,
      "---------- preprocess ----------",
      @preprocess,
    ].flatten.join("\n")
  end
end

class DenchPackage
  attr_reader :script

  def initialize(script, tmpdir)
    @script = script
    @tmpdir = tmpdir
  end

  public
  def self.create(script_path, runner)
    tmpdir = Dir.mktmpdir(nil, Dir.getwd())
    script = File.basename(script_path)
    package = DenchPackage.new(script, tmpdir)
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
  attr_reader :id, :node, :params

  def initialize(id, node, script_path, params)
    @id = id
    @node = node
    @script_path = script_path
    @params = params
    @remote_tmpdir = File.join(@node.wd, @id.to_s)
    @local_tmpdir = File.basename(@remote_tmpdir)
    @package = nil
    @pid = nil
  end

  public
  def prepare()
    @package = create_package()
    node.push(@package, @id.to_s)
  end

  def spawn()
    wd = @remote_tmpdir
    sshcmd = "ssh #{@node.host} 'cd #{wd}; sh runner.sh > stdout.log 2> stderr.log'"
    puts(sshcmd)
    @pid = Process.spawn(sshcmd, :out => STDOUT, :err => STDERR)
  end

  def finalize()
    Process.waitpid(@pid) if @pid
    delete_package(@package)
  end

  def to_s()
    "#{@id}: host = #{@node.host}, params = #{@params}"
  end

  private
  def runner()
    [
      '#!/bin/sh',
      @params.map{|param| "sh command.sh #{param}"}.join("\n")
    ].join("\n")
  end

  def create_package()
    DenchPackage.create(@script_path, runner())
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
    dstdir = "dench.result.#{@config.name}"
    Dir.mkdir(dstdir)
    nodes = @config.nodes
    processes = gen_processes(nodes, script_path, parameters)

    if @config.preprocess.dench
      @config.preprocess.dench.each do |p|
        cmd = "cd #{dstdir}; #{p}"
        puts(cmd)
        system(cmd)
      end
    end
    begin
      nodes.each{|node| node.prepare()}
      (@config.preprocess.node || []).each{|p| nodes.each{|node| node.exec(p)}}
      processes.each{|process| process.prepare()}
      processes.each{|process| process.spawn()}
    ensure
      # TODO handle error
      nodes.each{|node| node.pullall(dstdir)}
      processes.each{|process| process.finalize()}
      nodes.each{|node| node.finalize()}
    end
  end

  private
  def gen_processes(nodes, script_path, parameters)
    numprocs = nodes.inject(0){|i, s| i + s.process}
    process_params = Array.new(numprocs).map{[]}
    parameters.each_with_index{|param, idx|
      process_params[idx % process_params.size].push(param)
    }
    processes = []
    nodes.map{|s| Array.new(s.process).map{s}}.flatten.each_with_index{|node, i|
      processes.push(DenchProcess.new(i, node, script_path, process_params[i]))
    }
    processes
  end
end

config_hash = YAML.load(File.read(config_path))
config = DenchConfig.parse(config_hash)
dench = Dench.new(config)
dench.run(script_path, parameters)
