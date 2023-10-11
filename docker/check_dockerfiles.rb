#!/usr/bin/env ruby

# Checks that the Dockerfiles include all of the workspace members

require 'toml-rb'

DOCKERFILES = [
  "docker/cluster/compiler/Dockerfile",
  "docker/cluster/services/Dockerfile",
  "docker/cluster/worker/Dockerfile",
  "docker/single/Dockerfile"
]

# Get the current script directory
script_dir = File.dirname(__FILE__)

# Parse the Cargo.toml file and get all of the workspace members
toml = TomlRB.load_file(File.join(script_dir, '..', 'Cargo.toml'))
members = toml['workspace']['members']

# Check that each Dockerfile includes all of the workspace members
DOCKERFILES.each do |dockerfile|
  File.open(dockerfile, 'r') do |f|
    body = f.read
    members.each do |member|
      if !body.include?(" #{member} ")
        puts "ERROR: #{dockerfile} does not include #{member}"
        exit 1
      end
    end
  end
end

puts "All Dockerfiles include all workspace members"
