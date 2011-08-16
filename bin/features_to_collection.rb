#!/usr/bin/env ruby
features = ARGV[0].nil? ? STDIN.read.split("\n") : File.read(ARGV[0])
puts "{'type':'FeatureCollection','features':[#{features.map { |geojson| geojson.strip}.join(',')}]}"
