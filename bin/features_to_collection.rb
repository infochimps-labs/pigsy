#!/usr/bin/env ruby


collection = "{\"type\":\"FeatureCollection\", \"features\":["
features   = []
$stdin.each{|feature| features << feature.strip }
collection += features.join(",")
collection += "]}"
puts collection
