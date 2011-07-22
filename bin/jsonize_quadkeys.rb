#!/usr/bin/env jruby

#
# Reads quadkeys from stdin and serializes their bounding boxes as a
# geoJSON FeatureCollection
#

require 'java'

here = File.dirname(__FILE__)
Dir["#{here}/../**/*.jar", "/usr/lib/pig/**/*.jar"].each{|jar| require jar}

java_import 'com.infochimps.hadoop.pig.geo.GeoFeature'
java_import 'com.infochimps.hadoop.pig.geo.QuadKeyUtils'
java_import 'org.mapfish.geo.MfGeometry'
java_import 'org.json.JSONObject'

collection = "{\"type\":\"FeatureCollection\", \"features\":["
serialized_features = $stdin.map do |quadkey|
  quadkey.strip!
  box      = QuadKeyUtils.quadKeyToBox(quadkey)
  geometry = MfGeometry.new(box);
  feature  = GeoFeature.new(quadkey, geometry, JSONObject.new("{\"quadkey\":\""+quadkey+"\"}"))
  feature.serialize
end
collection += serialized_features.join(",")
collection += "]}"
puts collection
