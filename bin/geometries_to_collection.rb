#!/usr/bin/env jruby

require 'java'

here = File.dirname(__FILE__)
Dir["#{here}/../**/*.jar", "/usr/lib/pig/**/*.jar"].each{|jar| require jar}

java_import 'com.infochimps.hadoop.pig.geo.GeoFeature'
java_import 'com.infochimps.hadoop.pig.geo.QuadKeyUtils'
java_import 'org.mapfish.geo.MfGeometry'
java_import 'org.json.JSONObject'

#
# Actually need to make two collections
#
$stdin.each do |line|
  quadkey, pig_bag = line.strip.split("\t")

  collection  = "{\"type\":\"FeatureCollection\", \"features\":["
  serialized_features = []

  # add all the geometries in the bag to the collection
  serialized_features << pig_bag.gsub(/\{\(|\)\}/, '').split("),(")

  # now add the bounding box of the quadkey
  box      = QuadKeyUtils.quadKeyToBox(quadkey)
  geometry = MfGeometry.new(box);
  feature  = GeoFeature.new(quadkey, geometry, JSONObject.new("{\"quadkey\":\""+quadkey+"\"}"))

  collection += serialized_features.join(",")
  collection += "]}"

  puts [feature.serialize, collection].join("\t")
end
