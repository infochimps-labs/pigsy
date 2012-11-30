/*
 * Copyright 2010 LinkedIn, Inc
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
 
package pigsy.pig.json;

import java.io.IOException;
//
import datafu.pig.util.SimpleEvalFunc;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;

/**
 * UDF which converts a JSON-encoded string to its expanded self.
 *
 * The input string <pre>"I\u00f1t\u00ebrn\u00e2ti\u00f4n\u00e0liz\u00e6ti\u00f8n"</pre>
 * (note the quotes!) yields <pre>Iñtërnâtiônàlizætiøn</pre>
 */
public class JsonStrToString extends SimpleEvalFunc<String> {
  private JsonFactory jsonFactory = new JsonFactory();
  
  public String call(String val) throws IOException {
    if (val == null) return null;

    JsonParser jsonParser = jsonFactory.createJsonParser(val);
    if (jsonParser.nextToken() == JsonToken.END_OBJECT) return "";
    String data = jsonParser.getText();

    if (jsonParser.nextToken() != null) {
      System.out.println("Too much JSON in string: " + val);
      return null;
    }

    jsonParser.close();
    return data;
  }
}
