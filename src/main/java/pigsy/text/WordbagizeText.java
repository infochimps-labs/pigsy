/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pigsy.text;

import java.io.IOException;
import java.io.StringReader;
import java.util.Iterator;
import java.util.Set;
//
import java.util.HashMap;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.BagFactory;

/**
 * Wordbagize text accepts a pig Bag of disordered tokens
 *
 *     <code>tokens:{T:(token:chararray)}</code>
 *
 * and returns a tuple holding
 *
 * <ul>
 *   <li>the total count of usages</li>
 *   <li>the number of distinct terms</li>
 *   <li>the number of terms used only once</li>
 *   <li>a bag of tuples, where the first element is the count and the remainder is the correspoinding input tuple<li>
 * <ul>
 *   
 * That is,
 *
 * <code>wordbag:(tot_usages:int, num_terms:int, num_onces:int, wb:{T:(count:int,term:chararray)})</code>
 *
 * We assume your documents have fewer than 2^30 terms, so count is an int.
 *
 * <dt><b>Example:</b></dt>
 * <dd><code>
 * register varaha.jar;<br/>
 * documents    = LOAD 'documents' AS (doc_id:chararray, text:chararray);<br/>
 * tokenized    = FOREACH documents {
 *   tokens = TokenizeText(text);
 *   GENERATE
 *     doc_id AS doc_id,
 *     WordbagizeText(tokens) AS wordbag:(num_terms:int, tot_usages:int, {T:(term:chararray,count:int)});
 * };
 * </code></dd>
 *
 * <dt><b>Input</b> (13 usages of 7 terms; 5 of the terms were used once):</dt>
 * <dd><code>
 *   {(category:engineering),(companies),(england),(engineering),(companies),(england),(category:companies),(england),(industry),(category:manufacturing),(companies),(england),(england)}
 * </code></dd>
 *
 * <dt><b>Output</b></dt>
 * <dd><code>
 *   (13,7,5,{(5,england),(1,category:engineering),(1,category:companies),(1,category:manufacturing),(3,companies),(1,industry),(1,engineering)})
 * </code></dd>
 *
 * </dl>
 * 
 * @see
 * @author Philip (Flip) Kromer
 *
 */
public class WordbagizeText extends EvalFunc<Tuple> {

  private static TupleFactory tupleFactory = TupleFactory.getInstance();
  private static BagFactory   bagFactory   = BagFactory.getInstance();
  private static String       NOFIELD = "";

  public Tuple exec(Tuple input) throws IOException {
    if (input == null || input.size() < 1 || input.isNull(0)) return null;
    DataBag inputBag    = (DataBag) input.get(0);
    
    // Output bag
    DataBag wordbag     = bagFactory.newDefaultBag();
    Tuple   output      = tupleFactory.newTuple(4);
    
    // sum of counts for all terms
    int tot_usages = 0;
    // distinct terms seen
    int num_terms  = 0;
    // number of terms seen once
    int num_onces  = 0;
    // counts for each term
    Map<Tuple,Integer> elem_counts = new HashMap<Tuple,Integer>();

    try {
      // accumulate usage counts for each term
      for (Tuple elem : inputBag) {
        int count = elem_counts.containsKey(elem) ? elem_counts.get(elem) : 0;
        elem_counts.put(elem, count+1);
        tot_usages += 1;
      }
      num_terms = elem_counts.size();

      // find the count-of-terms-used-once (for smoothing)
      for (int count : elem_counts.values()) {
        if (count == 1) { num_onces += 1; }
      }
      
      // replace them into the returned bag
      for (Map.Entry<Tuple,Integer> elem_count : elem_counts.entrySet()) {
        Tuple   elem  = elem_count.getKey();
        Integer count = elem_count.getValue();
        Tuple   elfr = tupleFactory.newTuple(elem.size()+1);
        
        elfr.set(0, count);
        for(int ii=0; ii < elem.size(); ii++) { elfr.set(ii+1, elem.get(ii)); }
        
        wordbag.add(elfr);
      }
      
      output.set(0, tot_usages);
      output.set(1, num_terms);
      output.set(2, num_onces);
      output.set(3, wordbag);
      return output;
    }
    catch (Exception e) {
      throw new IOException(e);
    }
  }
}
