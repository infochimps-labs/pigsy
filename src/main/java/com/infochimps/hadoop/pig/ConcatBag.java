package com.infochimps.hadoop.pig;

import java.io.IOException;
import java.util.Properties;
import java.util.Iterator;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.*;
import org.apache.pig.impl.util.WrappedIOException;

/**
   Takes a pig bag of 1-element tuples and returns a comma-separated string.
   ConcatBag({(a),(b),(c)}) yields "a,b,c"
*/

public class ConcatBag extends EvalFunc<String> {
    public String exec(Tuple input) throws IOException {
      if (input == null || input.size() < 1 || input.isNull(0))
            return null;
        DataBag bag = (DataBag)input.get(0);
        StringBuffer result = new StringBuffer();
        Iterator itr = bag.iterator();
        while(itr.hasNext()) {
          Tuple t = (Tuple)itr.next();
          if (!t.isNull(0)) {
            result.append(t.get(0).toString());
            if (itr.hasNext()) {
              result.append(",");
            }
          }
        }
        return result.toString();
    }
}
