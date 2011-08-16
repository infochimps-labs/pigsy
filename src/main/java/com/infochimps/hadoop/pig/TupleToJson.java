package com.infochimps.hadoop.pig;

import java.io.IOException;
import java.util.Properties;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.util.WrappedIOException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.util.UDFContext;

/**
   Takes a pig tuple and transforms it into a simple json hash. It's important that the tuple contain only simple types, eg
   no tuples, bags, or maps.
   <p>
   This UDF will interrogate the schema for the tuple to determine the field names. Thus the only argument is the tuple to
   jsonize itself eg:
   <code>
   A = LOAD 'data' AS (a:chararray, b:int);
   B = FOREACH A GENERATE TupleToJson(a) AS jsonized;
   </code>
   for just the named field(s)
   <p>
   OR
   <code>
   B = FOREACH A GENERATE TupleToJson(*) AS jsonized;
   </code>
   for all fields
   <p>
   Warning: Strings are not escaped. Therefor, if you want them escaped properly you'll have to do that ahead of time.
 */
public class TupleToJson extends EvalFunc<String> {
    protected Schema schema_;

    private static String UDFCONTEXT_SCHEMA_KEY = "tupletojson.input_field_names";
    private static String DELIM = ",";
    
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() < 1)
            return null;

        UDFContext context = UDFContext.getUDFContext();
        Properties property = context.getUDFProperties(TupleToJson.class);
        String fieldString = property.getProperty(UDFCONTEXT_SCHEMA_KEY);
        String [] fieldNames = fieldString.split(DELIM);
        
        StringBuffer jsonBuf = new StringBuffer();
        jsonBuf.append("{");
        for(int i = 0; i < input.size(); i++) {
            try {
                                                
                byte type = DataType.findType(input.get(i));
                switch (type) {
                    case DataType.BYTEARRAY:
                    case DataType.CHARARRAY:
			jsonBuf.append("\"");
                        jsonBuf.append(fieldNames[i]);
                        jsonBuf.append("\":\"");
                        jsonBuf.append(input.get(i));
                        jsonBuf.append("\"");
                        break;
                    case DataType.DOUBLE:
                    case DataType.FLOAT:
                    case DataType.INTEGER:
                    case DataType.LONG:
			jsonBuf.append("\"");
                        jsonBuf.append(fieldNames[i]);
                        jsonBuf.append("\":");
                        jsonBuf.append(input.get(i));
                        break;
                    case DataType.NULL:
                      if (input.size()>1) {
                    	jsonBuf.append("\"");
                        jsonBuf.append(fieldNames[i]);
                        jsonBuf.append("\":");
                        jsonBuf.append("null");
                      } else {
                        return null;
                      }
                      break;
                }
		if( i != input.size()-1 ) {
                    jsonBuf.append(",");
                }

            } catch (Exception e) {
                log.warn("Failed to process input; error - " + e.getMessage());
                return null;
            }
        }
        jsonBuf.append("}");
        return jsonBuf.toString();
    }

    /**
       Black magic to get the field names.
     */
    public Schema outputSchema(Schema input) {
        StringBuilder builder = new StringBuilder();
        List<Schema.FieldSchema> fields = input.getFields();
        if (fields.size()==1 && fields.get(0).schema != null) {
            fields = fields.get(0).schema.getFields();
        }
        for (int i=0; i<fields.size(); i++) {
            builder.append(fields.get(i).alias);
            if (i != fields.size()-1) {
                builder.append(DELIM);
            }
        }

        UDFContext context = UDFContext.getUDFContext();
        Properties property = context.getUDFProperties(TupleToJson.class);
        String fieldList = property.getProperty(UDFCONTEXT_SCHEMA_KEY);
        
        if (fieldList==null) property.setProperty(UDFCONTEXT_SCHEMA_KEY, builder.toString());
        
        return super.outputSchema(input);
    }
}
