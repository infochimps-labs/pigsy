package com.infochimps.hadoop.pig.hbase;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;

import org.apache.pig.LoadStoreCaster;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadPushDown;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Utils;

import com.google.common.collect.Lists;

/**
   A pig storage class for writing HFiles (native hbase files) to the hdfs. In order for this
   work first GROUP the data by row key, project the row key out of the resulting columns bag
   with a FOREACH..GENERATE, and finally, ORDER the result by the row key (ascending). Since
   the ordering must be lexigraphic it is <b>very</b> important that the row key be a Pig chararray.
 */
public class HFileStorage extends StoreFunc {

    protected RecordWriter writer = null;
    private String tableURI;
    private byte[] tableName;
    private byte[] columnFamily;
    private String[] columnNames;
    private LoadCaster caster;
    
    /**
     * Constructor. Construct a HFile StoreFunc to write data out as HFiles. These
     * HFiles will then have to be imported with the hbase/bin/loadtable.rb tool. 
     * @param tN The HBase table name the data will ultimately wind up in. It does not need to exist ahead of time.
     * @param cF The HBase column family name for the table the data will wind up it. It does not need to exist ahead of time.
     * @param columnNames A comma separated list of column names descibing the fields in a tuple.
     */
    public HFileStorage(String tN, String cF, String names) {
        this.tableName    = Bytes.toBytes(tN);
        this.columnFamily = Bytes.toBytes(cF);
        this.columnNames  = names.split(",");
        this.caster = new Utf8StorageConverter();
    }

    public OutputFormat getOutputFormat() throws IOException {
        HFileOutputFormat outputFormat = new HFileOutputFormat();
        return outputFormat;
    }

    public void setStoreLocation(String location, Job job) throws IOException {
        FileOutputFormat.setOutputPath(job, new Path(location));
    }

    public void prepareToWrite(@SuppressWarnings("rawtypes") RecordWriter writer) throws IOException {
        this.writer = writer;
    }

    /**
       The function must get a tuple containing the following:
       <p>
       <ul>
       <li><b>row_key</b>: Field 0, the row key to use</li>
       <li><b>columns</b>: Field 1, a databag containing exactly one tuple. This tuple
       is assumed to be one 'record' where the positions in the tuple correspond to the
       positions in the corresponding columnNames (passed into HFileStorage constructor).
       Warning: Unless 'row_key' has an entry in the columnNames array you must project
       it out of the columns bag after doing a GROUP BY with Pig.</li>       
       </ul>
       <p>
       Each field in a 'record' is matched with its corresponding field name in the columnNames
       array. These are written into a TreeMap to maintain a lexigraphical sorting. Once all the
       fields have been written to the TreeMap it is finally written to HFileOutputFormat.

     */
    @SuppressWarnings("unchecked")
    public void putNext(Tuple t) throws IOException {
        if (t.size()==2 && !t.isNull(0) && !t.isNull(1)) {
            try {                
                byte[] rowKey = Bytes.toBytes(t.get(0).toString());
                DataBag columns = (DataBag)t.get(1);
                ImmutableBytesWritable hbaseRowKey = new ImmutableBytesWritable(rowKey);
                TreeSet<KeyValue> map = sortedKeyValues(rowKey, columns);
                for (KeyValue kv: map) {
                    writer.write(hbaseRowKey, kv);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
       Here we get a databag with one or more tuples in it. Each of these tuples is one
       record with the same row_key.
     */
    private TreeSet<KeyValue> sortedKeyValues(byte[] rowKey, DataBag columns) throws IOException {
        TreeSet<KeyValue> map = new TreeSet<KeyValue>(KeyValue.COMPARATOR);
        long ts = System.currentTimeMillis();
        for (Tuple column : columns) {
            for (int i = 0; i < column.size(); i++) {
                if (!column.isNull(i)) {
                    byte[] columnName = Bytes.toBytes(columnNames[i]);
                    byte[] value = objToBytes(column.get(i), DataType.findType(column.get(i)));
                    KeyValue kv = new KeyValue(rowKey, columnFamily, columnName, ts, value);
                    map.add(kv.clone());                    
                }
            }
        }
        return map;
    }

    @SuppressWarnings("unchecked")
    private byte[] objToBytes(Object o, byte type) throws IOException {
        LoadStoreCaster bytecaster = (LoadStoreCaster)caster;
        switch (type) {
        case DataType.BYTEARRAY: return ((DataByteArray) o).get();
        case DataType.BAG: return bytecaster.toBytes((DataBag) o);
        case DataType.CHARARRAY: return bytecaster.toBytes((String) o);
        case DataType.DOUBLE: return bytecaster.toBytes((Double) o);
        case DataType.FLOAT: return bytecaster.toBytes((Float) o);
        case DataType.INTEGER: return bytecaster.toBytes((Integer) o);
        case DataType.LONG: return bytecaster.toBytes((Long) o);
        
            // The type conversion here is unchecked. 
            // Relying on DataType.findType to do the right thing.
        case DataType.MAP: return bytecaster.toBytes((Map<String, Object>) o);
        
        case DataType.NULL: return null;
        case DataType.TUPLE: return bytecaster.toBytes((Tuple) o);
        case DataType.ERROR: throw new IOException("Unable to determine type of " + o.getClass());
        default: throw new IOException("Unable to find a converter for tuple field " + o);
        }
    }
}
