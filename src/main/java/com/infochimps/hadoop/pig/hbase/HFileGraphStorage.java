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
   A very simple StoreFunc for graph structured data. Given data in the following format:
   <p>
   (row_key, column_name, column_value)
   <p>
   OR
   <p>
   (row_key, column_name, column_value, timestamp)
   <p>
   HFileGraphStorage creates HFiles from the data. One HFile will be created per reduce task.
 */
public class HFileGraphStorage extends StoreFunc {

    protected RecordWriter writer = null;
    private byte[] tableName;
    private byte[] columnFamily;
    private LoadCaster caster;
    
    /**
     * Constructor. Construct a HFile StoreFunc to write data out as HFiles.
     * @param tN The HBase table name the data will ultimately wind up in. It does not need to exist ahead of time.
     * @param cF The HBase column family name for the table the data will wind up it. It does not need to exist ahead of time.
     */
    public HFileGraphStorage(String tN, String cF) {
        this.tableName    = Bytes.toBytes(tN);
        this.columnFamily = Bytes.toBytes(cF);
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

    @SuppressWarnings("unchecked")
    public void putNext(Tuple t) throws IOException {
        try {
            if (t.size() >= 3 && !t.isNull(0) && !t.isNull(1) && !t.isNull(2)) {
                Long ts = System.currentTimeMillis();
                if (t.size()==4) ts = (Long)t.get(3);
                byte[] rowKey = Bytes.toBytes(t.get(0).toString());
                ImmutableBytesWritable hbaseRowKey = new ImmutableBytesWritable(rowKey);
                
                byte[] columnName = Bytes.toBytes(t.get(1).toString());
                byte[] value = objToBytes(t.get(2), DataType.findType(t.get(2))); // only need to cast the value
                
                KeyValue kv = new KeyValue(rowKey, columnFamily, columnName, ts, value);
                writer.write(hbaseRowKey, kv);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
