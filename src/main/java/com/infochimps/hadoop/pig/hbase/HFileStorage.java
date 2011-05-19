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

//
// This can work. Note: 1. Need to run a total sort over the whole
// data set. 2. Need to sort the keyvalues before writing. It may be
// that simply calling ORDER with no other arguments in the pig script
// itself will be fine.
//
public class HFileStorage extends StoreFunc {

    protected RecordWriter writer = null;
    private String tableURI;
    private byte[] tableName;
    private byte[] columnFamily;
    private String[] columnNames;
    
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
    }

    public OutputFormat getOutputFormat() throws IOException {
        HFileOutputFormat outputFormat = new HFileOutputFormat();
        return outputFormat;
    }

    /**
       Whoa baby.
     */
    public void setStoreLocation(String location, Job job) throws IOException {
        FileOutputFormat.setOutputPath(job, new Path(location));
    }

    public void prepareToWrite(@SuppressWarnings("rawtypes") RecordWriter writer) throws IOException {
        this.writer = writer;
    }

    @SuppressWarnings("unchecked")
    public void putNext(Tuple t) throws IOException {
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

    // @SuppressWarnings("unchecked")
    // public void putNext(Tuple t) throws IOException {
    //     try {
    //         byte[] rowKey = Bytes.toBytes(t.get(0).toString());
    //         ImmutableBytesWritable hbaseRowKey = new ImmutableBytesWritable(rowKey);
    //             
    //         long ts = System.currentTimeMillis();                
    //         for (int i = 1; i < t.size(); i++) {
    //             if (!t.isNull(i)) {
    //                 byte[] columnName = Bytes.toBytes(columnNames[i-1]);
    //                 byte[] value = Bytes.toBytes(t.get(i).toString());
    //                 KeyValue kv = new KeyValue(rowKey, columnFamily, columnName, ts, value);
    //                 writer.write(hbaseRowKey, kv);                    
    //             }
    //         }
    //     } catch (Exception e) {
    //         throw new RuntimeException(e);
    //     }
    // }

    private TreeSet<KeyValue> sortedKeyValues(byte[] rowKey, DataBag columns) throws IOException {
        TreeSet<KeyValue> map = new TreeSet<KeyValue>(KeyValue.COMPARATOR);
        long ts = System.currentTimeMillis();
        int idx = 0;
        for (Tuple column : columns) {
            byte[] columnName = Bytes.toBytes(columnNames[idx]);
            System.out.println(column.get(0).toString());
            byte[] value = Bytes.toBytes(column.get(0).toString());
            System.out.println("Index is ["+idx+"]");
            KeyValue kv = new KeyValue(rowKey, columnFamily, columnName, ts, value);
            map.add(kv.clone());
            idx += 1;
        }
        return map;
    }
}
