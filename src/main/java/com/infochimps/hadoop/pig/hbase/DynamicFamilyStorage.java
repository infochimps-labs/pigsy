package com.infochimps.hadoop.pig.hbase;

import java.io.File;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Properties;

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
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.conf.Configuration;

import org.apache.pig.LoadCaster;
import org.apache.pig.StoreFunc;
import org.apache.pig.LoadPushDown;
import org.apache.pig.LoadStoreCaster;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.hadoop.hbase.HBaseBinaryConverter;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.BagFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;

import com.infochimps.hadoop.util.HadoopUtils;
import com.google.common.collect.Lists;

/**
 * Different from default HBaseStorage in that a field in the data is used as the column family name
 * and another field is used as the column name. Data must be arranged as a tuple in one of two ways:
 *
 * (row_key, column_family, column_name, column_value)
 *
 * OR
 *
 * (row_key, column_family, column_name, column_value, timestamp)
 *
 * Note that this is _only_ for storing data into and not for loading data from Hbase.
 *
 */
public class DynamicFamilyStorage extends StoreFunc implements StoreFuncInterface {
    
    private static final Log LOG = LogFactory.getLog(DynamicFamilyStorage.class);

    private final static String STRING_CASTER   = "UTF8StorageConverter";
    private final static String BYTE_CASTER     = "HBaseBinaryConverter";
    private final static String CASTER_PROPERTY = "pig.hbase.caster";
    
    private List<byte[]> columnList_ = Lists.newArrayList();
    private HTable m_table;
    private HBaseTableOutputFormat outputFormat = null;

    private Configuration m_conf;
    private RecordReader reader;
    private RecordWriter writer;
    private Scan scan;
    private String contextSignature = null;
    
    private LoadCaster caster_;
    private ResourceSchema schema_;
    private boolean initialized = false;
    private final String hbaseConfig_;

    private static final String HAS_BEEN_UPLOADED = "hbase.config.has_been_uploaded";
    private static final String HBASE_CONFIG_HDFS_PATH = "/tmp/hbase/hbase-site.xml"; // this will be overwritten
    private static final String DEFAULT_CONFIG = "/etc/hbase/conf/hbase-site.xml";
    private static final String LOCAL_SCHEME = "file://";
    
    public DynamicFamilyStorage() throws IOException {
        this(DEFAULT_CONFIG);
    }

    public DynamicFamilyStorage(String hbaseConfig) throws IOException {
        m_conf  = HBaseConfiguration.create();
        caster_ = new Utf8StorageConverter();
        hbaseConfig_ = hbaseConfig;
    }

    /**
       Since a local hadoop configuration object must be created for each hadoop
       task (that's the only way to talk to Hbase) it is necessary to re-add the
       hbase configuration each time.
     */
    @Override
    public OutputFormat getOutputFormat() throws IOException {
        if (outputFormat == null) {
            this.outputFormat = new HBaseTableOutputFormat();
            HBaseConfiguration.addHbaseResources(m_conf);
            String taskConfig = HadoopUtils.fetchFromCache((new File(hbaseConfig_)).getName(), m_conf);
            if (taskConfig == null) taskConfig = hbaseConfig_;
            m_conf.addResource(new Path(LOCAL_SCHEME+taskConfig));
            this.outputFormat.setConf(m_conf);            
        }
        return outputFormat;
    }

    @Override
    public void checkSchema(ResourceSchema s) throws IOException {
        if (! (caster_ instanceof LoadStoreCaster)) {
            LOG.error("Caster must implement LoadStoreCaster for writing to HBase.");
            throw new IOException("Bad Caster " + caster_.getClass());
        }
        schema_ = s;
    }

    @Override
    public void prepareToWrite(@SuppressWarnings("rawtypes") RecordWriter writer) throws IOException {
        this.writer = writer;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void putNext(Tuple t) throws IOException {
        if (t.size() < 4 || t.isNull(0) || t.isNull(1) || t.isNull(2) || t.isNull(3)) return;
            
        if (!initialized) {
            Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass(),
                                                                       new String[] {contextSignature});
            String serializedSchema = p.getProperty(contextSignature + "_schema");
            if (serializedSchema!= null) {
                schema_ = (ResourceSchema) ObjectSerializer.deserialize(serializedSchema);
            }
            initialized = true;
        }
        
        ResourceFieldSchema[] fieldSchemas = (schema_ == null) ? null : schema_.getFields();

        // Convert the row key to bytes properly
        byte[] rowKey = objToBytes(t.get(0), (fieldSchemas == null) ? DataType.findType(t.get(0)) : fieldSchemas[0].getType());
        
        if(rowKey != null && t.size() >= 4) {
            long ts = System.currentTimeMillis();
            Put put = new Put(rowKey);
            put.setWriteToWAL(false);
            
            byte[] family  = objToBytes(t.get(1), DataType.findType(t.get(1)));
            byte[] colName = objToBytes(t.get(2), DataType.findType(t.get(2)));
            byte[] colVal  = objToBytes(t.get(3), DataType.findType(t.get(3)));
            if (t.size() == 5) {
                try {
                    ts = Long.parseLong(t.get(4).toString());
                } catch (Exception e) {
                    ts = System.currentTimeMillis();
                }
            }
            if ((family != null) && (colName != null) && (colVal != null)) {
                put.add(family, colName, ts, colVal);
            }
            try {
                if (!put.isEmpty()) { // Don't try to write a row with 0 columns
                    writer.write(null, put);
                }
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
    }
    
    @SuppressWarnings("unchecked")
    private byte[] objToBytes(Object o, byte type) throws IOException {
        LoadStoreCaster caster = (LoadStoreCaster) caster_;
        switch (type) {
        case DataType.BYTEARRAY: return ((DataByteArray) o).get();
        case DataType.BAG: return caster.toBytes((DataBag) o);
        case DataType.CHARARRAY: return caster.toBytes((String) o);
        case DataType.DOUBLE: return caster.toBytes((Double) o);
        case DataType.FLOAT: return caster.toBytes((Float) o);
        case DataType.INTEGER: return caster.toBytes((Integer) o);
        case DataType.LONG: return caster.toBytes((Long) o);
        
        // The type conversion here is unchecked. 
        // Relying on DataType.findType to do the right thing.
        case DataType.MAP: return caster.toBytes((Map<String, Object>) o);
        
        case DataType.NULL: return null;
        case DataType.TUPLE: return caster.toBytes((Tuple) o);
        case DataType.ERROR: throw new IOException("Unable to determine type of " + o.getClass());
        default: throw new IOException("Unable to find a converter for tuple field " + o);
        }
    }

    @Override
    public String relToAbsPathForStoreLocation(String location, Path curDir)
    throws IOException {
        return location;
    }

    @Override
    public void setStoreFuncUDFContextSignature(String signature) {
        this.contextSignature = signature;
    }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        if (location.startsWith("hbase://")){
            job.getConfiguration().set(HBaseTableOutputFormat.OUTPUT_TABLE, location.substring(8));
        }else{
            job.getConfiguration().set(HBaseTableOutputFormat.OUTPUT_TABLE, location);
        }
        Properties props = UDFContext.getUDFContext().getUDFProperties(getClass(), new String[]{contextSignature});
        if (!props.containsKey(contextSignature + "_schema")) {
            props.setProperty(contextSignature + "_schema",  ObjectSerializer.serialize(schema_));
        }
        m_conf = HBaseConfiguration.addHbaseResources(job.getConfiguration());
        if (m_conf.get(HAS_BEEN_UPLOADED) == null) {
            HadoopUtils.uploadLocalFileIfChanged(new Path(LOCAL_SCHEME+hbaseConfig_), new Path(HBASE_CONFIG_HDFS_PATH), m_conf);
            HadoopUtils.shipIfNotShipped(new Path(HBASE_CONFIG_HDFS_PATH), m_conf);
            m_conf.set(HAS_BEEN_UPLOADED, "true");
        }
    }
    
    @Override
    public void cleanupOnFailure(String location, Job job) throws IOException {

    }
}
