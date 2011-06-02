/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.infochimps.hadoop.pig.hbase;

import java.io.File;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.HashMap;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadPushDown;
import org.apache.pig.LoadStoreCaster;
import org.apache.pig.OrderedLoadFunc;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.BagFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;

import org.apache.pig.backend.hadoop.hbase.HBaseBinaryConverter;

import com.infochimps.hadoop.util.HadoopUtils;
import com.infochimps.hadoop.pig.hbase.HBaseTableInputFormat.HBaseTableIFBuilder;
import com.google.common.collect.Lists;

/**
 * A HBase implementation of LoadFunc and StoreFunc.
 * <P>
 * Below is an example showing how to load data from HBase:
 * <pre>{@code
 * raw = LOAD 'hbase://SampleTable'
 *       USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
 *       'info:first_name info:last_name friends:* info:*', '-loadKey true -limit 5')
 *       AS (id:bytearray, first_name:chararray, last_name:chararray, friends_map:map[], info_map:map[]);
 * }</pre>
 * This example loads data redundantly from the info column family just to
 * illustrate usage. Note that the row key is inserted first in the result schema.
 * To load only column names that start with a given prefix, specify the column
 * name with a trailing '*'. For example passing <code>friends:bob_*</code> to
 * the constructor in the above example would cause only columns that start with
 * <i>bob_</i> to be loaded.
 * <P>
 * Below is an example showing how to store data into HBase:
 * <pre>{@code
 * copy = STORE raw INTO 'hbase://SampleTableCopy'
 *       USING org.apache.pig.backend.hadoop.hbase.HBaseStorage(
 *       'info:first_name info:last_name friends:* info:*')
 *       AS (info:first_name info:last_name buddies:* info:*);
 * }</pre>
 * Note that STORE will expect the first value in the tuple to be the row key.
 * Scalars values need to map to an explicit column descriptor and maps need to
 * map to a column family name. In the above examples, the <code>friends</code>
 * column family data from <code>SampleTable</code> will be written to a
 * <code>buddies</code> column family in the <code>SampleTableCopy</code> table.
 * 
 */
public class StaticFamilyStorage extends LoadFunc implements StoreFuncInterface, LoadPushDown, OrderedLoadFunc {
    
    private static final Log LOG = LogFactory.getLog(StaticFamilyStorage.class);

    private final static String STRING_CASTER = "UTF8StorageConverter";
    private final static String BYTE_CASTER = "HBaseBinaryConverter";
    private final static String CASTER_PROPERTY = "pig.hbase.caster";
    private final static String ASTERISK = "*";
    private final static String COLON = ":";
    private final static String ONE = "1";
    
    private List<ColumnInfo> columnInfo_ = Lists.newArrayList();
    private HTable m_table;
    private Configuration m_conf;
    private RecordReader reader;
    private RecordWriter writer;
    private HBaseTableOutputFormat outputFormat = null;    
    private Scan scan;
    private String contextSignature = null;

    private final CommandLine configuredOptions_;
    private final static Options validOptions_ = new Options();
    private final static CommandLineParser parser_ = new GnuParser();
    
    private boolean loadRowKey_;
    private final long limit_;
    private final int maxTableSplits_;
    private final int tsField_;
    private final int caching_;
    private final String hbaseConfig_;
    
    protected transient byte[] gt_;
    protected transient byte[] gte_;
    protected transient byte[] lt_;
    protected transient byte[] lte_;

    private LoadCaster caster_;

    private ResourceSchema schema_;
    private RequiredFieldList requiredFieldList;
    private boolean initialized = false;

    private static final String HAS_BEEN_UPLOADED = "hbase.config.has_been_uploaded";
    private static final String HBASE_CONFIG_HDFS_PATH = "/tmp/hbase/hbase-site.xml"; // this will be overwritten    
    private static final String LOCAL_SCHEME = "file://";
    
    private static void populateValidOptions() { 
        validOptions_.addOption("loadKey", false, "Load Key");
        validOptions_.addOption("gt", true, "Records must be greater than this value " +
                "(binary, double-slash-escaped)");
        validOptions_.addOption("lt", true, "Records must be less than this value (binary, double-slash-escaped)");   
        validOptions_.addOption("gte", true, "Records must be greater than or equal to this value");
        validOptions_.addOption("lte", true, "Records must be less than or equal to this value");
        validOptions_.addOption("caching", true, "Number of rows scanners should cache");
        validOptions_.addOption("limit", true, "Per-region limit");
        validOptions_.addOption("maxTableSplits", true, "Input splits (one per region) are combined until the total number of splits is less than maxTableSplits. A good heuristic is num_hadoop_machines*min((max_zookeeper_connections/max_map_tasks_per_machine),(max_zookeeper_connections/max_reduce_tasks_per_machine))");
        validOptions_.addOption("timestamp_field", true, "Zero based index of the field to use as the timestamp");
        validOptions_.addOption("config", true, "Full path to local hbase-site.xml");
        validOptions_.addOption("caster", true, "Caster to use for converting values. A class name, " +
                "HBaseBinaryConverter, or Utf8StorageConverter. For storage, casters must implement LoadStoreCaster.");
    }

    /**
     * Constructor. Construct a HBase Table LoadFunc and StoreFunc to load or store the cells of the
     * provided columns.
     * 
     * @param columnList
     *        columnlist that is a presented string delimited by space. To
     *        retreive all columns in a column family <code>Foo</code>, specify
     *        a column as either <code>Foo:</code> or <code>Foo:*</code>. To fetch
     *        only columns in the CF that start with <I>bar</I>, specify
     *        <code>Foo:bar*</code>. The resulting tuple will always be the size
     *        of the number of tokens in <code>columnList</code>. Items in the
     *        tuple will be scalar values when a full column descriptor is
     *        specified, or a bag of column descriptors to values when a column
     *        family is specified.
     *
     * @throws ParseException when unable to parse arguments
     * @throws IOException 
     */
    public StaticFamilyStorage(String columnList) throws ParseException, IOException {
        this(columnList,"");
    }

    /**
     * Constructor. Construct a HBase Table LoadFunc and StoreFunc to load or store. 
     * @param columnList
     * @param optString Loader options. Known options:<ul>
     * <li>-loadKey=(true|false)  Load the row key as the first column
     * <li>-gt=minKeyVal
     * <li>-lt=maxKeyVal 
     * <li>-gte=minKeyVal
     * <li>-lte=maxKeyVal
     * <li>-limit=numRowsPerRegion max number of rows to retrieve per region
     * <li>-caching=numRows  number of rows to cache (faster scans, more memory).
     * </ul>
     * @throws ParseException 
     * @throws IOException 
     */
    public StaticFamilyStorage(String columnList, String optString) throws ParseException, IOException {
        populateValidOptions();
        String[] colNames = columnList.split(" ");
        String[] optsArr  = optString.split(" ");
        try {
            configuredOptions_ = parser_.parse(validOptions_, optsArr);
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "[-config] [-loadKey] [-gt] [-gte] [-lt] [-lte] [-columnPrefix] [-caching] [-caster] [-limit] [-timestamp_field] [-maxTableSplits]", validOptions_ );
            throw e;
        }

        loadRowKey_ = configuredOptions_.hasOption("loadKey");  
        for (String colName : colNames) {
            columnInfo_.add(new ColumnInfo(colName));
        }

        m_conf = HBaseConfiguration.create();

        String defaultCaster = m_conf.get(CASTER_PROPERTY, STRING_CASTER);
        String casterOption = configuredOptions_.getOptionValue("caster", defaultCaster);
        if (STRING_CASTER.equalsIgnoreCase(casterOption)) {
            caster_ = new Utf8StorageConverter();
        } else if (BYTE_CASTER.equalsIgnoreCase(casterOption)) {
            caster_ = new HBaseBinaryConverter();
        } else {
            try {
              caster_ = (LoadCaster) PigContext.instantiateFuncFromSpec(casterOption);
            } catch (ClassCastException e) {
                LOG.error("Configured caster does not implement LoadCaster interface.");
                throw new IOException(e);
            } catch (RuntimeException e) {
                LOG.error("Configured caster class not found.", e);
                throw new IOException(e);
            }
        }

        hbaseConfig_ = configuredOptions_.getOptionValue("config", "/etc/hbase/conf/hbase-site.xml");
        caching_ = Integer.valueOf(configuredOptions_.getOptionValue("caching", "1000"));
        limit_   = Long.valueOf(configuredOptions_.getOptionValue("limit", "-1"));
        tsField_ = Integer.valueOf(configuredOptions_.getOptionValue("timestamp_field", "-1"));
        maxTableSplits_ = Integer.valueOf(configuredOptions_.getOptionValue("maxTableSplits", "100"));
        initScan();	    
    }

    private void initScan() {
        scan = new Scan();
        // Set filters, if any.
        if (configuredOptions_.hasOption("gt")) {
            gt_ = Bytes.toBytesBinary(Utils.slashisize(configuredOptions_.getOptionValue("gt")));
            addRowFilter(CompareOp.GREATER, gt_);
        }
        if (configuredOptions_.hasOption("lt")) {
            lt_ = Bytes.toBytesBinary(Utils.slashisize(configuredOptions_.getOptionValue("lt")));
            addRowFilter(CompareOp.LESS, lt_);
        }
        if (configuredOptions_.hasOption("gte")) {
            gte_ = Bytes.toBytesBinary(Utils.slashisize(configuredOptions_.getOptionValue("gte")));
            addRowFilter(CompareOp.GREATER_OR_EQUAL, gte_);
        }
        if (configuredOptions_.hasOption("lte")) {
            lte_ = Bytes.toBytesBinary(Utils.slashisize(configuredOptions_.getOptionValue("lte")));
            addRowFilter(CompareOp.LESS_OR_EQUAL, lte_);
        }

        // apply any column filters
        FilterList allColumnFilters = null;
        for (ColumnInfo colInfo : columnInfo_) {
            if (colInfo.isColumnMap() && colInfo.getColumnPrefix() != null) {

                // all column family filters roll up to one parent OR filter
                if (allColumnFilters == null) {
                    allColumnFilters = new FilterList(FilterList.Operator.MUST_PASS_ONE);
                }

                if (LOG.isInfoEnabled()) {
                    LOG.info("Adding family:prefix filters with values " +
                        Bytes.toString(colInfo.getColumnFamily()) + COLON +
                        Bytes.toString(colInfo.getColumnPrefix()));
                }

                // each column family filter consists of a FamilyFilter AND a PrefixFilter
                FilterList thisColumnFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
                thisColumnFilter.addFilter(new FamilyFilter(CompareOp.EQUAL,
                        new BinaryComparator(colInfo.getColumnFamily())));
                thisColumnFilter.addFilter(new ColumnPrefixFilter(
                        colInfo.getColumnPrefix()));

                allColumnFilters.addFilter(thisColumnFilter);
            }
        }

        if (allColumnFilters != null) {
            addFilter(allColumnFilters);
        }
    }
    
    private void addRowFilter(CompareOp op, byte[] val) {
        if (LOG.isInfoEnabled()) {
            LOG.info("Adding filter " + op.toString() +
                    " with value " + Bytes.toStringBinary(val));
        }
        addFilter(new RowFilter(op, new BinaryComparator(val)));
    }

    private void addFilter(Filter filter) {
        FilterList scanFilter = (FilterList) scan.getFilter();
        if (scanFilter == null) {
            scanFilter = new FilterList();
        }
        scanFilter.addFilter(filter);
        scan.setFilter(scanFilter);
    }

    @Override
    public Tuple getNext() throws IOException {
        try {
            if (!initialized) {
                Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass(),
                        new String[] {contextSignature});

                String projectedFields = p.getProperty(contextSignature+"_projectedFields");
                if (projectedFields != null) {
                    requiredFieldList = (RequiredFieldList) ObjectSerializer.deserialize(projectedFields);
                    pushProjection(requiredFieldList);
                }
                initialized = true;
            }
            if (reader.nextKeyValue()) {
                ImmutableBytesWritable rowKey = (ImmutableBytesWritable)reader.getCurrentKey();
                Result result = (Result)reader.getCurrentValue();

                int tupleSize = columnInfo_.size();

                // use a map of families -> qualifiers with the most recent
                // version of the cell. Fetching multiple vesions could be a
                // useful feature.
                NavigableMap<byte[], NavigableMap<byte[], byte[]>> resultsMap = result.getNoVersionMap();

                if (loadRowKey_){
                    tupleSize++;
                }
                Tuple tuple=TupleFactory.getInstance().newTuple(tupleSize);

                int startIndex=0;
                if (loadRowKey_){
                    tuple.set(0, new DataByteArray(rowKey.get()));
                    startIndex++;
                }
                
                for (int i = 0;i < columnInfo_.size(); ++i){
                    int currentIndex = startIndex + i;

                    ColumnInfo columnInfo = columnInfo_.get(i);
                    if (columnInfo.isColumnMap()) {
                        // It's a column family so we need to iterate and set all
                        // values found
                        NavigableMap<byte[], byte[]> cfResults = resultsMap.get(columnInfo.getColumnFamily());
                        DataBag bagged_family = BagFactory.getInstance().newDefaultBag();
                        
                        if (cfResults != null) {
                            for (byte[] quantifier : cfResults.keySet()) {
                                // We need to check against the prefix filter to
                                // see if this value should be included. We can't
                                // just rely on the server-side filter, since a
                                // user could specify multiple CF filters for the
                                // same CF.
                                if (columnInfo.getColumnPrefix() == null || columnInfo.hasPrefixMatch(quantifier)) {

                                    byte[] cell         = cfResults.get(quantifier);
                                    DataByteArray value = cell == null ? null : new DataByteArray(cell);

                                    Tuple entry = TupleFactory.getInstance().newTuple(2);
                                    entry.set(0, Bytes.toString(quantifier));
                                    entry.set(1, value);
                                    bagged_family.add(entry);
                                }
                            }
                        }
                        tuple.set(currentIndex, bagged_family);
                    } else {
                        // It's a column so set the value                      
                        if (result.containsColumn(columnInfo.getColumnFamily(), columnInfo.getColumnName())) {
                            byte[] cell=result.getValue(columnInfo.getColumnFamily(),
                                                        columnInfo.getColumnName());
                            DataByteArray value =
                                (cell == null || cell.length == 0) ? new DataByteArray(ONE) : new DataByteArray(cell);
                            tuple.set(currentIndex, value);                            
                        } else {
                            tuple.set(currentIndex, null);
                        }
                    }
                }

                if (LOG.isDebugEnabled()) {
                    for (int i = 0; i < tuple.size(); i++) {
                        LOG.debug("tuple value:" + tuple.get(i));
                    }
                }
                return tuple;
            }
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
        return null;
    }

    @Override
    public InputFormat getInputFormat() {      
        TableInputFormat inputFormat = new HBaseTableIFBuilder()
            .withLimit(limit_)
            .withMaxSplits(maxTableSplits_)
            .withGt(gt_)
            .withGte(gte_)
            .withLt(lt_)
            .withLte(lte_)
            .withConf(m_conf)
            .build();
        return inputFormat;
    }

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) {
        this.reader = reader;
    }

    @Override
    public void setUDFContextSignature(String signature) {
        this.contextSignature = signature;
    }

    @Override
    public void setLocation(String location, Job job) throws IOException {
        m_conf = job.getConfiguration();

        HBaseConfiguration.addHbaseResources(m_conf);
        if (m_conf.get(HAS_BEEN_UPLOADED) == null) {
            HadoopUtils.uploadLocalFile(new Path(LOCAL_SCHEME+hbaseConfig_), new Path(HBASE_CONFIG_HDFS_PATH), m_conf);
            HadoopUtils.shipIfNotShipped(new Path(HBASE_CONFIG_HDFS_PATH), m_conf);
            m_conf.set(HAS_BEEN_UPLOADED, "true");
        }
        String taskConfig = HadoopUtils.fetchFromCache((new File(hbaseConfig_)).getName(), m_conf);
        if (taskConfig == null) taskConfig = hbaseConfig_;
        m_conf.addResource(new Path(LOCAL_SCHEME+taskConfig));
                
        TableMapReduceUtil.addDependencyJars(job.getConfiguration(), 
            org.apache.hadoop.hbase.client.HTable.class,
            com.google.common.collect.Lists.class,
            org.apache.zookeeper.ZooKeeper.class);

        String tablename = location;
        if (location.startsWith("hbase://")){
           tablename = location.substring(8);
        }
        if (m_table == null) {
            m_table = new HTable(m_conf, tablename);
        }
        m_table.setScannerCaching(caching_);
        m_conf.set(TableInputFormat.INPUT_TABLE, tablename);

        // Set up scan if it is not already set up.
        if (m_conf.get(TableInputFormat.SCAN) != null) {
            return;
        }

        for (ColumnInfo columnInfo : columnInfo_) {
            // do we have a column family, or a column?
            if (columnInfo.isColumnMap()) {
                scan.addFamily(columnInfo.getColumnFamily());
            }
            else {
                scan.addColumn(columnInfo.getColumnFamily(),
                               columnInfo.getColumnName());
            }

        }
        if (requiredFieldList != null) {
            Properties p = UDFContext.getUDFContext().getUDFProperties(this.getClass(),
                    new String[] {contextSignature});
            p.setProperty(contextSignature + "_projectedFields", ObjectSerializer.serialize(requiredFieldList));
        }
        m_conf.set(TableInputFormat.SCAN, convertScanToString(scan));
    }

    @Override
    public String relativeToAbsolutePath(String location, Path curDir)
    throws IOException {
        return location;
    }

    private static String convertScanToString(Scan scan) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(out);
            scan.write(dos);
            return Base64.encodeBytes(out.toByteArray());
        } catch (IOException e) {
            LOG.error(e);
            return "";
        }

    }

    /**
     * Set up the caster to use for reading values out of, and writing to, HBase. 
     */
    @Override
    public LoadCaster getLoadCaster() throws IOException {
        return caster_;
    }
    
    /*
     * StoreFunc Methods
     * @see org.apache.pig.StoreFuncInterface#getOutputFormat()
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

    // Suppressing unchecked warnings for RecordWriter, which is not parameterized by StoreFuncInterface
    @Override
    public void prepareToWrite(@SuppressWarnings("rawtypes") RecordWriter writer) throws IOException {
        this.writer = writer;
    }

    // Suppressing unchecked warnings for RecordWriter, which is not parameterized by StoreFuncInterface
    @SuppressWarnings("unchecked")
    @Override
    public void putNext(Tuple t) throws IOException {
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
        Put put=new Put(objToBytes(t.get(0), 
                (fieldSchemas == null) ? DataType.findType(t.get(0)) : fieldSchemas[0].getType()));
        long ts = System.currentTimeMillis();

        // Allow for custom timestamp
        if (tsField_!=-1) {
            try {
                ts = Long.valueOf(t.get(tsField_).toString());
            } catch (Exception e) {
                ts = System.currentTimeMillis();
            }
        }
        
        if (LOG.isDebugEnabled()) {
            for (ColumnInfo columnInfo : columnInfo_) {
                LOG.debug("putNext -- col: " + columnInfo);
            }
        }

        for (int i=1;i<t.size();++i){
            ColumnInfo columnInfo = columnInfo_.get(i-1);
            if (LOG.isDebugEnabled()) {
                LOG.debug("putNext - tuple: "+i+", value="+t.get(i)+", cf:column="+columnInfo);
            }
            
            if (!columnInfo.isColumnMap()) {
                if ((columnInfo.getColumnFamily() != null) && (columnInfo.getColumnName() != null) && !t.isNull(i)) {
                    put.add(columnInfo.getColumnFamily(), columnInfo.getColumnName(),
                            ts, objToBytes(t.get(i), (fieldSchemas == null) ?
                                           DataType.findType(t.get(i)) : fieldSchemas[i].getType()));
                }
            } else {
                Map<String, Object> cfMap = (Map<String, Object>) t.get(i);
                for (String colName : cfMap.keySet()) {
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("putNext - colName="+colName+", class: "+colName.getClass());
                    }
                    // TODO deal with the fact that maps can have types now. Currently we detect types at
                    // runtime in the case of storing to a cf, which is suboptimal.
                    if ((columnInfo.getColumnFamily() != null) && (colName != null) && (cfMap.get(colName) != null)) {
                        put.add(columnInfo.getColumnFamily(), Bytes.toBytes(colName.toString()), ts,
                            objToBytes(cfMap.get(colName), DataType.findType(cfMap.get(colName))));
                    }
                }
            }
        }

        try {
            if (!put.isEmpty()) { // Don't try to write a row with 0 columns
                writer.write(null, put);
            }
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }
    
    @SuppressWarnings("unchecked")
    private byte[] objToBytes(Object o, byte type) throws IOException {
        LoadStoreCaster caster = (LoadStoreCaster) caster_;
        if (o == null) return null;
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
            HadoopUtils.uploadLocalFile(new Path(LOCAL_SCHEME+hbaseConfig_), new Path(HBASE_CONFIG_HDFS_PATH), m_conf);
            HadoopUtils.shipIfNotShipped(new Path(HBASE_CONFIG_HDFS_PATH), m_conf);
            m_conf.set(HAS_BEEN_UPLOADED, "true");
        }
    }

    @Override
    public void cleanupOnFailure(String location, Job job) throws IOException {
    }

    /*
     * LoadPushDown Methods.
     */
    
    @Override
    public List<OperatorSet> getFeatures() {
        return Arrays.asList(LoadPushDown.OperatorSet.PROJECTION);
    }

    @Override
    public RequiredFieldResponse pushProjection(
            RequiredFieldList requiredFieldList) throws FrontendException {
        List<RequiredField>  requiredFields = requiredFieldList.getFields();
        List<ColumnInfo> newColumns = Lists.newArrayListWithExpectedSize(requiredFields.size());

        // colOffset is the offset in our columnList that we need to apply to indexes we get from requiredFields
        // (row key is not a real column)
        int colOffset = loadRowKey_ ? 1 : 0;
        // projOffset is the offset to the requiredFieldList we need to apply when figuring out which columns to prune.
        // (if key is pruned, we should skip row key's element in this list when trimming colList)
        int projOffset = colOffset;
        this.requiredFieldList = requiredFieldList;

        if (requiredFieldList != null && requiredFields.size() > (columnInfo_.size() + colOffset)) {
            throw new FrontendException("The list of columns to project from HBase is larger than StaticFamilyStorage is configured to load.");
        }

        if (loadRowKey_ &&
                ( requiredFields.size() < 1 || requiredFields.get(0).getIndex() != 0)) {
                loadRowKey_ = false;
            projOffset = 0;
            }
        
        for (int i = projOffset; i < requiredFields.size(); i++) {
            int fieldIndex = requiredFields.get(i).getIndex();
            newColumns.add(columnInfo_.get(fieldIndex - colOffset));
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("pushProjection After Projection: loadRowKey is " + loadRowKey_) ;
            for (ColumnInfo colInfo : newColumns) {
                LOG.debug("pushProjection -- col: " + colInfo);
        }
        }
        columnInfo_ = newColumns;
        return new RequiredFieldResponse(true);
    }

    @Override
    public WritableComparable<InputSplit> getSplitComparable(InputSplit split)
            throws IOException {
        return new WritableComparable<InputSplit>() {
            TableSplit tsplit = new TableSplit();

            @Override
            public void readFields(DataInput in) throws IOException {
                tsplit.readFields(in);
            }

            @Override
            public void write(DataOutput out) throws IOException {
                tsplit.write(out);
            }

            @Override
            public int compareTo(InputSplit split) {
                return tsplit.compareTo((TableSplit) split);
            }
        };
    }

    /**
     * Class to encapsulate logic around which column names were specified in each
     * position of the column list. Users can specify columns names in one of 4
     * ways: 'Foo:', 'Foo:*', 'Foo:bar*' or 'Foo:bar'. The first 3 result in a
     * Map being added to the tuple, while the last results in a scalar. The 3rd
     * form results in a prefix-filtered Map.
     */
    private class ColumnInfo {

        final String originalColumnName;  // always set
        final byte[] columnFamily; // always set
        final byte[] columnName; // set if it exists and doesn't contain '*'
        final byte[] columnPrefix; // set if contains a prefix followed by '*'

        public ColumnInfo(String colName) {
            originalColumnName = colName;
            String[] cfAndColumn = colName.split(COLON, 2);

            //CFs are byte[1] and columns are byte[2]
            columnFamily = Bytes.toBytes(cfAndColumn[0]);
            if (cfAndColumn.length > 1 &&
                    cfAndColumn[1].length() > 0 && !ASTERISK.equals(cfAndColumn[1])) {
                if (cfAndColumn[1].endsWith(ASTERISK)) {
                    columnPrefix = Bytes.toBytes(cfAndColumn[1].substring(0,
                            cfAndColumn[1].length() - 1));
                    columnName = null;
                }
                else {
                    columnName   = Bytes.toBytes(cfAndColumn[1]);
                    columnPrefix = null;
                }
            } else {
              columnPrefix = null;
              columnName   = null;
            }
        }

        public byte[] getColumnFamily() { return columnFamily; }
        public byte[] getColumnName()   { return columnName; }
        public byte[] getColumnPrefix() { return columnPrefix; }
        public boolean isColumnMap()    { return columnName == null; }
        
        public boolean hasPrefixMatch(byte[] qualifier) {
            return Bytes.startsWith(qualifier, columnPrefix);
        }

        @Override
        public String toString() { return originalColumnName; }
    }

}
