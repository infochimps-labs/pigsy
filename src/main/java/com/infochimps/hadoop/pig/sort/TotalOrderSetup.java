package com.infochimps.hadoop.pig.sort;

import java.io.IOException;
import java.net.URI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.pig.builtin.PigStorage;

// We should use the ones provided with hadoop 0.21 when we upgrade
import com.infochimps.hadoop.partition.InputSampler;          
import com.infochimps.hadoop.partition.TotalOrderPartitioner;

/**
   Needs for serious documentation
 */
public class TotalOrderSetup extends PigStorage {

    InputSampler.Sampler<LongWritable, Text> sampler = null;
    private static final String HAS_BEEN_SETUP = "pig.total.order.has_been_setup";
    
    public void setLocation(String location, Job job) throws IOException {
        super.setLocation(location, job);
       
        if (sampler == null && job.getConfiguration().get(HAS_BEEN_SETUP) == null) {
            

            Path partitionFile = new Path(location, "_partitions");
        
            sampler = new InputSampler.RandomSampler<LongWritable, Text>(0.1,10000,10);
            TotalOrderPartitioner.setPartitionFile(job.getConfiguration(), partitionFile);
            
            try {
                InputSampler.writePartitionFile(job, sampler);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            try {
                URI partitionURI = new URI(partitionFile.toString() + "#_partitions");
                DistributedCache.addCacheFile(partitionURI, job.getConfiguration());
                DistributedCache.createSymlink(job.getConfiguration());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            job.getConfiguration().set(HAS_BEEN_SETUP, "true");
        }
    }
}
