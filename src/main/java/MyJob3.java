import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;

import java.net.URI;

/**
 * Created by: ccong
 * Date: 18/9/15 下午10:54
 */
public class MyJob3 {
    private static Logger logger = LoggerFactory.getLogger(MyJob3.class);
    public static void main(String args[]) {
        logger.info("Pub job3 is called");
        Configuration config = new Configuration();
        try {
            Job job = Job.getInstance(config);
            job.setJobName("Pub job3");
            job.setJarByClass(MyJob3.class);
            job.setMapperClass(MyMapper3.class);
            job.setReducerClass(MyReduce3.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            //Use the output of Job1 and Job2 as the input of this Job3
            job.addCacheFile(new URI("/usr/file/mr/pub/output1/part-r-00003"));//N
            //Map2 generate only one file with DF
            //set the output file as the cache file for job3
            job.addCacheFile(new URI("/usr/file/mr/pub/output2/part-r-00000"));//DF

            //pass the TF file generated by the Map1 : word_134343 2
            FileInputFormat.addInputPath(job,new Path("/usr/file/mr/pub/output1"));
            //Don't create the output folder
            FileOutputFormat.setOutputPath(job,new Path("/usr/file/mr/pub/output3"));
            boolean f = job.waitForCompletion(true);
            if(f) {
                logger.info("Pub job3 finished successfully");
            }

        }catch(Exception e) {
            System.out.println("error occured");
            e.printStackTrace();
        }
    }
}
