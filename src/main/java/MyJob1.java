import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by: ccong
 * Date: 18/9/15 下午10:54
 */
public class MyJob1 {
    private static Logger logger = LoggerFactory.getLogger(MyJob1.class);
    public static void main(String args[]) {
        logger.info("Pub job1 is called");
        Configuration config = new Configuration();
        try {
            Job job = Job.getInstance(config);
            job.setJobName("Pub job1");
            job.setJarByClass(MyJob1.class);
            job.setMapperClass(MyMapper1.class);
            job.setReducerClass(MyReduce1.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            //分4个reduce
            job.setNumReduceTasks(4);
            job.setPartitionerClass(MyPartition1.class);
            //提前创建input文件夹
            FileInputFormat.addInputPath(job,new Path("/usr/file/mr/pub/input"));
            //注意的是不要创建output文件夹
            FileOutputFormat.setOutputPath(job,new Path("/usr/file/mr/pub/output1"));
            //System.exit(job.waitForCompletion(true) ? 0 : 1);
            boolean f = job.waitForCompletion(true);
            if(f) {
                logger.info("Pub job1 finished successfully");
            }

        }catch(Exception e) {
            System.out.println("error occured");
            e.printStackTrace();
        }
    }
}
