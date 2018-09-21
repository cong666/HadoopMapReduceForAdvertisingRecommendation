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
public class MyJob2 {
    private static Logger logger = LoggerFactory.getLogger(MyJob2.class);
    public static void main(String args[]) {
        logger.info("Pub job2 is called");
        Configuration config = new Configuration();
        try {
            Job job = Job.getInstance(config);
            job.setJobName("Pub job2");
            job.setJarByClass(MyJob2.class);
            job.setMapperClass(MyMapper2.class);
            job.setCombinerClass(MyReduce2.class);
            job.setReducerClass(MyReduce2.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            //第一个job的输出作为这个job的输入
            FileInputFormat.addInputPath(job,new Path("/usr/file/mr/pub/output1"));
            //注意的是不要创建output文件夹
            FileOutputFormat.setOutputPath(job,new Path("/usr/file/mr/pub/output2"));
            boolean f = job.waitForCompletion(true);
            if(f) {
                logger.info("Pub job2 finished successfully");
            }

        }catch(Exception e) {
            System.out.println("error occured");
            e.printStackTrace();
        }
    }
}
