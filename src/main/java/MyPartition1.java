import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * Created by: ccong
 * Date: 18/9/16 下午10:15
 */
public class MyPartition1 extends HashPartitioner<Text, IntWritable> {

    //numReduceTasks is set in MyJob1 : job.setNumReduceTasks(4);
    //make key = "count" is written into the last group(group 3)
    //The others are written into group 0,1,2
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {
        if(key.toString().equals("count")) {
            return 3;
        } else {
            return super.getPartition(key, value,numReduceTasks-1);
        }
    }
}
