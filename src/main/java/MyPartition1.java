import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * Created by: ccong
 * Date: 18/9/16 下午10:15
 */
public class MyPartition1 extends HashPartitioner<Text, IntWritable> {

    //numReduceTasks 在MyJob1中被设置成了4
    //key为count的分到一组
    //其余的平均分配到0 1 2 三个MyJob组
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {
        if(key.toString().equals("count")) {
            return 3;
        } else {
            return super.getPartition(key, value,numReduceTasks-1);
        }
    }
}
