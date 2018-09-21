import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by: ccong
 * Date: 18/9/16 下午10:01
 *
 */
public class MyReduce2 extends Reducer<Text, IntWritable,Text,IntWritable> {
    Logger logger = LoggerFactory.getLogger(MyReduce2.class);

    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<IntWritable> value = values.iterator();

        int sum = 0;
        while(value.hasNext()) {
            IntWritable v = value.next();
            sum = sum+v.get();
        }
        //Calculate the DF
        context.write(key,new IntWritable(sum));
    }
}
