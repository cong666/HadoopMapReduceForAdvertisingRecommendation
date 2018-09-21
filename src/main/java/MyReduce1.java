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
 * Use partition to separate them into two different file
 * One file is to save the keyword of every user like : Nike_9279344397 2
 * Nike_78347837 2
 * Nike_29738422 1
 * Another file stock the 'count'
 * count 32
 */
public class MyReduce1 extends Reducer<Text, IntWritable,Text,IntWritable> {
    Logger logger = LoggerFactory.getLogger(MyReduce1.class);

    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<IntWritable> value = values.iterator();

        int sum = 0;
        while(value.hasNext()) {
            IntWritable v = value.next();
            sum = sum+v.get();
        }
        //sum the 'count' and word_id
        if(key.equals(new Text("count"))) {
            logger.info("Total count :"+sum);
            context.write(key,new IntWritable(sum));
        } else {
            context.write(key,new IntWritable(sum));
        }
    }
}
