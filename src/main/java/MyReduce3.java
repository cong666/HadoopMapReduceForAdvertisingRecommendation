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
 * 计算DF:某个词汇在所有微博中出现的次数
 * 九阳 3
 */
public class MyReduce3 extends Reducer<Text, IntWritable,Text,IntWritable> {
    Logger logger = LoggerFactory.getLogger(MyReduce3.class);

    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<IntWritable> value = values.iterator();

        int sum = 0;
        while(value.hasNext()) {
            IntWritable v = value.next();
            sum = sum+v.get();
        }
        //计算DF
        context.write(key,new IntWritable(sum));
    }
}
