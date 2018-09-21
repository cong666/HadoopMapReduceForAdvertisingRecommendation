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
 * 第一个job的输出
 * 使用partition将它们分到两个文件中
 * 一个文件存放单词出现的次数
 * 九阳_78347837 2
 * 九阳_29738422 1
 * 另外一个存放微博总数N
 * count 2834
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
        //此处显然可以不用if else 只是让你清楚地看到 reduce可能被调用两次 一次的key是w_id, 一次是count（微博总条数）
        if(key.equals(new Text("count"))) {
            logger.info("Total count :"+sum);
            context.write(key,new IntWritable(sum));
        } else {
            context.write(key,new IntWritable(sum));
        }
    }
}
