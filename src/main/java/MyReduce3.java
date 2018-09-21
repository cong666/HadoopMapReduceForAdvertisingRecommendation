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
 */
public class MyReduce3 extends Reducer<Text, Text,Text,Text> {
    Logger logger = LoggerFactory.getLogger(MyReduce3.class);

    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> value = values.iterator();

        //just output , there is no merge here
        while(value.hasNext()) {
            Text t = value.next();
            context.write(key,t);
        }
    }
}
