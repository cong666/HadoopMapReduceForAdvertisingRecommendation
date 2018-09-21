import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wltea.analyzer.IKSegmentation;
import org.wltea.analyzer.Lexeme;

import java.io.IOException;
import java.io.StringReader;

/**
 * Created by: ccong
 * Date: 18/9/16 下午9:24
 */
public class MyMapper1 extends Mapper<LongWritable, Text,Text, IntWritable> {

    Logger logger = LoggerFactory.getLogger(MyMapper1.class);

    //Calculate the TF (the number of occurrences of key words for every user）
    //Calculate the total number of the line :  N
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        String[] values = line.split(";");
        if(values.length==2) {
            String id = values[0];
            String content = values[1];
            StringReader sr = new StringReader(content);
            IKSegmentation iks= new IKSegmentation(sr,true);
            Lexeme word = null;
            while((word = iks.next())!=null) {
                String w = word.getLexemeText();
                //（TF : The number of occurrences of key words for every user）
                context.write(new Text(w+"_"+id),new IntWritable(1));
            }
            //Total number of the lines :  N (we use the same key to calculate the sum)
            context.write(new Text("count"),new IntWritable(1));
        }
    }
}
