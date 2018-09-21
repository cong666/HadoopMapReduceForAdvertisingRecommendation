import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wltea.analyzer.IKSegmentation;
import org.wltea.analyzer.Lexeme;

import java.io.IOException;
import java.io.StringReader;

/**
 * Created by: ccong
 * Date: 18/9/16 下午9:24
 *  Output of the first reduce :
 * * Nike_78347837 2
 *   Nike_29738422 1
 *  And this Mapper2 calculate the DF :  The number of occurrences of key words in all document
 *  so here output:
 *  Nike 2
 *
 */
public class MyMapper2 extends Mapper<LongWritable, Text,Text, IntWritable> {

    Logger logger = LoggerFactory.getLogger(MyMapper2.class);

    //This is the second mapper : calculate the DF
    //MyMapper1 : Calculate the TF and N
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fs = (FileSplit) context.getInputSplit();
        //00003 stock the 'count' value
        if(!fs.getPath().getName().contains("part-r-00003")) {
            String line = value.toString().trim();
            String[] values = line.split("\t");
            if(values.length>=2) {
                String wordAndId = values[0];
                String[] arr = wordAndId.split("_");
                String word = arr[0];
                context.write(new Text(word),new IntWritable(1));
            }

        }
    }
}
