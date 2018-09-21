import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
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
 *  第一个reduce的输出
 * * 九阳_78347837 2
 *  * 九阳_29738422 1
 *  然后这个MyMapper2要做的就是计算'九阳'在哪些微博中出现过 出现的次数DF
 *  即 :
 *  九阳 2  （在两个微博中出现了）
 *
 */
public class MyMapper2 extends Mapper<LongWritable, Text,Text, IntWritable> {

    Logger logger = LoggerFactory.getLogger(MyMapper2.class);

    //这是第二个mapper 接受第一个reduce的输出作为输入 用来统计DF:该词在哪些微博中出现过 计算出现的总数
    //MyMapper1 : 统计每个词在该条微博中出现的次数（TF）
    //MyMapper1 : 统计微博总条数 N
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fs = (FileSplit) context.getInputSplit();
        //因为00003是是count的文件
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
