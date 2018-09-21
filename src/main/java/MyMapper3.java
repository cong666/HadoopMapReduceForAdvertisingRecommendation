import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by: ccong
 * Date: 18/9/16 下午9:24
 *  This map is used to calculate the last result
 *
 */
public class MyMapper3 extends Mapper<LongWritable, Text,Text, Text> {

    Logger logger = LoggerFactory.getLogger(MyMapper3.class);

    public static Map<String,Integer> countMap = null;//"count" : 1213
    public static Map<String,Integer> dfMap = null;//"Nike": 23

    protected void setup(Context context) throws IOException, InterruptedException {
        if(countMap == null || countMap.size()==0 || dfMap==null || dfMap.size()==0) {
            /*
               Get 'N' cache files "/usr/file/mr/pub/output1/part-r-00003"
               and 'DF' cache file : "/usr/file/mr/pub/output1/part-r-00000"
            */
            URI[] uris = context.getCacheFiles();
            if(uris!=null) {
                for(URI uri : uris) {
                    if(uri.getPath().endsWith("part-r-00003")) {
                        FileReader fr = new FileReader(uri.getPath());
                        BufferedReader br = new BufferedReader(fr);
                        String line = br.readLine();
                        if(line.startsWith("count")) {
                            String[] arr = line.split("\t");
                            countMap = new HashMap<>();
                            countMap.put(arr[0],Integer.parseInt(arr[1].trim().toString()));
                        }
                        br.close();
                        fr.close();
                    } else if(uri.getPath().endsWith("part-r-00000")) {
                        dfMap = new HashMap<>();
                        FileReader fr = new FileReader(uri.getPath());
                        BufferedReader br = new BufferedReader(fr);
                        String line = br.readLine();
                        if(line.startsWith("count")) {
                            String[] arr = line.split("\t");

                            dfMap.put(arr[0],Integer.parseInt(arr[1].trim().toString()));
                        }
                        br.close();
                        fr.close();
                    }
                }
            }
        }
    }

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fs = (FileSplit) context.getInputSplit();
        //Read all TF file generated by Map1 (we have set up the output1 as the input path)
        if(!fs.getPath().getName().contains("part-r-00003")) {
            String line = value.toString().trim();
            //split : word_id 3
            String[] values = line.split("\t");
            if(values.length>=2) {
                int tf = Integer.parseInt(values[1].trim());
                String wordAndId = values[0];
                String[] arr = wordAndId.split("_");
                String word = arr[0];
                String userId  = arr[1];

                //calculate the weight : W = TF*Log(N/DF)
                double result = tf*Math.log(countMap.get("count")/dfMap.get(word));
                NumberFormat nf = NumberFormat.getInstance();
                nf.setMaximumFractionDigits(5);

                context.write(new Text(word),new Text(word+":"+nf.format(result)));
            }

        }
    }
}