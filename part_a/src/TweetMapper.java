import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;
import java.lang.Object;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TweetMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final IntWritable one = new IntWritable(1);
    private Text bin_id = new Text();
    public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
        	String[] fields = line.toString().split(";");


        if(fields.length == 4) {
          double tweet_length = fields[2].length();
          if(tweet_length <= 140) {
                if(tweet_length > 0) {
		    double bin_start = Math.ceil(tweet_length/5);
		    double bin_start_1 = bin_start*5;
		    double bin_start_2 = bin_start_1-4;
		    double bin_end =  Math.ceil(tweet_length/5);
		    double bin_end_1 = bin_end*5;
		    bin_id.set(new Text(bin_start_2 + "-" + bin_end_1));
                  context.write(bin_id,one);
                }
          }
    }
  }
}
