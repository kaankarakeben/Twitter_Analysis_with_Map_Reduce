import java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

enum CustomCounters {NUM_ATHLETES}

public class TweetMapperReplJoin {

  public static void runJob(String[] input, String output) throws Exception {


    Job job = Job.getInstance(new Configuration());
    Configuration conf = job.getConfiguration();

    job.setJarByClass(TweetMapperReplJoin.class);
    job.setMapperClass(TweetMapperReplJoinMapper.class);
    job.setReducerClass(TweetMapperReplJoinReducer.class);

    //Set number of reducers to 3
    job.setNumReduceTasks(3);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.addCacheFile(new Path("/data/medalistsrio.csv").toUri());

    Path outputPath = new Path(output);
    FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
    FileOutputFormat.setOutputPath(job, outputPath);
    outputPath.getFileSystem(conf).delete(outputPath,true);
    job.waitForCompletion(true);
  }

  public static void main(String[] args) throws Exception {
       runJob(Arrays.copyOfRange(args, 0, args.length-1), args[args.length-1]);
  }

}
