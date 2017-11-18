import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;


import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TweetMapperReplJoinMapper extends Mapper<Object, Text, Text, IntWritable> {

	private Hashtable<String, String> athleteInfo;
	Enumeration athleteNames;
	String key;
	

	private Text athleteName = new Text();
	private IntWritable one = new IntWritable(1);

	public void map(Object key, Text line, Context context) throws IOException, InterruptedException {

try{
		String[] fields = line.toString().split(";");

		if(fields.length == 4) {
			int tweet_length = fields[2].length();
			 if(tweet_length <= 140) {
				 if(tweet_length > 0) {

		// here we match the two tables in the joins

		String tweets = fields[2];
		String tweet = tweets.toString();

		athleteNames = athleteInfo.keys();

		while (athleteNames.hasMoreElements()) {
				     key =  athleteNames.nextElement();
				     if(tweet.contains(key.toString())) {
					 athleteName.set(new Text(key.toString() + ""));
					     context.write(athleteName, one);
				}
			}
		}
	 }
	}
		} catch(Exception e) {
					return;
				}
}


	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		athleteInfo = new Hashtable<String, String>();

		// We know there is only one cache file, so we only retrieve that URI
		URI fileUri = context.getCacheFiles()[0];

		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataInputStream in = fs.open(new Path(fileUri));

		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String line = null;
		try {
			// we discard the header row
			br.readLine();

			while ((line = br.readLine()) != null) {
				context.getCounter(CustomCounters.NUM_ATHLETES).increment(1);

				String[] fields = line.split(",");
				// Fields are: 1:Athlete 7:Sports
				if (fields.length >= 9)
					athleteInfo.put(fields[1], fields[7]);
			}
			br.close();
		} catch (IOException e1) {
		}

		super.setup(context);
	}
}
