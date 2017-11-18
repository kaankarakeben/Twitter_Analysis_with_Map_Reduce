import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.*;
import java.text.*;
import java.time.*;


public class HoursMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final IntWritable one = new IntWritable(1);

    public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
        	String[] fields = line.toString().split(";");

          if(fields.length == 4) {
            int tweet_length = fields[2].length();
            if(tweet_length <= 140) {
                  if(tweet_length > 0) {

                      try{
                          Date org_date = new Date(Long.parseLong(fields[0]));
                          SimpleDateFormat hourformat = new SimpleDateFormat("H");
                          //SimpleDateFormat dateFormat = new SimpleDateFormat("hh");
                          hourformat.setTimeZone(TimeZone.getTimeZone("Brazil/East"));
                          String hours = hourformat.format(org_date);
                          //Long hours = new Long(Long.parseLong(fields[0]));
                          //LocalDateTime localHours = LocalDateTime.ofEpochSecond(hours, 1000, ZoneOffset.ofHours(-3));
			                    //int hours = localDate.getHours();
                          context.write(new Text(hours+""),one);
                      } catch( Exception e){
                          return;
                      }

                  }
            }
          }
    }
}
