import java.io.IOException;
import java.util.*;
import java.lang.*;
import java.text.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class HashtagMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final IntWritable one = new IntWritable(1);
    private Text data = new Text();

    public void map(Object key, Text line, Context context) throws IOException, InterruptedException {
        String[] fields = line.toString().split(";");

        if(fields.length == 4) {
          int tweet_length = fields[2].length();
          if(tweet_length <= 140) {
                if(tweet_length > 0) {

                  try{
                       Date org_date = new Date(Long.parseLong(fields[0]));
                       SimpleDateFormat hourformat = new SimpleDateFormat("H");
                       hourformat.setTimeZone(TimeZone.getTimeZone("Brazil/East"));
                       String hours = hourformat.format(org_date);

                       if(hours.equals("22")) {        
			   StringTokenizer st = new StringTokenizer(fields[2].toString().toLowerCase(), "-- \t\n\r\f,.:;?![]'\"");
                                 while(st.hasMoreTokens()) {
                                   String tokens = st.nextToken().toString();
                                   if (tokens.matches("#\\w+")) {
                                     data.set(tokens);
                                     context.write(data, one); 
                                   }
                                }
                            }       
                                     
                      } catch( Exception e){
                                              return;
                                                      }
                          }                              
                        }
                      }
                    }
                  }