import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class RedditDateMapper
    extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        
        // store the line
        String line = value.toString();

        // compile regular expressions to get the date
        String pattern = "\\d{4}-\\d{2}-\\d{2}";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(line);
        String date = m.group(0);

        // extract the comment score
        String pattern = "-??\\d+\\.\\d+";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(line);
        double score = Double.parseDouble(m.group(0));

        context.write(new Text(date), new DoubleWritable(score));
    }
}
