import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class DailyChangesMapper
    extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        
        // store the line
        String[] line = value.toString().split(",");

        int day = 0, prev = 0, inc = 0;
        // iterate through the daily counts and calculate increases per day
        for (int i = 3; i < line.length; i++){
            inc = Integer.parseInt(line[i]) - prev;
            prev = Integer.parseInt(line[i]);

            context.write(new IntWritable(day++), new IntWritable(inc));
        }
    }
}
