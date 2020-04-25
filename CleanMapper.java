import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanMapper
    extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        
        String[] line = value.toString().split(",");
        
        String[] words = new String[]{line[2], line[3], line[7], line[8]};
        // Get only the columns we need
        String columns = String.join(",", words)
        context.write(new Text(line[0]), new Text(columns));
    }
}
