import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class DailyChangesReducer
    extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

    @Override
    public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {

        // calculate the total daily increases
        int count = 0;
        for (IntWritable value: values)
            count += value.get();

        context.write(key, new IntWritable(count));
    }
}
