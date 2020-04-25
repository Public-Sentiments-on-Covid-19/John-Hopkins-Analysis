import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.ArrayList;
import java.lang.Math;

public class RedditDateReducer
    extends Reducer<Text, DoubleWritable, Text, Text> {

    public static median 
    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
        throws IOException, InterruptedException {

        // store values to be calculated
        double sum = 0.0, standardDeviation = 0.0;
        int size = 0;
        double score, mean, median;

        // convert the iterable to a list
        List<Double> scores = new ArrayList<Double>();
        for (String value: values){
            score = Double.parseDouble(value.get().split(",")[0]);
            scores.add(score);

            // calculate the sum and the size
            sum += score;
            size++;
        }

        // calculate the median
        Array.sort(scores);
        if (size % 2 != 0)
            median = scores.get(size / 2);
        else
            median = (scores.get((size - 1)/ 2) + scores.get(size / 2)) / 2;

        // calculate the mean
        mean = sum / size;

        // calculate the standard deviation
        for (double score: scores){
            standardDeviation += Math.pow(score - mean, 2);
        }
        standardDeviation = Math.sqrt(standardDeviation/size);

        String line = String.format("%f, %f, %f\n", mean, median, standardDeviation);
        context.write(key, new Text(line));
    }
}
