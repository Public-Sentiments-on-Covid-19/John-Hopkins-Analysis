import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class CleanMapper
    extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        
        // store the line
        String line = value.toString();

        // skip if it is the header line
        if (!line.split(",")[2].equals("Lat")){

            // compile regular expression to seperate information
            String pattern = "[A-Za-z][\"*)]?,[0-9\\-]";
            Pattern r = Pattern.compile(pattern);
            Matcher m = r.matcher(line);
            if (m.find()){
                String text = m.group();

                // reformat location data so that it does not contain commas 
                int pos = line.indexOf(text) + text.indexOf(",");
                String location = line.substring(0, pos);
                location = location.replace(",", "-");
                String values = line.substring(pos + 1);

                context.write(new Text(location), new Text(values));
            }
        }
    }
}
