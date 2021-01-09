import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AsteroidReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int maxDiameter = Integer.MIN_VALUE;

        for (IntWritable value : values) {
            maxDiameter = Math.max(maxDiameter, value.get());
        }
        context.write(key, new IntWritable(maxDiameter));
    }
}