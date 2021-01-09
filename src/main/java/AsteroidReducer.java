import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class AsteroidReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {

        double maxDiameter = Integer.MIN_VALUE;

        for (DoubleWritable value : values) {
            maxDiameter = Math.max(maxDiameter, value.get());
        }
        context.write(key, new DoubleWritable(maxDiameter));
    }
}