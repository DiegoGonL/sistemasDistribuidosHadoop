import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AsteroidMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        //Dataset and the value of it columns comes from here: https://cneos.jpl.nasa.gov/sentry/
        String[] asteroid = line.split(",");

        String id = asteroid[0].replaceAll("\"", "");
        int minYear = Integer.parseInt(asteroid[1].replaceAll("\"", "").split("-")[0]);
        int maxYear = Integer.parseInt(asteroid[1].replaceAll("\"", "").split("-")[1]);

        Double diameter = Double.parseDouble(asteroid[6].replaceAll("\"", ""));

        if (minYear == maxYear && minYear>=2021 && minYear <=2031){
            context.write(new Text("Year [" + minYear + "]"), new DoubleWritable(diameter));
        }
    }
}
