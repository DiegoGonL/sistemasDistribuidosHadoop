import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AsteroidMapper extends Mapper<LongWritable, Text, Text, IntWritable> {


    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();

        //Dataset and the value of it columns comes from here: https://cneos.jpl.nasa.gov/sentry/
        String[] asteroid = line.split(",");

        String id = asteroid[0];
        int year = Integer.parseInt(asteroid[1]);
        int diameter = Integer.parseInt(asteroid[6]);

        if (year>=2021 && year <=2031){
            context.write(new Text(id), new IntWritable(diameter));
        }
    }
}
