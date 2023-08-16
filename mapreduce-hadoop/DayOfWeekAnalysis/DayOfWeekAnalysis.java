package flightanalysis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DayOfWeekAnalysis {
    public static class DayOfWeekMapper extends Mapper<LongWritable, Text, IntWritable, FloatWritable> {
        private IntWritable dayOfWeek = new IntWritable();
        private FloatWritable arrivalDelay = new FloatWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            if (line.startsWith("YEAR")) {
                return;
            }

            String[] attributes = line.split(",");

            if (attributes.length >= 24) {
                try {
                    int day = Integer.parseInt(attributes[3]);
                    float delay = Float.parseFloat(attributes[12]);

                    dayOfWeek.set(day);
                    arrivalDelay.set(delay);

                    context.write(dayOfWeek, arrivalDelay);

                    context.getCounter("DayOfWeekMapper", "TotalDelays_" + day).increment((int) delay);
                    context.getCounter("DayOfWeekMapper", "TotalFlights_" + day).increment(1);
                } catch (NumberFormatException e) {
                 
                    context.getCounter("DayOfWeekMapper", "InvalidRecords").increment(1);
                }
            }
        }
    }

    public static class DayOfWeekReducer extends Reducer<IntWritable, FloatWritable, Text, NullWritable> {
        private Text output = new Text();

        public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {
            float totalDelays = 0;
            int totalFlights = 0;

            for (FloatWritable value : values) {
                totalDelays += value.get();
                totalFlights++;
            }

            float avgDelay = totalDelays / totalFlights;
            String outputText = "Day of Week: " + key.toString() + "\tAverage Delay: " + avgDelay;
            output.set(outputText);

            context.write(output, NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Day of Week Analysis");

        job.setJarByClass(DayOfWeekAnalysis.class);
        job.setMapperClass(DayOfWeekMapper.class);
        job.setReducerClass(DayOfWeekReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
