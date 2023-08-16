package flightanalysis;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlightDelayAnalysis {
    public static class FlightDelayMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text airline = new Text();
        private IntWritable departureDelay = new IntWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            
          
            if (line.startsWith("YEAR")) {
                return;
            }
            
            String[] attributes = line.split(",");
            
            if (attributes.length >= 24) {
                String airlineName = attributes[4]; 
                String delayString = attributes[11]; 

                
                if (!delayString.isEmpty()) {
                    try {
                        int delay = Integer.parseInt(delayString);

                        airline.set(airlineName);
                        departureDelay.set(delay);

                        context.write(airline, departureDelay);
                    } catch (NumberFormatException e) {
                        String errorMsg = String.format("Error parsing delay value '%s' for airline '%s'",
                                delayString, airlineName);
                        context.getCounter("FlightDelayMapper", "NumberFormatException").increment(1);
                        context.getCounter("FlightDelayMapper", "NumFormatExceptionDetails").increment(1);
                        context.getCounter("FlightDelayMapper", errorMsg).increment(1);
                    }
                }
            }
        }
    }

    public static class FlightDelayReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        private DoubleWritable averageDelay = new DoubleWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int totalDelays = 0;
            int totalFlights = 0;

            for (IntWritable value : values) {
                totalDelays += value.get();
                totalFlights++;
            }

            double avgDelay = (double) totalDelays / totalFlights;
            averageDelay.set(avgDelay);

            context.write(new Text(getAirlineName(key.toString())), averageDelay);
        }

        private String getAirlineName(String iataCode) {
            switch (iataCode) {
                case "AA": return "American Airlines";
                case "AS": return "Alaska Airlines";
                case "B6": return "JetBlue Airways";
                case "DL": return "Delta Airlines";
                case "F9": return "Frontier Airlines";
                case "HA": return "Hawaiian Airlines";
                case "NK": return "Spirit Airlines";
                case "UA": return "United Airlines";
                case "VX": return "Virgin America";
                case "WN": return "Southwest Airlines";
                case "MQ": return "Mesa Airlines";
                case "OO": return "SkyWest Airlines";
                case "EV": return "ExpressJet Airlines";
                case "US": return "US Airways Inc.";
                default: return iataCode;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Flight Delay Analysis");

        job.setJarByClass(FlightDelayAnalysis.class);
        job.setMapperClass(FlightDelayMapper.class);
        job.setReducerClass(FlightDelayReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
