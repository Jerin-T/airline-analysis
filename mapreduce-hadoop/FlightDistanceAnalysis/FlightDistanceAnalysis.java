package flightanalysis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlightDistanceAnalysis {
	public static class FlightDistanceMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private Text distanceRange = new Text();
		private IntWritable distance = new IntWritable();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();

			
			if (line.startsWith("YEAR")) {
				return;
			}

			String[] attributes = line.split(",");

			if (attributes.length >= 24) {
				int flightDistance = Integer.parseInt(attributes[17]); // Distance (index 17)
				distance.set(flightDistance);

				String range = getDistanceRange(flightDistance);
				distanceRange.set(range);

				context.write(distanceRange, distance);
			}
		}

		private String getDistanceRange(int distance) {
			if (distance < 500) {
				return "Short Haul";
			} else if ( distance > 1500) {
				return "Long Haul";
			} else {
				return "Medium Haul";
			}
		}
	}

	public static class FlightDistanceReducer extends Reducer<Text, IntWritable, Text, Text> {
	    private Text result = new Text();

	    public void reduce(Text key, Iterable<IntWritable> values, Context context)
	            throws IOException, InterruptedException {
	        long totalFlights = 0;
	        long totalDistance = 0;
	        int maxDistance = 0;

	        for (IntWritable value : values) {
	            int distance = value.get();
	            totalFlights++;
	            if (distance > 0) {
	                totalDistance += distance;
	            }
	            maxDistance = Math.max(maxDistance, distance);
	        }

	      
	        double avgDistance = (double) totalDistance / totalFlights;

	       
	        String output = String.format("Total Flights: %d, Total Distance: %d, Max Distance: %d, Avg Distance: %.2f",
	                totalFlights, totalDistance, maxDistance, avgDistance);

	       
	        result.set(output);

	   
	        context.write(key, result);
	    }
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Flight Distance Analysis");

		job.setJarByClass(FlightDistanceAnalysis.class);
		job.setMapperClass(FlightDistanceMapper.class);
		job.setReducerClass(FlightDistanceReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0] + "/flights.csv"));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
