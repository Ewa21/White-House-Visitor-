import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static java.lang.StrictMath.abs;

public class VisitorHour extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new VisitorHour(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(this.getConf(), "Visitor Hour");
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setMapperClass(VisitorFilterMap.class);
		job.setReducerClass(VisitorFilterReduce.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(VisitorHour.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}


	public static class VisitorFilterMap extends Mapper<Object, Text, IntWritable, Text> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] data = line.split(",");
			SimpleDateFormat frmtIn = new SimpleDateFormat("MM/dd/yyyy HH:mm");
			Date appointmentDate;
			Date arrivalDate;
			if(!data[11].equals("") && !data[6].equals("")) {
				try {
					appointmentDate = frmtIn.parse(data[11]);
					arrivalDate = frmtIn.parse(data[6]);
					SimpleDateFormat frmtOut = new SimpleDateFormat("HH");
					int hour = Integer.parseInt(frmtOut.format(appointmentDate));
					StringBuilder newValue = new StringBuilder();
					long delay = getDateDiff(appointmentDate, arrivalDate);
					newValue.append(delay);
					newValue.append(",");
					newValue.append(data[0]);
					newValue.append(data[1]);
					newValue.append(data[2]);
					if(Math.abs(delay) <= 24*60){
						context.write(new IntWritable(hour), new Text(newValue.toString()));
					}

				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
		}
		private long getDateDiff(Date end, Date start) {
			long diffInMillies = end.getTime() - start.getTime();
			TimeUnit tu = TimeUnit.MINUTES;
			return tu.convert(diffInMillies, TimeUnit.MILLISECONDS);
		}
	}

	public static class VisitorFilterReduce extends Reducer<IntWritable, Text, IntWritable, Text> {

		@Override
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		    DecimalFormat df2 = new DecimalFormat("#.##");
			List<String> visitorsDelayed = new ArrayList<String>();
			List<String> visitorsOnTime = new ArrayList<String>();
			Integer counter=0;
			Integer delayCounter=0;
			Integer ontimeCounter=0;
			Long averageDelay=0L;
            Long averageAheadTime=0L;

			for (Text val : values) {
				String line = val.toString();
				String[] data = line.split(",");
				Long v = Long.valueOf(data[0]);
				if (v < 0) {
//					if (!visitorsDelayed.contains(data[1])) {
//						visitorsDelayed.add(data[1]);
						delayCounter++;
						averageDelay += v;
//					}
				}
				else if(v > 0 ){
//					if (!visitorsOnTime.contains(data[1])) {
//						visitorsOnTime.add(data[1]);
						ontimeCounter++;
                    averageAheadTime+=v;
//					}
				}
				counter++;
			}
				StringBuilder output = new StringBuilder();
				output.append(counter);
				output.append("\t");
				output.append(delayCounter);
				output.append("\t");
				if (delayCounter == 0) {
					output.append(0);
				} else {
					output.append(df2.format((double) averageDelay / (double) delayCounter));
				}
				output.append("\t");
				output.append(ontimeCounter);
                output.append("\t");
                if (ontimeCounter == 0) {
                    output.append(0);
                } else {
                    output.append(df2.format((double) averageAheadTime / (double) ontimeCounter));
                }
                output.append("\t");
                double error = ((double) averageAheadTime + (double) abs(averageDelay)) / (double) counter;
                output.append(df2.format(error));
				context.write(key, new Text(output.toString()));

		}
	}
}
