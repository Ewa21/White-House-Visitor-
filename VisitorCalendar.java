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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class VisitorCalendar extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new VisitorCalendar(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(this.getConf(), "Visitor Calendar");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setMapperClass(VisitorFilterMap.class);
		job.setReducerClass(VisitorFilterReduce.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(VisitorCalendar.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}


	public static class VisitorFilterMap extends Mapper<Object, Text, Text, Text> {
		Integer N;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {

			Configuration conf = context.getConfiguration();

			this.N = conf.getInt("N", 10);
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] data = line.split(",");
			if(!data[11].equals("")) {
				StringBuilder newKey = new StringBuilder();
				newKey.append(data[0]);
				newKey.append(" ");
				newKey.append(data[1]);
				newKey.append(" ");
				newKey.append(data[2]);
				context.write(new Text(newKey.toString()), new Text(data[11]));
			}
		}
	}

	public static class VisitorFilterReduce extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			Date first = null;
			Date last = null;
			Integer counter=0;
			SimpleDateFormat frmtIn = new SimpleDateFormat("MM/dd/yyyy HH:mm");
			for (Text val : values) {
				try {
					Date appointmentDate = frmtIn.parse(val.toString());
					if((first==null) || appointmentDate.before(first)){
						first = appointmentDate;
					}
					if((last==null) || appointmentDate.after(last)){
						last = appointmentDate;
					}
					counter++;
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
			StringBuilder output = new StringBuilder();
			output.append(" ");
			output.append(first);
			output.append(" ");
			output.append(last);
			output.append(" ");
			output.append(counter);
			context.write(key, new Text(output.toString()));

		}
	}
}
