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

import java.util.*;

public class VisitorFilter extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new VisitorFilter(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(this.getConf(), "Visitor Filter");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setMapperClass(VisitorFilterMap.class);
		job.setReducerClass(VisitorFilterReduce.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(VisitorFilter.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}


	public static class VisitorFilterMap extends Mapper<Object, Text, Text, IntWritable> {
		Integer N;

		@Override
		protected void setup(Context context) throws IOException,InterruptedException {

			Configuration conf = context.getConfiguration();

			this.N = conf.getInt("N", 10);
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] data = line.split(",");
			if(data[6].equals("") || data[11].equals("")) {
				Random random = new Random();
				Integer x = random.nextInt();
				context.write(value, new IntWritable(x));
			}
		}
	}

	public static class VisitorFilterReduce extends Reducer<Text, IntWritable, Text, NullWritable> {
		Integer N;
		TreeMap<Integer, String> buffor;
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();

			this.N = conf.getInt("N", 10);
			this.buffor = new TreeMap<Integer,String>();
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			if(buffor.size() > 0) {
				int counter = 0;
				for (Map.Entry<Integer, String> entry : buffor.entrySet()) {
					context.write(new Text(entry.getValue()), NullWritable.get());
					counter++;
					if (counter == N) {
						break;
					}
				}
			}
		}

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			for(IntWritable v :values){
				buffor.put(Integer.valueOf(v.get()), key.toString());
			}
			if(buffor.size() == 100){
				int counter =0;
				for (Map.Entry<Integer, String> entry : buffor.entrySet()) {
					context.write(new Text(entry.getValue()), NullWritable.get());
					counter++;
					if(counter == N){
						break;
					}
				}
				buffor = null;
				buffor = new TreeMap<Integer,String>();
			}
		}
	}
}
