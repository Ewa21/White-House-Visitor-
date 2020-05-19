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

public class VisitorSearcher extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new VisitorSearcher(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(this.getConf(), "Visitor Searcher");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setMapperClass(VisitorFilterMap.class);
		job.setReducerClass(VisitorFilterReduce.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setJarByClass(VisitorSearcher.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}


	public static class VisitorFilterMap extends Mapper<Object, Text, Text, IntWritable> {
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
				StringBuilder newKey = new StringBuilder();
				newKey.append(data[0]);
				newKey.append(" ");
				newKey.append(data[1]);
				newKey.append(" ");
				newKey.append(data[2]);
				newKey.append(";");
				newKey.append(" ");
				newKey.append(data[19]);
				newKey.append(" ");
				newKey.append(data[20]);
				context.write(new Text(newKey.toString()), new IntWritable(1));
		}
	}

	public static class VisitorFilterReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
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
			for (Map.Entry<Integer, String> entry : buffor.entrySet()) {
				context.write(new Text(entry.getValue()), new IntWritable(entry.getKey()));
			}
		}

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			buffor.put(sum, key.toString());
			if(buffor.size() > N) {
				buffor.remove(buffor.firstKey());
			}
		}
	}
}
