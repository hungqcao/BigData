package pair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import common.MyPair;

public class PairApp extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new PairApp(), args);
		System.exit(res);

	}

	@Override
	public int run(String[] arg0) throws Exception {

//		if (arg0.length != 2) {
//			System.out.println("usage: [input] [output]");
//			System.exit(-1);
//		}
//
//		Path inputPath = new Path(arg0[0]);
//		Path outputPath = new Path(arg0[1]);
//		
//		// Create configuration
//		Configuration conf = new Configuration(true);
//
//		// Create job
//		Job job = Job.getInstance(conf);
//		job.setJarByClass(PairApp.class);
//
//		// Setup MapReduce
//		job.setMapperClass(PairMapper.class);
//		job.setReducerClass(PairReducer.class);
//		job.setPartitionerClass(PairPartitioner.class);
//
//		// Specify key / value
//		job.setOutputKeyClass(MyPair.class);
//		job.setOutputValueClass(FloatWritable.class);
//		job.setMapOutputKeyClass(MyPair.class);
//		job.setMapOutputValueClass(IntWritable.class);
//
//		// Input
//		FileInputFormat.addInputPath(job, inputPath);
//		job.setInputFormatClass(TextInputFormat.class);
//
//		// Output
//		FileOutputFormat.setOutputPath(job, outputPath);
//		job.setOutputFormatClass(TextOutputFormat.class);
//
//		// Delete output if exists
//		FileSystem hdfs = FileSystem.get(conf);
//		if (hdfs.exists(outputPath))
//			hdfs.delete(outputPath, true);
//
//		// Execute job
//		int code = job.waitForCompletion(true) ? 0 : 1;
		
//		return code;
		return 0;
	}

}
