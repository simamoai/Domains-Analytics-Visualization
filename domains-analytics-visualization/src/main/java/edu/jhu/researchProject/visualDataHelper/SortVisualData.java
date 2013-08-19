package edu.jhu.researchProject.visualDataHelper;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.MultithreadedMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

public class SortVisualData extends Configured {

	public static enum MyCounter {
		TOTAL_RECORDS_PROCESSED_IN_MAP
	}

	public void run() throws IOException, InterruptedException,
			ClassNotFoundException {
		Job job = new Job();

		job.setJarByClass(SortVisualData.class);

		job.setJobName("Sort Visual Data");

		FileInputFormat.setInputPaths(job, new Path("src/main/resources/data"));
		FileOutputFormat.setOutputPath(job, new Path("src/main/resources/visualData"));

		job.setMapperClass(VisualDataMapper.class);
		job.setReducerClass(Reducer.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(100);

		MultithreadedMapper.setMapperClass(job, VisualDataMapper.class);
		MultithreadedMapper.setNumberOfThreads(job, 1000);
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}
