package edu.jhu.researchProject.visualDataHelper;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import edu.jhu.researchProject.mapReduceProcess.ExtractFromCompressedData.MyCounter;

public class VisualDataMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	private IntWritable count = new IntWritable();
	private Text words = new Text();
	
	int i = 0;

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		String[] arr = line.split(" ");
		Counter totalRecordsProcessedCounter = context.getCounter(MyCounter.TOTAL_RECORDS_PROCESSED_IN_MAP);
		if (arr.length == 2) {
			words.set(arr[0]);
			count.set(Integer.parseInt(arr[1]));
		}
		context.write(count, words);
		totalRecordsProcessedCounter.increment(1);
	}
}
