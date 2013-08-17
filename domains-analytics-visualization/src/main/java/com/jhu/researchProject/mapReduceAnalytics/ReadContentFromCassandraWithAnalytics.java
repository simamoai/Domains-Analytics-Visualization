package com.jhu.researchProject.mapReduceAnalytics;

import java.util.Arrays;
import org.apache.cassandra.hadoop.ColumnFamilyInputFormat;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadContentFromCassandraWithAnalytics extends Configured {

	static final String OUTPUT_REDUCER_VAR = "output_reducer";
	private static final Logger logger = LoggerFactory
			.getLogger(ReadContentFromCassandraWithAnalytics.class);
	private static final String CONF_COLUMN_NAME = "columnname";
	static final String KEYSPACE = "webpagekeyspace";
	static final String COLUMN_FAMILY = "accessedwebpages";

	public static enum MyCounter {
		TOTAL_RECORDS_PROCESSED_IN_MAP
	}

	public int run() throws Exception {
		String outputReducerType = "filesystem";
		logger.info("output reducer type: " + outputReducerType);
		Job job = new Job(new Configuration());
		String columnName = "pageContent";
		job.getConfiguration().set(CONF_COLUMN_NAME, columnName);
		job.setJobName("ReadContentFromCassandraWithAnalytics");
		job.setJarByClass(ReadContentFromCassandraWithAnalytics.class);
		job.setMapperClass(DomainContentMapper.class);
		job.setCombinerClass(ReducerToFilesystem.class);
		job.setReducerClass(ReducerToFilesystem.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileOutputFormat.setOutputPath(job, new Path("output"));

		job.setInputFormatClass(ColumnFamilyInputFormat.class);
		ConfigHelper.setInputRpcPort(job.getConfiguration(), "9160");
		ConfigHelper
				.setInputInitialAddress(job.getConfiguration(), "localhost");
		ConfigHelper.setInputPartitioner(job.getConfiguration(),
				"org.apache.cassandra.dht.RandomPartitioner");
		ConfigHelper.setInputColumnFamily(job.getConfiguration(), KEYSPACE,
				COLUMN_FAMILY);
		SlicePredicate predicate = new SlicePredicate().setColumn_names(Arrays
				.asList(ByteBufferUtil.bytes(columnName)));
		ConfigHelper.setInputSlicePredicate(job.getConfiguration(), predicate);

		job.waitForCompletion(true);

		return 0;
	}
}
