package com.jhu.researchProject.mapReduceAnalytics;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public  class DomainContentMapper extends Mapper<ByteBuffer, SortedMap<ByteBuffer, IColumn>, Text, IntWritable>
{
	private static final Logger logger = LoggerFactory.getLogger(DomainContentMapper.class);
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    int i = 0;
    
    public void map(ByteBuffer key, SortedMap<ByteBuffer, IColumn> columns, Context context) throws IOException, InterruptedException
    {
    	logger.info("key = "+ByteBufferUtil.string(key));
    	for(Entry<ByteBuffer, IColumn> columnEntry : columns.entrySet()) {
    		logger.info("column key = "+ByteBufferUtil.string(columnEntry.getKey()));
    		logger.info("columns value = "+ByteBufferUtil.string(columnEntry.getValue().value()));
    		i++;
        StringTokenizer itr = new StringTokenizer(ByteBufferUtil.string(columnEntry.getValue().value()));
        while (itr.hasMoreTokens())
        {
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }
    }
}