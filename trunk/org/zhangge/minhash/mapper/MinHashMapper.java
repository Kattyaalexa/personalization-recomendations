package org.zhangge.minhash.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MinHashMapper extends Mapper<IntWritable, Text, Text, Text> {

	@Override
	protected void map(IntWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
	}

	
}
