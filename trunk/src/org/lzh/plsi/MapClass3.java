package org.lzh.plsi;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MapClass3 extends MapReduceBase implements Mapper<Text,DoubleWritable,Text,DoubleWritable> {

	public void configure(JobConf job) {
		
	}

	public void map(Text key, DoubleWritable values,
			OutputCollector<Text, DoubleWritable> output, Reporter reporter)	throws IOException {
		
		
	}

	
}
