package org.lzh.plsi;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputs;

public class Reduce extends MapReduceBase implements Reducer<Text,DoubleWritable,Text,DoubleWritable> {
	
	private  MultipleOutputs mos;
	private OutputCollector<Text,DoubleWritable> collectors;
	
	public void configure(JobConf conf) {
		mos = new MultipleOutputs(conf);
	}
	public void close() throws IOException {
		mos.close();
	}
	
	public void reduce(Text key, Iterator<DoubleWritable> values,
			OutputCollector<Text,DoubleWritable> output,Reporter reporter) throws IOException{
		double sum = 0;
		
		while(values.hasNext()) {
			sum += values.next().get();
		}
		if(key.toString().contains("==")){
			output.collect(key, new DoubleWritable(sum));
		}else{
			collectors = mos.getCollector("intermedia", reporter);
			collectors.collect(key, new DoubleWritable(sum));
		}
		
	}
}
