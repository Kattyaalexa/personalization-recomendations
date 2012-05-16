package org.lzh.plsi;

import java.io.IOException;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableMap;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MapClass1 extends MapReduceBase implements TableMap<Text,Text> {

	public void map(ImmutableBytesWritable key,Result values,OutputCollector<Text,Text> output,Reporter reporter) 
			throws IOException{
		for(KeyValue kv : values.raw()){
			String value = Bytes.toString(kv.getQualifier());
			output.collect(new Text(Bytes.toString(key.get())), new Text(value));
		}
	}
	
}