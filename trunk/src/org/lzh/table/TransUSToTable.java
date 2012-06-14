package org.lzh.table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.lzh.table.GenTable;

/**
 *  把u.base中每个uer对item的评分转换成0 , 1. 并写到HBase中 
 * @author lzh
 *
 */
public class TransUSToTable extends Configured implements Tool {

	public static class MapClass extends Mapper<LongWritable,Text,Text,Text> {

		protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split("	");
			context.write(new Text(line[0]),new Text(line[1]+"	"+line[2]));
		}
		
	}
	
	public static class Reduce extends TableReducer<Text,Text,ImmutableBytesWritable>{
		
		protected void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException {
			float sum = 0;
			List<String> valueList = new ArrayList<String>();
			for(Text val : values){
				String[] line = val.toString().split("	");
				sum += Integer.parseInt(line[1].toString());
				
				valueList.add(val.toString());
			}
			sum /= valueList.size();
			
			for(int i=0;i<valueList.size();i++){
				String[] line = valueList.get(i).split("	");
				if(Integer.parseInt(line[1].toString()) >= sum){
					Put put = new Put(Bytes.toBytes("u"+key.toString()));
					put.add(Bytes.toBytes("story"),Bytes.toBytes("s"+line[0]),Bytes.toBytes("s"+line[0]));
					context.write(new ImmutableBytesWritable(Bytes.toBytes("u"+key.toString())),put);
				}
			}
		}
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf = HBaseConfiguration.create(conf);
		
		Job job = new Job(conf,"Transform data to talbe");
		job.setJarByClass(TransUSToTable.class);
		
		FileInputFormat.setInputPaths(job,new Path(args[0]));
		
		job.setMapperClass(MapClass.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		TableMapReduceUtil.initTableReducerJob(GenTable.UT,Reduce.class,job);
		
		System.exit(job.waitForCompletion(true)?0:1);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),new TransUSToTable(),args);
		System.exit(res);
	}
}
