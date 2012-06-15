package org.lzh.table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 把u.base中每个uer对item的评分转换成0 , 1. 并写到文件中
 * @author lzh
 *
 */
public class TransUSToFile extends Configured implements Tool {

	public static class MapClass extends Mapper<LongWritable,Text,Text,Text> {

		protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split("	");
			context.write(new Text(line[0]),new Text(line[1]+"	"+line[2]));
		}
		
	}
	
	public static class Reduce extends Reducer<Text,Text,Text,Text>{
		
		//private MultipleOutputs<Text,Text> mos;
		
		protected void setup(Context context) throws IOException,InterruptedException {
			//mos = new MultipleOutputs<Text,Text>(context);
		}
		
		protected void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException {
			float sum = 0;
			List<String> valueList = new ArrayList<String>();
			for(Text val : values){
				String[] line = val.toString().split("	");
				sum += Integer.parseInt(line[1].toString());
				
				valueList.add(val.toString());
			}
			sum /= valueList.size();
			//mos.write(key, new Text(sum+""),"/user/lzh/average.ua");
			
			for(int i=0;i<valueList.size();i++){
				String[] line = valueList.get(i).split("	");
				if(Integer.parseInt(line[1].toString()) >= sum){
					context.write(key,new Text(line[0]));
				}
			}
		}
		
		protected void cleanup(Context context) throws IOException,InterruptedException {
			//mos.close();
		}
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		
		Job job = new Job(conf,"Transform data to file");
		job.setJarByClass(TransUSToFile.class);
		
		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		job.setMapperClass(MapClass.class);
		job.setReducerClass(Reduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true)?0:1);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),new TransUSToFile(),args);
		System.exit(res);
	}
}
