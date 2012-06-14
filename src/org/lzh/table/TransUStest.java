package org.lzh.table;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TransUStest extends Configured implements Tool{

	public static class MapClass extends Mapper<LongWritable,Text,Text,Text> {
		private Hashtable<String,Float> hashTable = new Hashtable<String,Float>();

		protected void setup(Context context) throws IOException,InterruptedException {
			
			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			BufferedReader br;
			for(int i=0;i<cacheFiles.length;i++) {
				String line;
				String[] tokens;
				br = new BufferedReader(new FileReader(cacheFiles[i].toString()));
				
				while((line=br.readLine()) != null) {
					tokens = line.split("\t",2);
					hashTable.put(tokens[0],Float.parseFloat(tokens[1]));
				}
				br.close();
			}
		}
		
		protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
			String[] line = value.toString().split("	");
			if(hashTable.get(line[0])<Float.parseFloat(line[2])){
				context.write(new Text(line[0]),new Text(line[1]));
			}
		}
		
	}
	
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		
		Job job = new Job(conf,"Tranform UStest");
		job.setJarByClass(TransUStest.class);
		DistributedCache.addCacheFile(new Path("average.ua-r-00000").toUri(),job.getConfiguration());
		FileInputFormat.setInputPaths(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		
		job.setMapperClass(MapClass.class);
		job.setNumReduceTasks(0);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true)?0:1);
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),new TransUStest(),args);
		System.exit(res);
	}
}
