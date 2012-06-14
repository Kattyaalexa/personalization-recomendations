package org.lzh.table;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Hashtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.lzh.table.GenTable;

public class ComputePsz extends Configured implements Tool{
	
	public static class PszMap extends Mapper<LongWritable,Text,Text,FloatWritable> {

		private Hashtable<String,Float> hashTable = new Hashtable<String,Float>();
		
		protected void setup(Context context) throws IOException,InterruptedException {
			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
			
			if(cacheFiles!=null && cacheFiles.length>0){
				String line;
				String[] tokens;
				BufferedReader br = new BufferedReader(new FileReader(cacheFiles[0].toString()));
				
				while((line=br.readLine()) != null) {
					tokens = line.split("\t");
					hashTable.put(tokens[0],Float.parseFloat(tokens[1]));
				}
				br.close();
				System.out.println("======"+hashTable.size());
			}
		}

		protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
			float psz;
			String[] line = value.toString().split("	", 2);
			String[] sz = line[0].split("==");
			
			psz = Float.parseFloat(line[1])/hashTable.get(sz[1]);
			
			context.write(new Text(line[0]),new FloatWritable(psz));
		}
		
	}
	public static class PszReduce extends TableReducer<Text,FloatWritable,ImmutableBytesWritable>{
		
		protected void reduce(Text key,Iterable<FloatWritable> values,Context context) throws IOException,InterruptedException {
			String[] sz = key.toString().split("==");
			Put put = new Put(Bytes.toBytes(sz[0]));
			for(FloatWritable psz : values){
				put.add(Bytes.toBytes("clusters_plsi"),Bytes.toBytes(sz[1]),Bytes.toBytes(psz+""));
			}
			context.write(new ImmutableBytesWritable(Bytes.toBytes(sz[0])), put);
		}
	}
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		conf = HBaseConfiguration.create(conf);
		
		Job job = new Job(conf,"compute Psz");
		job.setJarByClass(ComputePsz.class);
		
		DistributedCache.addCacheFile(URI.create("intermediaNz"),job.getConfiguration());
		
		FileInputFormat.setInputPaths(job, new Path("intermediaNsz"));
		
		job.setMapperClass(PszMap.class);
		TableMapReduceUtil.initTableReducerJob(GenTable.ST,PszReduce.class, job);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);
		
		System.exit(job.waitForCompletion(true)?0:1);
		return 0;
	}

	public static void main(String[] args) throws Exception{
		int res = ToolRunner.run(new Configuration(),new ComputePsz(), args);
		System.exit(res);
	}
}
