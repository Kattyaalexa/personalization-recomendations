package org.lzh.newplsi;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class NewClient extends Configured implements Tool{
	
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf = HBaseConfiguration.create(conf);
		
		Job job = new Job(conf,"plsi");
		job.setJarByClass(NewClient.class);
		//FileOutputFormat.setOutputPath(job,new Path(args[1]));
		conf.set("intermedia",args[0]);
		DistributedCache.addCacheFile(new Path(args[0]).toUri(),job.getConfiguration());
		
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("clusters"));
		scan.addFamily(Bytes.toBytes("history"));
		
		TableMapReduceUtil.initTableMapperJob(Bytes.toBytes("UT"),
																											scan,
																											MapClass.class, 
																											Text.class, 
																											DoubleWritable.class,
																											job);
		TableMapReduceUtil.initTableReducerJob("UT",
																												Reduce.class,
																												job);
		//job.setReducerClass(Reduce.class);
		System.exit(job.waitForCompletion(true)?0:1);
		return 0;
	}

	public static void main(String[] args) throws Exception{
		int res = ToolRunner.run(new Configuration(), new NewClient(), args);
		System.exit(res);
	}
}
