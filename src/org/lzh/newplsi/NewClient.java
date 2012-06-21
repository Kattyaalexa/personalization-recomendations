package org.lzh.newplsi;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.lzh.table.GenTable;

public class NewClient extends Configured implements Tool{
	
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf = HBaseConfiguration.create(conf);
		
		Job job = new Job(conf,"new plsi");
		job.setJarByClass(NewClient.class);
		//FileOutputFormat.setOutputPath(job,new Path(args[1]));
		//conf.set("intermedia",args[0]);
		DistributedCache.addCacheFile(new Path("intermediaNz").toUri(),job.getConfiguration());
		DistributedCache.addCacheFile(new Path("intermediaNsz").toUri(),job.getConfiguration());
		
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("clusters_plsi"));
		scan.addFamily(Bytes.toBytes("story"));
		
		TableMapReduceUtil.initTableMapperJob(Bytes.toBytes(GenTable.UT),
																											scan,
																											MapClass.class, 
																											Text.class, 
																											FloatWritable.class,
																											job);
		TableMapReduceUtil.initTableReducerJob(GenTable.UT,
																												Reduce.class,
																												job);
		
		job.waitForCompletion(true);
		//System.exit(job.waitForCompletion(true)?0:1);
		return 0;
	}

	public static void main(String[] args) throws Exception{
		int[] res = new int[20];
		for(int i=0;i<res.length;i++){
			res[i] = ToolRunner.run(new Configuration(), new NewClient(), args);
		}
		System.exit(0);
		/*int res = ToolRunner.run(new Configuration(), new NewClient(), args);
		System.exit(res);*/
				
	}
}
