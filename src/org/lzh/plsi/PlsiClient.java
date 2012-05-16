package org.lzh.plsi;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableMapReduceUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapred.lib.ChainReducer;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class PlsiClient extends Configured implements Tool{
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf = HBaseConfiguration.create();
		
		JobConf job = new JobConf(conf,PlsiClient.class);
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		/*Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("history"));
		scan.setCaching(500);
		scan.setCacheBlocks(false);*/
		TableMapReduceUtil.initTableMapJob("UT","history",MapClass1.class,Text.class,Text.class,job);
				
		JobConf map1Conf = new JobConf(false);
		ChainMapper.addMapper(job,MapClass1.class,ImmutableBytesWritable.class,Result.class,Text.class,Text.class,true,map1Conf);
		
		JobConf map2Conf = new JobConf(false);
		ChainMapper.addMapper(job,MapClass2.class,Text.class,Text.class,Text.class,DoubleWritable.class,true,map2Conf);
		
		JobConf reducerConf = new JobConf(false);
		MultipleOutputs.addNamedOutput(reducerConf,"intermedia",TextOutputFormat.class,Text.class,DoubleWritable.class);
		//FileOutputFormat.setOutputPath(reducerConf,new Path("/user/lzh/intertemp"));
		ChainReducer.setReducer(job,Reduce.class,Text.class,DoubleWritable.class,Text.class,DoubleWritable.class,true,reducerConf);
		
		//job.setNumReduceTasks(0);
		JobClient.runJob(job);
		return 0;
	}
	
	public static void main(String[] args) throws Exception{
		int res = ToolRunner.run(new Configuration(), new PlsiClient(), args);
		System.exit(res);
	}
}
