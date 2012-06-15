package org.zhangge.minhash;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.zhangge.CommonUtil;
import org.zhangge.minhash.mapper.MinHashMapper;
import org.zhangge.minhash.random.SeedGenerator;
import org.zhangge.minhash.reducer.MinHashReducer;


public class MinHashClient {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		SeedGenerator SG = new SeedGenerator();//首先生成一下随机数种子
		SG.generateSeeds(CommonUtil.MinHash_p * CommonUtil.MinHash_q, CommonUtil.filepath + CommonUtil.seedvalue);
		MinHashClient mhc = new MinHashClient();
		mhc.run();
	}
	
	/**
	 * 调用minhash算法把用户分集群
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ClassNotFoundException
	 */
	public void run() throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		conf = HBaseConfiguration.create(conf);
		Job job = new Job(conf, "MinHashClient");
		job.setJarByClass(MinHashClient.class);
		//FileSystem fs = FileSystem.get(conf);
		//fs.copyFromLocalFile(false, true, new Path(args[0]+"SeedValues"),new Path("/MinHash/SeedValues"));
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(CommonUtil.UT_Family1));
		TableMapReduceUtil.initTableMapperJob(CommonUtil.UT,
												scan,
												MinHashMapper.class, 
												ImmutableBytesWritable.class,
												ImmutableBytesWritable.class,
												job);
		TableMapReduceUtil.initTableReducerJob(CommonUtil.UT,
												MinHashReducer.class,
												job);
		job.waitForCompletion(true);
	}
}
