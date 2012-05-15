package org.zhangge.minhash.mapper;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.zhangge.minhash.hashfunction.functions.MurmurHash;

public class MinHashMapper extends TableMapper<ImmutableBytesWritable, ImmutableBytesWritable> {

//	private HashFunctionFactory hashFunction = HashFunctionFactory.getInstance();
	private int p = 2;
	private int q = 10;
	
	@Override
	protected void map(ImmutableBytesWritable row, Result values, Context context)
			throws IOException, InterruptedException {
		
//		File file = new File(DistributedCache.getLocalCacheFiles(context.getConfiguration())[0].toString()+"/SeedValues");
		File file = new File("/home/zhangge/SeedValues");
		FileReader fr = new FileReader(file);
		BufferedReader br = new BufferedReader(fr);
		
		ImmutableBytesWritable value = row;
		ImmutableBytesWritable key = null;
		List<StringBuilder> clusterIds = new ArrayList<StringBuilder>();
		
		for (int i = 0; i < q; i++) {
			StringBuilder clusterId = new StringBuilder();
			for (int j = 0; j < p; j++) {
				Integer seed = Integer.valueOf(br.readLine());
				//这是方法一，由种子值获得对应hash函数，缺点是：hash函数太少了。
//				HashFunction hashFun = hashFunction.getHashFunction(seed);
				long minValue = Long.MAX_VALUE;
				for (KeyValue kv : values.list()) {
					String storyId = Bytes.toString(kv.getQualifier());//拿到的是列，而不是值
//					long tempValue = hashFun.hash(storyId);
					//方法二，直接由种子值作为hash函数的发生器
					byte[] hashKey = storyId.getBytes();
					long tempValue = Math.abs(MurmurHash.hash64(hashKey, hashKey.length, seed));
					if (tempValue < minValue) {
						minValue = tempValue;
					}
				}
				clusterId.append(minValue);
			}
			clusterId.append("%").append(i+1);
			clusterIds.add(clusterId);
		}
		
		for (int i = 0; i < clusterIds.size(); i++) {
			key = new ImmutableBytesWritable();
			key.set(clusterIds.get(i).toString().getBytes());
			context.write(key, value);
		}
	}
	
}