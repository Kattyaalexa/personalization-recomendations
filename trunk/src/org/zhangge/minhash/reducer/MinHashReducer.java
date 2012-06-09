package org.zhangge.minhash.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.zhangge.CommonUtil;

public class MinHashReducer extends TableReducer <ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {

	protected void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> value, Context context)
			throws IOException, InterruptedException {

//System.out.print("-----clusterid:" + Bytes.toString(key.get()));
		List<ImmutableBytesWritable> valuesList = new ArrayList<ImmutableBytesWritable>();
		Iterator<ImmutableBytesWritable> values = value.iterator();
		while (values.hasNext()) {//把iterator转换成List
			ImmutableBytesWritable val = values.next();
//System.out.print(":" + Bytes.toString(val.get()));
			ImmutableBytesWritable value_new = new ImmutableBytesWritable();
			value_new.set(val.get());
			valuesList.add(value_new);
		}
//System.out.println();
		if (valuesList.size() >= CommonUtil.MinHash_StandardClusterSize) {//如果一个集群的用户数太少，则不需要这个集群
			String clusterId = Bytes.toString(key.get());
//			String[] IdNum = clusterId.split(CommonUtil.split_cluster);
			String family = CommonUtil.UT_Family2;
//			String column = CommonUtil.UT_Family2_Column + IdNum[1];
System.out.print("clusterid:" + clusterId);
			for (int i = 0; i < valuesList.size(); i++) {
				ImmutableBytesWritable userId = valuesList.get(i);
System.out.print(":" + Bytes.toString(userId.get()));
				Put put = new Put(userId.get());
				put.add(Bytes.toBytes(family), Bytes.toBytes(clusterId), Bytes.toBytes(String.valueOf(valuesList.size())));
				context.write(userId, put);
			}
System.out.println();
		}
	}

}
