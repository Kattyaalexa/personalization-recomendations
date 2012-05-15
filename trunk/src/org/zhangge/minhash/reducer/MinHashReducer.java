package org.zhangge.minhash.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;

public class MinHashReducer extends TableReducer <ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable> {

	private int standardClusterSize = 0;//目前测试只有一个人
	
	protected void reduce(ImmutableBytesWritable key, Iterable<ImmutableBytesWritable> value, Context context)
			throws IOException, InterruptedException {
		
		List<ImmutableBytesWritable> valuesList = new ArrayList<ImmutableBytesWritable>();
		Iterator<ImmutableBytesWritable> values = value.iterator();
		while (values.hasNext()) {
			valuesList.add(values.next());
		}
		
		if (valuesList.size() > standardClusterSize) {
			for (ImmutableBytesWritable userId : valuesList) {
				String clusterId = Bytes.toString(key.get());
				String[] IdNum = clusterId.split("%");
				Put put = new Put(userId.get());
				String family = "clusters";
				String column = "clusterId" + IdNum[1];
				put.add(family.getBytes(), column.getBytes(), IdNum[0].getBytes());
				context.write(userId, put);
			}
		}
	}

}
