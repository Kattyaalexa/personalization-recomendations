package org.zhangge.updaterequest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.zhangge.CommonUtil;

public class NewsStatisticsServer {
	
	private HTable usertable;
	private HTable storytable;
	private Result user_storys;
	private Result user_clusters;
	private Result story_clicktimes;
	private HBaseAdmin admin;

	/**
	 * 连接hbase数据库
	 * @throws IOException
	 */
	public void connectToHbase() throws IOException {
		Configuration config = HBaseConfiguration.create();
		admin = new HBaseAdmin(config);
		
//		admin.disableTable(Bytes.toBytes(CommonUtil.ST));
//		admin.deleteTable(Bytes.toBytes(CommonUtil.ST));
//		//创建ST表
//				HTableDescriptor htd2 = new HTableDescriptor(CommonUtil.ST);
//				HColumnDescriptor hcd3 = new HColumnDescriptor(CommonUtil.ST_Family1);
//				HColumnDescriptor hcd4 = new HColumnDescriptor(CommonUtil.ST_Family2);
//				htd2.addFamily(hcd3);
//				htd2.addFamily(hcd4);
//				admin.createTable(htd2);
		
		usertable = new HTable(config, CommonUtil.UT.getBytes());
		storytable = new HTable(config, CommonUtil.ST.getBytes());
	}
	
	/**
	 * 用一个uid从UT表取出一个用户的cluster信息和点击历史
	 * @param uid
	 * @throws IOException
	 */
	public void fetchFromUT(String uid) throws IOException {
		Get get1 = new Get(Bytes.toBytes(uid));
		get1.addFamily(Bytes.toBytes(CommonUtil.UT_Family1));
		user_storys = usertable.get(get1);
		
		Get get2 = new Get(Bytes.toBytes(uid));
		get2.addFamily(Bytes.toBytes(CommonUtil.UT_Family2));
		user_clusters = usertable.get(get2);
	}
	
	/**
	 * 根据sid从ST表取出story的信息
	 * @param sid
	 * @throws IOException 
	 */
	public void fetchFromST(String sid) throws IOException {
		Get get = new Get(Bytes.toBytes(sid));
		get.addFamily(Bytes.toBytes(CommonUtil.ST_Family1));
		story_clicktimes = storytable.get(get);
	}
	
	/**
	 * 更新ST表信息
	 * @throws IOException
	 */
	public void updateST() throws IOException {
		List<KeyValue> cluster_result = user_clusters.list();
		List<KeyValue> story_result = user_storys.list();
		for (KeyValue story : story_result) {//遍历用户的所有的点击历史
			String storyId = new String(story.getQualifier());//获取story id
			fetchFromST(storyId);//首先获取ST表原来的信息
			if (cluster_result != null) {
				for (KeyValue cluster : cluster_result) {//遍历用户的所有的集群
					String clusterId = new String(cluster.getValue());//获取集群id
					byte[] click_times = story_clicktimes.getValue(Bytes.toBytes(CommonUtil.ST_Family1), Bytes.toBytes(clusterId));
					Integer clicktimes = 0;
					if (click_times != null) {//不为空则加1，否则初始设为1
//System.out.println(new String(click_times));
						clicktimes = Integer.valueOf(new String(click_times)) + 1;
					} else {
						clicktimes = 1;
					}
					Put put = new Put(story.getQualifier());
					put.add(Bytes.toBytes(CommonUtil.ST_Family1), Bytes.toBytes(clusterId), story.getTimestamp(), Bytes.toBytes(new String(clicktimes.toString())));
					storytable.put(put);
				}
			}
		}
	}
	
	/**
	 * 读取所有的uid
	 * @param filepath
	 * @throws IOException
	 */
	public void readUids(String filepath) throws IOException {
		File file = new File(filepath);
		BufferedReader br = new BufferedReader(new FileReader(file));
		String line = null;
		while((line = br.readLine()) != null) {
			fetchFromUT(line);
			updateST();
		}
	}
	
	public static void main(String[] args) throws IOException {
		NewsStatisticsServer NSS = new NewsStatisticsServer();
		NSS.connectToHbase();
		NSS.readUids(CommonUtil.filepath + CommonUtil.uid_set);
		NSS.admin.close();
	}
}
