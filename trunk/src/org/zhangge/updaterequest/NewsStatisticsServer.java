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

	/**
	 * 连接hbase数据库
	 * @throws IOException
	 */
	public void connectToHbase() throws IOException {
		Configuration config = HBaseConfiguration.create();
		
		usertable = new HTable(config, CommonUtil.UT.getBytes());
		storytable = new HTable(config, CommonUtil.ST.getBytes());
	}
	
	/**
	 * 用一个uid从UT表取出一个用户的cluster信息和点击历史
	 * @param uid
	 * @throws IOException
	 */
	public void fetchFromUT(String uid) throws IOException {
//		Scan scan = new Scan(get);
//		ResultScanner rs = usertable.getScanner(scan);
//		for (Result result : rs) {
//			System.out.println(result);
//			user_result = result;
//		}
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
		if (cluster_result != null) {
			for (KeyValue cluster : cluster_result) {//遍历用户的所有的集群
				String clusterId = new String(cluster.getValue());//获取集群id
				if (story_result != null) {
					for (KeyValue story : story_result) {//遍历用户的所有的点击历史
						String storyId = new String(story.getQualifier());//获取story id
						fetchFromST(storyId);//首先获取ST表原来的信息
						List<KeyValue> story_click = story_clicktimes.list();
						if (story_click != null) {//如果初始表为空
							for (KeyValue keyValue : story_click) {//遍历单一点击历史在ST表的所有信息
								byte[] qualifier = keyValue.getQualifier();
								String clusterid;
								Integer clicktimes;
								if (story_result == null) {//如果初始的时候没有数据
									clusterid = clusterId;
									clicktimes = 1;
								} else {
									clusterid = new String(qualifier);
									clicktimes = Integer.valueOf(new String(keyValue.getValue())) + 1;
								}
								Put put = new Put(story.getQualifier());
								put.add(keyValue.getFamily(), Bytes.toBytes(clusterid), keyValue.getTimestamp(), Bytes.toBytes(clicktimes.toString()));
								storytable.put(put);
							}
						} else {
							Put put = new Put(story.getQualifier());
							put.add(Bytes.toBytes(CommonUtil.ST_Family1), Bytes.toBytes(clusterId), story.getTimestamp(), Bytes.toBytes(new String("1")));
							storytable.put(put);
						}
					}
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
		String filepath = "/home/zhangge/workspace/PersonalizationRecomendations/ml-100k/";
		NewsStatisticsServer NSS = new NewsStatisticsServer();
		NSS.connectToHbase();
		
		NSS.readUids(filepath + "uids_average");
	}
}
