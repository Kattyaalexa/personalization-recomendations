package org.zhangge.updaterequest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
	public HBaseAdmin admin;
	//为了提高性能，缓存ST表数据，Key:storyid Value: (key:clusterid value:clicktimes)
	private Map<String, Map<String, Integer>> story_clicktimes = new HashMap<String, Map<String, Integer>>();
	
	public NewsStatisticsServer() {
	}
	
	public NewsStatisticsServer(HBaseAdmin admin) throws IOException {
		this.admin = admin;
		usertable = new HTable(admin.getConfiguration(), CommonUtil.UT.getBytes());
		storytable = new HTable(admin.getConfiguration(), CommonUtil.ST.getBytes());
	}

	/**
	 * 连接hbase数据库
	 * @throws IOException
	 */
	public void connectToHbase() throws IOException {
		Configuration config = HBaseConfiguration.create();
		admin = new HBaseAdmin(config);
		
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
	 * 因为ST表里面初始没有一点数据，所以为了提高性能，首先在缓存ST表数据，最后一起写道数据库
	 */
	public void updateSTCash() {
		List<KeyValue> cluster_result = user_clusters.list();
		List<KeyValue> story_result = user_storys.list();
		if (cluster_result != null) {//需要判断这个用户有没有集群信息
			for (KeyValue story : story_result) {//遍历用户的所有的点击历史
				String storyId = new String(story.getQualifier());//获取story id
				Map<String, Integer> clickMap = story_clicktimes.get(storyId);
				if (clickMap == null) {
					clickMap = new HashMap<String, Integer>();
				}
				for (KeyValue cluster : cluster_result) {//遍历用户的所有的集群
					String clusterId = new String(cluster.getQualifier());//获取集群id
					Integer clicktimes = clickMap.get(clusterId);//获取点击次数
					if (clicktimes == null) {
						clicktimes = 1;
					} else {
						clicktimes += 1;
					}
System.out.println(storyId + ":" + clusterId + ":" + clicktimes);
					clickMap.put(clusterId, clicktimes);
				}
				story_clicktimes.put(storyId, clickMap);
			}
		}
	}
	
	/**
	 * 更新ST表信息
	 * @throws IOException
	 */
	public void updateST() throws IOException {
		List<Put> puts = new ArrayList<Put>();
		Set<String> storyIds = story_clicktimes.keySet();
		for (String storyId : storyIds) {
			Map<String, Integer> clicktimes = story_clicktimes.get(storyId);
			Set<String> clusterIds = clicktimes.keySet();
			Put put = new Put(Bytes.toBytes(storyId));//添加row
			for (String clusterId : clusterIds) {
				Integer clicks = clicktimes.get(clusterId);
System.out.println(storyId + ":" + clusterId + ":" + clicks);
				put.add(Bytes.toBytes(CommonUtil.ST_Family1), Bytes.toBytes(clusterId), Bytes.toBytes(clicks.toString()));
			}
			puts.add(put);
		}
		storytable.put(puts);
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
			String[] parts = line.split(CommonUtil.split_user);
			String uid = "u" + parts[0];
			fetchFromUT(uid);
			updateSTCash();
		}
		br.close();
		updateST();
	}
	
	public Map<String, Map<String, Integer>> getStory_clicktimes() {
		return story_clicktimes;
	}

	public static void main(String[] args) throws IOException {
		NewsStatisticsServer NSS = new NewsStatisticsServer();
		NSS.connectToHbase();
		NSS.readUids(CommonUtil.filepath + CommonUtil.uid_set);
		NSS.admin.close();
	}
}
