package org.zhangge.recomendrequest;

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
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.zhangge.CommonUtil;

public class NewsPeronalizationServer {

	private HTable usertable;
	private HTable storytable;
	private Result user_clusters;
	private Map<String, Integer> sum_clicks = new HashMap<String, Integer>();//存放每个集群的总点击数，集群id，点击数
	private Map<String, Map<String, Double>> ranklist = new HashMap<String, Map<String, Double>>();//存放推荐分数列表
	private List<Result> storyList = new ArrayList<Result>();
	private ResultScanner rs;
	
	/**
	 * 维护每个集群的总点击数
	 * @throws IOException
	 */
	public void summarizeClicks() throws IOException {
		fetchFromST();
		for (Result result : rs) {
			storyList.add(result);
			List<KeyValue> resultList = result.list();
			if (resultList != null) {
				for (KeyValue keyValue : resultList) {
					String clusterId = new String(keyValue.getQualifier());
					String clicktimes = new String(keyValue.getValue());
					if (sum_clicks.containsKey(clusterId)) {
						sum_clicks.put(clusterId, sum_clicks.get(clusterId) + Integer.valueOf(clicktimes));
					} else {
						sum_clicks.put(clusterId, Integer.valueOf(clicktimes));
					}
				}
			}
		}
//		Set<String> keys = sum_clicks.keySet();
//		for (String key : keys) {
//			System.out.println(key + ": " + sum_clicks.get(key));
//		}
	}
	
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
			makeRankedStories(line);
		}
	}
	
	/**
	 * 根据uid从UT表读取用户的集群信息
	 * @param uid
	 * @throws IOException
	 */
	public void fetchFromUT(String uid) throws IOException {
		Get get = new Get(Bytes.toBytes(uid));
		get.addFamily(Bytes.toBytes(CommonUtil.UT_Family2));
		user_clusters = usertable.get(get);
	}
	
	/**
	 * 从ST表读取所有的数据
	 * @throws IOException
	 */
	public void fetchFromST() throws IOException {
		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes(CommonUtil.ST_Family1));
		rs = storytable.getScanner(scan);
	}
	
	/**
	 * 根据uid产生推荐分数
	 * @param uid
	 */
	public void makeRankedStories(String uid) {
		Map<String, Double> scores = new HashMap<String, Double>();
		List<KeyValue> clustersList = user_clusters.list();
		if (clustersList != null) {
			for (KeyValue cluster : clustersList) {//遍历用户所有的集群
				byte[] clusterid = cluster.getValue();
				String column = new String(cluster.getQualifier());
				if (column.substring(0, 9).equals(CommonUtil.UT_Family2_Column)) {
					for (Result result : storyList) {//遍历所有的story
						List<KeyValue> resultList = result.list();
						if (resultList != null) {
							for (KeyValue keyValue : resultList) {//遍历每个story的集群信息
								if (Bytes.equals(clusterid, keyValue.getQualifier())) {
									String storyId = new String(keyValue.getRow());
									Double sum = Double.valueOf(sum_clicks.get(new String(clusterid)));
									Double clicks = Double.valueOf(new String(keyValue.getValue()));
									Double score = clicks / sum;
									if (scores.containsKey(storyId)) {
										scores.put(storyId, scores.get(storyId) + score);
									} else {
										scores.put(storyId, score);
									}
								}
							}
						}
					}
				}
			}
		}
		ranklist.put(uid, scores);
	}
	
	public void writeToFile() {
		Set<String> keys = ranklist.keySet();
		for (String key : keys) {
			Map<String, Double> scores = ranklist.get(key);
			Set<String> ks = scores.keySet();
			for (String k : ks) {
				Double score = scores.get(k);
				System.out.println(key + ":" + k + ":" + score);
			}
		}
	}
	
	public static void main(String[] args) throws IOException {
		String filepath = "/home/zhangge/workspace/PersonalizationRecomendations/ml-100k/";
		NewsPeronalizationServer NPS = new NewsPeronalizationServer();
		NPS.connectToHbase();
		NPS.summarizeClicks();
		NPS.readUids(filepath + "uids_average");
		NPS.writeToFile();
	}
}
