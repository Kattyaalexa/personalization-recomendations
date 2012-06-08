package org.zhangge.recomendrequest;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
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
	private HBaseAdmin admin;
	private ArrayList<Integer> uids = new ArrayList<Integer>();//用于存放用户id
	private ArrayList<Integer> storyids = new ArrayList<Integer>();//用于存放story id
	private ArrayList<Integer> scores = new ArrayList<Integer>();//用于存放打分
	private Map<String, Double> average_score = new HashMap<String, Double>();//存放每个用户的平均分数
	private Map<String, Set<String>> candidate = new HashMap<String, Set<String>>();//存放候选story
	
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
		admin = new HBaseAdmin(config);
		
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
	 * @throws IOException 
	 */
	public void makeRankedStories(String uid) throws IOException {
		Map<String, Double> scores = new HashMap<String, Double>();
		List<KeyValue> clustersList = user_clusters.list();
		if (clustersList != null) {
			for (KeyValue cluster : clustersList) {//遍历用户所有的集群
				byte[] clusterid = cluster.getValue();
				String column = new String(cluster.getQualifier());
				if (column.substring(0, 9).equals(CommonUtil.UT_Family2_Column)) {
					for (Result result : storyList) {//遍历所有的story
						byte[] story_clicktimes = result.getValue(Bytes.toBytes(CommonUtil.ST_Family1), clusterid);
						if (story_clicktimes != null) {
							String storyId = new String(result.getRow());
							Set<String> storyIdSet = candidate.get(Bytes.toString(clusterid));
							if (storyIdSet == null) {
								storyIdSet = new HashSet<String>();
							}
							storyIdSet.add(storyId);
							candidate.put(Bytes.toString(clusterid), storyIdSet);
							Double sum = Double.valueOf(sum_clicks.get(new String(clusterid)));
							Double clicks = Double.valueOf(new String(story_clicktimes));
							Double score = clicks / sum;
//System.out.println(storyId + ":" + clicks + ":" + sum + ":" + score);
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
		ranklist.put(uid, scores);
	}
	
	/**
	 * 把侯选的story写到文件
	 * @param filepath
	 * @throws IOException
	 */
	public void writeCandidate(String filepath) throws IOException {
		FileWriter fileWriter = new FileWriter(filepath);
		BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
		Set<String> keys = candidate.keySet();
		for (String key : keys) {
			Set<String> storys = candidate.get(key);
			for (String story : storys) {
				bufferedWriter.write(key + ":" + story);
				bufferedWriter.newLine();
			}
		}
		bufferedWriter.flush();
		bufferedWriter.close();
		fileWriter.close();
	}
	
	/**
	 * 产生推荐分数写到文件里面去
	 * @param filepath
	 * @throws IOException
	 */
	public void writeToFile(String filepath) throws IOException {
		FileWriter fileWriter = new FileWriter(filepath);
		BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
		Set<String> keys = ranklist.keySet();
		for (String key : keys) {
			Map<String, Double> scores = ranklist.get(key);
			Set<String> ks = scores.keySet();
			for (String k : ks) {
				Double score = scores.get(k);
				bufferedWriter.write(key + ":" + k + ":" + score);
				bufferedWriter.newLine();
//System.out.println(key + ":" + k + ":" + score);
			}
		}
		bufferedWriter.flush();
		bufferedWriter.close();
		fileWriter.close();
	}
	
	public void computePrecisionAndRecall() throws NumberFormatException, IOException {
		File file = new File(CommonUtil.filepath + CommonUtil.test_set);
		BufferedReader br = new BufferedReader(new FileReader(file));
		int count = 0;//统计每个用户的点击总数
		double sumScore = 0;//累加每个用户的总分数
		double average = 0;//计算得到的平均值
		String lastuid = null;//上一个用户id
		String line = null;
		while((line = br.readLine()) != null) {
			String[] parts = line.split(CommonUtil.split_char);
			if (!parts[0].equals(lastuid) && lastuid != null) {//统计每个
				average = sumScore / count;
				average_score.put(lastuid, average);
				count = 0;
				sumScore = 0;
			}
			count ++;
			sumScore += Integer.valueOf(parts[2]);
			uids.add(Integer.valueOf(parts[0]));
			storyids.add(Integer.valueOf(parts[1]));
			scores.add(Integer.valueOf(parts[2]));
			lastuid = parts[0];
		}
		//添加最后一个用户
		average = sumScore / count;
		average_score.put(lastuid, average);
	}
	
	public static void main(String[] args) throws IOException {
		NewsPeronalizationServer NPS = new NewsPeronalizationServer();
		NPS.connectToHbase();
		NPS.summarizeClicks();
		NPS.readUids(CommonUtil.filepath + CommonUtil.uid_set);
		NPS.writeToFile(CommonUtil.filepath + CommonUtil.recommand_scores);
		NPS.computePrecisionAndRecall();
		NPS.writeCandidate(CommonUtil.filepath + CommonUtil.candidate);
		NPS.admin.close();
	}
}
