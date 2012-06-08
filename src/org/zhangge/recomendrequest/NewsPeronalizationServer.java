package org.zhangge.recomendrequest;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
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
	private double bingo = 0;//推荐和test数据集的交集个数
	private double recommand_size = 0;//推荐的个数
	private double precision = 0;
	private double recall = 0;
	
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
		br.close();
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
	 * 对推荐列表进行排序
	 * @param oriMap
	 * @return
	 */
	public Map<String, Double> sortMapByValue(Map<String, Double> oriMap) {  
	    Map<String, Double> sortedMap = new LinkedHashMap<String, Double>();  
	    if (oriMap != null && !oriMap.isEmpty()) {  
	        List<Map.Entry<String, Double>> entryList = new ArrayList<Map.Entry<String, Double>>(oriMap.entrySet());  
	        Collections.sort(entryList, new Comparator<Map.Entry<String, Double>>() {
                public int compare(Map.Entry<String, Double> o1, Map.Entry<String, Double> o2) {
                		double result = o2.getValue() - o1.getValue();
                		if (result > 0) {
							return 1;
						} else if (result < 0) {
							return -1;
						} else {
							return 0;
						}
	                }
				});  
	        Iterator<Map.Entry<String, Double>> iter = entryList.iterator();  
	        Map.Entry<String, Double> tmpEntry = null;  
	        while (iter.hasNext()) {  
	            tmpEntry = iter.next();  
	            sortedMap.put(tmpEntry.getKey(), tmpEntry.getValue());  
	        }  
	    }  
	    return sortedMap;  
	}  
	
	/**
	 * 产生推荐分数写到文件里面去
	 * @param filepath
	 * @throws IOException
	 */
	public void writeScoreToFile(String filepath) throws IOException {
		FileWriter fileWriter = new FileWriter(filepath);
		BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
		Set<String> keys = ranklist.keySet();
		for (String key : keys) {
			int i = 1;
			Map<String, Double> scores = ranklist.get(key);
			scores = sortMapByValue(scores);
			Set<String> ks = scores.keySet();
			for (String k : ks) {
				if (i <= CommonUtil.recommond_number) {
					Double score = scores.get(k);
					bufferedWriter.write(i + ":" + key + ":" + k + ":" + score);
					bufferedWriter.newLine();
					i ++;
//System.out.println(key + ":" + k + ":" + score);
				}
			}
			ranklist.put(key, scores);
		}
		bufferedWriter.flush();
		bufferedWriter.close();
		fileWriter.close();
	}
	
	/**
	 * 计算precision和call
	 * @throws NumberFormatException
	 * @throws IOException
	 */
	public void computePrecisionAndRecall() throws NumberFormatException, IOException {
		//读取test数据集
		File file = new File(CommonUtil.filepath + CommonUtil.test_set);
		BufferedReader br = new BufferedReader(new FileReader(file));
		String line = null;
		while((line = br.readLine()) != null) {
			String[] parts = line.split(CommonUtil.split_char);
			uids.add(Integer.valueOf(parts[0]));
			storyids.add(Integer.valueOf(parts[1]));
			scores.add(Integer.valueOf(parts[2]));
		}
		br.close();
		//读取平均分数
		File file2 = new File(CommonUtil.filepath + CommonUtil.average_set);
		BufferedReader br2 = new BufferedReader(new FileReader(file2));
		String line2 = null;
		while((line2 = br2.readLine()) != null) {
			String[] parts = line2.split(CommonUtil.split_average);
			average_score.put(parts[0], Double.valueOf(parts[1]));
		}
		br2.close();
		
		for (int i = 0; i < uids.size(); i++) {
			Integer uid = uids.get(i);
			if (scores.get(i) >= average_score.get(uid.toString())) {
				Map<String, Double> scoresMap = ranklist.get(uid.toString());
				if (scoresMap.containsKey(storyids.get(i).toString())) {
					bingo ++;
				}
			}
		}
		
		Set<String> uidSet = ranklist.keySet();
		for (String us : uidSet) {
			Map<String, Double> scoreMap = ranklist.get(us);
			if (scoreMap.size() <= CommonUtil.recommond_number) {
				recommand_size += scoreMap.size();
			} else {
				recommand_size += CommonUtil.recommond_number;
			}
		}
		
		precision = bingo / recommand_size;
		recall = bingo / uids.size();
System.out.println("bingo:" + bingo);
System.out.println("recommand_size:" + recommand_size);
System.out.println("uids.size:" + uids.size());
System.out.println("precision:" + precision);
System.out.println("recall:" + recall);
	}
	
	public static void main(String[] args) throws IOException {
		NewsPeronalizationServer NPS = new NewsPeronalizationServer();
		NPS.connectToHbase();
		NPS.summarizeClicks();
		NPS.readUids(CommonUtil.filepath + CommonUtil.uid_set);
		NPS.writeScoreToFile(CommonUtil.filepath + CommonUtil.recommand_scores);
		NPS.computePrecisionAndRecall();
		NPS.writeCandidate(CommonUtil.filepath + CommonUtil.candidate);
		NPS.admin.close();
	}
}
