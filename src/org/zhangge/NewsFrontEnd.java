package org.zhangge;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;


/**
 * @author zhangge
 * 模拟前端把点击历史写入到UT表
 * 其实数据是读取movieline的80%
 */
public class NewsFrontEnd {

	//解析movieline的数据，读取到内存里面
	private ArrayList<String> uids = new ArrayList<String>();//用于存放用户id
	private ArrayList<String> storyids = new ArrayList<String>();//用于存放story id
	private ArrayList<Integer> scores = new ArrayList<Integer>();//用于存放打分
	private ArrayList<Long> timestamp = new ArrayList<Long>();//用于存放时间
	
	private Map<String, ArrayList<Integer>> user_story = new HashMap<String, ArrayList<Integer>>();//存放用户和他的所有点击历史，用于计算平均分
	private Map<String, Integer> average_score = new HashMap<String, Integer>();//存放每个用户的平均分数
	
	private HBaseAdmin admin;
	
	public static void main(String[] args) throws IOException, InterruptedException {
		NewsFrontEnd nfe = new NewsFrontEnd();
		//先计算平均分
		nfe.writeAverageData(CommonUtil.filepath + CommonUtil.user_data, CommonUtil.filepath + CommonUtil.average_set);
		nfe.readData(CommonUtil.filepath + CommonUtil.train_set);
		nfe.admin.close();
	}
	
	/**
	 * 把平均值写到一个文件里面去，以便再用
	 * @param filepath
	 * @throws IOException
	 */
	public void writeAverageData(String userdatafilepath, String averagefilepath) throws IOException {
		//首先从总数据里面读取用户和rating数据
		FileReader fileReader = new FileReader(userdatafilepath);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		String line = null;
		while ((line = bufferedReader.readLine()) != null) {
			String[] parts = line.split("\t");
			String uid = "u" + parts[0];
			Integer rating = Integer.valueOf(parts[2]);
			ArrayList<Integer> clickStory = user_story.get(uid);
			if (clickStory == null) {
				clickStory = new ArrayList<Integer>();
			}
			clickStory.add(rating);
			user_story.put(uid, clickStory);
		}
		bufferedReader.close();
		fileReader.close();
		
		//计算平均分，然后写到文件
		FileWriter fileWriter = new FileWriter(averagefilepath);
		BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
		Set<String> uids = user_story.keySet();
		for (String uid : uids) {
			ArrayList<Integer> clickStory = user_story.get(uid);
			int sum = 0;
			for (Integer rating : clickStory) {
				sum += rating;
			}
			int count = clickStory.size();
			int average = sum / count;
			average_score.put(uid, average);
			bufferedWriter.write(uid + CommonUtil.split_average + average);
			bufferedWriter.newLine();
		}
		bufferedWriter.flush();
		bufferedWriter.close();
		fileWriter.close();
	}
	
	/**
	 * 把Movieline的数据解析到内存，计算平均分，然后写入数据库
	 * @param filepath
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	public void readData(String filepath) throws IOException, InterruptedException {
		File file = new File(filepath);
		BufferedReader br = new BufferedReader(new FileReader(file));
		String line = null;
		while((line = br.readLine()) != null) {
			String[] parts = line.split(CommonUtil.split_char);
			String uid = "u" + parts[0];
			uids.add(uid);
			storyids.add("s" + parts[1]);
			scores.add(Integer.valueOf(parts[2]));
			timestamp.add(Long.valueOf(parts[3]));
		}
		generateToHbase(uids.size());
	}
	
	/**
	 * 生成数据库数据
	 * @param userId用户ID
	 * @param news
	 * @param count
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	public void generateToHbase(int count) throws IOException, InterruptedException {
		Configuration config = HBaseConfiguration.create();
		admin = new HBaseAdmin(config);
		
		//先删除旧表
		if (admin.tableExists(Bytes.toBytes(CommonUtil.UT))) {
			if (!admin.isTableDisabled(Bytes.toBytes(CommonUtil.UT))) {
				admin.disableTable(Bytes.toBytes(CommonUtil.UT));
				admin.deleteTable(Bytes.toBytes(CommonUtil.UT));
			}
		}
		if (admin.tableExists(Bytes.toBytes(CommonUtil.ST))) {
			if (!admin.isTableDisabled(CommonUtil.ST)) {
				admin.disableTable(Bytes.toBytes(CommonUtil.ST));
				admin.deleteTable(Bytes.toBytes(CommonUtil.ST));
			}
		}
		
		//创建UT表
		HTableDescriptor htd1 = new HTableDescriptor(CommonUtil.UT);//一个表
		HColumnDescriptor hcd1 = new HColumnDescriptor(CommonUtil.UT_Family1);//第一个列族
		HColumnDescriptor hcd2 = new HColumnDescriptor(CommonUtil.UT_Family2);//第二个列族
		HColumnDescriptor hcd3 = new HColumnDescriptor(CommonUtil.UT_Family3);//第三个列族
		htd1.addFamily(hcd1);
		htd1.addFamily(hcd2);
		htd1.addFamily(hcd3);
		admin.createTable(htd1);
		
		//创建ST表
		HTableDescriptor htd2 = new HTableDescriptor(CommonUtil.ST);
		HColumnDescriptor hcd4 = new HColumnDescriptor(CommonUtil.ST_Family1);
		HColumnDescriptor hcd5 = new HColumnDescriptor(CommonUtil.ST_Family2);
		HColumnDescriptor hcd6 = new HColumnDescriptor(CommonUtil.ST_Family3);
		htd2.addFamily(hcd4);
		htd2.addFamily(hcd5);
		htd2.addFamily(hcd6);
		admin.createTable(htd2);
		
		byte[] tablename = htd1.getName();
		HTableDescriptor[] tables = admin.listTables();
		if (tables.length != 1 && Bytes.equals(tablename, tables[0].getName())) {
			throw new IOException("Failed create of table");
		}
		
		HTable table = new HTable(config, tablename);
		List<Put> puts = new ArrayList<Put>();
		for (int i = 0; i < count; i++) {
			String uid = uids.get(i);
			if (scores.get(i) > average_score.get(uid)) {
				byte[] row = Bytes.toBytes(uid);
				Put put = new Put(row);
				byte[] family = Bytes.toBytes(CommonUtil.UT_Family1);
				put.add(family, Bytes.toBytes(storyids.get(i).toString()), timestamp.get(i), Bytes.toBytes(timestamp.get(i).toString()));
				puts.add(put);
			}
		}
		table.put(puts);
	}

	public ArrayList<String> getUids() {
		return uids;
	}

	public HBaseAdmin getAdmin() {
		return admin;
	}

	public Map<String, Integer> getAverage_score() {
		return average_score;
	}
	
}
