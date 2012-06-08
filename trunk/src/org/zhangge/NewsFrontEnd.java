package org.zhangge;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
	private ArrayList<Integer> uids = new ArrayList<Integer>();//用于存放用户id
	private ArrayList<Integer> storyids = new ArrayList<Integer>();//用于存放story id
	private ArrayList<Integer> scores = new ArrayList<Integer>();//用于存放打分
	private ArrayList<Long> timestamp = new ArrayList<Long>();//用于存放时间
	
	private Map<String, Double> average_score = new HashMap<String, Double>();//存放每个用户的平均分数
	
	public static void main(String[] args) throws IOException, InterruptedException {
		NewsFrontEnd nfe = new NewsFrontEnd();
		nfe.readData(CommonUtil.filepath + CommonUtil.train_set);
		nfe.writeUidData(CommonUtil.filepath + CommonUtil.uid_set);
		nfe.writeAverageData(CommonUtil.filepath + CommonUtil.average_set);
	}
	
	/**
	 * 把用户id写到一个文件里面去，以便再用
	 * @param filepath
	 * @throws IOException 
	 */
	public void writeUidData(String filepath) throws IOException {
		HashSet<Integer> unique_uids = new HashSet<Integer>();
		for (Integer uid : uids) {
			unique_uids.add(uid);
		}
		FileWriter fileWriter = new FileWriter(filepath);
		BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
		//bufferedWriter.write(String.valueOf(average));
		//bufferedWriter.newLine();
		Iterator<Integer> uid_iterator = unique_uids.iterator();
		while (uid_iterator.hasNext()) {
			bufferedWriter.write(uid_iterator.next().toString());
			bufferedWriter.newLine();
		}
		bufferedWriter.flush();
		bufferedWriter.close();
		fileWriter.close();
	}
	
	/**
	 * 把平均值写到一个文件里面去，以便再用
	 * @param filepath
	 * @throws IOException
	 */
	public void writeAverageData(String filepath) throws IOException {
		FileWriter fileWriter = new FileWriter(filepath);
		BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
		Set<String> akeys = average_score.keySet();
		for (String key : akeys) {
			Double average = average_score.get(key);
			bufferedWriter.write(key + ":" + average);
			bufferedWriter.newLine();
		}
		bufferedWriter.flush();
		bufferedWriter.close();
		fileWriter.close();
	}
	
	/**
	 * 把Movieline的数据解析到内存，然后写入数据库
	 * @param filepath
	 */
	public void readData(String filepath) {
		try {
			File file = new File(filepath);
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
				timestamp.add(Long.valueOf(parts[3]));
				lastuid = parts[0];
			}
			//添加最后一个用户
			average = sumScore / count;
			average_score.put(lastuid, average);
			generateToHbase(uids.size());
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
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
		HBaseAdmin admin = new HBaseAdmin(config);
		
		//先删除旧表
		admin.disableTable(Bytes.toBytes(CommonUtil.UT));
		admin.deleteTable(Bytes.toBytes(CommonUtil.UT));
		admin.disableTable(Bytes.toBytes(CommonUtil.ST));
		admin.deleteTable(Bytes.toBytes(CommonUtil.ST));
		
		//创建UT表
		HTableDescriptor htd1 = new HTableDescriptor(CommonUtil.UT);//一个表
		HColumnDescriptor hcd1 = new HColumnDescriptor(CommonUtil.UT_Family1);//第一个列族
		HColumnDescriptor hcd2 = new HColumnDescriptor(CommonUtil.UT_Family2);//第二个列族
		htd1.addFamily(hcd1);
		htd1.addFamily(hcd2);
		admin.createTable(htd1);
		
		//创建ST表
		HTableDescriptor htd2 = new HTableDescriptor(CommonUtil.ST);
		HColumnDescriptor hcd3 = new HColumnDescriptor(CommonUtil.ST_Family1);
		HColumnDescriptor hcd4 = new HColumnDescriptor(CommonUtil.ST_Family2);
		htd2.addFamily(hcd3);
		htd2.addFamily(hcd4);
		admin.createTable(htd2);
		
		byte[] tablename = htd1.getName();
		HTableDescriptor[] tables = admin.listTables();
		if (tables.length != 1 && Bytes.equals(tablename, tables[0].getName())) {
			throw new IOException("Failed create of table");
		}
		
		HTable table = new HTable(config, tablename);
		for (int i = 0; i < count; i++) {
			String uid = uids.get(i).toString();
			if (scores.get(i) > average_score.get(uid)) {
				byte[] row = Bytes.toBytes(uid);
				Put put = new Put(row);
				byte[] family = Bytes.toBytes(CommonUtil.UT_Family1);
				put.add(family, Bytes.toBytes(storyids.get(i).toString()), timestamp.get(i), Bytes.toBytes(timestamp.get(i).toString()));
				table.put(put);
			}
		}
		admin.close();
	}
}
