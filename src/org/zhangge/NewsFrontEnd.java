package org.zhangge;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;

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

	private ArrayList<Integer> uids = new ArrayList<Integer>();//用于存放用户id
	private ArrayList<Integer> storyids = new ArrayList<Integer>();//用于存放story id
	private ArrayList<Integer> scores = new ArrayList<Integer>();//用于存放打分
	private ArrayList<Long> timestamp = new ArrayList<Long>();//用于存放时间
	private double average;
	
	public static void main(String[] args) throws IOException, InterruptedException {
		String filepath = "/home/zhangge/workspace/PersonalizationRecomendations/ml-100k/";
		NewsFrontEnd nfe = new NewsFrontEnd();
		nfe.readData(filepath + "ua.base");
		nfe.writeData(filepath + "uids_average");
	}
	
	/**
	 * 把平均值和用户id写到一个文件里面去，以便再用
	 * @param filepath
	 * @throws IOException 
	 */
	public void writeData(String filepath) throws IOException {
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
	 * 把Movieline的数据写入数据库
	 * @param filepath
	 */
	public void readData(String filepath) {
		try {
			File file = new File(filepath);
			BufferedReader br = new BufferedReader(new FileReader(file));
			int count = 0;//统计 总数
			double sumScore = 0;//累加总分数
			String line = null;
			while((line = br.readLine()) != null) {
				count ++;
				String[] parts = line.split("\t");
				sumScore += Integer.valueOf(parts[2]);
				uids.add(Integer.valueOf(parts[0]));
				storyids.add(Integer.valueOf(parts[1]));
				scores.add(Integer.valueOf(parts[2]));
				timestamp.add(Long.valueOf(parts[3]));
			}
System.out.println(sumScore);
System.out.println(count);
			average = sumScore / count;
System.out.println(average);
			generateToHbase(count);
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
			if (scores.get(i) > average) {
				byte[] row = Bytes.toBytes(uids.get(i).toString());
				Put put = new Put(row);
				byte[] family = Bytes.toBytes(CommonUtil.UT_Family1);
				put.add(family, Bytes.toBytes(storyids.get(i).toString()), timestamp.get(i), Bytes.toBytes(timestamp.get(i).toString()));
				//put.add(databytes, Bytes.toBytes(storyids.get(i).toString()), Bytes.toBytes(String.valueOf(date.getTime())));
				table.put(put);
			}
		}
	}
}
