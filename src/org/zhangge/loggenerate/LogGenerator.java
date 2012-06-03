package org.zhangge.loggenerate;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.zhangge.CommonUtil;

public class LogGenerator {
	
	private ArrayList<Integer> uids = new ArrayList<Integer>();//用于存放用户id
	private ArrayList<Integer> storyids = new ArrayList<Integer>();//用于存放story id
	private ArrayList<Integer> scores = new ArrayList<Integer>();//用于存放打分
	private ArrayList<Long> timestamp = new ArrayList<Long>();//用于存放时间
	private double average;

	public static void main(String[] args) throws IOException, InterruptedException {
		String filepath = "/home/zhangge/workspace/PersonalizationRecomendations/ml-100k/";
		//new LogGenerator().generateToFile("newslog", path, "zhgeaits@gmail.com", "china.html", 10000);
		LogGenerator lg = new LogGenerator();
		lg.readData(filepath + "ua.base");
		//new LogGenerator().generateToHbase("zhgeaits@gmail.com", "china.html", 10000);
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
			int sumScore = 0;//累加总分数
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
			average = sumScore / count;
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
		
		HTableDescriptor htd = new HTableDescriptor(CommonUtil.UT);//一个表
		HColumnDescriptor hcd1 = new HColumnDescriptor(CommonUtil.UT_Family1);//第一个列族
		HColumnDescriptor hcd2 = new HColumnDescriptor(CommonUtil.UT_Family2);//第二个列族
		htd.addFamily(hcd1);
		htd.addFamily(hcd2);
		admin.createTable(htd);
		byte[] tablename = htd.getName();
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
	
	/**
	 * 产生测试日志到文本文件
	 * @param filename 输出文件名
	 * @param path 输出路径
	 * @param user 用户
	 * @param news 新闻
	 * @param count 数量
	 * @throws IOException 
	 */
	public void generateToFile(String filename, String path, String user,String news, int count) throws IOException {
		FileWriter fileWriter = new FileWriter(path + filename);
		BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
		for (int i = 0; i < count; i++) {
			bufferedWriter.write(user + " http://news.google.com/" + news + (i+1));
			bufferedWriter.newLine();
		}
		bufferedWriter.flush();
		bufferedWriter.close();
		fileWriter.close();
	}
}
