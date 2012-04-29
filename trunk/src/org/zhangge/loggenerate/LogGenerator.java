package org.zhangge.loggenerate;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class LogGenerator {

	public static void main(String[] args) throws IOException, InterruptedException {
		//String path = "/home/zhangge/workspace/PersonalizationRecomendations/testdata/";
		//new LogGenerator().generateToFile("newslog", path, "zhgeaits@gmail.com", "china.html", 10000);
		new LogGenerator().generateToHbase("zhgeaits@gmail.com", "china.html", 10000);
	}
	
	/**
	 * 生成数据库数据
	 * @param userId用户ID
	 * @param news
	 * @param count
	 * @throws IOException
	 * @throws InterruptedException 
	 */
	public void generateToHbase(String userId, String news, int count) throws IOException, InterruptedException {
		Configuration config = HBaseConfiguration.create();
		
		HBaseAdmin admin = new HBaseAdmin(config);
		
		admin.disableTable(Bytes.toBytes("UserTable"));
		admin.deleteTable(Bytes.toBytes("UserTable"));
		
		HTableDescriptor htd = new HTableDescriptor("UserTable");//一个表
		HColumnDescriptor hcd1 = new HColumnDescriptor("clusters");//一个列族
		HColumnDescriptor hcd2 = new HColumnDescriptor("story");//第二个列族
		htd.addFamily(hcd1);
		htd.addFamily(hcd2);
		admin.createTable(htd);
		byte[] tablename = htd.getName();
		HTableDescriptor[] tables = admin.listTables();
		if (tables.length != 1 && Bytes.equals(tablename, tables[0].getName())) {
			throw new IOException("Failed create of table");
		}
		
		HTable table = new HTable(config, tablename);
		byte[] row = Bytes.toBytes(userId);
		Put put = new Put(row);
		byte[] databytes = Bytes.toBytes("story");
		for (int i = 0; i < count; i++) {
			String col = news + i;
			Date date = new Date();
			put.add(databytes, Bytes.toBytes(col), Bytes.toBytes(String.valueOf(date.getTime())));
			table.put(put);
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
