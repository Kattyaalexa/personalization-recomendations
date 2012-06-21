package org.lzh.table;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 生成UT，ST表，并写入对应clusters_plsi及其值
 * @author lzh
 *
 */
public class GenTable {
	
	public static String UT = "UserTable";//用户表表名
	public static String ST = "StoryTable";//新闻表表名
	public static String ZT = "ClusterTable";//用于存放属于某个cluster的story
	public static String DATA_BASE = "/user/lzh/ua.base";//用于training的数据
	public static String DATA_TEST_INIT = "/user/lzh/u5.test";//用于test的原始数据
	public static String DATA_TEST_MID = "/user/lzh/u5.test.out";
	public static String DATA_TEST_FIANL = "/home/lzh/udata/u5.test.out";//用于test的数据
	public static String UIDS = "/home/lzh/udata/u.user";//所有用户的id
	public static String REC_SCORE_PLSI = "/home/lzh/udata/rec_score_plsi";//最后结果写到此文件中
	
	private static Configuration conf = null;
	private static int CLUSTER_NUM= 20; //cluster的数目
	private static int USER_NUM = 943;	 //user的数目
	private static int NEWS_NUM = 1682;	 //news的数目
	private static String candidate = "/home/lzh/udata/candidate.set";//把数据写到ZT表
	
	static {
		conf = HBaseConfiguration.create();
		conf.addResource("hbase-site.xml"); //this is default,so don't have to write this here
	}
	private static float[] p = new float[CLUSTER_NUM];
	private static Random rd = new Random();
	
	public static void main(String[] args) throws IOException{
		GenTable gt = new GenTable();
		
		//gt.createTables();
		
		//gt.generateUT(USER_NUM);
		gt.generateST(NEWS_NUM);
		//gt.generateZT(candidate);
		
		//new GenInterData().genInterData();
	}
	/**
	 * 创建表UT ST ZT
	 * @throws IOException
	 */
	private void createTables() throws IOException{
		HBaseAdmin admin = new HBaseAdmin(conf);
		
		if(admin.tableExists(UT)){
			admin.disableTable(UT);
			admin.deleteTable(UT);	
		}
		HTableDescriptor htdut = new HTableDescriptor(UT);
		htdut.addFamily(new HColumnDescriptor("clusters_plsi"));
		htdut.addFamily(new HColumnDescriptor("story"));
		admin.createTable(htdut);
		if(admin.tableExists(ST)){
			admin.disableTable(ST);
			admin.deleteTable(ST);
		}
		HTableDescriptor htdst = new HTableDescriptor(ST);
		htdst.addFamily(new HColumnDescriptor("clusters_plsi"));
		htdst.addFamily(new HColumnDescriptor("covisitation"));
		admin.createTable(htdst);
		if(admin.tableExists(ZT)){
			admin.disableTable(ZT);
			admin.deleteTable(ZT);		
		}	
		HTableDescriptor htdzt = new HTableDescriptor(ZT);
		htdzt.addFamily(new HColumnDescriptor("story"));
		admin.createTable(htdzt);
	}
	/**
	 * 生成数据库数据UT
	 * @param n 生成n个user
	 * @throws IOException
	 */
	private void generateUT(int n) throws IOException {
		HTable table = new HTable(conf,UT);
		List<Put> putlist = new ArrayList<Put>();
		for(int i=1;i<=n;i++){
			generateP();			
			for(int j=0;j<p.length;j++){
				Put put = new Put(Bytes.toBytes("u"+i));
				put.add(Bytes.toBytes("clusters_plsi"),Bytes.toBytes("z"+(j+1)),Bytes.toBytes(p[j]+""));
				putlist.add(put);
			}
			/*int historyNum = rd.nextInt(5)+1;
			for(int j=0;j<historyNum;j++){
				int history = rd.nextInt(NEWS_NUM)+1;
				put.add(Bytes.toBytes("history"),Bytes.toBytes("s"+history),Bytes.toBytes("s"+history));
				table.put(put);
			}*/
		}
		table.put(putlist);
		System.out.println("generateUT completed");
	}
	
	/**
	 * 生成数据库数据ST
	 * @param m 生成m个news
	 * @throws IOException
	 */
	private void generateST(int m) throws IOException {
		HTable table = new HTable(conf,ST);
		List<Put> putlist = new ArrayList<Put>();
		for(int i=1;i<=m;i++){
			//generateP();
			
			for(int j=0;j<p.length;j++){
				Put put = new Put(Bytes.toBytes("s"+i));
				put.add(Bytes.toBytes("clusters_plsi"),Bytes.toBytes("z"+(j+1)),Bytes.toBytes(0+""));
				putlist.add(put);
			}
		}
		table.put(putlist);
		System.out.println("generateST completed");
	}
	
	/**
	 * 生成数据库数据ZT
	 * @param m
	 * @throws IOException
	 */
	private void generateZT(String filepath) throws IOException {
		HTable table = new HTable(conf,ZT);
		List<Put> putlist = new ArrayList<Put>();
		File file = new File(filepath);
		BufferedReader br = new BufferedReader(new FileReader(file));
		String line = null;
		while((line = br.readLine()) != null) {
			String[] parts = line.split(":");
			Put put = new Put(parts[0].getBytes());
			put.add("story".getBytes(),parts[1].getBytes(),parts[1].getBytes());
			putlist.add(put);
		}
		br.close();
		table.put(putlist);
		System.out.println("generateZT completed");
	}
	
	/**
	 * 随机生成包含CLUSTER_NUM个小数，并且其和为1
	 */
	private void generateP() {
		float sum = 0;
		
		for(int ii=0;ii<p.length;ii++){
			p[ii] = rd.nextFloat();
			sum += p[ii];
		}
		for(int ii=0;ii<p.length;ii++){
			p[ii] =  p[ii]/sum;
		}
		
	}
}