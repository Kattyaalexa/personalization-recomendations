package org.lzh.table;

import java.io.IOException;
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
 * 生成UT，ST表，并写入对应Clusters及其值
 * @author lzh
 *
 */
public class GenTable {
	
	private static Configuration conf = null;
	private static int CLUSTER_NUM= 20; //cluster的数目
	private static int USER_NUM = 943;	 //user的数目
	private static int NEWS_NUM = 1682;	 //news的数目
	
	static {
		conf = HBaseConfiguration.create();
		conf.addResource("hbase-site.xml"); //this is default,so don't have to write this here
	}
	public static float[] p = new float[CLUSTER_NUM];
	public static Random rd = new Random();
	public static void main(String[] args) throws IOException{
		
		HBaseAdmin admin = new HBaseAdmin(conf);
		
		if(admin.tableExists("UT")){
			admin.disableTable("UT");
			admin.deleteTable("UT");
			
		}
		if(admin.tableExists("ST")){
			admin.disableTable("ST");
			admin.deleteTable("ST");
		}
		/*if(admin.tableExists("SZ")){
			admin.disableTable("SZ");
			admin.deleteTable("SZ");
		}*/
		HTableDescriptor htdut = new HTableDescriptor("UT");
		htdut.addFamily(new HColumnDescriptor("clusters"));
		htdut.addFamily(new HColumnDescriptor("history"));
		admin.createTable(htdut);
		
		HTableDescriptor htdst = new HTableDescriptor("ST");
		htdst.addFamily(new HColumnDescriptor("clusters"));
		htdst.addFamily(new HColumnDescriptor("covisitation"));
		admin.createTable(htdst);
		
		/*HTableDescriptor htdsz = new HTableDescriptor("SZ");
		htdsz.addFamily(new HColumnDescriptor("szfamily"));
		admin.createTable(htdsz);*/
		
		generateUT(USER_NUM);
		generateST(NEWS_NUM);
		//generateSZ();
		System.out.println("create tables successfully");
	}
	
	/**
	 * 生成数据库数据UT
	 * @param n 生成n个user
	 * @throws IOException
	 */
	public static void generateUT(int n) throws IOException {
		HTable table = new HTable(conf,"UT");
		Put put = null;
		for(int i=1;i<=n;i++){
			generateP();
			put = new Put(Bytes.toBytes("u"+i));
			for(int j=0;j<p.length;j++){
				put.add(Bytes.toBytes("clusters"),Bytes.toBytes("z"+(j+1)),Bytes.toBytes(p[j]+""));
				table.put(put);
			}
			/*int historyNum = rd.nextInt(5)+1;
			for(int j=0;j<historyNum;j++){
				int history = rd.nextInt(NEWS_NUM)+1;
				put.add(Bytes.toBytes("history"),Bytes.toBytes("s"+history),Bytes.toBytes("s"+history));
				table.put(put);
			}*/
		}
	}
	
	/**
	 * 生成数据库数据ST
	 * @param m 生成m个news
	 * @throws IOException
	 */
	public static void generateST(int m) throws IOException {
		HTable table = new HTable(conf,"ST");
		Put put = null;
		for(int i=1;i<=m;i++){
			//generateP();
			put = new Put(Bytes.toBytes("s"+i));
			
			for(int j=0;j<p.length;j++){
				put.add(Bytes.toBytes("clusters"),Bytes.toBytes("z"+(j+1)),Bytes.toBytes(0+""));
				table.put(put);
			}
		}
	}
	
	/**
	 * 生成存储N(z,s)和N(z)的数据库
	 * @throws IOException
	 */
	/*public static void generateSZ() throws IOException {
		HTable table = new HTable(conf,"SZ");
		Put put = new Put(Bytes.toBytes("szkey"));
		for(int i=1;i<=CLUSTER_NUM;i++){
			put.add(Bytes.toBytes("szfamily"),Bytes.toBytes("z"+i),Bytes.toBytes((rd.nextDouble()+1.0)*10+""));
			for(int j=1;j<=NEWS_NUM;j++){
				put.add(Bytes.toBytes("szfamily"),Bytes.toBytes("s"+j+"=="+"z"+i),Bytes.toBytes(rd.nextDouble()+""));
				table.put(put);
			}
		}
	}*/
	
	/**
	 * 随机生成包含CLUSTER_NUM个小数，并且其和为1
	 */
 	public static void generateP() {
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