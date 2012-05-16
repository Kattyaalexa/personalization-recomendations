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

public class GenTable {
	
	private static Configuration conf = null;
	private static int CLUSTERS = 5; 
	
	static {
		conf = HBaseConfiguration.create();
		conf.addResource("hbase-site.xml"); //this is default,so don't have to write this here
	}
	public static double[] p = new double[CLUSTERS];
	public static Random rd = new Random();
	public static void main(String[] args) throws IOException{
		
		HBaseAdmin admin = new HBaseAdmin(conf);
		
		if(admin.tableExists("UT")||admin.tableExists("ST")){
			System.out.println("this table is already exist!");
		} else {
			HTableDescriptor htdut = new HTableDescriptor("UT");
			htdut.addFamily(new HColumnDescriptor("clusters"));
			htdut.addFamily(new HColumnDescriptor("history"));
			admin.createTable(htdut);
			
			HTableDescriptor htdst = new HTableDescriptor("ST");
			htdst.addFamily(new HColumnDescriptor("clusters"));
			htdst.addFamily(new HColumnDescriptor("covisitation"));
			admin.createTable(htdst);
			System.out.println("create tables successfully");
		}
		generateUT(10);
		generateST(10);
		
	}
	
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
			int historyNum = rd.nextInt(5)+1;
			for(int j=0;j<historyNum;j++){
				int history = rd.nextInt(9)+1;
				put.add(Bytes.toBytes("history"),Bytes.toBytes("s"+history),Bytes.toBytes("s"+history));
				table.put(put);
			}
		}
	}
	
	public static void generateST(int m) throws IOException {
		HTable table = new HTable(conf,"ST");
		Put put = null;
		for(int i=1;i<=m;i++){
			generateP();
			put = new Put(Bytes.toBytes("s"+i));
			
			for(int j=0;j<p.length;j++){
				put.add(Bytes.toBytes("clusters"),Bytes.toBytes("z"+(j+1)),Bytes.toBytes(p[j]+""));
				table.put(put);
			}
		}
	}
	
 	public static void generateP() {
		double sum = 0;
		
		for(int ii=0;ii<p.length;ii++){
			p[ii] = rd.nextDouble();
			sum += p[ii];
		}
		for(int ii=0;ii<p.length;ii++){
			p[ii] =  p[ii]/sum;
		}
		
	}
}