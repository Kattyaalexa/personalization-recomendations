package org.lzh.table;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class GenIntermedia {
	private static int CLUSTER_NUM= 5; //cluster的数目
	private static int NEWS_NUM = 10;	 //news的数目
	
	public static void main(String[] args) throws IOException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(args[0]),conf);
		
		OutputStream out = fs.create(new Path(args[0]));
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
		
		Random rd = new Random();
		
		for(int i=1;i<=CLUSTER_NUM;i++){
			bw.write("z"+i+"\t"+(rd.nextDouble()+1.0)*10);
			bw.newLine();
			for(int j=1;j<=NEWS_NUM;j++){
				bw.write("s"+j+"=="+"z"+i+"\t"+rd.nextDouble());
				bw.newLine();
			}
		}
		
		bw.close();
	}
}
