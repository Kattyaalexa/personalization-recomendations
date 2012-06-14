package org.lzh.table;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 *  生成最初的N(z),N(s|z)到指定的文件中
 * @author lzh
 *
 */
public class GenInterData {
	private static int CLUSTER_NUM= 20; //cluster的数目
	private static int NEWS_NUM = 1683;	 //news的数目
	
	public void genInterData() throws IOException{
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		//FileSystem fs = FileSystem.get(URI.create(args[0]),conf);
		OutputStream outNz = fs.create(new Path("intermediaNz"));
		OutputStream outNsz = fs.create(new Path("intermediaNsz"));
		BufferedWriter bwNz = new BufferedWriter(new OutputStreamWriter(outNz));
		BufferedWriter bwNsz = new BufferedWriter(new OutputStreamWriter(outNsz));
		Random rd = new Random();
		
		for(int i=1;i<=CLUSTER_NUM;i++){
			bwNz.write("z"+i+"\t"+(rd.nextFloat()+1.0)*10);
			bwNz.newLine();
			for(int j=1;j<=NEWS_NUM;j++){
				bwNsz.write("s"+j+"=="+"z"+i+"\t"+rd.nextFloat());
				bwNsz.newLine();
			}
		}
		bwNz.close();
		bwNsz.close();
		System.out.println("generate intermedia data successfully");
	}
}
