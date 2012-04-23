package org.zhangge.minhash.random;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class SeedGenerator {

	public static void main(String[] args) throws IOException {
//		SeedGenerator sg = new SeedGenerator();
		new SeedGenerator().generateSeeds(100, "/home/zhangge/seedValues");
//		System.out.println(sg.getOneSeed());
	}
	
	/**
	 * 生成一个随机数 
	 */
	public String getOneSeed() {
		
		Random random = new Random();
		int seed = random.nextInt() % Integer.MAX_VALUE;
		
		return Integer.toString(Math.abs(seed));
		
//		long length = 999999999999999999L;
//		return Long.toString(Math.round((Math.random() * length)));
		
	}
	
	public void generateSeeds(int num, String pathName) throws IOException {
		FileWriter file = new FileWriter(pathName);
		BufferedWriter bufferedWriter = new BufferedWriter(file);
		String seed = null;
		for (int i = 0; i < num; i++) {
			seed = this.getOneSeed();
			bufferedWriter.write(seed);
			bufferedWriter.newLine();
		}
		bufferedWriter.flush();
		bufferedWriter.close();
		file.close();
	}
}
