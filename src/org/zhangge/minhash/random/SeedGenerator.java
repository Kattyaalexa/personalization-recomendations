package org.zhangge.minhash.random;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

public class SeedGenerator {
	
	private Set<Integer> seeds = new HashSet<Integer>();//记录种子的一个库
	private Integer[] intSeeds = null;

	public static void main(String[] args) throws IOException {
//		SeedGenerator sg = new SeedGenerator();
		new SeedGenerator().generateSeeds(100, "/home/zhangge/seedValues");
//		System.out.println(sg.getOneSeed());
	}
	
	/**
	 * 生成一个随机数 
	 */
	public Integer generateOneSeed() {
		
		Random random = new Random();
		int seed = random.nextInt() % Integer.MAX_VALUE;
		Integer normalSeed = Math.abs(seed);
		this.seeds.add(normalSeed);
		
		return normalSeed;
		
//		long length = 999999999999999999L;
//		return Long.toString(Math.round((Math.random() * length)));
		
	}
	
	/**
	 * 产生随机数并写入到文件
	 * @param num
	 * @param pathName
	 * @throws IOException
	 */
	public void generateSeeds(int num, String pathName) throws IOException {
		FileWriter file = new FileWriter(pathName);
		BufferedWriter bufferedWriter = new BufferedWriter(file);
		String seed = null;
		for (int i = 0; i < num; i++) {
			Integer intSeed = generateOneSeed();//生成一个种子
			while (seeds.contains(intSeed)) {
				intSeed = generateOneSeed();
			}
			this.addSeeds(intSeed);//先加入库再写到文件
			seed = Integer.toString(intSeed);
			bufferedWriter.write(seed);
			bufferedWriter.newLine();
		}
		bufferedWriter.flush();
		bufferedWriter.close();
		file.close();
	}
	
	/**
	 * 往库里面添加一个种子
	 * @param seed
	 */
	public void addSeeds(Integer seed) {
		seeds.add(seed);
	}
	
	/**
	 * 根据索引获得一个seed
	 * @param index
	 * @return
	 */
	public Integer getOneSeed(int index) {
		if (index >= seeds.size()) {
			return null;
		}
		if (intSeeds == null) {
			intSeeds = seeds.toArray(intSeeds);
		}
		
		return intSeeds[index];
	}
	
	/**
	 * 获得种子库
	 * @return
	 */
	public Set<Integer> getSeeds() {
		return seeds;
	}
}
