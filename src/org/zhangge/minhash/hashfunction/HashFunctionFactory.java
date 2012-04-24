package org.zhangge.minhash.hashfunction;

import java.util.HashMap;
import java.util.Map;

import org.zhangge.minhash.random.SeedGenerator;

public class HashFunctionFactory {

	private Map<Integer, HashFunction> hashFunctions = new HashMap<Integer, HashFunction>();
	private HashFunctionFactory hashFunctionFactory = null;
	private SeedGenerator seedGenerator = new SeedGenerator();
	
	private HashFunctionFactory() {
		
	}
	
	/**
	 * 做成了单例模式
	 * @return
	 */
	public HashFunctionFactory getInstance(){
		if (hashFunctionFactory != null) {
			return hashFunctionFactory;
		}
		hashFunctionFactory = new HashFunctionFactory();
		hashFunctionFactory.addHashFunction(seedGenerator.getOneSeed(0), new CRCHash());
		hashFunctionFactory.addHashFunction(seedGenerator.getOneSeed(1), new APHash());
		hashFunctionFactory.addHashFunction(seedGenerator.getOneSeed(2), new DJFHash());
		hashFunctionFactory.addHashFunction(seedGenerator.getOneSeed(3), new SDBMHash());
		hashFunctionFactory.addHashFunction(seedGenerator.getOneSeed(4), new CRCHash());
		hashFunctionFactory.addHashFunction(seedGenerator.getOneSeed(5), new CRCHash());
		hashFunctionFactory.addHashFunction(seedGenerator.getOneSeed(6), new CRCHash());
		hashFunctionFactory.addHashFunction(seedGenerator.getOneSeed(7), new CRCHash());
		hashFunctionFactory.addHashFunction(seedGenerator.getOneSeed(8), new CRCHash());
		hashFunctionFactory.addHashFunction(seedGenerator.getOneSeed(9), new CRCHash());
		hashFunctionFactory.addHashFunction(seedGenerator.getOneSeed(10), new CRCHash());
		hashFunctionFactory.addHashFunction(seedGenerator.getOneSeed(11), new CRCHash());
		hashFunctionFactory.addHashFunction(seedGenerator.getOneSeed(12), new CRCHash());
		hashFunctionFactory.addHashFunction(seedGenerator.getOneSeed(13), new CRCHash());
		hashFunctionFactory.addHashFunction(seedGenerator.getOneSeed(14), new CRCHash());
		hashFunctionFactory.addHashFunction(seedGenerator.getOneSeed(15), new CRCHash());
		hashFunctionFactory.addHashFunction(seedGenerator.getOneSeed(16), new CRCHash());
		hashFunctionFactory.addHashFunction(seedGenerator.getOneSeed(17), new CRCHash());
		hashFunctionFactory.addHashFunction(seedGenerator.getOneSeed(18), new CRCHash());
		hashFunctionFactory.addHashFunction(seedGenerator.getOneSeed(19), new CRCHash());
		
		
		return hashFunctionFactory;
	}
	
	/**
	 * @param seedValue 随机的与hash函数对应的种子值
	 * @return
	 */
	public HashFunction getHashFunction(int seedValue) {
		HashFunction hash = this.hashFunctions.get(seedValue);
		return hash;
	}
	
	/**
	 * 添加hash函数
	 * @param key
	 * @param value
	 */
	public void addHashFunction(Integer key, HashFunction value) {
		this.hashFunctions.put(key, value);
	}
}
