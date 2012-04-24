package org.zhangge.minhash.hashfunction;

import java.util.HashMap;
import java.util.Map;

import org.zhangge.minhash.hashfunction.functions.APHash;
import org.zhangge.minhash.hashfunction.functions.AdditiveHash;
import org.zhangge.minhash.hashfunction.functions.BKDRHash;
import org.zhangge.minhash.hashfunction.functions.CRC32;
import org.zhangge.minhash.hashfunction.functions.CRCHash;
import org.zhangge.minhash.hashfunction.functions.DEKHash;
import org.zhangge.minhash.hashfunction.functions.DJFHash;
import org.zhangge.minhash.hashfunction.functions.ELFHash;
import org.zhangge.minhash.hashfunction.functions.FNVHash;
import org.zhangge.minhash.hashfunction.functions.FNVHash2;
import org.zhangge.minhash.hashfunction.functions.JSHash;
import org.zhangge.minhash.hashfunction.functions.MixHash;
import org.zhangge.minhash.hashfunction.functions.OneByOneHash;
import org.zhangge.minhash.hashfunction.functions.PJWHash;
import org.zhangge.minhash.hashfunction.functions.RSHash;
import org.zhangge.minhash.hashfunction.functions.RotatingHash;
import org.zhangge.minhash.hashfunction.functions.SDBMHash;
import org.zhangge.minhash.hashfunction.functions.SimpleHash;
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
            hashFunctionFactory.addHashFunction(seedGenerator.getOneSeed(4), new AdditiveHash());
            hashFunctionFactory.addHashFunction(seedGenerator.getOneSeed(5), new ELFHash());
            hashFunctionFactory.addHashFunction(seedGenerator.getOneSeed(6), new BKDRHash());
            hashFunctionFactory.addHashFunction(seedGenerator.getOneSeed(7), new CRC32());
            hashFunctionFactory.addHashFunction(seedGenerator.getOneSeed(8), new CRCHash());
            hashFunctionFactory.addHashFunction(seedGenerator.getOneSeed(9), new DEKHash());
            hashFunctionFactory.addHashFunction(seedGenerator.getOneSeed(10), new FNVHash());
            hashFunctionFactory.addHashFunction(seedGenerator.getOneSeed(11), new FNVHash2());
            hashFunctionFactory.addHashFunction(seedGenerator.getOneSeed(12), new JSHash());
            hashFunctionFactory.addHashFunction(seedGenerator.getOneSeed(13), new MixHash());
            hashFunctionFactory.addHashFunction(seedGenerator.getOneSeed(14), new OneByOneHash());
            hashFunctionFactory.addHashFunction(seedGenerator.getOneSeed(15), new PJWHash());
            hashFunctionFactory.addHashFunction(seedGenerator.getOneSeed(16), new RotatingHash());
            hashFunctionFactory.addHashFunction(seedGenerator.getOneSeed(17), new RSHash());
            hashFunctionFactory.addHashFunction(seedGenerator.getOneSeed(18), new SimpleHash());
            
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
