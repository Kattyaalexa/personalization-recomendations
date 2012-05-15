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

public class HashFunctionFactory {
	
	private static Map<Integer, HashFunction> hashFunctions = new HashMap<Integer, HashFunction>();
    private static HashFunctionFactory hashFunctionFactory = null;
    
    private HashFunctionFactory() {
    }
    
    /**
     * 做成了单例模式
     * @return
     */
    public static HashFunctionFactory getInstance(){
            if (hashFunctionFactory != null) {
                    return hashFunctionFactory;
            }
            hashFunctionFactory = new HashFunctionFactory();
            
            hashFunctionFactory.addHashFunction(0, new CRCHash());
            hashFunctionFactory.addHashFunction(1, new APHash());
            hashFunctionFactory.addHashFunction(2, new DJFHash());
            hashFunctionFactory.addHashFunction(3, new SDBMHash());
            hashFunctionFactory.addHashFunction(4, new AdditiveHash());
            hashFunctionFactory.addHashFunction(5, new ELFHash());
            hashFunctionFactory.addHashFunction(6, new BKDRHash());
            hashFunctionFactory.addHashFunction(7, new CRC32());
            hashFunctionFactory.addHashFunction(8, new CRCHash());
            hashFunctionFactory.addHashFunction(9, new DEKHash());
            hashFunctionFactory.addHashFunction(10, new FNVHash());
            hashFunctionFactory.addHashFunction(11, new FNVHash2());
            hashFunctionFactory.addHashFunction(12, new JSHash());
            hashFunctionFactory.addHashFunction(13, new MixHash());
            hashFunctionFactory.addHashFunction(14, new OneByOneHash());
            hashFunctionFactory.addHashFunction(15, new PJWHash());
            hashFunctionFactory.addHashFunction(16, new RotatingHash());
            hashFunctionFactory.addHashFunction(17, new RSHash());
            hashFunctionFactory.addHashFunction(18, new SimpleHash());
            
            return hashFunctionFactory;
    }
    
    /**
     * @param seedValue 随机的与hash函数对应的种子值
     * @return
     */
    public HashFunction getHashFunction(Integer seedValue) {
            HashFunction hash = hashFunctions.get(seedValue);
            if (hash == null) {
            	hash = hashFunctions.get(seedValue % hashFunctions.size());
			}
            return hash;
    }
    
    /**
     * 添加hash函数
     * @param key
     * @param value
     */
    public void addHashFunction(Integer key, HashFunction value) {
            hashFunctions.put(key, value);
    }
}
