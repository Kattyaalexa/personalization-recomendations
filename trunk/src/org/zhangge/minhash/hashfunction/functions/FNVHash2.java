package org.zhangge.minhash.hashfunction.functions;

import org.zhangge.minhash.hashfunction.HashFunction;

public class FNVHash2 implements HashFunction{

	@Override
	public int hash(String key) {
		final int p = 16777619;  
        int hash = (int)2166136261L;  
        for(int i=0;i<key.length();i++)  
            hash = (hash ^ key.charAt(i)) * p;  
        hash += hash << 13;  
        hash ^= hash >> 7;  
        hash += hash << 3;  
        hash ^= hash >> 17;  
        hash += hash << 5;  
        return hash;  
	}

}
