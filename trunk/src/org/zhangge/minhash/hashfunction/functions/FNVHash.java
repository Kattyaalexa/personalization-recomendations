package org.zhangge.minhash.hashfunction.functions;

import org.zhangge.minhash.hashfunction.HashFunction;

public class FNVHash implements HashFunction {

	@Override
	public int hash(String key) {
		int M_SHIFT = 0;  
        int hash = (int)2166136261L;  
        byte[] data = key.getBytes();
        for(byte b : data)  
            hash = (hash * 16777619) ^ b;  
        if (M_SHIFT == 0)  
            return hash;  
        return (hash ^ (hash >> M_SHIFT)) & 0x7FFFFFFF;
	}

}
