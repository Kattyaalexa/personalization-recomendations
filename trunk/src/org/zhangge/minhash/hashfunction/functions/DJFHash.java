package org.zhangge.minhash.hashfunction.functions;

import org.zhangge.minhash.hashfunction.HashFunction;

public class DJFHash implements HashFunction {

	@Override
	public int hash(String key) {
		int hash = 5381;
		for (int i = 0; i < key.length(); i++) {
			hash += (hash << 5) + key.charAt(i);
		}
		
		return hash & 0x7FFFFFFF;
	}

}
