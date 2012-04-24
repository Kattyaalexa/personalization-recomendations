package org.zhangge.minhash.hashfunction.functions;

import org.zhangge.minhash.hashfunction.HashFunction;

public class JSHash implements HashFunction {

	@Override
	public int hash(String key) {
		int hash = 1315423911;
		for (int i = 0; i < key.length(); i++) {
			hash ^= (hash << 5) + key.charAt(i) + (hash >> 2);
		}
		return hash & 0x7FFFFFFF;
	}

}
