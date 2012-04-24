package org.zhangge.minhash.hashfunction.functions;

import org.zhangge.minhash.hashfunction.HashFunction;

public class BKDRHash implements HashFunction {

	@Override
	public int hash(String key) {
		int seed = 131;//31 131 1313 13131 1313131 etc....
		int hash = 0;
		
		for (int i = 0; i < key.length(); i++) {
			hash = hash * seed + key.charAt(i);
		}
		
		return hash & 0x7FFFFFFF;
	}

}
