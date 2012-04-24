package org.zhangge.minhash.hashfunction.functions;

import org.zhangge.minhash.hashfunction.HashFunction;

public class SimpleHash implements HashFunction {

	@Override
	public int hash(String key) {
		int hash = 0;
		for (int i = 0; i < key.length(); i++) {
			hash = 31 * hash + key.charAt(i);
		}
		return hash;
	}

}
