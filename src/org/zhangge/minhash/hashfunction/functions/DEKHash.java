package org.zhangge.minhash.hashfunction.functions;

import org.zhangge.minhash.hashfunction.HashFunction;

public class DEKHash implements HashFunction {

	@Override
	public int hash(String key) {
		int hash = key.length();

		for (int i = 0; i < key.length(); i++) {
			hash = ((hash << 5) ^ (hash >> 27)) ^ key.charAt(i);
		}

		return (hash & 0x7FFFFFFF);
	}

}
