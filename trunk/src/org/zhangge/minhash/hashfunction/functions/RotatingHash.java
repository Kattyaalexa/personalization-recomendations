package org.zhangge.minhash.hashfunction.functions;

import org.zhangge.minhash.hashfunction.HashFunction;

public class RotatingHash implements HashFunction {

	@Override
	public int hash(String key) {
		int hash, i;
		for (hash = key.length(), i = 0; i < key.length(); ++i)
			hash = (hash << 4) ^ (hash >> 28) ^ key.charAt(i);
		return hash;
	}

}
