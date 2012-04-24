package org.zhangge.minhash.hashfunction.functions;

import org.zhangge.minhash.hashfunction.HashFunction;

public class MixHash implements HashFunction {

	@Override
	public int hash(String key) {
		long hash = key.hashCode();
		hash <<= 32;
		hash = new FNVHash2().hash(key);
		return (int) hash;
	}

}
