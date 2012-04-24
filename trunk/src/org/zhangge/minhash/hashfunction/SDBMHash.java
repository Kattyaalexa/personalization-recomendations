package org.zhangge.minhash.hashfunction;

public class SDBMHash implements HashFunction {

	@Override
	public int hash(String key) {
		int hash = 0;
		for (int i = 0; i < key.length(); i++) {
//			hash = key.charAt(i) + (hash << 6) + (hash << 16) - hash;
			hash = 65599*hash + key.charAt(i);
		}
		return hash & 0x7FFFFFFF;
	}

}
