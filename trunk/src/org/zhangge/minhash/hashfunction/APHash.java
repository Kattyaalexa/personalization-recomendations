package org.zhangge.minhash.hashfunction;

public class APHash implements HashFunction {

	@Override
	public int hash(String key) {
		int hash = 0;
		for (int i = 0; i < key.length(); i++) {
			if ((i & 1) == 0) {
				hash ^= ((hash << 7) ^ key.charAt(i) ^ (hash >> 3));
			} else {
				hash ^= (~((hash << 11) ^ key.charAt(i) ^ (hash >> 5)));
			}
			
		}
		return hash & 0x7FFFFFFF;
	}

}
