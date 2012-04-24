package org.zhangge.minhash.hashfunction.functions;

import org.zhangge.minhash.hashfunction.HashFunction;

public class RSHash implements HashFunction {

	@Override
	public int hash(String key) {
		int b = 378551;
		int a = 63689;
		int hash = 0;
		for (int i = 0; i < key.length(); i++) {
			hash = hash * a + key.charAt(i);
			a *= b;
		}
		return hash & 0x7FFFFFFF;
	}

}
