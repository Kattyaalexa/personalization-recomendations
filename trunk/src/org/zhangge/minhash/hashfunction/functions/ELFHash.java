package org.zhangge.minhash.hashfunction.functions;

import org.zhangge.minhash.hashfunction.HashFunction;

public class ELFHash implements HashFunction {

	@Override
	public int hash(String key) {
		int hash = 0;
		int x = 0;
		for (int i = 0; i < key.length(); i++) {
			hash = (hash << 4) + key.charAt(i);
			if ((x = hash & 0xF0000000) != 0) {
				hash ^= (x >> 24);
				hash &= ~x;
			}
			
		}
		
		return hash & 0x7FFFFFFF;
	}

}
