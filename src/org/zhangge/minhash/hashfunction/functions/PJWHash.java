package org.zhangge.minhash.hashfunction.functions;

import org.zhangge.minhash.hashfunction.HashFunction;

public class PJWHash implements HashFunction {

	@Override
	public int hash(String key) {
		int BitsInUnignedInt = Integer.SIZE;
		int ThreeQuarters = ((BitsInUnignedInt * 3) / 4);
		int OneEighth = (BitsInUnignedInt / 8); 
		int HighBits = (0xFFFFFFFF) << (BitsInUnignedInt - OneEighth);
		int hash = 0;
		int test = 0;
		for (int i = 0; i < key.length(); i++) {
			hash = (hash << OneEighth) + key.charAt(i);
			if ((test = hash & HighBits) != 0) {
				 hash = ((hash ^ (test >> ThreeQuarters)) & (~HighBits));
			}
		}
		
		return hash & 0x7FFFFFFF;
	}

}
