package org.zhangge.minhash.hashfunction.functions;

import org.zhangge.minhash.hashfunction.HashFunction;

public class CRCHash implements HashFunction {

	@Override
	public int hash(String key) {
		int nleft = key.length();
		int sum = 0;
		int answer = 0;
		int index = 0;
		while (nleft > 1) {
			sum += key.charAt(index);
			index ++;
			nleft -= 2;
		}
		
		if (1 == nleft) {
			answer = key.charAt(index);
			sum += answer;
		}
		
		sum = (sum >> 16) + (sum & 0xFFFF);
		sum += (sum >> 16);
		answer = ~sum;
		return Math.abs(answer & 0xFFFFFFFF);
	}

}
