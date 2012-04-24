package org.zhangge.minhash.hashfunction;

/**
 * @author zhangge
 * 定义一个hash函数接口
 */
public interface HashFunction {

	
	
	/**
	 * 产生hash-value，现在先用返回int大小，以后再换成long
	 * @param key
	 * @return
	 */
	public int hash(String key);
	
}
