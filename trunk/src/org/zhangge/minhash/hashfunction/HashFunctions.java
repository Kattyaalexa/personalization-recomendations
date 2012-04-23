package org.zhangge.minhash.hashfunction;

import java.security.MessageDigest;

public class HashFunctions {
	
	/**
	 * MASK值，随便找一个值，最好是质数
	 */
	static int M_MASK = 0x8765fed1;
	 
	public static void main(String[] args) throws Exception {
		HashFunctions hf = new HashFunctions();
		String key = "http://www.google.com/news/china_new_hope.html";
		System.out.println(key.hashCode());
		System.out.println(hf.simpleHash(key));
		System.out.println(hf.RSHash(key));
		System.out.println(hf.JSHash(key));
		System.out.println(Integer.SIZE);
		System.out.println(hf.PJWHash(key));
		System.out.println(hf.APHash(key));
		System.out.println(hf.BKDRHash(key));
		System.out.println(hf.CRCHash(key));//负数
		System.out.println(hf.DJFHash(key));
		System.out.println(hf.ELFHash(key));
		System.out.println(hf.SDBMHash(key));
		System.out.println(hf.sha1_text(key));
		System.out.println(String.valueOf(hf.sha1(key)));
	}
	
	  
	/**
	 * 计算sha1
	 * 
	 * @param text
	 *            文本
	 * @return 16进制表示的hash值
	 * @throws Exception
	 */
	public String sha1_text(String text) throws Exception {
		// 4位值对应16进制字符  
		char[] m_byteToHexChar = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };  
		byte[] hash = sha1(text);
		StringBuilder ret = new StringBuilder(hash.length * 2);
		for (byte b : hash) {
			int d = (b & 0xff);
			ret.append(m_byteToHexChar[(d & 0xf)]);
			d >>= 4;
			ret.append(m_byteToHexChar[(d & 0xf)]);
		}
		return ret.toString();
	} 
	
	
	/**
	 * 计算sha1
	 * 
	 * @param text
	 *            文本
	 * @return 字节数组
	 * @throws Exception
	 */
	public byte[] sha1(String text) throws Exception {
		MessageDigest md;
		md = MessageDigest.getInstance("SHA-1");
		byte[] sha1hash = new byte[64];
		byte[] input = text.getBytes("utf-8");
		md.update(input, 0, input.length);
		sha1hash = md.digest();
		return sha1hash;
	}
	
	/**
	 * 混合hash算法，输出64位的值
	 */
	public long mixHash(String str) {
		long hash = str.hashCode();
		hash <<= 32;
		hash |= this.FNVHash1(str);
		return hash;
	}
	
	/**
	 * DEK算法
	 */
	public int DEKHash(String str) {
		int hash = str.length();

		for (int i = 0; i < str.length(); i++) {
			hash = ((hash << 5) ^ (hash >> 27)) ^ str.charAt(i);
		}

		return (hash & 0x7FFFFFFF);
	} 
	
	/**
	 * 一次一个hash
	 * 
	 * @param key
	 *            输入字符串
	 * @return 输出hash值
	 */
	public int oneByOneHash(String key) {
		int hash, i;
		for (hash = 0, i = 0; i < key.length(); ++i) {
			hash += key.charAt(i);
			hash += (hash << 10);
			hash ^= (hash >> 6);
		}
		hash += (hash << 3);
		hash ^= (hash >> 11);
		hash += (hash << 15);
		// return (hash & M_MASK);
		return hash;
	}
	
	/**
	 * 查表Hash
	 * @param key
	 * @param hash
	 * @return
	 */
	public int crc32(String key, int hash) {  
		int crctab[] = {  
			0x00000000, 0x77073096, 0xee0e612c, 0x990951ba, 0x076dc419, 0x706af48f,  0xe963a535, 0x9e6495a3, 0x0edb8832, 0x79dcb8a4, 0xe0d5e91e, 0x97d2d988,  0x09b64c2b, 0x7eb17cbd, 0xe7b82d07, 0x90bf1d91, 0x1db71064, 0x6ab020f2,  0xf3b97148, 0x84be41de, 0x1adad47d, 0x6ddde4eb, 0xf4d4b551, 0x83d385c7,  0x136c9856, 0x646ba8c0, 0xfd62f97a, 0x8a65c9ec, 0x14015c4f, 0x63066cd9,  0xfa0f3d63, 0x8d080df5, 0x3b6e20c8, 0x4c69105e, 0xd56041e4, 0xa2677172,  0x3c03e4d1, 0x4b04d447, 0xd20d85fd, 0xa50ab56b, 0x35b5a8fa, 0x42b2986c,  0xdbbbc9d6, 0xacbcf940, 0x32d86ce3, 0x45df5c75, 0xdcd60dcf, 0xabd13d59,  0x26d930ac, 0x51de003a, 0xc8d75180, 0xbfd06116, 0x21b4f4b5, 0x56b3c423,  0xcfba9599, 0xb8bda50f, 0x2802b89e, 0x5f058808, 0xc60cd9b2, 0xb10be924,  0x2f6f7c87, 0x58684c11, 0xc1611dab, 0xb6662d3d, 0x76dc4190, 0x01db7106,  0x98d220bc, 0xefd5102a, 0x71b18589, 0x06b6b51f, 0x9fbfe4a5, 0xe8b8d433,  0x7807c9a2, 0x0f00f934, 0x9609a88e, 0xe10e9818, 0x7f6a0dbb, 0x086d3d2d,  0x91646c97, 0xe6635c01, 0x6b6b51f4, 0x1c6c6162, 0x856530d8, 0xf262004e,  0x6c0695ed, 0x1b01a57b, 0x8208f4c1, 0xf50fc457, 0x65b0d9c6, 0x12b7e950,  0x8bbeb8ea, 0xfcb9887c, 0x62dd1ddf, 0x15da2d49, 0x8cd37cf3, 0xfbd44c65,  0x4db26158, 0x3ab551ce, 0xa3bc0074, 0xd4bb30e2, 0x4adfa541, 0x3dd895d7,  0xa4d1c46d, 0xd3d6f4fb, 0x4369e96a, 0x346ed9fc, 0xad678846, 0xda60b8d0,  0x44042d73, 0x33031de5, 0xaa0a4c5f, 0xdd0d7cc9, 0x5005713c, 0x270241aa,  0xbe0b1010, 0xc90c2086, 0x5768b525, 0x206f85b3, 0xb966d409, 0xce61e49f,  0x5edef90e, 0x29d9c998, 0xb0d09822, 0xc7d7a8b4, 0x59b33d17, 0x2eb40d81,  0xb7bd5c3b, 0xc0ba6cad, 0xedb88320, 0x9abfb3b6, 0x03b6e20c, 0x74b1d29a,  0xead54739, 0x9dd277af, 0x04db2615, 0x73dc1683, 0xe3630b12, 0x94643b84,  0x0d6d6a3e, 0x7a6a5aa8, 0xe40ecf0b, 0x9309ff9d, 0x0a00ae27, 0x7d079eb1,  0xf00f9344, 0x8708a3d2, 0x1e01f268, 0x6906c2fe, 0xf762575d, 0x806567cb,  
			0x196c3671, 0x6e6b06e7, 0xfed41b76, 0x89d32be0, 0x10da7a5a, 0x67dd4acc,  0xf9b9df6f, 0x8ebeeff9, 0x17b7be43, 0x60b08ed5, 0xd6d6a3e8, 0xa1d1937e,  0x38d8c2c4, 0x4fdff252, 0xd1bb67f1, 0xa6bc5767, 0x3fb506dd, 0x48b2364b,  0xd80d2bda, 0xaf0a1b4c, 0x36034af6, 0x41047a60, 0xdf60efc3, 0xa867df55,  0x316e8eef, 0x4669be79, 0xcb61b38c, 0xbc66831a, 0x256fd2a0, 0x5268e236,  0xcc0c7795, 0xbb0b4703, 0x220216b9, 0x5505262f, 0xc5ba3bbe, 0xb2bd0b28,  0x2bb45a92, 0x5cb36a04, 0xc2d7ffa7, 0xb5d0cf31, 0x2cd99e8b, 0x5bdeae1d,  0x9b64c2b0, 0xec63f226, 0x756aa39c, 0x026d930a, 0x9c0906a9, 0xeb0e363f,  0x72076785, 0x05005713, 0x95bf4a82, 0xe2b87a14, 0x7bb12bae, 0x0cb61b38,  0x92d28e9b, 0xe5d5be0d, 0x7cdcefb7, 0x0bdbdf21, 0x86d3d2d4, 0xf1d4e242,  0x68ddb3f8, 0x1fda836e, 0x81be16cd, 0xf6b9265b, 0x6fb077e1, 0x18b74777,  0x88085ae6, 0xff0f6a70, 0x66063bca, 0x11010b5c, 0x8f659eff, 0xf862ae69,  0x616bffd3, 0x166ccf45, 0xa00ae278, 0xd70dd2ee, 0x4e048354, 0x3903b3c2,  0xa7672661, 0xd06016f7, 0x4969474d, 0x3e6e77db, 0xaed16a4a, 0xd9d65adc,  0x40df0b66, 0x37d83bf0, 0xa9bcae53, 0xdebb9ec5, 0x47b2cf7f, 0x30b5ffe9,  0xbdbdf21c, 0xcabac28a, 0x53b39330, 0x24b4a3a6, 0xbad03605, 0xcdd70693,  0x54de5729, 0x23d967bf, 0xb3667a2e, 0xc4614ab8, 0x5d681b02, 0x2a6f2b94,  0xb40bbe37, 0xc30c8ea1, 0x5a05df1b, 0x2d02ef8d  
		};  
	  int i;  
	  for (hash=key.length(), i=0; i<key.length(); ++i)  
	    hash = (hash >> 8) ^ crctab[(hash & 0xff) ^ key.charAt(i)];  
	  return hash;  
	}
	
	/**
	 * 改進型的32位FNV算法
	 * @param data
	 * @return
	 */
	public int FNVHash1(String data) {  
        final int p = 16777619;  
        int hash = (int)2166136261L;  
        for(int i=0;i<data.length();i++)  
            hash = (hash ^ data.charAt(i)) * p;  
        hash += hash << 13;  
        hash ^= hash >> 7;  
        hash += hash << 3;  
        hash ^= hash >> 17;  
        hash += hash << 5;  
        return hash;  
	}
	
    /**
     * 32位FNV算法
     * @param data
     * @return
     */
    public int FNVHash(String key) {  
    	int M_SHIFT = 0;  
        int hash = (int)2166136261L;  
        byte[] data = key.getBytes();
        for(byte b : data)  
            hash = (hash * 16777619) ^ b;  
        if (M_SHIFT == 0)  
            return hash;  
        return (hash ^ (hash >> M_SHIFT)) & 0x7FFFFFFF;  
    }
	
	/**
	 * 旋转hash 与JS相同拉
	 * @param key
	 * @return
	 */
	public int rotatingHash(String key) {
		int hash, i;
		for (hash = key.length(), i = 0; i < key.length(); ++i)
			hash = (hash << 4) ^ (hash >> 28) ^ key.charAt(i);
		return hash;
	}
	
	/**
	 * 加法hash
	 * @param key
	 * @return
	 */
	public int additiveHash(String key) {
		int hash, i;
		for (hash = key.length(), i = 0; i < key.length(); i++)
			hash += key.charAt(i);
		return hash;
	}
	
	/**
	 * 就是java的String的hashCode方法
	 * @param key
	 * @return
	 */
	public int simpleHash(String key) {
		int hash = 0;
		for (int i = 0; i < key.length(); i++) {
			hash = 31 * hash + key.charAt(i);
		}
		return hash;
	}
	
	/**
	 * RS Hash Function
	 * @param key
	 * @return
	 */
	public int RSHash(String key) {
		int b = 378551;
		int a = 63689;
		int hash = 0;
		for (int i = 0; i < key.length(); i++) {
			hash = hash * a + key.charAt(i);
			a *= b;
		}
		return hash & 0x7FFFFFFF;
	}
	
	/**
	 * PJW Hash Function
	 * @param key
	 * @return
	 */
	public int PJWHash(String key) {
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
	
	/**
	 * @param key
	 * @return
	 */
	public int JSHash(String key) {
		int hash = 1315423911;
		for (int i = 0; i < key.length(); i++) {
			hash ^= (hash << 5) + key.charAt(i) + (hash >> 2);
		}
		return hash & 0x7FFFFFFF;
	}
	
	/**
	 * ELF Hash Function
	 * @param key
	 * @return
	 */
	public int ELFHash(String key) {
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
	
	/**
	 * BKDR Hash Function
	 * @param key
	 * @return
	 */
	public int BKDRHash(String key) {
		int seed = 131;//31 131 1313 13131 1313131 etc....
		int hash = 0;
		
		for (int i = 0; i < key.length(); i++) {
			hash = hash * seed + key.charAt(i);
		}
		
		return hash & 0x7FFFFFFF;
	}
	
	/**
	 * SDBM Hash Function
	 * @param key
	 * @return
	 */
	public int SDBMHash(String key) {
		int hash = 0;
		for (int i = 0; i < key.length(); i++) {
//			hash = key.charAt(i) + (hash << 6) + (hash << 16) - hash;
			hash = 65599*hash + key.charAt(i);
		}
		return hash & 0x7FFFFFFF;
	}
	
	/**
	 * DJF Hash Function
	 * @param key
	 * @return
	 */
	public int DJFHash(String key) {
		int hash = 5381;
		for (int i = 0; i < key.length(); i++) {
			hash += (hash << 5) + key.charAt(i);
		}
		
		return hash & 0x7FFFFFFF;
	}
	
	/**
	 * AP Hash Function
	 * @param key
	 * @return
	 */
	public int APHash(String key) {
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
	
	/**
	 * CRC Hash Function
	 * @param key
	 * @return
	 */
	public int CRCHash(String key) {
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
