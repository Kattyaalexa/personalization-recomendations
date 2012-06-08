package org.zhangge;

public class CommonUtil {

	public static String UT = "UserTable";//用户表表名
	public static String ST = "StoryTable";//新闻表表名

	public static String UT_Family1 = "story";//UT表列族1
	public static String UT_Family2 = "clusters";//UT表列族2
	public static String UT_Family2_Column = "clusterId";//统一这个列的前缀
	public static String ST_Family1 = "click-times";//ST表列族1
	public static String ST_Family2 = "covisit-times";//ST表列族2
	
	public static int MinHash_q = 20;//MinHash里面的q，就是每个用户所属集群的数量
	public static int MinHash_p = 2;//MinHash里面的p，就是产生集群id所连接的hash值的个数
	public static int MinHash_StandardClusterSize = 2;//精确一个集群的大小，最少10人
	
	public static String filepath = "/home/zhangge/workspace/PersonalizationRecomendations/ml-100k/";//文件的路径
}
