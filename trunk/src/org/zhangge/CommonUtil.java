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
	public static int MinHash_StandardClusterSize = 7;//精确一个集群的大小，最少10人
	
	public static String filepath = "/home/zhangge/workspace/PersonalizationRecomendations/ml-100k/";//文件的路径
	public static String train_set = "u1.base";//训练学习数据
	public static String test_set = "u1.test";//测试验证数据
	public static String uid_set = "uids.set";//用于存放用户id
	public static String average_set = "averager.set";//用于存放每个用户的平均值
	public static String seedvalue = "SeedValues";//存放随机种子值的文件
	public static String recommand_scores = "recommand_scores";//存放推荐分数
	public static String candidate = "candidate.set";//用于存放候选story
	public static String split_char = "\t";//movieline的数据分隔符
	public static String split_cluster = "%";//在mapreduce传送当中分开clusterid和cluster编号
	
}
