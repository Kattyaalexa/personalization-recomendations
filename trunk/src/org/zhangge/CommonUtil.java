package org.zhangge;

public class CommonUtil {

	public static String UT = "UserTable";//用户表表名
	public static String ST = "StoryTable";//新闻表表名

	public static String UT_Family1 = "story";//UT表列族1
	public static String UT_Family2 = "clusters";//UT表列族2
	public static String UT_Family3 = "clusters_plsi";//UT表列族3，for lz‘s require
	public static String UT_Family2_Column = "clusterId";//统一这个列的前缀
	public static String ST_Family1 = "click-times";//ST表列族1
	public static String ST_Family2 = "covisit-times";//ST表列族2
	public static String ST_Family3 = "clusters_plsi";//ST表列族3，for lz‘s require
	
	public static int MinHash_q = 20;//MinHash里面的q，就是每个用户所属集群的数量
	public static int MinHash_p = 2;//MinHash里面的p，就是产生集群id所连接的hash值的个数
	public static int MinHash_StandardClusterSize = 10;//精确一个集群的大小，最少10人
	
	public static String filepath = "/home/zhangge/workspace/PersonalizationRecomendations/ml-100k/";//文件的路径
	public static String train_set = "u5.base";//训练学习数据
	public static String test_set = "u5.test";//测试验证数据

	public static String user_data = "u.data";//所有的总数据，用于计算平均分
	public static String uid_set = "u.user";//用于存放用户id
	public static String average_set = "averager.set";//用于存放每个用户的平均值
	public static String seedvalue = "SeedValues";//存放随机种子值的文件
	public static String recommand_scores = "recommand_scores";//存放推荐分数
	public static String candidate = "candidate.set";//用于存放候选story
	public static String precision_recall = "precisionrecall";//存放precision和recall
	
	public static String split_char = "\t";//movieline的数据分隔符
	public static String split_average = ":";//存放用户平均分数的分隔符
	public static String split_user = "\\|";
	
//	public static int recommond_number = 100;//推荐数目
	public static int Threshold_min = 1;//设置推荐最小值的门槛的指数集合起始值
	public static int Threshold_max = 40;//设置推荐最小值的门槛的指数集合结束值
	
}
