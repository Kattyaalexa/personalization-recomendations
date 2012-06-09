package org.zhangge;

import java.io.IOException;

import org.zhangge.minhash.MinHashClient;
import org.zhangge.minhash.random.SeedGenerator;
import org.zhangge.recomendrequest.NewsPeronalizationServer;
import org.zhangge.updaterequest.NewsStatisticsServer;

public class Evaluation {

	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		//第一步，NFE读取movieline数据，计算平均分，把用户id和平均分写到文件，story写到hbase
		NewsFrontEnd NFE = new NewsFrontEnd();
		NFE.readData(CommonUtil.filepath + CommonUtil.train_set);
		NFE.writeUidData(CommonUtil.filepath + CommonUtil.uid_set);
		NFE.writeAverageData(CommonUtil.filepath + CommonUtil.average_set);
		
		//第二步，调用minhash算法把用户分集群
		SeedGenerator SG = new SeedGenerator();//首先生成一下随机数种子
		SG.generateSeeds(CommonUtil.MinHash_p * CommonUtil.MinHash_q, CommonUtil.filepath + CommonUtil.seedvalue);
		MinHashClient MHC = new MinHashClient();
		MHC.run();
		
		//第三步，NSS读取用户id，更新点击数到ST表
		NewsStatisticsServer NSS = new NewsStatisticsServer();
		NSS.connectToHbase();
		NSS.readUids(CommonUtil.filepath + CommonUtil.uid_set);
		NSS.admin.close();
		
		//第四步：NPS维护每个集群的总点击数，为每个用户计算推荐分数并写到文件里面去，然后计算precision和call
		NewsPeronalizationServer NPS = new NewsPeronalizationServer();
		NPS.connectToHbase();
		NPS.summarizeClicks();
		NPS.readUids(CommonUtil.filepath + CommonUtil.uid_set);
		NPS.writeScoreToFile(CommonUtil.filepath + CommonUtil.recommand_scores);
		NPS.computePrecisionAndRecall(CommonUtil.filepath + CommonUtil.precision_recall);
		NPS.writeCandidate(CommonUtil.filepath + CommonUtil.candidate);
		NPS.admin.close();
	}
}
