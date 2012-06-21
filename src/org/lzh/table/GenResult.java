package org.lzh.table;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.lzh.table.GenTable;

public class GenResult {

	private static Configuration conf = null;
	private static HTable ut;
	private static HTable st;
	private static HTable zt;
	private static Map<String, Map<String, Float>> reclist = new HashMap<String, Map<String, Float>>();//存放推荐集uid,<sid,score>
	private static float rec_Num = 0;
	private static float test_Num = 0;
	private static float bingo = 0;
	private static float precision = 0;
	private static float recall = 0;
	static {
		conf = HBaseConfiguration.create();
		conf.addResource("hbase-site.xml");
		try {
			ut	= new HTable(conf,GenTable.UT);
			st = new HTable(conf,GenTable.ST);
			zt = new HTable(conf,GenTable.ZT);
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
	
	/**
	 * 遍历所有用户,调用genRecForUser()得到reclist
	 * @param filepath
	 * @throws IOException
	 */
	/*public void readUids(String filepath) throws IOException {
		File file = new File(filepath);
		BufferedReader br = new BufferedReader(new FileReader(file));
		String uid = null;
		while((uid = br.readLine()) != null) {
			genRecForUser(uid);
		}
		br.close();
	}*/
	public void readUids() throws IOException {
		for(int i=1;i<=943;i++){
			genRecForUser("u"+i);
		}
	}
	
	/**
	 * 调用genRecForUser()得到reclist
	 * @param uid
	 * @throws IOException
	 */
	public void genRecForUser(String uid) throws IOException{
		Map<String, Float> scores = new HashMap<String, Float>();
		
		float[] pzu = new float[20];
		
		Get getPzu = new Get(uid.getBytes());
		getPzu.addFamily("clusters_plsi".getBytes());
		Result rs_pzu = ut.get(getPzu);//从UT中取出用户所属的clusters的pzu放进数组pzu[]
		for(int i=0;i<20;i++){
			pzu[i] = Float.parseFloat( Bytes.toString(rs_pzu.getValue("clusters_plsi".getBytes(),("z"+(i+1)).getBytes()) ));
		}
		
		Get getClusters = new Get(uid.getBytes());//此row_key是userid
		getClusters.addFamily("clusters".getBytes());
		Result rs_clusters = ut.get(getClusters);//从UT中取出用户所属的clusters
		
		for(KeyValue kv1:rs_clusters.raw()){
			
			Get getStory = new Get(kv1.getQualifier());//此row_key是clusterid
			getStory.addFamily("story".getBytes());
			Result rs_story = zt.get(getStory);//从ZT中取出每个cluster里的story
			
			for(KeyValue kv2:rs_story.raw()){
				String s = Bytes.toString(kv2.getQualifier());
				Get getPsz = new Get(s.getBytes());//此row_key是storyid
				getPsz.addFamily("clusters_plsi".getBytes());
				Result rs_psz = st.get(getPsz);//从ST中取出每个cluster_plsi里的psz
				
				float sum = 0;
				for(int i=0;i<20;i++){
					sum += pzu[i] * Float.parseFloat( Bytes.toString(rs_psz.getValue("clusters_plsi".getBytes(),("z"+(i+1)).getBytes()) ));
				}
				
				if(sum>=0.07){
System.out.println(uid+"	"+ s +"	"+ sum);
					scores.put(s,sum);
					rec_Num++;
				}
				
			}
		}
		//reclist.put(uid,sortMapByValue(scores));
		reclist.put(uid,scores);
	}
	
	/**
	 * 对推荐集进行排序
	 * @param oriMap
	 * @return
	 */
	public Map<String, Float> sortMapByValue(Map<String, Float> oriMap) {  
	    Map<String, Float> sortedMap = new LinkedHashMap<String, Float>();  
	    if (oriMap != null && !oriMap.isEmpty()) {  
	        List<Map.Entry<String, Float>> entryList = new ArrayList<Map.Entry<String, Float>>(oriMap.entrySet());  
	        Collections.sort(entryList, new Comparator<Map.Entry<String, Float>>() {
                public int compare(Map.Entry<String, Float> o1, Map.Entry<String, Float> o2) {
                	Float result = o2.getValue() - o1.getValue();
                		if (result > 0) {
							return 1;
						} else if (result < 0) {
							return -1;
						} else {
							return 0;
						}
	                }
				});  
	        Iterator<Map.Entry<String, Float>> iter = entryList.iterator();  
	        Map.Entry<String, Float> tmpEntry = null;  
	        while (iter.hasNext()) {  
	            tmpEntry = iter.next();  
	            sortedMap.put(tmpEntry.getKey(), tmpEntry.getValue());  
	        }  
	    }  
	    return sortedMap;  
	}
	
	/**
	 * 产生推荐分数写到文件里面去
	 * @param filepath
	 * @throws IOException
	 */
	public void writeScoreToFile(String filepath) throws IOException {
		FileWriter fileWriter = new FileWriter(filepath);
		BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
		Set<String> keys = reclist.keySet();
		for (String key : keys) {
			Map<String, Float> scores = reclist.get(key);
			Set<String> ks = scores.keySet();
			for (String k : ks) {
				Float score = scores.get(k);
				bufferedWriter.write(key + ":" + k + ":" + score);
				bufferedWriter.newLine();
			}
		}
		bufferedWriter.flush();
		bufferedWriter.close();
		fileWriter.close();
	}	
	
	public void computePrecisionAndRecall(String filepath) throws IOException{
		File file = new File(filepath);
		BufferedReader br = new BufferedReader(new FileReader(file));
		Map<String, Float> scores = null ;
		String line = null;
		String last_uid = null;
		String uid = null;
		while((line = br.readLine()) != null){
			String[] parts = line.split("	");
			test_Num++;
			if(!parts[0].equals(last_uid)){
				uid = parts[0];
				last_uid = uid;
				scores = reclist.get(uid);				
			}
			if(scores!=null&&scores.containsKey(parts[1])){
				bingo++;
			}
		}
		
		precision = bingo / rec_Num;
		recall = bingo / test_Num;
System.out.println("bingo:" + bingo);
System.out.println("recommand_size:" + rec_Num);
System.out.println("test_size:" + test_Num);
System.out.println("precision:" + precision);
System.out.println("recall:" + recall);

	}
	public static void main(String[] args) throws IOException {
		GenResult gr = new GenResult();
		gr.readUids();
		//gr.readUids(GenTable.UIDS);
		//gr.writeScoreToFile(GenTable.REC_SCORE_PLSI);
		gr.computePrecisionAndRecall(GenTable.DATA_TEST_FIANL);
	}
}
