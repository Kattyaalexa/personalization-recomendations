package org.lzh.table;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class GenResult {

	private static Configuration conf = null;
	private static HTable ut;
	private static HTable st;
	private static HTable zt;
	
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
	
	public void genRecForUser(String uid) throws IOException{
		
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
				
				if(sum!=0){
					System.out.println(uid+"	"+ s +"	"+ sum);
				}
				
			}
		}
	}
	
	public void readUids(String filepath) throws IOException {
		File file = new File(filepath);
		BufferedReader br = new BufferedReader(new FileReader(file));
		String uid = null;
		while((uid = br.readLine()) != null) {
			genRecForUser(uid);
		}
		br.close();
	}
	
	public static void main(String[] args) throws IOException {
		
	}
}
