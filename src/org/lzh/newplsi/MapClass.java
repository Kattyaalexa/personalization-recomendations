package org.lzh.newplsi;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;


public class MapClass extends TableMapper<Text,FloatWritable>{
	
	private Hashtable<Text,Float> hashTable = new Hashtable<Text,Float>();//存放intermediaNz,intermediaNsz的数据
	

	protected void setup(Context context) throws IOException,InterruptedException {
		
		Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		BufferedReader br = null;
		for(int i=0;i<cacheFiles.length;i++) {
			String line;
			String[] tokens;
			
			br = new BufferedReader(new FileReader(cacheFiles[i].toString()));
			while((line=br.readLine()) != null) {
				tokens = line.split("\t",2);
				hashTable.put(new Text(tokens[0]),Float.parseFloat(tokens[1]));
			}
			
		}
		br.close();		
		
	}
	
	protected void map(ImmutableBytesWritable key,Result values,Context context) throws IOException,InterruptedException {
		
		List<KeyValue> clusters_kv = new ArrayList<KeyValue>();
		List<KeyValue> history_kv = new ArrayList<KeyValue>();
		//Hashtable<Text,Float> hashTable_usersum = new Hashtable<Text,Float>();//存放usersum数据
		//float u_sum = 0; //记录每个user所有的sum值
		
		//把values的内容分发到clusters_kv和history_kv中
		for(KeyValue kv : values.raw()) {
			if("clusters_plsi".equals(Bytes.toString(kv.getFamily()))){
				clusters_kv.add(kv);	//把UT表clusters里面的内容都放进clusters_kv
			} else if("story".equals(Bytes.toString(kv.getFamily()))){
				history_kv.add(kv);	//把UT表history里面的内容都放进history_kv
			}
		}
		//计算sum的值，即q*中的分母
		int story_size = history_kv.size();
		for(int i=0;i<story_size;i++){
			
			float sum = 0;	//sum是q*中的分母
			Text z[] = new Text[20];	//z临时保存hashTable中的Key,对应于N(z)
			Text zs[] = new Text[20]; //zs临时保存hashTable中的Key,形式是s==z,对应于N(z|s)
			float psu[] = new float[20];
			float pzu = 0;	//p(z|u)的值
			float q = 0;	//q*的值
			
			for(int j=0;j<clusters_kv.size();j++){
				z[j] = new Text(clusters_kv.get(j).getQualifier());
				zs[j] = new Text(Bytes.toString(history_kv.get(i).getQualifier())+"=="+z[j].toString());
				pzu = Bytes.toFloat(clusters_kv.get(j).getValue());
				psu[j] = (hashTable.get(zs[j])/hashTable.get(z[j]))*pzu;
				sum += psu[j];
			}
			
			//u_sum += sum;
			//利用上面的sum计算q*
			for(int j=0;j<clusters_kv.size();j++){
				//z = new Text(clusters_kv.get(j).getQualifier());
				//zs = new Text(Bytes.toString(history_kv.get(i).getQualifier())+"=="+z.toString());
				//pzu = Bytes.toFloat(clusters_kv.get(j).getValue());
				//q = ( (hashTable.get(zs)/hashTable.get(z))*pzu )/sum;
				q = psu[j]/sum;
				context.write(zs[j],new FloatWritable(q));
				context.write(z[j],new FloatWritable(q));
				//hashTable_usersum.put(new Text("@"+"=="+Bytes.toString(key.get())+"=="+z.toString()), q);
				context.write(new Text("@"+"=="+Bytes.toString(key.get())+"=="+z[j].toString()),new FloatWritable(q/story_size));
			}
			
		}
		/*Set<Text> keys = hashTable_usersum.keySet();
		for(Text k:keys){
			float temp = hashTable_usersum.get(k)/story_size;
			context.write(k, new FloatWritable(temp));
		}*/
		//利用上面的sum计算q*
		/*for(int i=0;i<history_kv.size();i++){
			for(int j=0;j<clusters_kv.size();j++){
				z = new Text(clusters_kv.get(j).getQualifier());
				zs = new Text(Bytes.toString(history_kv.get(i).getQualifier())+"=="+z.toString());
				pzu = Bytes.toFloat(clusters_kv.get(j).getValue());
				q = ( (hashTable.get(zs)/hashTable.get(z))*pzu )/sum;
				
				context.write(zs,new FloatWritable(q));
				context.write(z,new FloatWritable(q));
				context.write(new Text("@"+"=="+Bytes.toString(key.get())+"=="+z.toString()),new FloatWritable(q));
			}
		}*/
	}

}
