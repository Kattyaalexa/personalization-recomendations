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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;


public class MapClass extends TableMapper<Text,DoubleWritable>{
	
	private Hashtable<Text,Double> hashTable = new Hashtable<Text,Double>();

	protected void setup(Context context) throws IOException,InterruptedException {
		
		Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		
		if(cacheFiles != null && cacheFiles.length > 0) {
			String line;
			String[] tokens;
			BufferedReader br = new BufferedReader(new FileReader(cacheFiles[0].toString()));
			
			while((line=br.readLine()) != null) {
				tokens = line.split("\t",2);
				hashTable.put(new Text(tokens[0]),Double.parseDouble(tokens[1]));
			}
			br.close();
		}
	}
	
	protected void map(ImmutableBytesWritable key,Result values,Context context) throws IOException,InterruptedException {
		
		List<KeyValue> clusters_kv = new ArrayList<KeyValue>();
		List<KeyValue> history_kv = new ArrayList<KeyValue>();
		double sum = 0;	//sum是q*中的分母
		Text z = null;	//z临时保存hashTable中的Key,对应于N(z)
		Text zs = null; //zs临时保存hashTable中的Key,形式是s==z,对应于N(z|s)
		double pzu = 0;	//p(z|u)的值
		double q = 0;	//q*的值
		//把values的内容分发到clusters_kv和history_kv中
		for(KeyValue kv : values.raw()) {
			if("clusters".equals(Bytes.toString(kv.getFamily()))){
				clusters_kv.add(kv);	//把UT表clusters里面的内容都放进clusters_kv
			} else {
				history_kv.add(kv);	//把UT表history里面的内容都放进history_kv
			}
		}
		//计算sum的值，即q*中的分母
		for(int i=0;i<history_kv.size();i++){
			for(int j=0;j<clusters_kv.size();j++){
				z = new Text(clusters_kv.get(j).getQualifier());
				zs = new Text(Bytes.toString(history_kv.get(i).getQualifier())+"=="+z.toString());
				pzu = Bytes.toDouble(clusters_kv.get(j).getValue());
				sum += (hashTable.get(zs)/hashTable.get(z))*pzu;
			}
		}
		//利用上面的sum计算q*
		for(int i=0;i<history_kv.size();i++){
			for(int j=0;j<clusters_kv.size();j++){
				z = new Text(clusters_kv.get(j).getQualifier());
				zs = new Text(Bytes.toString(history_kv.get(i).getQualifier())+"=="+z.toString());
				pzu = Bytes.toDouble(clusters_kv.get(j).getValue());
				q = ( (hashTable.get(zs)/hashTable.get(z))*pzu )/sum;
				
				context.write(zs,new DoubleWritable(q));
				context.write(z,new DoubleWritable(q));
				context.write(new Text("@"+"=="+Bytes.toString(key.get())+"=="+z.toString()),new DoubleWritable(q));
			}
		}
	}

	/*protected void cleanup(Context context) throws IOException,InterruptedException {
		
	}*/

}
