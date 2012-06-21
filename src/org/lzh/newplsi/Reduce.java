package org.lzh.newplsi;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;

public class Reduce extends TableReducer<Text,FloatWritable,ImmutableBytesWritable>{
	
	private List<String> ls = new ArrayList<String>();//存放intermediaNz,intermediaNsz的数据
		
	/*protected void setup(Context context) throws IOException,InterruptedException {
		conf = context.getConfiguration();
		Path[] cacheFiles = DistributedCache.getLocalCacheFiles(conf);
		BufferedReader br = null;
		for(int i=0;i<cacheFiles.length;i++) {
			String line;
			String[] tokens;
			if(cacheFiles[i].toString().equals("usersum")){
				br = new BufferedReader(new FileReader(cacheFiles[i].toString()));
				while((line=br.readLine()) != null) {
					tokens = line.split("\t",2);
					hashTable.put(new Text(tokens[0]),Float.parseFloat(tokens[1]));
				}
			}
		}
		br.close();		
		
	}*/
	protected void reduce(Text key,Iterable<FloatWritable> values,Context context)	throws IOException,InterruptedException {
		
		float sum = 0;
		
		for(FloatWritable dw : values){
			sum += dw.get();
		}
		if(key.toString().startsWith("@")){
			String[] s = key.toString().split("==");
			Put put = new Put(Bytes.toBytes(s[1]));
			put.add(Bytes.toBytes("clusters_plsi"),Bytes.toBytes(s[2]),Bytes.toBytes(sum+""));
			
			context.write(new ImmutableBytesWritable(Bytes.toBytes(s[1])), put);
		}else{
			ls.add(key.toString()+"\t"+sum);
		}
		//System.out.println("hashtable==="+ls.size());
	}

	protected void cleanup(Context context) throws IOException,InterruptedException {
		
		FileSystem fs = FileSystem.get(context.getConfiguration());
		
		OutputStream outNz = fs.create(new Path("intermediaNz"));
		OutputStream outNsz = fs.create(new Path("intermediaNsz"));
		BufferedWriter bwNz = new BufferedWriter(new OutputStreamWriter(outNz));
		BufferedWriter bwNsz = new BufferedWriter(new OutputStreamWriter(outNsz));
		
		for(int i=0;i<ls.size();i++){
			if(ls.get(i).startsWith("s")){
				bwNsz.write(ls.get(i));
				bwNsz.newLine();
			} else{
				bwNz.write(ls.get(i));
				bwNz.newLine();
			}
			
		}
		bwNsz.close();
		bwNz.close();
	}
}
