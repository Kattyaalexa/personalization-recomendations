package org.lzh.plsi;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MapClass2 extends MapReduceBase implements Mapper<Text,Text,Text,DoubleWritable>{
	
	private static Configuration conf = null;
	private static HTable ut;
	private static HTable st;
	static {
		conf = HBaseConfiguration.create();
		conf.addResource("hbase-site.xml");
	}
	public void configure(JobConf job){
		
		super.configure(job);
		try {
			ut = new HTable(conf,"UT");
			st = new HTable(conf,"ST");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public void map(Text u,Text s,OutputCollector<Text,DoubleWritable> output,Reporter reporter) throws IOException{
		
		Get getpzu = new Get(Bytes.toBytes(u.toString()));
		Get getpsz = new Get(Bytes.toBytes(s.toString()));
		getpzu.addFamily(Bytes.toBytes("clusters"));
		getpsz.addFamily(Bytes.toBytes("clusters"));
		Result rspzu = ut.get(getpzu);
		Result rspsz = st.get(getpsz);

		KeyValue[] pzu = rspzu.raw();
		KeyValue[] psz = rspsz.raw();
		double sum = 0;
		for(int i=0;i<pzu.length;i++){
			
			sum += Bytes.toDouble(pzu[i].getValue())*Bytes.toDouble(psz[i].getValue());
			
		}
		for(int i=0;i<pzu.length;i++){
			
			double tempQ = Bytes.toDouble(pzu[i].getValue())*Bytes.toDouble(psz[i].getValue());
			output.collect(u,new DoubleWritable(tempQ/sum));
			output.collect(new Text(u.toString()+"=="+Bytes.toString(pzu[i].getQualifier())),new DoubleWritable(tempQ/sum));
			output.collect(new Text(s.toString()+"=="+Bytes.toString(pzu[i].getQualifier())),new DoubleWritable(tempQ/sum));
			output.collect(new Text(Bytes.toString(pzu[i].getQualifier())),new DoubleWritable(tempQ/sum));
		}
	}

}
