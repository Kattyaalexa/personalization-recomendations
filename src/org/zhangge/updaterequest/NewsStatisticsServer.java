package org.zhangge.updaterequest;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class NewsStatisticsServer {

	public void connectToHbase() throws MasterNotRunningException, ZooKeeperConnectionException {
		Configuration config = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(config);
		
	}
	
	public void fetchFromUT() {
		
	}
	
	public void updateST() {
		
	}
}
