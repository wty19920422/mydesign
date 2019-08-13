package cn.zkjt.data.transmit.flume_ng_zk_datatransmit.process;

import java.io.IOException;
import org.apache.flume.Context;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.zkjt.data.transmit.flume_ng_zk_datatransmit.ZKDataTransmitSinkConstants;

import org.apache.zookeeper.ZooDefs.Ids;

public class ZKThread implements Runnable{

	private String connectString;
	private int sessionTimeout = 2000;
	private String storePath;
	private ZooKeeper zk;
	
	private Logger logger = LoggerFactory.getLogger(getClass());
	
	public ZKThread(Context context) {
		connectString = context.getString(ZKDataTransmitSinkConstants.ZOOKEEPER_LIST);
		storePath = context.getString(ZKDataTransmitSinkConstants.ZOOKEEPER_PATH, ZKDataTransmitSinkConstants.DEFAULT_ZOOKEEPER_PATH);
		logger.info("sssssssssssssshuju----------------");
		System.out.println("----------------------ffffffffffffffffffff------");
	}
	
	
	@Override
	public void run() {
		// TODO Auto-generated method stub
		
		try {
			zk = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
				public void process(WatchedEvent event) {
					try {
						logger.info("path:{}, list:{}", storePath, connectString);
						System.out.println("----------------------xxxxxxxxxxxxxx------");
						Stat exists = zk.exists(storePath, false);
						if(exists == null) {
							zk.create(storePath, "".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
						}
						byte[] data = zk.getData(storePath, true, null);
						ConstantData.commonData = new String(data);
					}catch (Exception e) {
						e.printStackTrace();
					}
				}	
			});
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	
			
	}

}
