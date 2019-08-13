package cn.zkjt.data.transmit.flume_ng_zk_datatransmit.datawriter.mongo;

import org.apache.flume.Context;
import org.apache.flume.Event;

import cn.zkjt.data.transmit.flume_ng_zk_datatransmit.connect.DataBaseConnect;
import cn.zkjt.data.transmit.flume_ng_zk_datatransmit.connect.MongoDBConnect;
import cn.zkjt.data.transmit.flume_ng_zk_datatransmit.writer.DataWriter;

public class MongoDataWriter implements DataWriter {

	private DataBaseConnect dataConnect;
	
	public MongoDataWriter() {
		dataConnect = new MongoDBConnect();
	}
	
	public void write(Event event, String flag) {
		dataConnect.dataStore(event, flag);
	}

	public void setContext(Context context) {
		dataConnect.setContext(context);
	}

	public void start() {	
		dataConnect.start();
	}

	public void stop() {
		dataConnect.stop();
	}

}
