package cn.zkjt.data.transmit.flume_ng_zk_datatransmit.connect;

import org.apache.flume.Context;
import org.apache.flume.Event;

public interface DataBaseConnect {
	void start();
	void setContext(Context context);
	void stop();
	void dataStore(Event event, String flag);
}
