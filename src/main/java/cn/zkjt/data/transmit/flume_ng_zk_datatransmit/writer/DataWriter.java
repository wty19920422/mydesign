package cn.zkjt.data.transmit.flume_ng_zk_datatransmit.writer;

import org.apache.flume.Context;
import org.apache.flume.Event;

public interface DataWriter {	
	void write(Event event, String flag);
	void setContext(Context context);
	void start();
	void stop();
}
