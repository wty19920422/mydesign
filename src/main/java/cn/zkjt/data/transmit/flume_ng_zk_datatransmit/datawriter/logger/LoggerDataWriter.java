package cn.zkjt.data.transmit.flume_ng_zk_datatransmit.datawriter.logger;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.zkjt.data.transmit.flume_ng_zk_datatransmit.writer.DataWriter;

public class LoggerDataWriter implements DataWriter{

	private Logger logger = LoggerFactory.getLogger(getClass());

	@Override
	public void write(Event event, String flag) {
		// TODO Auto-generated method stub
		logger.info("receive data:{} !!", new String(event.getBody()));
	}

	@Override
	public void setContext(Context context) {
		logger.info("set context:{} !!", context.toString());
		// TODO Auto-generated method stub
		
	}

	@Override
	public void start() {
		// TODO Auto-generated method stub
		logger.info("logger writer start !!");
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		logger.info("logger writer stop !!");
	}
	
}
