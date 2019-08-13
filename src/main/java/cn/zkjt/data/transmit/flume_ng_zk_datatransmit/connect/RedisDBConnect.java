package cn.zkjt.data.transmit.flume_ng_zk_datatransmit.connect;

import java.util.Calendar;

import org.apache.flume.Context;
import org.apache.flume.Event;

import cn.zkjt.data.transmit.flume_ng_zk_datatransmit.configure.redis.RedisWriteConstants;
import redis.clients.jedis.Jedis;

public class RedisDBConnect implements DataBaseConnect{

	private String host;
	private int port;
	private String password;
	private int dbNum;
	
	private Jedis jedis;
	
	@Override
	public void start() {
		// TODO Auto-generated method stub
		jedis = new Jedis(host, port);
		jedis.auth(password);
		jedis.select(dbNum);
	}

	@Override
	public void setContext(Context context) {
		// TODO Auto-generated method stub
		host = context.getString(RedisWriteConstants.REDIS_HOST);
		port = context.getInteger(RedisWriteConstants.REDIS_PORT, RedisWriteConstants.DEFAULT_REDIS_PORT);
		password = context.getString(RedisWriteConstants.REDIS_PASSWORD);
		dbNum = context.getInteger(RedisWriteConstants.REDIS_DATABASE, RedisWriteConstants.DEFAULT_REDIS_DATABASE);
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		jedis.close();
	}

	@Override
	public void dataStore(Event event, String flag) {
		// TODO Auto-generated method stub
		Calendar calendar = Calendar.getInstance();
			
		int year = calendar.get(Calendar.YEAR);
		int month = calendar.get(Calendar.MONTH) + 1;
		int day = calendar.get(Calendar.DAY_OF_MONTH);
		int hour = calendar.get(Calendar.HOUR_OF_DAY);

		String keyName = "" + year + "_" + month + "_" + day + "_" + hour;
		jedis.rpush(keyName, new String(event.getBody()));
	}

}
