package cn.zkjt.data.transmit.flume_ng_zk_datatransmit.process;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TypeMap {
	private Logger log = LoggerFactory.getLogger(getClass());
	private static final Map<String, String> map = new HashMap<String, String>();
	
	public TypeMap() {
		map.put("hdfs", "HDFSDataWriter");
		map.put("file", "FileDataWriter");
		map.put("kafka", "KafkaDataWriter");
		map.put("redis", "RedisDataWriter");
		map.put("mongo", "MongoDataWriter");
		map.put("logger", "LoggerDataWriter");
	}
	
	public String getClassName(String type) {
		String typeName = map.get(type);
		if(typeName == null) {
			log.warn("传入类型异常, {}类型不在netty, hdfs, logger中, 默认选择logger", type);
			return map.get("logger");
		}
		return map.get(type);
	}
	
}
