package cn.zkjt.data.transmit.flume_ng_zk_datatransmit;

public class ZKDataTransmitSinkConstants {

	public static final String SENDTYPE = "sendtype";
	
	public static final String DEFAULT_SENDTYPE = "logger";
	/**
	 * Comma-separated list of hostname:port. If the port is not present the default
	 * port 27017 will be used.
	 */
	public static final String ZOOKEEPER_LIST = "zookeeperlist";
	public static final String ZOOKEEPER_PATH = "zookeeperpath";
	
	public static final String DEFAULT_ZOOKEEPER_PATH = "/zkdata";

	/**
	 * Database name.
	 */
	

	
	/**
	 * Maximum number of events the sink should take from the channel per
	 * transaction, if available. Defaults to 100.
	 */
	public static final String BATCH_SIZE = "batchSize";

	/**
	 * The default batch size.
	 */
	public static final int DEFAULT_BATCH_SIZE = 100;

}
