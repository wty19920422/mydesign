package cn.zkjt.data.transmit.flume_ng_zk_datatransmit.writer;

public interface DataWriterFactoryInterface {
	public DataWriter createWriter(String typeName);
}
