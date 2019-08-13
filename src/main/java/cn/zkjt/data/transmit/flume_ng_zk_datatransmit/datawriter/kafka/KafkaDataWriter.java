package cn.zkjt.data.transmit.flume_ng_zk_datatransmit.datawriter.kafka;

import java.util.Properties;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import cn.zkjt.data.transmit.flume_ng_zk_datatransmit.configure.kafka.KafkaWriteConstants;
import cn.zkjt.data.transmit.flume_ng_zk_datatransmit.writer.DataWriter;

public class KafkaDataWriter implements DataWriter {

	private String brokerList;
	private String topic;
	private int partitionNum;

	private KafkaProducer<String, byte[]> producer;

	private ProducerRecord<String, byte[]> record;
	
	public void write(Event event, String flag) {
		
		int code = Math.abs(new String(event.getBody()).hashCode());
		int partitionID = code % partitionNum;
				
		record = new ProducerRecord<String, byte[]>(topic, partitionID, "data",
				event.getBody());
		producer.send(record);
	}

	public void setContext(Context context) {
		brokerList = context.getString(KafkaWriteConstants.KAFKA_BROKERLIST);
		topic = context.getString(KafkaWriteConstants.KAFKA_TOPIC);
		partitionNum = context.getInteger(KafkaWriteConstants.KAFKA_PARTITION_NUM, KafkaWriteConstants.DEFAULT_KAFKA_PARTITION_NUM);
	}

	public void start() {	
		Properties properties=new Properties();
       // properties.put("bootstrap.servers",brokerList);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
         properties.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
//        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getClass());
        properties.put("value.serializer","org.apache.kafka.common.serialization.ByteArraySerializer");
//        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getClass());
        //properties.put("client.id","producer.client.id.demo");//指定客户端ID
        properties.put(ProducerConfig.CLIENT_ID_CONFIG,"producer.client.id.demo");//指定客户端ID
        
        producer = new KafkaProducer<String, byte[]>(properties);
        
	}

	public void stop() {
		// TODO Auto-generated method stub
		producer.close();
	}

}
