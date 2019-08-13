package cn.zkjt.data.transmit.flume_ng_zk_datatransmit;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;

import cn.zkjt.data.transmit.flume_ng_zk_datatransmit.process.ConstantData;
import cn.zkjt.data.transmit.flume_ng_zk_datatransmit.process.ZKThread;
import cn.zkjt.data.transmit.flume_ng_zk_datatransmit.writer.DataWriter;
import cn.zkjt.data.transmit.flume_ng_zk_datatransmit.writer.DataWriterFactory;


public class ZKDataTransmitSink extends AbstractSink implements Configurable {

	private static final Logger logger = LoggerFactory.getLogger(ZKDataTransmitSink.class);

	private int batchSize;
	private SinkCounter sinkCounter;
	
	private String type;
	private DataWriter dataWriter;
	private Context context;
	
	private Thread zkThread;

	public Status process() throws EventDeliveryException {
		Status status = Status.READY;
		Channel channel = getChannel();
		Transaction transaction = channel.getTransaction();

		try {
			transaction.begin();
			long count = 0;
			for (count = 0; count < batchSize; ++count) {
				Event event = channel.take();
				if (event == null) {
					break;
				}
				String commonData = ConstantData.commonData;
				logger.info("common data : {}", commonData);
				dataWriter.write(event, commonData);		
			}
			if (count <= 0) {
				sinkCounter.incrementBatchEmptyCount();
				status = Status.BACKOFF;
			} else {
				if (count < batchSize) {
					sinkCounter.incrementBatchUnderflowCount();
					status = Status.BACKOFF;
				} else {
					sinkCounter.incrementBatchCompleteCount();
				}

				sinkCounter.addToEventDrainAttemptCount(count);
			}

			transaction.commit();
			sinkCounter.addToEventDrainSuccessCount(count);

		} catch (Throwable t) {
			try {
				transaction.rollback();
			} catch (Exception e) {
				logger.error("Exception during transaction rollback.", e);
			}

			logger.error("Failed to commit transaction. Transaction rolled back.", t);
			if (t instanceof Error || t instanceof RuntimeException) {
				Throwables.propagate(t);
			} else {
				throw new EventDeliveryException("Failed to commit transaction. Transaction rolled back.", t);
			}
		} finally {
			if (transaction != null) {
				transaction.close();
			}
		}

		return status;
	}

	@Override
	public synchronized void start() {	
		logger.info("Starting sink");
		
		dataWriter = new DataWriterFactory().createWriter(type);
		dataWriter.setContext(context);
		dataWriter.start();
		
		zkThread = new Thread(new ZKThread(context));
		zkThread.start();
		
		
		sinkCounter.start();
		try {
			sinkCounter.incrementConnectionCreatedCount();
		} catch (Exception e) {
			logger.error("Exception while connecting to Redis", e);
			sinkCounter.incrementConnectionFailedCount();
		}

		super.start();
		logger.info("Sink started");
	}

	@Override
	public synchronized void stop() {
		logger.info("Stopping sink");
		dataWriter.stop();
		sinkCounter.incrementConnectionClosedCount();
		sinkCounter.stop();
		super.stop();
		logger.info("Sink stopped");
	}

	public void configure(Context context) {
		this.context = context;
		type = context.getString(ZKDataTransmitSinkConstants.SENDTYPE, ZKDataTransmitSinkConstants.DEFAULT_SENDTYPE);		
		batchSize = context.getInteger(ZKDataTransmitSinkConstants.BATCH_SIZE, ZKDataTransmitSinkConstants.DEFAULT_BATCH_SIZE);
		if (sinkCounter == null) {
			sinkCounter = new SinkCounter(getName());
		}
	}

}
