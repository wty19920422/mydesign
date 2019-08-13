package cn.zkjt.data.transmit.flume_ng_zk_datatransmit.datawriter.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Calendar;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.zkjt.data.transmit.flume_ng_zk_datatransmit.configure.file.FileWriteConstants;
import cn.zkjt.data.transmit.flume_ng_zk_datatransmit.writer.DataWriter;

public class FileDataWriter implements DataWriter{

	private Logger logger = LoggerFactory.getLogger(getClass());
	
	private String writePath;

	public void write(Event event, String flag) {
		
		Calendar calendar = Calendar.getInstance();
		
		if(!(writePath.endsWith("/") || writePath.endsWith("\\"))) {
			writePath = writePath + "/";
		}
		
		int year = calendar.get(Calendar.YEAR);
		int month = calendar.get(Calendar.MONTH) + 1;
		int day = calendar.get(Calendar.DAY_OF_MONTH);
		int hour = calendar.get(Calendar.HOUR_OF_DAY);
		
		String storePath = writePath + year + "/" + month + "/" + day + "/";
		String fileName = "" + year + "_" + month + "_" + day + "_" + hour;
		
		File storeFile = new File(storePath);
		File writeFile = new File(fileName);
		
		if(!storeFile.exists()) {
			storeFile.mkdirs();
		}
			
		try {
			RandomAccessFile randomFile = new RandomAccessFile(storePath + writeFile, "rw");
			logger.info("receive data:{} !!", new String(event.getBody()));
			randomFile.seek(randomFile.length());
			randomFile.write(event.getBody());
			randomFile.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public void setContext(Context context) {
		// TODO Auto-generated method stub
		logger.info("set context:{} !!", context.toString());
		writePath = context.getString(FileWriteConstants.FILE_WRITE_PATH, FileWriteConstants.DEFAULT_FILE_WRITE_PATH);		
	}

	public void start() {
		// TODO Auto-generated method stub
		logger.info("file writer start !!");
	}

	public void stop() {
		// TODO Auto-generated method stub
		logger.info("file writer stop !!");
	}

}
