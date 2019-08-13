package cn.zkjt.data.transmit.flume_ng_zk_datatransmit.datawriter.hdfs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Calendar;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import cn.zkjt.data.transmit.flume_ng_zk_datatransmit.ZKDataTransmitSinkConstants;
import cn.zkjt.data.transmit.flume_ng_zk_datatransmit.configure.hdfs.HDFSWriteConstants;
import cn.zkjt.data.transmit.flume_ng_zk_datatransmit.writer.DataWriter;

public class HDFSDataWriter implements DataWriter{

	private FileSystem fs;
	private Configuration conf;
	private String uri;
	private String hdfsUser;
	private String writePath;

	public void write(Event event, String flag) {
		
		Calendar calendar = Calendar.getInstance();
		
		// TODO Auto-generated method stub
		if(!(writePath.endsWith("/") || writePath.endsWith("\\"))){
			writePath = writePath + "/";
		}
		
		int year = calendar.get(Calendar.YEAR);
		int month = calendar.get(Calendar.MONTH) + 1;
		int day = calendar.get(Calendar.DAY_OF_MONTH);
		int hour = calendar.get(Calendar.HOUR_OF_DAY);
		String storePath = writePath + year + "/" + month + "/" + day + "/";
		String fileName = "" + year + "_" + month + "_" + day + "_" + hour;
		Path fsStorePath = new Path(storePath);
		Path fsFilePath = new Path(storePath + "/" +fileName);
		try {
			if(!fs.exists(fsStorePath)) {
				fs.mkdirs(fsStorePath);
			}
			if(!fs.exists(fsFilePath)) {
				FSDataOutputStream createFile = fs.create(fsFilePath);
				createFile.write(event.getBody());
				createFile.flush();
				createFile.close();
			}
			FSDataOutputStream append = fs.append(fsFilePath);
			append.write(event.getBody());
			append.flush();
			append.close();

		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}

	public void setContext(Context context) {
		// TODO Auto-generated method stub
		uri = context.getString(HDFSWriteConstants.HDFS_URI);
		hdfsUser = context.getString(HDFSWriteConstants.HDFS_USER, HDFSWriteConstants.DEFAULT_HDFS_USER);
		writePath = context.getString(HDFSWriteConstants.HDFS_WRITE_PATH, HDFSWriteConstants.DEFAULT_HDFS_WRITE_PATH);		
	}

	public void start() {
		// TODO Auto-generated method stub
		conf = new Configuration();
		conf.setBoolean("dfs.support.append", true);
		conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
		conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
		try {
			fs = FileSystem.get(new URI(uri), conf, hdfsUser);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void stop() {
		// TODO Auto-generated method stub
		
	}

}
