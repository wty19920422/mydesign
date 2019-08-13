package cn.zkjt.data.transmit.flume_ng_zk_datatransmit.connect;

import java.util.ArrayList;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import cn.zkjt.data.transmit.flume_ng_zk_datatransmit.configure.mongo.MongoWriteConstants;

public class MongoDBConnect implements DataBaseConnect{

	private String host;
	private int port;
	private String database;
	private String collection;
	private String user;
	private String password;
	
	private MongoClient mongoClient;
	private MongoDatabase mongoDB;
	private MongoCollection<Document> mongoCollection;
	
	@Override
	public void start() {
		// TODO Auto-generated method stub
		
//		MongoCredential credential=MongoCredential.createCredential(user, database, password.toCharArray());
//		
//		MongoClientOptions options = MongoClientOptions.builder()
//				.connectionsPerHost(100)
//				.maxWaitTime(10000)
//				.socketTimeout(0)
//				.connectTimeout(0)
//				.readPreference(ReadPreference.primary()).build();
//
//		
//		ServerAddress serverurl=new ServerAddress(host, port);
//		List<ServerAddress> listServer = new ArrayList<ServerAddress>();
//		listServer.add(serverurl);
//		
//		mongoClient = new MongoClient(listServer, credential, options);
//		
//		mongoDB = mongoClient.getDatabase(database);
//		
//		mongoCollection = mongoDB.getCollection(collection);
		
		ServerAddress serverurl=new ServerAddress(host, port);
		List<ServerAddress> listServer=new ArrayList<ServerAddress>();
		listServer.add(serverurl);
		MongoCredential credential=MongoCredential.createCredential(user, database, password.toCharArray());
		List<MongoCredential> listCre=new ArrayList<MongoCredential>();
		listCre.add(credential);
		mongoClient = new MongoClient(listServer, listCre);
		mongoDB = mongoClient.getDatabase(database);
		mongoCollection = mongoDB.getCollection(collection);

	}

	@Override
	public void setContext(Context context) {
		// TODO Auto-generated method stub
		host = context.getString(MongoWriteConstants.MONGO_HOST);
		port = context.getInteger(MongoWriteConstants.MONGO_PORT, MongoWriteConstants.DEFAULT_MONGO_PORT);
		user = context.getString(MongoWriteConstants.MONGO_USER);
		password = context.getString(MongoWriteConstants.MONGO_PASSWORD);
		database = context.getString(MongoWriteConstants.MONGO_DATABASE);
		collection = context.getString(MongoWriteConstants.MONGO_COLLECTION);
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		mongoClient.close();
	}

	@Override
	public void dataStore(Event event, String flag) {
		// TODO Auto-generated method stub
		Document doc = new Document();
		doc.append("test", new String(event.getBody()));
		mongoCollection.insertOne(doc);
	}

}
