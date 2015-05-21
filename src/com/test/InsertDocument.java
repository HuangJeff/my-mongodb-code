/**
 * 
 */
package com.test;

import java.net.UnknownHostException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.util.JSON;

/**
 * In this tutorial, we show you 4 ways to insert below JSON data into a “document“, via Java MongoDB API.
 * @author Jeff
 * @see http://www.mkyong.com/mongodb/java-mongodb-insert-a-document/
 */
public class InsertDocument {
	//Data Format
	/*
	{
		"database" : "mkyongDB",
		"table" : "hosting",
		"detail" : 
			{
				records : 99,
				index : "vps_index1",
				active : "true"
			}
		}
	}
	*/
	private String mongohome = null; //mongo.home
	private String host = null; //mongo.dumphost
	private int port = 27017; //mongo.dumpport
	private String db_name = null; //mongo.dumpdb
	
	private DB db = null;
	private DBCollection collection = null;
	private String collectionName = null;
	
	/**
	 * 初始化MongoDB設定資訊
	 */
	public InsertDocument() throws UnknownHostException {
		this.initConfig();
		
		MongoClient mongoClient;
		try {
			mongoClient = new MongoClient(host, port);
		} catch (UnknownHostException e) {
			e.printStackTrace();
			throw e;
		}
		db = mongoClient.getDB( db_name );
		
		collection = db.getCollection(collectionName);
	}
	
	private void initConfig() {
		mongohome = "C:/trs-standalone/mongodb-win32-x86_64-2.2.2"; //mongo.home
	    host = "192.168.1.25"; //mongo.dumphost
	    //host = "192.168.1.107"; //window's mongo.host
		//host = "localhost"; //window's mongo.host
		//host = "192.168.1.123";
	    //port = 27017; //mongo.dumpport
	    db_name = "MyTest"; //mongo.dumpdb
	    collectionName = "TestData";
	}
	
	/**
	 * 指定Collection Name
	 * @param collection_Name : Collection Name
	 */
	public void setCollectionsName(String collection_Name) {
		collection = db.getCollection(collection_Name);
	}
	
	/**
	 * 1. BasicDBObject example
	 */
	public void insertByBasicDBObj() {
		BasicDBObject document = new BasicDBObject();
		document.put("database", "mkyongDB");
		document.put("table", "hosting");
		
		BasicDBObject documentDetail = new BasicDBObject();
		documentDetail.put("records", 99);
		documentDetail.put("amounts", new Double(9999.0));
		documentDetail.put("index", "vps_index1");
		documentDetail.put("active", "true");
		
		document.put("detail", documentDetail);
		
		document.put("status", false);
		document.put("created", new Date());
		
		collection.insert(document);
		
	}
	
	/**
	 * 2. BasicDBObjectBuilder example
	 */
	public void insertByBasicDBObjectBuilder() {
		BasicDBObjectBuilder documentBuilder = BasicDBObjectBuilder.start()
				.add("database", "mkbuilderDB")
				.add("table", "hosting");
		
		BasicDBObjectBuilder documentBuilderDetail = BasicDBObjectBuilder.start()
				.add("records", 99)
				.add("amounts", new Double(789999.0))
				.add("index", "vps_index1")
				.add("active", "true");
		
		documentBuilder.add("detail", documentBuilderDetail.get())
				.add("status", true)
				.add("created", new Date());
		
		collection.insert(documentBuilder.get());
	}
	
	/**
	 * 3. Map example<br>
	 * 發現塞進去的順序，不會照寫的順序排列(與上面兩個不同之處，也有可能是用hashMap)
	 */
	public void insertByMap() {
		Map<String, Object> documentMap = new HashMap<String, Object>();
		documentMap.put("database", "mkMapDB");
		documentMap.put("table", "hosting");
		
		Map<String, Object> documentMapDetail = new HashMap<String, Object>();
		documentMapDetail.put("records", 99);
		documentMapDetail.put("amounts", new Double(12222222.0));
		documentMapDetail.put("index", "vps_index1");
		documentMapDetail.put("active", "true");
		
		documentMap.put("detail", documentMapDetail);
		
		documentMap.put("status", false);
		documentMap.put("created", new Date());
		
		collection.insert(new BasicDBObject(documentMap));
	}
	
	/**
	 * 4. JSON parse example
	 */
	public void insertByJson() {
		String json = "{'database' : 'mkyongDB','table' : 'hosting'," +
				  "'detail' : {'records' : 99, 'index' : 'vps_index1', 'active' : 'true'}}}";
		
		DBObject dbObject = (DBObject)JSON.parse(json);
		
		collection.insert(dbObject);
	}
	
	//Output
	public void findData() {
		DBCursor cursorDocJSON = collection.find();
		while (cursorDocJSON.hasNext()) {
			System.out.println(cursorDocJSON.next());
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		InsertDocument inDoc = null;
		try {
			inDoc = new InsertDocument();
			
			System.out.println("BasicDBObject example...");
			//inDoc.setCollectionsName(""); //指定 Collections Name
			//1. BasicDBObject example
			inDoc.insertByBasicDBObj();
			
			System.out.println("BasicDBObjectBuilder example...");
			//2. BasicDBObjectBuilder example
			inDoc.insertByBasicDBObjectBuilder();
			
			System.out.println("Map example...");
			//3. Map example
			inDoc.insertByMap();
			
			System.out.println("JSON parse example...");
			//4. JSON parse example
			inDoc.insertByJson();
			
			//find Data
			inDoc.findData();
			
			System.out.println("finish...");
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
