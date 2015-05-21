/**
 * 
 */
package com.mongo.type;

import java.net.UnknownHostException;
import java.util.Iterator;
import java.util.Set;

import org.bson.BSONObject;
import org.bson.types.BasicBSONList;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

/**
 * 取得MongoDB中每一個欄位的資料型態(Int32、Double、Int64、String、Data)
 * @author asus1
 */
public class MongoDBDataType {
	private String mongohome = null; //mongo.home
	private String host = null; //mongo.dumphost
	private int port = 0; //mongo.dumpport
	private String db_name = null; //mongo.dumpdb
	
	/**
	 * 
	 */
	public MongoDBDataType() {
		this.initConfig();
	}
	
	private void initConfig() {
		mongohome = "C:/trs-standalone/mongodb-win32-x86_64-2.2.2"; //mongo.home
	    //host = "192.168.1.69"; //mongo.dumphost
	    //host = "192.168.1.107"; //window's mongo.host
		host = "localhost"; //window's mongo.host
		//host = "192.168.1.123";
	    port = 27200; //mongo.dumpport
	    db_name = "test"; //mongo.dumpdb
	}
	
	/**
	 * 撈資料同時把資料欄位的資訊印出來
	 */
	public void findData() {
		MongoClient mongoClient = null;
		try {
			mongoClient = new MongoClient(host, port);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		DB db = mongoClient.getDB( db_name );
		
		String collectionName = "statsForasus1-PC";
		DBCollection collection = db.getCollection(collectionName);
		
		Set<String> colls = db.getCollectionNames();
		System.out.println("collectionName=" + collectionName + " allSize =" + colls.size());
		
		DBCursor cursorDocJSON = collection.find();
		/*while (cursorDocJSON.hasNext()) {
			System.out.println(cursorDocJSON.next());
		}*/
		if (cursorDocJSON.hasNext()) {
			//System.out.println(cursorDocJSON.next());
			//DBObject obj = cursorDocJSON.next();
			
			//這種方式可以取得到"欄位值"，以及透過欄位值取得value
			//以及取得value的Type
			BSONObject bsonObj = cursorDocJSON.next();
			for(Iterator<String> it = bsonObj.keySet().iterator(); it.hasNext() ;) {
				String str = it.next();
				Object val = bsonObj.get(str);
				System.out.println("str = " + str + " val = " + val +
						" type = " + val.getClass().getName());
			}
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("Find Star...");
		MongoDBDataType mddt = new MongoDBDataType();
		mddt.findData();
		System.out.println("Find End...");
	}
}
