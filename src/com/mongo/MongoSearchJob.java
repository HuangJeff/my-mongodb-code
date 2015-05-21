/**
 * 
 */
package com.mongo;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

/**
 * 查詢<br>
 * 1.指定Collection針對特定欄位，取得特定字串的值
 * @author asus1
 */
public class MongoSearchJob {
	
	private String mongohome = null; //mongo.home
	private String host = null; //mongo.dumphost
	private String port = null; //mongo.dumpport
	private String db_name = null; //mongo.dumpdb
	
	/**
	 * 
	 */
	public MongoSearchJob(String activityName) throws Exception {
		this.initConfig();
		
	}
	
	private void initConfig() {
		mongohome = "C:/trs-standalone/mongodb-win32-x86_64-2.2.2"; //mongo.home
	    //host = "192.168.1.69"; //mongo.dumphost
	    //host = "192.168.1.107"; //window's mongo.host
		//host = "localhost"; //window's mongo.host
		host = "192.168.1.123";
	    port = null; //mongo.dumpport
	    db_name = "tptrs"; //mongo.dumpdb
	    
	}
	
	/**
	 * 指定Collection針對特定欄位，取得特定字串的值。<br>
	 * 依傳入的Collections Name，Field來查詢指定的字串(依正則運算)
	 * @param collectionName : Collections Name
	 * @param field : 被查詢的欄位
	 * @param regex : 被查詢的值（正則運算）
	 * @throws Exception
	 */
	private void getSpecifyValue(String collectionName, String field, String regex) throws Exception {
		MongoClient mongoClient = new MongoClient(host);
		DB db = mongoClient.getDB( db_name );
		DBCollection dbColl = db.getCollection(collectionName);
		
		System.out.println("dbColl = " + dbColl);
	}
	
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String activityName = "";
		try {
			MongoSearchJob job = new MongoSearchJob(activityName);
			//取得指定欄位值
			String collName = "";
			String field = "";
			String regex = "";
			job.getSpecifyValue(collName, field, regex);
			
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
