/**
 * 
 */
package com.thinkpower;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.bson.BSONObject;

import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

/**
 * Replica Set
 * @author jeff
 */
public class ReplicaSet {
	private MongoClient mongoClient = null;
	private DB db;
	/**
	 * Constructor
	 */
	public ReplicaSet(String ip, int port, String dbName) {
		this.getMongoClient(ip, port);
		
		this.db = mongoClient.getDB(dbName);
	}
	
	/**
	 * Get ReplSet information from admin DB
	 * @return
	 */
	public void getReplSetInfo() {
		CommandResult result = db.command( "replSetGetStatus" );
		
		System.out.println("ok?? " + result.ok());
		
		String setName = (String)result.get( "set" );
		//DBObject repl = (DBObject)result.get( RS_REPL );
		
		List members = (List)result.get( "members" );
		
		System.out.println("setName = " + setName);
		System.out.println("members.size() = " + members.size());
		System.out.println("members = " + result.get( "members" ));
		
		for(int i = 0; i < members.size(); i++) {
			BSONObject item = (BSONObject)members.get(i);
			//System.out.println("BSONObject = " + item);
			for(String key_str : item.keySet()) {
				if("name".equals(key_str)) {
					System.out.println(key_str + " = " + item.get(key_str));
				} else if("health".equals(key_str)) {
					System.out.println(key_str + " = " + item.get(key_str));
				} else if("state".equals(key_str)) {
					System.out.println(key_str + " = " + item.get(key_str));
				} else if("stateStr".equals(key_str)) {
					System.out.println(key_str + " = " + item.get(key_str));
				} else if("optimeDate".equals(key_str)) {
					SimpleDateFormat sdf = new SimpleDateFormat();
					String strDate = null;
					strDate = sdf.format((Date)item.get(key_str));
					System.out.println(key_str + " = " + strDate);
				}
			}
			System.out.println("==================");
		}
		
	}
	
	private void getMongoClient(String ip, int port) {
		try {
			mongoClient =  new MongoClient( new ServerAddress( ip , port ));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			String dbIp = "192.168.1.112";
			int port = 27011;
			String dbName = "admin";
			ReplicaSet rs = new ReplicaSet(dbIp, port, dbName);
			
			rs.getReplSetInfo();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
