/**
 * 
 */
package com.thinkpower;

import org.bson.BSONObject;

import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

/**
 * 測試Shard Cluster
 * @author jeff
 */
public class ShardCluster {
	private MongoClient mongoClient = null;
	private DB db = null;
	/**
	 * 
	 */
	public ShardCluster(String ip, int port, String dbName) {
		this.getMongoClient(ip, port);
		
		this.db = mongoClient.getDB(dbName);
	}
	
	/**
	 * execute command : getShardMap
	 * @return
	 */
	private void getShardMapComm() {
		CommandResult result = db.command( "getShardMap" );
		if(result.ok()) {
			BSONObject obj = (BSONObject)result.get("map");
			System.out.println("" + obj);
			String config = obj.get("config").toString();
			System.out.println("config = " + obj.get("config"));
			String[] aryOfConfig = config.split(",");
			for(String item : aryOfConfig) {
				System.out.println("config item = " + item);
			}
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
			int port = 27019;
			String dbName = "admin";
			
			ShardCluster sc = new ShardCluster(dbIp, port, dbName);
			sc.getShardMapComm();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
