/**
 * 
 */
package com.log.rotate;

import java.net.UnknownHostException;

import com.mongodb.MongoClient;

/**
 * @author asus1
 *
 */
public class RunLogRotateCommand {
	
	public MongoClient getDBConn() {
		MongoClient mongoClient = null;
		try {
			mongoClient = new MongoClient( "192.168.1.93" );
		} catch(UnknownHostException ukhe) {
			ukhe.printStackTrace();
		}
		return mongoClient;
	}
	
	/**
	 * 
	 */
	public RunLogRotateCommand() {
		MongoClient dbConn = null;
		try {
			dbConn = this.getDBConn();
			
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if(dbConn != null)
					dbConn.close();
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new RunLogRotateCommand();
	}
}
