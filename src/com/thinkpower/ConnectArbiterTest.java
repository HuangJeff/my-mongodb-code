/**
 * 
 */
package com.thinkpower;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import com.mongodb.*;

/**
 * 測試連結Arbiter<br>
 * 1.因為發生『com.mongodb.MongoServerSelectionException: 
 * 		Unable to connect to any server that satisfies the selector CompositeServerSelector』
 * 	，所以測試一下。<br>
 * @author Jeff
 */
public class ConnectArbiterTest {
	
	/**
	 * 
	 */
	public ConnectArbiterTest() {
		
	}
	
	/**
	 * 1.測試：com.mongodb.MongoServerSelectionException <br>
	 * @param args
	 */
	public static void main(String[] args) {
		MongoClient mongoClient = null;
		try {
			String hosts = "192.168.1.127"; //"WinDB:27053";
			int port = 27053;
			//MongoCredential mongoAuthInfo =
			//		MongoCredential.createMongoCRCredential(
			//				"manager", "admin", "1qaz2wsx".toCharArray());
			
			mongoClient = new MongoClient(new ServerAddress( hosts, port )
							//, Arrays.asList(mongoAuthInfo)
							);
			
			//by host
			//mongoClient = new MongoClient(new ServerAddress(hosts, port));
			
			System.out.println("Credentials List => " + mongoClient.getCredentialsList());
			
			System.out.println("mongoClient => " + mongoClient);
			
			//System.out.println("testConnect---MongoClient = " + mongoClient.getAddress());
			System.out.println("testConnect---MongoClient = " + mongoClient.getAllAddress());
			System.out.println("testConnect---MongoClient = " + mongoClient.getServerAddressList());
			
			System.out.println("isSlaveOk = " + mongoClient.getReadPreference().isSlaveOk());
			//if(!mongoClient.getReadPreference().isSlaveOk())
			//	mongoClient.setReadPreference(ReadPreference.secondaryPreferred());
			System.out.println("isSlaveOk = " + mongoClient.getReadPreference().isSlaveOk());
			
			try {
				DB mydb = mongoClient.getDB("test");
				CommandResult cmm = mydb.command( "serverStatus" );
				System.out.println("Command [serverStatus] = " + cmm );
				
				CommandResult cmm2 = mydb.command( "ping" );
				System.out.println("Command [ping] = " + cmm2 );
				
				List<String> dbs = mongoClient.getDatabaseNames();
				for(String db : dbs) {
					System.out.println("DB = " + db);
				}
			//} catch(com.mongodb.MongoServerSelectionException e) {
			} catch(Exception e) {
					String errmsg = "Host [" + hosts + ":" + port + "]"
							+ "(IF Arbiter?? the Message is OK) is MongoServerSelectionException:" + e.getMessage();
					System.err.println(errmsg);
				}
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if(mongoClient != null)
				mongoClient.close();
		}
	}
}
