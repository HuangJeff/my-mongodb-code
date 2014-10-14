/**
 * 
 */
package com.export;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.mongodb.DB;
import com.mongodb.MongoClient;

/**
 * @author asus1
 *
 */
public class ExportData1 {
	private List<String> exportCollectionList;
	
	private String outputFolder = null;
	
	private String mongohome = null; //mongo.home
	private String host = null; //mongo.dumphost
	private String port = null; //mongo.dumpport
	private String db_name = null; //mongo.dumpdb
	
	private void initConfig() {
		mongohome = "D:/trs-standalone/mongodb-win32-x86_64-2.2.2"; //mongo.home
	    //host = "192.168.1.69"; //mongo.dumphost
	    //host = "192.168.1.107"; //window's mongo.host
		host = "localhost"; //window's mongo.host
	    port = null; //mongo.dumpport
	    db_name = "tptrs"; //mongo.dumpdb
	    
	    //outputFolder = "D:/mongoExportData/"; //export folder
	    outputFolder = "D:/mongoExportData_cthouse/"; //export folder
	}
	
	/**
	 * 
	 */
	public ExportData1(String activityName) throws Exception {
		this.initConfig();
		
		this.getSpecifyCollections(activityName);
		
		if(exportCollectionList != null) {
			for(String colName : exportCollectionList)
				System.out.println(colName);
		}
		
		if(exportCollectionList != null) {
			for(String colName : exportCollectionList)
				try {
					this.doCommandWork(colName);
				} catch(Exception e) {
					System.err.println("★Collection Name : " + colName);
					System.err.println("★Exception : " + e.getMessage());
				}
		}
	}
	
	/**
	 * 取得相關活動(activityName)的Collection Name
	 * @param activityName
	 * @throws Exception
	 */
	private void getSpecifyCollections(String activityName) throws Exception {
		MongoClient mongoClient = new MongoClient(host);
		
		DB db = mongoClient.getDB( db_name );
		
		Set<String> colls = db.getCollectionNames();
		
		for (String s : colls) {
			if(s.endsWith(activityName)) {
		    	//System.out.println(s);
		    	if(exportCollectionList == null)
		    		exportCollectionList = new ArrayList<String>();
		    	exportCollectionList.add(s);
			}
		}
	}
	
	/**
	 * 
	 * @throws Exception
	 */
	private void doCommandWork(String collectionName) throws Exception {
		String collection = collectionName;
	    String outputlocation = outputFolder + collectionName + ".txt"; //needs to be asigned a random number name
	    
	    String command = String.format(mongohome + "/bin/mongoexport " +
	            "--host %s " +
	            //"--port %s " +
	            "--db %s " + 
	            "--collection %s " + 
	            //"--query %s " +
	            //"--fields _id,name,note,sDate,eDate " + 
	            "--out %s " + 
	            "--slaveOk true " + 
	            //"--csv " +
	            "-vvvvv",
	            //host,port,db,collection,query,outputlocation);
	            host,db_name,collection,outputlocation);
	    
	    //logger.info(command);
	    System.out.println(command);
	    try{            
	            Runtime rt = Runtime.getRuntime();              
	            Process pr = rt.exec(command);
	            //StreamGobbler errorGobbler = new StreamGobbler(pr.getErrorStream(),"ERROR",logger);
	            //StreamGobbler outputGobbler = new StreamGobbler(pr.getInputStream(),"OUTPUT",logger);
	            //errorGobbler.start();
	            //outputGobbler.start();
	            int exitVal = pr.waitFor();
	            
	            //logger.info(String.format("Process executed with exit code %d",exitVal));
	            System.out.println(String.format("Process executed with exit code %d", exitVal));
	    }catch(Exception e){
	        //logger.error(String.format("Error running task. Exception %s", e.toString()));
	    	throw e;
	    }
	}
	
	/**
	 * 測試用<br>
	 * 匯出CSV指定的欄位
	 */
	private void doTestWork01() throws Exception {
		//get the host for performing the mongo dump
	    /*String mongohome = GlimmerServer.config.getString("mongo.home");
	    String host = GlimmerServer.config.getString("mongo.dumphost");
	    String port = GlimmerServer.config.getString("mongo.dumpport");
	    String db = GlimmerServer.config.getString("mongo.dumpdb");*/
	    
	    String collection = "Activity_root_A_1408951308321";
	    String query = "'{date : new Date(1320451200000)}'"; //needs to be a proper query for mongo
	    String outputlocation = "D:/mongoData/output.txt"; //needs to be asigned a random number name

	    String command = String.format(mongohome + "/bin/mongoexport " +
	            "--host %s " +
	            //"--port %s " +
	            "--db %s " +        
	            "--collection %s " +                
	            //"--query %s " +
	            "--fields _id,name,note,sDate,eDate " +               
	            "--out %s " +           
	            "--slaveOk true " +         
	            "--csv " +
	            "-vvvvv",
	            //host,port,db,collection,query,outputlocation);
	            host,db_name,collection,outputlocation);

	    //logger.info(command);
	    System.out.println(command);
	    try{            
	            Runtime rt = Runtime.getRuntime();              
	            Process pr = rt.exec(command);
	            //StreamGobbler errorGobbler = new StreamGobbler(pr.getErrorStream(),"ERROR",logger);
	            //StreamGobbler outputGobbler = new StreamGobbler(pr.getInputStream(),"OUTPUT",logger);
	            //errorGobbler.start();
	            //outputGobbler.start();
	            int exitVal = pr.waitFor();
	            
	            //logger.info(String.format("Process executed with exit code %d",exitVal));
	            System.out.println(String.format("Process executed with exit code %d", exitVal));
	    }catch(Exception e){
	        //logger.error(String.format("Error running task. Exception %s", e.toString()));
	    	throw e;
	    }
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			String tmp = "1408951308321";
			
			//IP：192.168.1.107 mongodb test data
			tmp = "1392359197344";
			
			//IP:localhost  中信房屋
			tmp = "1401334909981";
			
			new ExportData1(tmp);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}