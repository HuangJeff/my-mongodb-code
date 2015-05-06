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
		mongohome = "C:/trs-standalone/mongodb-win32-x86_64-2.2.2"; //mongo.home
	    //host = "192.168.1.69"; //mongo.dumphost
	    //host = "192.168.1.107"; //window's mongo.host
		//host = "localhost"; //window's mongo.host
		host = "192.168.1.123";
	    port = null; //mongo.dumpport
	    db_name = "tptrs"; //mongo.dumpdb
	    
	    outputFolder = "D:/9.mongoExportData/"; //export folder
	    //outputFolder = "D:/9.mongoExportData_cthouse/"; //export folder
	}
	
	/**
	 * 
	 */
	public ExportData1(String activityName) throws Exception {
		this.initConfig();
		
		this.getSpecifyCollections(activityName);
		//this.getAllCollections();
		
		if(exportCollectionList != null) {
			for(String colName : exportCollectionList)
				System.out.println(colName);
		}
		
		if(exportCollectionList != null) {
			for(String colName : exportCollectionList)
				try {
					//匯出所有資料(No Query)
					//this.doCommandWorkExportAll(colName);
					
					//匯出Query的資料
					//this.doCommandWorkExportByQuery(colName);
					
					//匯出資料-1
					//針對特定欄位過濾
					String fields = "_class,dataDate,statisticTime,bounceRate,averageDuration,averageClick,averageAmount,averageRecommendAmount,numberOfUsers";
					this.doCommandWorkExportByQuery(colName, null, fields);
				} catch(Exception e) {
					System.err.println("★Collection Name : " + colName);
					System.err.println("★Exception : " + e.getMessage());
				}
		}
	}
	
	/**
	 * 取得所有的Collection Name
	 * @throws Exception
	 */
	private void getAllCollections() throws Exception {
		MongoClient mongoClient = new MongoClient(host);
		
		DB db = mongoClient.getDB( db_name );
		
		Set<String> colls = db.getCollectionNames();
		
		for (String s : colls) {
			if(exportCollectionList == null)
	    		exportCollectionList = new ArrayList<String>();
	    	exportCollectionList.add(s);
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
	 * 匯出所有資料(No Query)
	 * @throws Exception
	 */
	private void doCommandWorkExportAll(String collectionName) throws Exception {
		String collection = collectionName;
	    String outputlocation = outputFolder + collectionName + ".txt"; //needs to be asigned a random number name
	    //String query = "'{created_at : { $gt:ISODate(\"2014-10-24T13:00:00.000Z\"), $lt:ISODate(\"2014-10-24T14:00:00.999Z\") }}'";
	    
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
	            host, db_name, collection, outputlocation);
	    		//host, db_name, query, collection, outputlocation);
	    
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
	 * 匯出Query出的資料，此處來組Query條件
	 * @throws Exception
	 */
	private void doCommandWorkExportByQuery(String collectionName) throws Exception {
		String query = "\"{oriUserID : 'a'}\""; //oriUserID 欄位 Type is String
	    query = "\"{userID : 1}\""; //userID 欄位 Type is Integer
	    //query = "'{\"created_at\":{\"$gt\":new Date(2014,09,28)}}'";
	    query = "'{created_at : {$gt : ISODate(\"2014-03-07T22:00:00Z\")}}'";
	    
	    String fields = "_id,name,note,sDate,eDate";
	    
	    this.doCommandWorkExportByQuery(collectionName, query, fields);
	}
	
	/**
	 * 匯出Query出的資料
	 * @param collectionName collections name
	 * @param query 查詢條件
	 * @param outputField 查詢輸出欄位
	 * @throws Exception
	 */
	private void doCommandWorkExportByQuery(String collectionName, String query, String outputField) throws Exception {
		String collection = collectionName;
	    String outputlocation = outputFolder + collectionName + ".txt"; //needs to be asigned a random number name
	    /*
	    //Sample Query
	    String query = "\"{oriUserID : 'a'}\""; //oriUserID 欄位 Type is String
	    query = "\"{userID : 1}\""; //userID 欄位 Type is Integer
	    //query = "'{\"created_at\":{\"$gt\":new Date(2014,09,28)}}'";
	    query = "'{created_at : {$gt : ISODate(\"2014-03-07T22:00:00Z\")}}'";
	    */
	    String command = null;
	    if( (query != null && query.trim().length() > 0) &&
	    		(outputField != null && outputField.trim().length() > 0) ) {
	    	//針對輸出的欄位的話， --csv 要打開，否則還是一樣會輸出所有欄位
	    	command = String.format(mongohome + "/bin/mongoexport " +
	            "--host %s " +
	            //"--port %s " +
	            "--db %s " + 
	            "--collection %s " + 
	            "--query %s " +
	            //"--fields _id,name,note,sDate,eDate " + 
	            "--fields %s " +
	            "--out %s " + 
	            "--slaveOk true " + 
	            "--csv " +
	            "-vvvvv",
	            //host,port,db,collection,query,outputlocation);
	            host, db_name, collection, query, outputField, outputlocation);
	    } else if(query != null && query.trim().length() > 0) {
	    	command = String.format(mongohome + "/bin/mongoexport " +
		            "--host %s " +
		            //"--port %s " +
		            "--db %s " + 
		            "--collection %s " + 
		            "--query %s " +
		            "--out %s " + 
		            "--slaveOk true " + 
		            //"--csv " +
		            "-vvvvv",
		            host, db_name, collection, query, outputlocation);
	    } else if(outputField != null && outputField.trim().length() > 0) {
	    	//針對輸出的欄位的話， --csv 要打開，否則還是一樣會輸出所有欄位
	    	command = String.format(mongohome + "/bin/mongoexport " +
		            "--host %s " +
		            //"--port %s " +
		            "--db %s " + 
		            "--collection %s " + 
		            //"--fields _id,name,note,sDate,eDate " + 
		            "--fields %s " +
		            "--out %s " + 
		            "--slaveOk true " + 
		            "--csv " +
		            "-vvvvv",
		            //host,port,db,collection,query,outputlocation);
		            host, db_name, collection, outputField, outputlocation);
	    } else {
	    	command = String.format(mongohome + "/bin/mongoexport " +
		            "--host %s " +
		            //"--port %s " +
		            "--db %s " + 
		            "--collection %s " + 
		            "--out %s " + 
		            "--slaveOk true " + 
		            //"--csv " +
		            "-vvvvv",
		            //host,port,db,collection,query,outputlocation);
		            host, db_name, collection, outputlocation);
	    }
	    
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
	            if(exitVal == 0)
	            	System.out.println(String.format("Process executed with exit code %d", exitVal));
	            else 
	            	System.err.println(String.format("Process executed with exit code %d", exitVal));
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
	            "--query %s " +
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
			//活動代碼
			String tmp = "1408951308321";
			
			//IP：192.168.1.107 mongodb test data
			tmp = "1392359197344";
			
			//IP:localhost  中信房屋
			//完整名稱
			tmp = "generalData_T01187263_B_1410759774603";
			//tmp = "generalData_T01187263_B_1402450563984";
			tmp = "Product_T04222671_A_1411200949414";
			//tmp = "MUP_T84305300_B_1503191349478";
			
			//匯出網站訪問統計報表的資訊(From 住商)
			tmp = "Statistic_Session_T36948676_A_1409039122078";
			
			new ExportData1(tmp);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
