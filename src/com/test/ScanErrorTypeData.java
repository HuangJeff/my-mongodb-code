/**
 * 
 */
package com.test;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

/**
 * 針對指定欄位進行資料型態的檢查(ex:Date/Number...)
 * @author asus1
 */
public class ScanErrorTypeData {
	private String mongohome = null; //mongo.home
	private String host = null; //mongo.dumphost
	private int port = 0; //mongo.dumpport
	private String db_name = null; //mongo.dumpdb
	
	private DBCollection dbColObj = null;
	
	private void initConfig() {
		mongohome = "C:/trs-standalone/mongodb-win32-x86_64-2.2.2"; //mongo.home
	    //host = "192.168.1.69"; //mongo.dumphost
	    //host = "192.168.1.107"; //window's mongo.host
		//host = "localhost"; //window's mongo.host
		host = "192.168.1.123";
	    port = 27017; //mongo.dumpport
	    db_name = "tptrs"; //mongo.dumpdb
	}
	
	/**
	 * 
	 */
	public ScanErrorTypeData(String colName) throws Exception {
		this.initConfig();
		
		this.getSpecifyCollection(colName);
	}
	
	/**
	 * 取得collectionName的Collection物件
	 * @param collectionName : Collections Name
	 * @throws Exception
	 */
	private void getSpecifyCollection(String collectionName) throws Exception {
		MongoClient mongoClient = new MongoClient(host, port);
		DB db = mongoClient.getDB( db_name );
		DBCollection dbColl = db.getCollection(collectionName);
		
		System.out.println(collectionName + " size = " + dbColl.getCount());
		
		this.dbColObj = dbColl;
	}
	
	/**
	 * 檢查欄位日期格式
	 * @param field : 要被檢查的欄位
	 * @param fixFlag : 是否需要被處理
	 * @param date : 如果要被處理的預設值
	 * @throws Exception
	 */
	public void checkDateType(String field, boolean fixFlag, Date date) throws Exception {
		DBCursor cursor = this.dbColObj.find();
		String colName = cursor.getCollection().getFullName();
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
		
		int numOfType = 0; //記錄欄位型態錯誤的個數
		int numOfFieldNotFound = 0; //記錄無此欄位的個數
	    while(cursor.hasNext()) {
	       DBObject obj = cursor.next();
	       String rowsKey = obj.get("_id").toString(); //只有 id 一定有
	       if(obj.containsField(field)) {
	    	   Object result = obj.get(field);
	    	   boolean isRightFlag = false; //判斷型別正確與否Flag
	    	   //檢查型別
	    	   //System.out.println(rowsKey + " = " + result);
	    	   dateFormat.parse(result.toString());
	    	   
	    	   System.out.println(rowsKey + " = " + result + " = " + dateFormat.isLenient());
	    	   
	    	   if(isRightFlag) {
		    	   //是否修復，若否  只顯示於畫面上
		    	   if(fixFlag) {
		    		   
		    	   } else {
		    		   //only display
		    		   if(numOfType < 20) {
		    			   
		    		   }
		    	   }
	    		   numOfType++;
	    	   }
	       } else {
	    	   if(numOfFieldNotFound < 20) {
		    	   String msg = colName + " IN " + rowsKey +
		    			   " Can't found Field [" + field + "]";
		    	   System.out.println(msg);
	    	   }
	    	   numOfFieldNotFound++;
	       }
	    }
	    System.out.println("Error Type Number is " + numOfType + " rows.");
	    System.out.println("Total Not Found Number is " + numOfFieldNotFound + " rows.");
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String collections = "Product_T04222671_A_1411200949414";
		ScanErrorTypeData setd = null;
		try {
			setd = new ScanErrorTypeData(collections);
			//檢查日期
			setd.checkDateType("outDate", false, null);
			
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
