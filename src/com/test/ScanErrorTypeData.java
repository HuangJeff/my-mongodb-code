/**
 * 
 */
package com.test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteResult;

/**
 * 針對指定欄位進行資料型態的檢查(ex:Date/Number...)
 * @author asus1
 */
public class ScanErrorTypeData {
	private String mongohome = null; //mongo.home
	private String host = null; //mongo.dumphost
	private int port = 0; //mongo.dumpport
	private String db_name = null; //mongo.dumpdb
	
	private DB db = null;
	
	private List<String> aryOfCollections; //List Of Collection Name
	
	//更新資料Map
	//階層--第一層：Key：Collections PK(_id)，第二層：Key：欄位名稱 Value：要Update的值
	Map<String, Map<String, String>> storeUpdMap = new HashMap<String, Map<String, String>>();
	
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
	 * 建構子
	 * @param colName : Collection Name
	 * @param isEndFlag : collection name 字串後段比對(String.endsWith)。true:後置 / false:前置
	 * @throws Exception
	 */
	public ScanErrorTypeData(String colName, boolean isEndFlag) throws Exception {
		this.initConfig();
		
		this.getSpecifyCollection(colName, isEndFlag);
	}
	
	/**
	 * 取得所有符後collectionName的Collection物件
	 * @param collectionName : Collections Name
	 * @param isEndFlag : collection name 字串後段比對(String.endsWith)。true:後置 / false:前置
	 * @throws Exception
	 */
	private void getSpecifyCollection(String collectionName, boolean isEndFlag) throws Exception {
		MongoClient mongoClient = new MongoClient(host, port);
		this.db = mongoClient.getDB( db_name );
		//DBCollection dbColl = db.getCollection(collectionName);
		//System.out.println(collectionName + " size = " + dbColl.getCount());
		//this.dbColObj = dbColl;
		
		if(aryOfCollections == null)
    		aryOfCollections = new ArrayList<String>();
		
		Set<String> colls = this.db.getCollectionNames();
		System.out.println("collectionName=" + collectionName + " isEndFlag=" + isEndFlag +
				" allSize =" + colls.size());
		for (String s : colls) {
			if(isEndFlag) {
		    	//System.out.println(s);
		    	if(s.endsWith(collectionName))
		    		aryOfCollections.add(s);
			} else {
				if(s.startsWith(collectionName))
		    		aryOfCollections.add(s);
			}
		}
	}
	
	/**
	 * 檢查欄位日期格式
	 * @param dbColl : Collections Object
	 * @param field : 要被檢查的欄位
	 * @param fixFlag : 是否需要被處理
	 * @param date : 如果要被處理的預設值
	 */
	public void checkDateType(DBCollection dbColl, String field, boolean fixFlag, Date date) {
		DBCursor cursor = dbColl.find();
		String colName = cursor.getCollection().getFullName();
		
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
		String defaultDateStr = null;
		if(fixFlag) {
			defaultDateStr = dateFormat.format(date);
		}
		
		int numOfType = 0; //記錄欄位型態錯誤的個數
		int numOfFieldNotFound = 0; //記錄無此欄位的個數
		int numOfTotalUpd = 0; //更新筆數
	    while(cursor.hasNext()) {
	       DBObject obj = cursor.next();
	       Object _id_Obj = obj.get("_id"); //只有 id 一定有
	       String rowsKey = _id_Obj.toString();
	       if(obj.containsField(field)) {
	    	   Object result = obj.get(field);
	    	   boolean isRightFlag = true; //判斷型別正確與否Flag
	    	   //檢查型別
	    	   //System.out.println(rowsKey + " = " + result);
	    	   
	    	   try {
	    		   dateFormat.parse(result.toString());
	    	   } catch (ParseException e) {
	    		   isRightFlag = false;
	    		   //System.err.println(e.getMessage());
	    	   }
	    	   
	    	   //System.out.println(rowsKey + " = " + result + " isRightFlag = " + isRightFlag);
	    	   
	    	   if(!isRightFlag) {
	    		   //only display
	    		   if(numOfType < 20) {
	    			   System.out.println(colName + " Error Type " + numOfType + " rows. result is =*" + result + "*");
	    		   }
	    		   numOfType++;
	    		   
	    		   //是否修復，若否  只顯示於畫面上
		    	   if(fixFlag) {
		    		   if(numOfTotalUpd < 20)
		    			   System.out.println("Update Data _id = " + rowsKey +
		    					   " field = " + field + " Data = " + defaultDateStr);
		    		   //如果不用 $set 則會清空資料，只將目前這筆資料塞進去(只剩這欄位)
		    		   //DBObject jo = new BasicDBObject(field, defaultDateStr);
		    		   
		    		   //$set Data Object
		    		   DBObject jo = new BasicDBObject();
		    		   jo.put("$set", new BasicDBObject(field, defaultDateStr));
		    		   
		    		   dbColl.update(new BasicDBObject("_id", _id_Obj), jo, false, false);
		    		   numOfTotalUpd++;
		    	   }
	    	   }
	       } else {
	    	   if(numOfFieldNotFound < 10) {
		    	   String msg = colName + " IN " + rowsKey +
		    			   " Can't found Field [" + field + "]";
		    	   System.out.println(msg);
	    	   }
	    	   numOfFieldNotFound++;
	       }
	    }
	    if(numOfType > 0)
	    	System.out.println("Error Type Number is " + numOfType + " rows.");
	    if(numOfFieldNotFound > 0)
	    	System.out.println("Total Not Found Number is " + numOfFieldNotFound + " rows.");
	    if(numOfTotalUpd > 0)
	    	System.out.println("Total Update Data Rows is " + numOfTotalUpd + " rows.");
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String collections = "Product_T12499995_A_1402562462595";
		//collections = "Product";
		ScanErrorTypeData setd = null;
		try {
			setd = new ScanErrorTypeData(collections, false);
			
			for (String s : setd.aryOfCollections) {
				//System.out.println(s);
				try {
					DBCollection dbColl = setd.db.getCollection(s);
					//檢查日期
					setd.checkDateType(dbColl, "outDate", false, new Date());
					
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
		System.out.println("===ScanErrorTypeData Is Finish===");
	}
}
