/**
 * 
 */
package com.mongo;

import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.QueryBuilder;
import com.mongodb.WriteResult;
import com.util.Paramters;
import com.util.PropertiesUtil;

/**
 * @author asus1
 */
public class MongoWork {
	private PropertiesUtil propUtil = null;
	private Map<String, String> propertyMap = null;
	private MongoClient mongoClient = null;
	
	
	/**
	 * Mongo資料處理流<br>
	 * 1.取得設定資料<br>
	 * 2.取得MongoDB連線<br>
	 * 3.組裝要處理的Collection資訊<br>
	 * 4.組裝CRUD Conditions<br>
	 */
	public MongoWork() {
		try {
			//1.取得設定資料
			propUtil = new PropertiesUtil();
			propertyMap = propUtil.getPropertyMap();
			//2.取得MongoDB連線
			this.linkToMongoDB();
			
			//3.組裝要處理的Collection資訊
			List<String> listOfCollections = this.getGeneralDataCollection();
			
			//TODO 目前此功能暫寫死 = Delete
			String crudAction = "S";
			if(propertyMap.containsKey(Paramters.crud_action)) {
				//action = propertyMap.get(Paramters.crud_action);
			}
			
			//4.組裝CRUD Conditions
			DBObject crudCondition = this.getUserSpecifyWorkCondition();
			
			if(crudCondition != null) {
				//執行條件動作
				if("S".equals(crudAction)) {
					this.doMongoQuery(listOfCollections, crudCondition);
				} else if("D".equals(crudAction)) {
					this.doMongoDelete(listOfCollections, crudCondition);
				}
			} else {
				//crudCondition is Null
				System.err.println("crudCondition is Null!!!");
			}
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			if(mongoClient != null)
				mongoClient.close();
		}
	}
	
	/**
	 * 連結資料庫
	 */
	private void linkToMongoDB() throws UnknownHostException {
		String dbIp = propertyMap.get(Paramters.mongo_ip);
		
		boolean dbPortFlag = propertyMap.containsKey(Paramters.mongo_port);
		int dbPort = 0;
		if(dbPortFlag)
			dbPort = Integer.parseInt(propertyMap.get(Paramters.mongo_port));
		
//		System.out.println("Here is----" + dbIp + "  " + dbPort);
		
		if(dbPortFlag)
			mongoClient = new MongoClient( dbIp , dbPort );
		else
			mongoClient = new MongoClient( dbIp );
//		System.out.println("mongoClient = " + mongoClient);
	}
	
	/**
	 * 組裝Collection:generalData 相關資訊，因為可能會有多個Collection的原因<br>
	 * 20140911 Jeff<br>
	 * Hot Code : generalData 這個字串來組成 Collection<br>
	 * @return List of Collections
	 */
	private List<String> getGeneralDataCollection() {
		List<String> listOfCollection = new ArrayList<String>();
		//TODO Hot Code 寫死
		String collectionName = "generalData";
		//取得相關Collection資訊
		//帳號
		String acct = propertyMap.get(Paramters.userName);
		//活動ID
		String activeId = propertyMap.get(Paramters.activity_id);
		//data type
		String dataType = propertyMap.get(Paramters.data_type);
		//目前已知，只有dataType有可能有兩組
		String[] aryOfDataType = dataType.split(",");
		/*
		 * Collection Name的範例：
		 * generalData_root_A_1409707337638
		 * CollectionName + user帳號 + dataType + 活動ID
		 */
		for(String itemOfDataType : aryOfDataType) {
			listOfCollection.add(collectionName + "_" + acct + "_" + itemOfDataType + "_" + activeId);
		}
		return listOfCollection;
	}
	
	/**
	 * 組裝SQL Condition。<br>
	 * 依使用者指定的日期來限定查詢的資料數<br>
	 */
	private DBObject getUserSpecifyWorkCondition() throws Exception {
		DBObject crudCondition = null;
		String saveDataRange = propertyMap.get(Paramters.saveDataRange);
		if(saveDataRange == null || saveDataRange.trim().length() == 0) {
			//throw new Exception("saveDataRange IS NULL !!! Can't Remove any Data!!");
			return crudCondition;
		}
		int tmpInt = 0;
		try {
			tmpInt = Integer.parseInt(saveDataRange);
			tmpInt = -tmpInt;
		} catch(NumberFormatException nfe) {
			//throw nfe;
			return crudCondition;
		}
		Calendar cal_fromDate = Calendar.getInstance();
		/*
		 * ★★★
		 * http://blog.xiaorui.cc/tag/mongodb-%E6%97%A5%E6%9C%9F/
		 * 大家一定要注意下时间，时间要指定注入，别用mongodb自带的 new date了，他的时间是utc的时间，和咱们中国时区少8个小时。
		 * ★★★
		 */
		cal_fromDate.add(Calendar.HOUR, 8);
		//保留指定的"月"數
		cal_fromDate.add(Calendar.MONTH, tmpInt);
		Date fromDate = cal_fromDate.getTime();
		
		crudCondition = BasicDBObjectBuilder.start("$lt", fromDate).get();
		System.out.println("crudCondition : " + crudCondition.toString());
		
		return crudCondition;
	}
	
	/**
	 * 進行Collection相關動作：查詢<br>
	 * 
	 * @param listOfCollections : 要被處理的Collection
	 * @param action : CRUD 那樣動作
	 * @param condition : CRUD 的動作條件
	 */
	private void doMongoQuery(List<String> listOfCollections, DBObject condition) {
		DB db = mongoClient.getDB( Paramters.MONGO_DB );
		
		BasicDBObject query = new BasicDBObject();
		query.put("created_at", condition);
		
		System.out.println("condition ==> " + query.toString());
		
		for(String collectionName : listOfCollections) {
			DBCollection coll = db.getCollection(collectionName);
			
			System.out.println(collectionName + " : " + coll.count() + " --- " + coll.count(query));
		}
		
	}
	
	/**
	 * 進行Collection相關動作:刪除<br>
	 * 
	 * @param listOfCollections : 要被處理的Collection
	 * @param delCondition : CRUD 的動作條件
	 */
	private void doMongoDelete(List<String> listOfCollections, DBObject delCondition) {
		DB db = mongoClient.getDB( Paramters.MONGO_DB );
		
		//測試  小於2014-07-02T01:50:18Z
		BasicDBObject query = new BasicDBObject();
		System.out.println(delCondition.toString());
		query.put("created_at", delCondition);
		
		System.out.println(query.toString());
		
		for(String collectionName : listOfCollections) {
			DBCollection coll = db.getCollection(collectionName);
			System.out.println(collectionName + " : " + coll.count() + " --- " + coll.count(query));
			
			//進行 remove 動作
			WriteResult wresult = coll.remove(query);
			System.out.println("Result::" + wresult.getN() + "  " + wresult.getError() + "  " + wresult.toString());
		}
		
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new MongoWork();
	}
}
