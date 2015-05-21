/**
 * 
 */
package com.test;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.util.Paramters;

/**
 * 測試用
 * @author asus1
 * @JarFile：mongo-java-driver-2.10.1.jar
 */
public class TestMongo {
	// To directly connect to a single MongoDB server (note that this will not auto-discover the primary even
	// if it's a member of a replica set:
	MongoClient mongoClient = null;
	
	/**
	 * 
	 */
	public TestMongo() throws Exception {
		this.init();
		
		//DBObject crudCondition = this.getActivityCollection("1410061654464");
		//3.組裝要處理的Collection資訊
		List<String> listOfCollections = this.getGeneralDataCollection();
		//將欄位：created_at的日期打亂(要進行日期區間查詢)
		this.updateFieldCreatedAt();
		//整理欄位created_at日期的資訊，藉以整理資料的分佈情形
		this.distinctFieldCreatedAt(listOfCollections);
		
	}
	
	/**
	 * 設定資料庫連線
	 * @throws Exception
	 */
	private void init() throws Exception {
		// To directly connect to a single MongoDB server (note that this will not auto-discover the primary even
		// if it's a member of a replica set:
		//mongoClient = new MongoClient();
		// or
		//mongoClient = new MongoClient( "192.168.1.93" );
		// or
		mongoClient = new MongoClient( "localhost" , 27017 );
		// or, to connect to a replica set, with auto-discovery of the primary, supply a seed list of members
		//mongoClient = new MongoClient(Arrays.asList(new ServerAddress("localhost", 27017),
		//                                      new ServerAddress("localhost", 27018),
		//                                      new ServerAddress("localhost", 27019)));
		
	}
	
	private DBObject getActivityCollection(String activityName) throws Exception {
		DB db = mongoClient.getDB( "tptrs" );
		
		String saveDataRange = "3";
		if(saveDataRange == null || saveDataRange.trim().length() == 0) {
			throw new Exception("saveDataRange IS NULL !!! Can't Remove any Data!!");
		}
		int tmpInt = 0;
		try {
			tmpInt = Integer.parseInt(saveDataRange);
			tmpInt = - tmpInt;
		} catch(NumberFormatException nfe) {
			throw nfe;
		}
		
		DBObject crudCondition = null;
		Calendar cal_fromDate = Calendar.getInstance();
		cal_fromDate.add(Calendar.MONTH, tmpInt);
		Date fromDate = cal_fromDate.getTime();
		
		crudCondition = BasicDBObjectBuilder.start("$lt", fromDate).get();
		//System.out.println(crudCondition.toString());
		
		return crudCondition;
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
		String acct = "root";
		//活動ID
		String activeId = "1410061654464";
		//data type
		String dataType = "A";
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
	 * 整理欄位created_at日期的資訊，藉以整理資料的分佈情形
	 * @param listOfCollections 要處理之Collections
	 */
	private void distinctFieldCreatedAt(List<String> listOfCollections) {
		System.out.println("★==========★");
		DB db = mongoClient.getDB( Paramters.MONGO_DB );
		
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		
		for(String collectionName : listOfCollections) {
			DBCollection coll = db.getCollection(collectionName);
			
			System.out.println(coll.getFullName() + " : " + coll.count());
			String key = "created_at";
			/*List ansList = coll.distinct(key);
			System.out.println("ansList.size():" + ansList.size());
			for(Object item : ansList) {
				System.out.println("" + item);
			}*/
			
			Map<String, Integer> map = new HashMap<String, Integer>();
			DBCursor dbCursor = coll.find();
			while(dbCursor.hasNext()) {
				DBObject dbObj = dbCursor.next();
				Object res = dbObj.get(key); //created_at		type:date
				String strDate = format.format(res).trim();
				//System.out.println(strDate + " --- " + dbObj.get("created_at"));
				if(map.containsKey(strDate)) {
					Integer integer = map.get(strDate);
					map.remove(strDate);
					integer = integer + 1;
					map.put(strDate, integer);
				} else {
					map.put(strDate, 1);
				}
			}
			System.out.println("☆--" +map.size());
			for(Iterator<Map.Entry<String, Integer>> it = map.entrySet().iterator();it.hasNext();) {
				Map.Entry<String, Integer> me = it.next();
				System.out.println("ME Key:" + me.getKey() + "  Value:" + me.getValue());
			}
			
		}
	}
	
	/**
	 * 將欄位：created_at的日期打亂(要進行日期區間查詢)<br>
	 * Collection ： generalData_root_A_1410061654464
	 */
	private void updateFieldCreatedAt() {
		System.out.println("☆==========☆");
		//五種日期
		Calendar cal01 = Calendar.getInstance();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
		//1.Now
		Date now = cal01.getTime();
		//2.20天前
		cal01.setTime(now);
		cal01.add(Calendar.DAY_OF_MONTH, -20);
		Date _20Day = cal01.getTime();
		//3.三個月前
		cal01.setTime(now);
		cal01.add(Calendar.MONTH, -3);
		Date _3Mon = cal01.getTime();
		System.out.println("now:" + format.format(now) + " _20Day:" + format.format(_20Day)
				+ " _3Mon:" + format.format(_3Mon));
		
		DB db = mongoClient.getDB( Paramters.MONGO_DB );
		String collectionsName = "generalData_root_A_1410061654464";
		String key = "created_at";
		
		DBCollection coll = db.getCollection(collectionsName);
		
		DBCursor dbCursor = coll.find();
		while(dbCursor.hasNext()) {
			DBObject res = dbCursor.next();
			Object value = res.get("_id");
			BasicDBObject queryCondition = new BasicDBObject("_id", value);
			BasicDBObject updateCondition = null;
			int tmpI = new java.util.Random().nextInt(3); //0、1、2
			//System.out.println("tmpI：" + tmpI);
			if(tmpI == 1) {
				updateCondition = new BasicDBObject(key, now);
			} else if(tmpI == 2) {
				updateCondition = new BasicDBObject(key, _20Day);
			} else {
				updateCondition = new BasicDBObject(key, _3Mon);
			}
			
			coll.update(queryCondition, updateCondition);
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			new TestMongo();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
