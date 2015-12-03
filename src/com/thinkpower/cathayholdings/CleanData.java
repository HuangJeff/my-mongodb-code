/**
 * 
 */
package com.thinkpower.cathayholdings;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;

/**
 * 負責清除資料<br>
 * 
 * @author jeff
 * @date 2015/12/02
 */
public class CleanData {
	private Mongo mongo = null;
	
	private final String gridFS_Suffix1 = ".chunks";
	private final String gridFS_Suffix2 = ".files";
	private final String meta_Suffix = "_meta";
	
	//DB List
	private List<DB> listOfDB = new ArrayList<DB>();
	
	/**
	 * @param dbUrl : 192.168.1.103:27017
	 * @param dbNameList : DB List
	 */
	public CleanData(String dbUrl, String dbNameList) throws Exception {
		long s1 = System.currentTimeMillis();
		System.out.println("DB URL is " + dbUrl);
		
		try {
			//mongo = new Mongo("192.168.1.103", 27027);
			mongo = new Mongo(dbUrl);
		} catch (UnknownHostException e) {
			e.printStackTrace();
			throw e;
		}
		
		String[] aryOfDbName = dbNameList.split(",");
		
		for(String dbName : aryOfDbName) {
			DB db = mongo.getDB("admin");
			//v2.10 api Authenticates to db
			boolean isAuth = db.authenticate("manager", "1qaz2wsx".toCharArray());
			System.out.println("DB ["+ dbName + "] Authenticates to [admin] is OK?? " + isAuth);
			
			db = mongo.getDB(dbName);
			
			listOfDB.add(db);
		}
		
		long s3 = System.currentTimeMillis();
		System.out.println("★★★Constructor Create DB Connect Time is [" + (s3 - s1) + " ms.]");
	}
	
	/**
	 * 清空Collection內的所有Data<br>
	 * 1.find all collections from db <br>
	 * 2.count data rows from each collections <br>
	 * 3.delete Data <br>
	 * 4.count again
	 */
	private void cleanData() {
		for(DB db : listOfDB) {
			System.out.println("=====Clean DB [" + db.getName() + "]=====");
			
			Set<String> collections = db.getCollectionNames();
			for(String _colName : collections) {
				if(!(_colName.endsWith(gridFS_Suffix1) ||
						_colName.endsWith(gridFS_Suffix2) ||
						_colName.endsWith(meta_Suffix))
				) {
					continue;
				}
				DBCollection collection = db.getCollection(_colName);
				if(collection.getCount() == 0)
					continue;
				//count data rows
				System.out.println("=====Collections [" + _colName + "]====="
						+ "Data Rows [" + collection.getCount() + "]=====");
				
				BasicDBObject document = new BasicDBObject();
				// Delete All documents from collection Using blank BasicDBObject
				collection.remove(document);
				
				//count data rows
				System.out.println("=====Collections [" + _colName + "]====="
						+ "Data Rows [" + collection.getCount() + "]=====");
			}
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		CleanData cleanData = null;
		try {
			System.out.println("*** ****** ***");
			System.out.println("*** 完整傳入參數個數:[DBUrl DBName imgFilePath dataRows numberOfThreads]");
			System.out.println("*** DBUrl :\t\t一條DB URL (必填)***");
			System.out.println("*** DBName :\t\tList Of DB Name(以逗號分隔)(default:NULL字串)");
			System.out.println("*** ****** ***\n");
			
			String url = args[0];
			String dbList = args[1];
			
			System.out.println("Args url = " + url);
			System.out.println("Args dbList = " + dbList);
			
			if("NULL".equalsIgnoreCase(url) ||
					url == null || url.trim().length() == 0)
				throw new Exception("DB URL is Empty.");
			if("NULL".equalsIgnoreCase(dbList) ||
					dbList == null || dbList.trim().length() == 0)
				throw new Exception("DB Name is Empty.");
			
			cleanData = new CleanData(url, dbList);
			
			cleanData.cleanData();
			
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
