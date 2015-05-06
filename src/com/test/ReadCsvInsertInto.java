/**
 * 
 */
package com.test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;

import au.com.bytecode.opencsv.CSVReader;

/**
 * 讀取一個CSV檔案，並將檔案內容塞入MongoDB中
 * @author Jeff
 */
public class ReadCsvInsertInto {
	private String mongohome = null; //mongo.home
	private String host = null; //mongo.dumphost
	private int port = 27017; //mongo.dumpport
	private String db_name = null; //mongo.dumpdb
	
	private void initConfig() {
		mongohome = "C:/trs-standalone/mongodb-win32-x86_64-2.2.2"; //mongo.home
	    //host = "192.168.1.69"; //mongo.dumphost
	    //host = "192.168.1.107"; //window's mongo.host
		//host = "localhost"; //window's mongo.host
		host = "192.168.1.88";
	    //port = null; //mongo.dumpport
	    db_name = "tptrs"; //mongo.dumpdb
	}
	
	/**
	 * 
	 */
	public ReadCsvInsertInto(String filePath) {
		this.initConfig();
		
		CSVReader cR = null;
		try {
			File f = new File(filePath);
			FileReader fr = new FileReader(f);
			cR = new CSVReader(fr);
			
			List<String[]> l = cR.readAll();
			
			//DB Collections
			Mongo mongo = new Mongo(host, port);
			DB db = mongo.getDB(db_name);
			DBCollection collection = db.getCollection("Statistic_Session_root_A_1505051418490");
			
			for(int i=0;i<l.size();i++) {
				String[] str_arr2 = (String[])l.get(i);
				//欄位：_class,dataDate,statisticTime,bounceRate,
				//averageDuration,averageClick,averageAmount,averageRecommendAmount,
				//numberOfUsers
				//★insert into 用最簡單的Map方式來塞
				Map<String, Object> documentMap = new HashMap<String, Object>();
				documentMap.put("_class", str_arr2[0]);
				documentMap.put("dataDate", str_arr2[1]);
				documentMap.put("statisticTime", str_arr2[2]);
				documentMap.put("bounceRate", Double.parseDouble(str_arr2[3]));
				
				documentMap.put("averageDuration", Double.parseDouble(str_arr2[4]));
				documentMap.put("averageClick", Double.parseDouble(str_arr2[5]));
				documentMap.put("averageAmount", Double.parseDouble(str_arr2[6]));
				documentMap.put("averageRecommendAmount", Double.parseDouble(str_arr2[7]));
				
				documentMap.put("numberOfUsers", Integer.parseInt(str_arr2[8]));
				
				collection.insert(new BasicDBObject(documentMap));
			}
			System.out.println("total size : " + l.size() + " finish.");
		} catch(IOException ioe) {
			ioe.printStackTrace();
		} catch(Exception e) {
			e.printStackTrace();
		} finally {
			if(cR != null) {
				try {
					cR.close();
				} catch(IOException ioe) {
					ioe.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String filePath = "D:\\9.mongoExportData\\outPutFile.csv";
		new ReadCsvInsertInto(filePath);
	}
}
