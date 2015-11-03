/**
 * 
 */
package com.thinkpower.cathayholdings;

import java.io.File;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.WriteConcern;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSInputFile;

/**
 * 負責塞資料，將分成塞GridFS與一般型的資料兩種<br>
 * 2015/10/13 [完成]
 * @author jeff
 * @date 2015/10/13
 */
public class InsertData {
	private Mongo mongo = null;
	//一般Collection
	private DB db = null;
	private String dbName = "fsDB";
	private String collectionName = "pocfiles_meta";	//mongodb meta collection name
	private DBCollection collection = null;
	
	//GridFS
	private DB gridfsDb = null;
	private String gridfsDbName = "gridFSTest";
	private GridFS gridfs = null;
	private String gridfsName = "pocfiles";	//gridfs collection name
	
	
	private List<String> storeKeyList = new ArrayList<String>();
	
	/**
	 * @param dbUrl : 192.168.1.103:27017
	 */
	public InsertData(String dbUrl) throws Exception {
		long s1 = System.currentTimeMillis();
		System.out.println("DB URL is " + dbUrl);
		try {
			//mongo = new Mongo("192.168.1.103", 27027);
			mongo = new Mongo(dbUrl);
		} catch (UnknownHostException e) {
			e.printStackTrace();
			throw e;
		}
		//一般DB
		db = mongo.getDB("admin");
		//v2.10 api Authenticates to db
		boolean isAuth = db.authenticate("manager", "1qaz2wsx".toCharArray());
		System.out.println("一般DB Authenticates to [admin] is OK?? " + isAuth);
		db = mongo.getDB(dbName);
		collection = db.getCollection(collectionName);
		
		//gridFS
		gridfsDb = mongo.getDB("admin");
		//v2.10 api Authenticates to db
		boolean isAuthGridFS = gridfsDb.authenticate("manager", "1qaz2wsx".toCharArray());
		System.out.println("gridFS DB Authenticates to [admin] is OK?? " + isAuthGridFS);
		gridfsDb = mongo.getDB(gridfsDbName);
		gridfs = new GridFS(gridfsDb, gridfsName);
		
		long s3 = System.currentTimeMillis();
		System.out.println("Time is [" + (s3 - s1) + " ms.]");
	}
	
	/**
	 * 文字型資料
	 */
	public void insertGeneralData(int forLoops) {
		long s1 = System.currentTimeMillis();
		try {
			for(int i=0;i<forLoops;i++) {
				BasicDBObject info = new BasicDBObject();
				//info.put("_id", getIDKey("GN"));
		        info.put("version", "1.0");
		        info.put("filename", getRandomString());
		        info.put("filepath", getRandomString());
		        
				collection.insert(info, WriteConcern.SAFE);
				if((i % 20000) == 0)
					System.out.println("Rows = " + i);
			}
			long s2 = System.currentTimeMillis();
			System.out.println("insertGeneralData Time is [" + (s2 - s1) + " ms.]");
		} catch(Exception e) {
			e.printStackTrace(System.err);
		}
	}
	
	/**
	 * GridFS資料
	 */
	public void insertGridFSData(int forLoops, String filePath) {
		long s1 = System.currentTimeMillis();
		try {
			File _file = new File(filePath);
			//System.out.println("_file is " + _file.isDirectory());
			File[] aryOfFile = _file.listFiles();
			for(int i=0;i<forLoops;i++) {
				for(File file : aryOfFile) {
					//
					// Store the file to MongoDB using GRIDFS
					//
					GridFSInputFile gfsFile = gridfs.createFile(file);
					gfsFile.setId(getIDKey("FS"));
					gfsFile.setFilename(file.getName());
					gfsFile.save();
				}
			}
			long s2 = System.currentTimeMillis();
			System.out.println("insertGridFSData Time is [" + (s2 - s1) + " ms.]");
		} catch(Exception e) {
			e.printStackTrace(System.err);
		}
	}
	
	/**
	 * 取得Key值<br>
	 * _id/files_id均為數值。shard1 = 0~5000。shard2 = 5000~9999(用Random來產生)
	 * @return
	 */
	public int getIDKey(String str) {
		int rtnValue = 0;
		boolean runFlag = true;
		while(runFlag) {
			double randomNum = Math.random();
			int ans = (int)(10000 * randomNum);
			System.out.println("randomNum = " + randomNum + " ans = " + ans);
			String key = str + "_" + ans;
			if(!storeKeyList.contains(key)) {
				rtnValue = ans;
				storeKeyList.add(key);
				runFlag = false;
			}
		}
		return rtnValue;
	}
	
	
	public static String getRandomString()
	{
		char seeds[] = {'a','b','c','d','e','f','g','0','1','2','3','4','5','6','7','8','9','0'};
		int strLen = (int)Math.round(Math.random() * 10) + 5;
		char randStr[] = new char[strLen];
		for (int i=0;i<randStr.length;i++)
		{
			randStr[i] = seeds[(int)Math.round(Math.random() * (seeds.length - 1))];
		}
		String returnStr = new String(randStr);
		return returnStr;
	}
	
	/**
	 * Insert 一般資料 透過多執行序方式
	 * @param forLoops 迴圈數
	 */
	public void insertByMutiThread(final int forLoops) {
		int threadSize = 5;
		for(int i=0;i<threadSize;i++) {
			String t_name = "t_" + i;
			Thread t = new Thread(new Runnable() {
				@Override
				public void run() {
					insertGeneralData(forLoops);
				}
			}, t_name);
			System.out.println("Thread Name = " + t.getName());
			t.start();
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			String url = args[0];
			int forLoops = Integer.parseInt(args[1]);
			String filePath = args[2];	//Data/thinkpower
			
			InsertData indata = new InsertData(url);
			//一般
			//indata.insertGeneralData(forLoops);
			//gridFS
			//indata.insertGridFSData(forLoops, filePath);
			
			//一般ByThread
			indata.insertByMutiThread(forLoops);
		} catch(Exception e) {
			e.printStackTrace(System.err);
		}
	}
}
