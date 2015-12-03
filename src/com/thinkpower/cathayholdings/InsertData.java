/**
 * 
 */
package com.thinkpower.cathayholdings;

import java.io.File;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
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
	/*
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
	*/
	//DB List
	private List<DB> listOfDB = new ArrayList<DB>();
	
	//private String collectionName = "pocfiles_meta";	//mongodb meta collection name
	//private String gridfsName = "pocfiles";	//gridfs collection name
	
	
	private List<String> storeKeyList = new ArrayList<String>();
	
	/**
	 * Constructor [For Test]
	 */
	public InsertData() {}
	
	/**
	 * @param dbUrl : 192.168.1.103:27017
	 * @param dbNameList : DB List
	 */
	public InsertData(String dbUrl, String dbNameList) throws Exception {
		long s1 = System.currentTimeMillis();
		System.out.println("DB URL is " + dbUrl);
		
		try {
			//mongo = new Mongo("192.168.1.103", 27027);
			mongo = new Mongo(dbUrl);
		} catch (UnknownHostException e) {
			e.printStackTrace();
			throw e;
		}
		/*
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
		*/
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
	 * 文字型資料
	 */
	/*public void insertGeneralData(int forLoops) {
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
	}*/
	
	/**
	 * GridFS資料
	 */
	/*public void insertGridFSData(int forLoops, String filePath) {
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
	}*/
	
	/**
	 * 整合 insert into "文字檔"與"圖檔" Collections 的行為
	 * @param forLoops
	 * @param hasImgFlag
	 * @param filePath
	 */
	public void insertData(DB focusDB, int forLoops, boolean hasImgFlag, String filePath) {
		long s1 = System.currentTimeMillis();
		long rows = 0;
		try {
			String _dbName = focusDB.getName();
			String _collectionName = _dbName + "_meta";
			String _gridfsName = _dbName;
			
			System.out.println("DB [" + _dbName + "] Collection Name [" + _collectionName + "]. "
					+ "GridfsName = " + _gridfsName);
			
			DBCollection collection = focusDB.getCollection(_collectionName);
			
			File[] aryOfFile = null;
			GridFS gridfs = null;
			if(hasImgFlag) {
				gridfs = new GridFS(focusDB, _gridfsName);
				
				File _file = new File(filePath);
				//System.out.println("_file is " + _file.isDirectory());
				aryOfFile = _file.listFiles();
			}
			
			for(int i=0;i<forLoops;i++) {
				BasicDBObject info = new BasicDBObject();
				info.put("_id", getIDKey(_collectionName));
		        info.put("version", "1.0");
		        info.put("filename", getRandomString());
		        info.put("filepath", getRandomString());
		        
				collection.insert(info, WriteConcern.SAFE);
				
				if(hasImgFlag) {
					for(File file : aryOfFile) {
						rows += 1;
						//
						// Store the file to MongoDB using GRIDFS
						//
						GridFSInputFile gfsFile = gridfs.createFile(file);
						gfsFile.setId(getIDKey(_gridfsName));
						gfsFile.setFilename(file.getName());
						gfsFile.save();
					}
				} else {
					rows += 1;
				}
				
				if(i > 10000 && (i % 20000) == 0)
					System.out.println("DB : " + _dbName + " DataRows : " + i);
			}
			
			long s2 = System.currentTimeMillis();
			System.out.println("Insert into DB [" + _dbName + "] "
					+ "DataRows is [" + rows + "] Time is [" + (s2 - s1) + " ms.]");
		} catch(Exception e) {
			e.printStackTrace(System.err);
		}
	}
	
	/**
	 * 取得Key值<br>
	 * _id/files_id均為數值。Range between in (1000000000000000 ~ 4000000000000000)<br>
	 * Shard	Tags	Range<br>
	 * SA1		OLD		1000000000000000 =< OLD <2000000000000000 <br>
	 * SA1		NHC		2000000000000000 =< NHC <3000000000000000 <br>
	 * SA2		LKC		3000000000000000 =< LKC <4000000000000000 <br>
	 * @return
	 */
	public long getIDKey(String keyName) {
		long s1 = System.currentTimeMillis();
		long runTimes = 0; //判斷取Key時，跑幾次
		long rtnValue = 0;
		boolean runFlag = true;
		while(runFlag) {
			runTimes++;
			//double randomNum = Math.random();
			long ans = (long)(Math.random() * (4000000000000000L - 1000000000000000L + 1)) + 1000000000000000L;
			//System.out.println("randomNum = " + randomNum + " ans = " + ans);
			//ans = 2497550709547400L;
			//System.out.println("ans = " + ans);
			
			//if(ans >= 1000000000000000L && ans < 4000000000000000L) {
			//	System.out.println("ans = " + ans);
			//}
			String key = keyName + String.valueOf(ans); //collectionName + random Value
			if(!storeKeyList.contains(key)) {
				rtnValue = ans;
				storeKeyList.add(key);
				runFlag = false;
			}
		}
		long s2 = System.currentTimeMillis();
		//如果取得key值超過1秒，就print
		if((s2 - s1) >= 1000) {
			Date _d = new Date();
			SimpleDateFormat sdFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss.SSS");
			System.out.println("***Get ID Key [" + rtnValue + "] More than 1 sec [" + (s2 - s1) + " ms.] "
					+ "RunTimes = " + runTimes + " Now is " + sdFormat.format(_d) + "***");
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
	 * Insert 一般資料 透過多執行序方式<br>
	 * 設計：一台DB-->有50條Thread在執行<br>
	 * @param threadSize Thread數
	 * @param forLoops 迴圈數
	 * @param hasGridFSFlag 有無GridFS
	 * @param imgPath GridFS 圖檔路徑
	 */
	public void insertByMutiThread(final int threadSize, final int forLoops,
			final boolean hasGridFSFlag, final String imgPath) {
		for(final DB db : listOfDB) {
			//int threadSize = 50;
			for(int i=0;i<threadSize;i++) {
				String t_name = db.getName() + "_thread_" + i;
				Thread t = new Thread(new Runnable() {
					@Override
					public void run() {
						//insertGeneralData(forLoops);
						//if(hasGridFSFlag)
						//	insertGridFSData(forLoops, imgPath);
						
						insertData(db, forLoops, hasGridFSFlag, imgPath);
					}
				}, t_name);
				System.out.println("Thread Name = " + t.getName());
				t.start();
			}
		}
	}
	
	/**
	 * 參數(共五組)：<br>
	 * DBUrl : 一條DB URL<br>
	 * DBName : List Of DB Name(以逗號分隔)<br>
	 * imgFilePath : 若為空，表示只塞"文字檔"(不處理gridFS)<br>
	 * dataRows : 資料筆數 <br>
	 * numberOfThreads : 壓測力量大小(Thread數目)
	 * @param args : [DBUrl DBName imgFilePath dataRows numberOfThreads]
	 */
	public static void main(String[] args) {
		InsertData indata = null;
		try {
			System.out.println("*** ****** ***");
			System.out.println("*** 完整傳入參數個數:[DBUrl DBName imgFilePath dataRows numberOfThreads]");
			System.out.println("*** DBUrl :\t\t一條DB URL (必填)***");
			System.out.println("*** DBName :\t\tList Of DB Name(以逗號分隔)(default:NULL字串)");
			System.out.println("*** imgFilePath :\tImage檔案資料夾路徑(default:NULL字串(只塞<文字檔>))");
			System.out.println("*** dataRows :\t\t資料筆數(default:NULL字串)");
			System.out.println("*** numberOfThreads :\t壓測力量大小(Thread數目)(default:NULL字串)");
			System.out.println("*** ****** ***\n");
			
			String url = args[0];
			String dbList = null;
			String filePath = null;	//Data/thinkpower
			int dataRows = 1;
			int numOfThreads = 1;
			
			try {
				dbList = args[1];
				if("NULL".equalsIgnoreCase(dbList))
					dbList = null;
			} catch(Exception e) {}
			
			try {
				filePath = args[2];	//Data/thinkpower
				if("NULL".equalsIgnoreCase(filePath))
					filePath = null;
			} catch(Exception e) {}
			
			try {
				dataRows = Integer.parseInt(args[3]);
			} catch(Exception e) {}
			try {
				numOfThreads = Integer.parseInt(args[4]);
			} catch(Exception e) {}
			
			System.out.println("Args url = " + url);
			System.out.println("Args dbList = " + dbList);
			System.out.println("Args filePath = " + filePath);
			System.out.println("Args dataRows = " + dataRows);
			System.out.println("Args Threads = " + numOfThreads);
			
			//只有dbUrl是必需要有的，其它若無，採預設值
			if("NULL".equalsIgnoreCase(url) ||
					url == null || url.trim().length() == 0)
				throw new Exception("DB URL is Empty.");
			
			if(dbList == null || dbList.trim().length() == 0)
				dbList = "noDB";
			
			boolean imgFlag = false; //預設「沒有圖檔」
			if(filePath != null && filePath.trim().length() > 0)
				imgFlag = true; //有圖檔
			
			//Test Key
			/*System.out.println("==========Test Key==========");
			indata = new InsertData();
			for(int i=0;i<dataRows;i++)
				indata.getIDKey();
			
			for(String _key : indata.storeKeyList) {
				System.out.println("Key is " + _key);
			}*/
			
			
			indata = new InsertData(url, dbList);
			//一般
			//indata.insertGeneralData(forLoops);
			//gridFS
			//indata.insertGridFSData(forLoops, filePath);
			
			//一般ByThread
			indata.insertByMutiThread(numOfThreads, dataRows, imgFlag, filePath);
		} catch(Exception e) {
			e.printStackTrace(System.err);
		}
	}
}
