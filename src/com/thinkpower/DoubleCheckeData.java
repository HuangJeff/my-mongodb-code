/**
 * 
 */
package com.thinkpower;

import java.io.ByteArrayOutputStream;
import java.net.UnknownHostException;

import org.bson.types.ObjectId;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;

/**
 * 確認資料數是否一致<br>
 * 執行程式：GridFSTest.java後，從資料庫查詢的結果<br>
 * db['pocfiles.files'].count();	160000	<br>
 * db['pocfiles.chunks'].count();	82992	<br>
 * db.pocfiles_meta.count();		160000	<br>
 * 理當這三個值都應該為160000，但chunks不一樣，特地寫此程式來比對<br>
 * db['pocfiles.files'] == db['pocfiles.chunks'] <br>
 * 1.db['pocfiles.files']._id == db['pocfiles.chunks'].files_id<br>
 * 2.db['pocfiles.files'].chunkSize == db['pocfiles.chunks'].data (get byte array)
 * @author jeff
 */
public class DoubleCheckeData {
	static Mongo mongo = null;
	static DB db = null;
	static DBCollection collection = null;
	static GridFS gridfs = null;
	//static String collectionName = "pocfiles_meta";	//mongodb meta collection name
	static String gridfsName = "pocfiles";	//gridfs collection name
	
	/**
	 * 
	 */
	public DoubleCheckeData() {
		
	}
	
	//查詢檔案
	//public static ByteArrayOutputStream getData(String filename) throws Exception{
	public static ByteArrayOutputStream getData(ObjectId _objId) throws Exception{
		// This query fetches the files I need
		//GridFSDBFile gridFSDBFile = gridfs.findOne(new BasicDBObject("filename",filename));
		GridFSDBFile gridFSDBFile = gridfs.findOne(_objId);
		
//		long chunkSize = gridFSDBFile.getChunkSize();
//		if(chunkSize == 0) {
//			System.out.println("chunkSize is 0. file id:" + _objId);
//			return null;
//		}
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		try {
			gridFSDBFile.writeTo(out);
		} catch(com.mongodb.MongoException me) {
			System.err.println(me.getMessage());
			return null;
		}
		//System.out.println("2.取得DB檔案，filename=" + gridFSDBFile.getFilename());
		//System.out.println("");
		return out;
	}
	
	/**
	 * 比較：<br>
	 * 1.db['pocfiles.files']._id == db['pocfiles.chunks'].files_id<br>
	 * 2.db['pocfiles.files'].chunkSize == db['pocfiles.chunks'].data (get byte array)
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			args = new String[]{"None", "0", "192.168.1.127:27200"};
			String filePath = args[0];
			int forLoops = Integer.parseInt(args[1]);
			String url = args[2];
			
			System.out.println("DB URL is " + url + ",filePath=" + filePath + "  forLoops is " + forLoops);
			long t1 = System.currentTimeMillis();
			try {
				//mongo = new Mongo("192.168.1.103", 27027);
				mongo = new Mongo(url);
			} catch (UnknownHostException e) {
				e.printStackTrace();
				throw e;
			}
			db = mongo.getDB("gridFSTest");
			//collection = db.getCollection(collectionName);
			gridfs = new GridFS(db, gridfsName);
			
			DBCursor cursor = gridfs.getFileList();
			long t2 = System.currentTimeMillis();
			System.out.println("Load DB and Get Cursor = " + (t2 - t1) + " ms.");
			int totalCount = 0;
			int successCount = 0;
			int failsCount = 0;
			int limit = 0;
			while(cursor.hasNext()) {
				totalCount++;
				DBObject obj = cursor.next();
				/*
				 * Sample
				 * { "_id" : { "$oid" : "55963e7b0263a192b940c56c"} , "chunkSize" : 262144 , "length" : 3326 , "md5" : "f81b9aae006b4ce31fcc97373307a929" , "filename" : "image001.png" , "contentType" :  null  , "uploadDate" : { "$date" : "2015-07-03T07:49:15.964Z"} , "aliases" :  null }
				 * 
				 */
				//System.out.println(obj);
				if(totalCount % 5000 == 0) {
					long t3 = System.currentTimeMillis();
					System.out.println("total rows " + totalCount + " each 5000 rows = " + (t3 - t1) + " ms.");
				}
				
				//check 1.db['pocfiles.files']._id == db['pocfiles.chunks'].files_id
				//1.查詢資料
				ByteArrayOutputStream out = getData((ObjectId)obj.get("_id"));
				if(out == null) {
					failsCount++;
					continue;
				}
				
				successCount++;
				
				/*
				//check size
				byte[] b1 = out.toByteArray();
				
				System.out.println("DB檔案大小(chunkSize) " + obj.get("chunkSize") + " ==(原始檔案大小)Data size==" + b1.length);
				
				//寫出成檔案
				GridFSDBFile gridFSDBFile = gridfs.findOne((ObjectId)obj.get("_id"));
				File f = new File(filePath + "\\" + obj.get("filename").toString());
				writeOutFile(gridFSDBFile, f);
				*/
				//if(limit > 4)
				//	break;
				//limit++;
			}
			System.out.println("Totle Count:" + totalCount + " Success Count:" + successCount + " Fails Count:" + failsCount);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
