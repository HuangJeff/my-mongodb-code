/**
 * 
 */
package com.thinkpower;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.UnknownHostException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.WriteConcern;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

/**
 * 測試 MongoDB GridFS
 * @author jeff
 */
public class GridFSTest {
	//
	// Connect to MongoDB (without authentification for the time being)
	// And get a handle on the collection used to store the metadata
	//
	static Mongo mongo = null;
	static DB db = null;
	static DBCollection collection = null;
	static GridFS gridfs = null;
	static String filename = "001招攬訪問報告書暨生調表!@!00001510301.tif";
	static String collectionName = "pocfiles_meta";
	static String gridfsName = "pocfiles";
	static String filepath = "E:/" + filename;
	static File file = new File(filepath);
	
	/**
	 * 
	 */
	public GridFSTest() {
	}
	
	public static void fileCheck(ByteArrayOutputStream fis1) throws Exception{
		
		InputStream fis2 = new FileInputStream(file);
		
		byte[] b1=fis1.toByteArray();
		
		byte[] b2=new byte[fis2.available()];
		fis2.read(b2);
		
		String a1 = new String(b1);
		String a2 = new String(b2);
		if(a1.equals(a2)){
			System.out.println("3.檔案比對結果:True");
		}else{
			System.out.println("3.檔案比對結果：False");
		}
		
		System.out.println("DB檔案大小=" + b1.length);
		System.out.println("原始檔案大小=" + b2.length);
		System.out.println("");
		fis1.close();
		fis2.close();
	}
	
	//查詢檔案
	public static ByteArrayOutputStream getData() throws Exception{
		// This query fetches the files I need
		GridFSDBFile gridFSDBFile = gridfs.findOne(new BasicDBObject("filename",filename));
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		gridFSDBFile.writeTo(out);
		System.out.println("2.取得DB檔案，filename=" + gridFSDBFile.getFilename());
		System.out.println("");
		return out;
	}
	
	public static ByteArrayInputStream parse(ByteArrayOutputStream baos) throws Exception
    {
        ByteArrayInputStream swapStream = new ByteArrayInputStream(baos.toByteArray());
        return swapStream;
    }
	
	//移除檔案
	public static void deleteData() throws Exception{
		gridfs.remove(gridfs.findOne(filename));
		
		BasicDBObject info = new BasicDBObject();
        info.put("version", "1.0");
        info.put("filename", filename);
        
		collection.remove(info);
		
		System.out.println("4.DB測試資掉刪除成功!");
		
	}
	
	//把檔案寫入到GridFS
	public static void insertData() throws Exception{
 
		//
		// Store the file to MongoDB using GRIDFS
		//
		
		GridFSInputFile gfsFile = gridfs.createFile(file);
		gfsFile.setFilename(filename);
		gfsFile.save();
 
		//
		// Let's create a new JSON document with some "metadata" information on the download
		//
		BasicDBObject info = new BasicDBObject();
                info.put("version", "1.0");
                info.put("filename", filename);
                info.put("filepath", filepath);
 
        //
        // Let's store our document to MongoDB
        //
		collection.insert(info, WriteConcern.SAFE);
		System.out.println("1.資料寫入成功!");
		System.out.println("");
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			mongo = new Mongo("localhost", 27017);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		db = mongo.getDB("test");
		collection = db.getCollection(collectionName);
		gridfs = new GridFS(db, gridfsName);
		
		try {
			//1.新增檔案
			insertData();
			
			//2.查詢資料
			ByteArrayOutputStream out = getData();
			
			//3.比對檔案
			fileCheck(out);
			
			//4.刪除DB測試資料
			deleteData();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
