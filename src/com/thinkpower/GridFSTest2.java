package com.thinkpower;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.UnknownHostException;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.WriteConcern;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;

/**
 * 測試 MongoDB GridFS<br>
 * step 2 : Read --> insert again<br>
 * 讀取檔後重新在塞回DB
 * @author asus1
 *
 */
public class GridFSTest2 {
	static Mongo mongo = null;
	static DB db = null;
	static DBCollection collection = null;
	static GridFS gridfs = null;
	static String collectionName = "pocfiles_meta";	//mongodb meta collection name
	static String gridfsName = "pocfiles";	//gridfs collection name
	
	public GridFSTest2() {
		
	}
	
	//把檔案寫入到GridFS
	public static void insertData(InputStream in, String newName) throws Exception{
 
		//
		// Store the file to MongoDB using GRIDFS
		//
		
		GridFSInputFile gfsFile = gridfs.createFile(in, newName);
		gfsFile.setFilename(newName);
		gfsFile.save();
		
		//
		// Let's create a new JSON document with some "metadata" information on the download
		//
		//BasicDBObject info = new BasicDBObject();
        //        info.put("version", "1.0");
        //        info.put("filename", file.getName());
        //        info.put("filepath", filepath);
 
        //
        // Let's store our document to MongoDB
        //
		//collection.insert(info, WriteConcern.SAFE);
		//System.out.println("1.資料寫入成功!");
		//System.out.println("");
	}
	
	//查詢檔案
	//public static ByteArrayOutputStream getData(String filename) throws Exception{
	public static ByteArrayOutputStream getData(ObjectId _objId) throws Exception{
		// This query fetches the files I need
		//GridFSDBFile gridFSDBFile = gridfs.findOne(new BasicDBObject("filename",filename));
		GridFSDBFile gridFSDBFile = gridfs.findOne(_objId);
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		gridFSDBFile.writeTo(out);
		System.out.println("2.取得DB檔案，filename=" + gridFSDBFile.getFilename());
		System.out.println("");
		return out;
	}
	
	public static void writeOutFile(GridFSDBFile in, File out) throws Exception {
		in.writeTo(out);
	}
	
	public static void fileCheck(ByteArrayOutputStream fis1, File file) throws Exception{
		
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
	
	
	public static void main(String[] args) {
		try {
			//取得參數
			if(args.length != 3) {
				throw new Exception("參數個數須為3個[filePath forLoops DBURL]！");
			}
			long s1 = System.currentTimeMillis();
			
			String filePath = args[0];
			int forLoops = Integer.parseInt(args[1]);
			String url = args[2];
			
			System.out.println("DB URL is " + url + ",filePath=" + filePath);
			
			try {
				//mongo = new Mongo("192.168.1.103", 27027);
				mongo = new Mongo(url);
			} catch (UnknownHostException e) {
				e.printStackTrace();
				throw e;
			}
			db = mongo.getDB("gridFSTest");
			collection = db.getCollection(collectionName);
			gridfs = new GridFS(db, gridfsName);
			
			for(int i = 0;i < forLoops; i++) {
				String file1 = "001招攬訪問報告書暨生調表!@!00001510301.tif";
				String file2 = "002要保人須知!@!00001610301.tif";
				String file3 = "003個人資料保護法應告知事項!@!00027710201.tif";
				String file4 = "004招攬「非投資型」保險商品應遵循事項檢核表!@!00076010301.tif";
				String file5 = "005不分紅要保書(行動保險)1!@!MC000001110301.tif";
				String file6 = "006不分紅要保書(行動保險)2!@!MC000001210301.tif";
				String file7 = "007不分紅要保書(行動保險)3!@!MC000001310301.tif";
				String file8 = "008不分紅要保書(行動保險)4!@!MC000001410301.tif";
				
				String n_file1 = "001招攬訪問報告書暨生調表!@!00001510301-v2.tif";
				String n_file2 = "002要保人須知!@!00001610301-v2.tif";
				String n_file3 = "003個人資料保護法應告知事項!@!00027710201-v2.tif";
				String n_file4 = "004招攬「非投資型」保險商品應遵循事項檢核表!@!00076010301-v2.tif";
				String n_file5 = "005不分紅要保書(行動保險)1!@!MC000001110301-v2.tif";
				String n_file6 = "006不分紅要保書(行動保險)2!@!MC000001210301-v2.tif";
				String n_file7 = "007不分紅要保書(行動保險)3!@!MC000001310301-v2.tif";
				String n_file8 = "008不分紅要保書(行動保險)4!@!MC000001410301-v2.tif";
				
				GridFSDBFile gridFSDBFile1 = gridfs.findOne(file1);
				GridFSDBFile gridFSDBFile2 = gridfs.findOne(file2);
				GridFSDBFile gridFSDBFile3 = gridfs.findOne(file3);
				GridFSDBFile gridFSDBFile4 = gridfs.findOne(file4);
				GridFSDBFile gridFSDBFile5 = gridfs.findOne(file5);
				GridFSDBFile gridFSDBFile6 = gridfs.findOne(file6);
				GridFSDBFile gridFSDBFile7 = gridfs.findOne(file7);
				GridFSDBFile gridFSDBFile8 = gridfs.findOne(file8);
				
//				System.out.println("file1 name:" + gridFSDBFile1.getFilename() +
//						",ChunkSize:" + gridFSDBFile1.getChunkSize() + ",length:" + gridFSDBFile1.getLength());
//				System.out.println("file2 name:" + gridFSDBFile2.getFilename() +
//						",ChunkSize:" + gridFSDBFile2.getChunkSize() + ",length:" + gridFSDBFile2.getLength());
//				System.out.println("file3 name:" + gridFSDBFile3.getFilename() +
//						",ChunkSize:" + gridFSDBFile3.getChunkSize() + ",length:" + gridFSDBFile3.getLength());
				
				insertData(gridFSDBFile1.getInputStream(), n_file1);
				insertData(gridFSDBFile2.getInputStream(), n_file2);
				insertData(gridFSDBFile3.getInputStream(), n_file3);
				insertData(gridFSDBFile4.getInputStream(), n_file4);
				insertData(gridFSDBFile5.getInputStream(), n_file5);
				insertData(gridFSDBFile6.getInputStream(), n_file6);
				insertData(gridFSDBFile7.getInputStream(), n_file7);
				insertData(gridFSDBFile8.getInputStream(), n_file8);
				
				if(i == (forLoops / 2))
					System.out.println("Run " + i + " rows.Time is [" +
							(System.currentTimeMillis() - s1) + " ms.]");
			}
			
			long s2 = System.currentTimeMillis();
			System.out.println("Total Time is [" + (s2 - s1) + " ms.]");
			
//			DBCursor cursor = gridfs.getFileList();
//			//DBCursor cursor = collection.find();
//			
//			while(cursor.hasNext()) {
//				DBObject obj = cursor.next();
//				/*
//				 * Sample
//				 * { "_id" : { "$oid" : "55963e7b0263a192b940c56c"} , "chunkSize" : 262144 , "length" : 3326 , "md5" : "f81b9aae006b4ce31fcc97373307a929" , "filename" : "image001.png" , "contentType" :  null  , "uploadDate" : { "$date" : "2015-07-03T07:49:15.964Z"} , "aliases" :  null }
//				 * 
//				 */
//				//System.out.println(obj);
//				//1.查詢資料
//				ByteArrayOutputStream out = getData((ObjectId)obj.get("_id"));
//				/*
//				//check size
//				byte[] b1 = out.toByteArray();
//				
//				System.out.println("DB檔案大小(chunkSize) " + obj.get("chunkSize") + " ==(原始檔案大小)Data size==" + b1.length);
//				
//				//寫出成檔案
//				GridFSDBFile gridFSDBFile = gridfs.findOne((ObjectId)obj.get("_id"));
//				File f = new File(filePath + "\\" + obj.get("filename").toString());
//				writeOutFile(gridFSDBFile, f);
//				*/
//				
//			}
			
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
