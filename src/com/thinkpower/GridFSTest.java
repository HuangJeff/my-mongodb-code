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
	//static String filename = "001招攬訪問報告書暨生調表!@!00001510301.tif";	//要寫入的檔案名稱
	static String collectionName = "pocfiles_meta";	//mongodb meta collection name
	static String gridfsName = "pocfiles";	//gridfs collection name
	//static String filepath = "E:/" + filename;	//檔名路徑(範例是在E:/)
	//static File file = new File(filepath);
	
	/**
	 * 
	 */
	public GridFSTest() {
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
	
	//查詢檔案
	public static ByteArrayOutputStream getData(String filename) throws Exception{
		// This query fetches the files I need
		GridFSDBFile gridFSDBFile = gridfs.findOne(new BasicDBObject("filename",filename));
		
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		gridFSDBFile.writeTo(out);
		//System.out.println("2.取得DB檔案，filename=" + gridFSDBFile.getFilename());
		//System.out.println("");
		return out;
	}
	
	public static ByteArrayInputStream parse(ByteArrayOutputStream baos) throws Exception
    {
        ByteArrayInputStream swapStream = new ByteArrayInputStream(baos.toByteArray());
        return swapStream;
    }
	
	//移除檔案
	public static void deleteData(String filename) throws Exception{
		gridfs.remove(gridfs.findOne(filename));
		
		BasicDBObject info = new BasicDBObject();
        info.put("version", "1.0");
        info.put("filename", filename);
        
		collection.remove(info);
		
		System.out.println("4.DB測試資掉刪除成功!");
		
	}
	
	//把檔案寫入到GridFS
	public static void insertData(String filepath, File file) throws Exception{
 
		//
		// Store the file to MongoDB using GRIDFS
		//
		
		GridFSInputFile gfsFile = gridfs.createFile(file);
		gfsFile.setFilename(file.getName());
		gfsFile.save();
 
		//
		// Let's create a new JSON document with some "metadata" information on the download
		//
		BasicDBObject info = new BasicDBObject();
                info.put("version", "1.0");
                info.put("filename", file.getName());
                info.put("filepath", filepath);
 
        //
        // Let's store our document to MongoDB
        //
		collection.insert(info, WriteConcern.SAFE);
		//System.out.println("1.資料寫入成功!");
		//System.out.println("");
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			//取得參數
			if(args.length != 4) {
				throw new Exception("參數個數須為4個[FilePath forLoopNumber DBURL compareFlag]！");
			}
			long s1 = System.currentTimeMillis();
			
			String filePath = args[0];
			int forLoops = Integer.parseInt(args[1]);
			String url = args[2];
			boolean compareFlag = Boolean.parseBoolean(args[3]);
			
			System.out.println("filePath is " + filePath + "  forLoops is " + forLoops);
			System.out.println("DB URL is " + url);
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
			
			long s2 = System.currentTimeMillis();
			System.out.println("Connect DB [" + (s2 - s1) + " ms.]");
			
			long totalRows = 0;
			long successRows = 0;
			long failRows = 0;
			File _file = new File(filePath);
			//System.out.println("_file is " + _file.isDirectory());
			File[] aryOfFile = _file.listFiles();
			
			for(int i = 0;i < forLoops; i++) {
				for(File f : aryOfFile) {
					totalRows++;
					String filepath = f.getPath();
					String filename = f.getName();
					//System.out.println(i + " = " + filename + " " + filepath);
					try {
						//1.新增檔案
						insertData(filepath, f);
						
						//2.查詢資料
						ByteArrayOutputStream out = getData(filename);
						
						if(compareFlag) {
							//3.比對檔案
							fileCheck(out, f);
						}
						//4.刪除DB測試資料
						//deleteData(filename);
						
						successRows++;
					} catch(Exception e) {
						System.err.println(e.getMessage());
						failRows++;
					}
				}
				if(i == (forLoops / 2))
					System.out.println("Run " + i + " rows.Time is [" +
							(System.currentTimeMillis() - s1) + " ms.]");
			}
			
			long s3 = System.currentTimeMillis();
			System.out.println("Total[" + totalRows + "]rows。Success[" + successRows + "]rows。"
					+ "Fails[" + failRows + "]rows。Time is [" + (s3 - s1) + " ms.]");
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
