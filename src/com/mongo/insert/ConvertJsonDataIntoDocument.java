/**
 * 
 */
package com.mongo.insert;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Map;
import java.util.TreeMap;

import javax.json.Json;
import javax.json.stream.JsonParser;
import javax.json.stream.JsonParser.Event;

/**
 * 取得JSON格式的Data，並塞入MongoDB中。
 * @author jeff
 */
public class ConvertJsonDataIntoDocument {
	private String path = "Data/";
	private File file = null;
	
	private String mongohome = null; //mongo.home
	private String host = null; //mongo.dumphost
	private int port = 0; //mongo.dumpport
	private String db_name = null; //mongo.dumpdb
	
	private Map<String, ?> tm = null; //存放資料集
	
	/**
	 * Constructor<br>
	 * 1.init File<br>
	 * 2.init MongoDB Setting<br>
	 * @param assignFileName 指定要處理文件名(完整檔名[包含副檔名])
	 */
	public ConvertJsonDataIntoDocument(String assignFileName) {
		//取得對應檔案
		File fileRoot = new File(path);
		File[] fileList = fileRoot.listFiles();
		for(File f : fileList) {
			System.out.println(f.getName());
			if(f.getName().equals(assignFileName)) {
				file = f;
				break;
			}
		}
		
		//初始化MongoDB Setting
		this.initConfig();
	}
	
	private void initConfig() {
		mongohome = "C:/trs-standalone/mongodb-win32-x86_64-2.2.2"; //mongo.home
	    //host = "192.168.1.69"; //mongo.dumphost
	    //host = "192.168.1.107"; //window's mongo.host
		host = "localhost"; //window's mongo.host
		//host = "192.168.1.123";
	    port = 27017; //mongo.dumpport
	    db_name = "Sample"; //mongo.dumpdb
	}
	
	/**
	 * 解析JSON Format Data
	 * @throws FileNotFoundException 
	 */
	public void convertJsonData() throws FileNotFoundException {
		if(file == null) {
			return;
		}
		tm = new TreeMap<String, Object>();
		
		InputStream fis = new FileInputStream(file);
		
		JsonParser jsonParser = Json.createParser(fis);
		
		if (jsonParser.hasNext()) {
			Event event = jsonParser.next();
			System.out.println(event);
			event = jsonParser.next();
			System.out.println(event);
			event = jsonParser.next();
			System.out.println(event);
			event = jsonParser.next();
			System.out.println(event);
			event = jsonParser.next();
			System.out.println(event);
			event = jsonParser.next();
			System.out.println(event);
			event = jsonParser.next();
			System.out.println(event);
			event = jsonParser.next();
			System.out.println(event);
			event = jsonParser.next();
			System.out.println(event);
			event = jsonParser.next();
			System.out.println(event);
			event = jsonParser.next();
			System.out.println(event);
			event = jsonParser.next();
			System.out.println(event);
			event = jsonParser.next();
			System.out.println(event);
			event = jsonParser.next();
			System.out.println(event);
			event = jsonParser.next();
			System.out.println(event);
			event = jsonParser.next();
			System.out.println(event);
		}
	}
	
	
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String fileName = "zips.json";
		try {
			ConvertJsonDataIntoDocument convertObj = new ConvertJsonDataIntoDocument(fileName);
			
			convertObj.convertJsonData();
			
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
