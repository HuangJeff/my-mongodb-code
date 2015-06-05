/**
 * 
 */
package com.mongo.insert;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;

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
	
	private DB db = null;
	private DBCollection collection = null;
	private String collectionName = null;
	
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
		
		//Create DB Object
		MongoClient mongoClient = null;
		try {
			mongoClient = new MongoClient(host, port);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		}
		db = mongoClient.getDB( db_name );
		collection = db.getCollection(collectionName);
	}
	
	private void initConfig() {
		mongohome = "C:/trs-standalone/mongodb-win32-x86_64-2.2.2"; //mongo.home
	    //host = "192.168.1.69"; //mongo.dumphost
	    //host = "192.168.1.107"; //window's mongo.host
		//host = "localhost"; //window's mongo.host
		host = "192.168.1.103";
	    port = 27017; //mongo.dumpport
	    db_name = "Sample"; //mongo.dumpdb
	    collectionName = "sampleCollection";//mongo collection name
	}
	
	/**
	 * 解析JSON Format Data
	 * @throws FileNotFoundException 
	 */
	public Map<String, Map<String, Object>> convertJsonData() throws FileNotFoundException, IOException {
		Map<String, Map<String, Object>> tm = null;
		if(file == null) {
			return tm;
		}
		//InputStream fis = null;
		BufferedReader br = null;
		JsonReader jsonReader = null;
		try {
			String inputStr = null;
			tm = new TreeMap<String, Map<String, Object>>();
			int countOfStObj = 0;	//記錄START_OBJECT個數
			long t1 = System.currentTimeMillis();
			//fis = new FileInputStream(file);
			br = new BufferedReader(new FileReader(file));
			long t2 = System.currentTimeMillis();
			/*
			 * 1.採do...while()是 br內一開始就有第一筆Data，如果用while loop的話，會lost第一筆Data(br + br.readLine())
			 * 2.透過JsonReader讀取Json Object，然後將Json內的欄位、Data一一呈列出來，塞入Map中
			 */
			do {
				try {
					//System.out.println(br.readLine());
					//System.out.println(br);
					//jsonReader = Json.createReader(fis);
					jsonReader = Json.createReader(br);
					
					JsonObject jsonObj = jsonReader.readObject();
					//System.out.println("jsonObj = " + jsonObj);
					
					//如果來源非Array，便會如下拋出Exception
					//javax.json.JsonException: Cannot read JSON array, found JSON object
					//JsonArray jsonAry = jsonReader.readArray();
					//System.out.println("jsonAry = " + jsonAry);
					
					Map<String, Object> subMap = null;
					String _idKey = null;
					for(Iterator<Map.Entry<String,JsonValue>> it = jsonObj.entrySet().iterator(); it.hasNext() ;) {
						Map.Entry<String,JsonValue> obj = it.next();
						String key = obj.getKey();
						JsonValue jv = obj.getValue();
						//System.out.println("key = " + key + " JsonValue.valueType = " + jv.getValueType() +
						//		" toString = " + jv.toString());
						if(subMap == null) {
							subMap = new HashMap<String, Object>();
						}
						if("_id".equals(key))
							_idKey = jv.toString();
						
						switch(jv.getValueType()) {
						case STRING:
							subMap.put(key, jv.toString());
							break;
						case ARRAY://TODO 作Array 20150602 jeff (JsonArray extends List/Collection...)
							//JsonArrayBuilder jab = Json.createArrayBuilder().add(jv);
							//JsonArray jsonAry = jab.build();
							//如上會在建一層Array去包(不對)，因為 JsonArray extends JsonValue
							JsonArray jsonAry = (JsonArray)jv;
							//System.out.println("jsonAry = " + jsonAry + " size = " + jsonAry.size());
							/*for(JsonValue tmp : jsonAry) {
								System.out.println("VT = " + tmp.getValueType() + " toString = " + tmp.toString());
							}*/
							//TODO Array中也有各種型態需要在加以判定
							subMap.put(key, jsonAry.toArray());
							break;
						case OBJECT:
							//subMap.put(key, jv.);
							//在看看OBJECT會發什麼東西，另外"日期"是否放此處？
							break;
						case NUMBER:
							JsonNumber jn = (JsonNumber)jv;
							Number number = 0;
							//if(".".indexOf(jv.toString()) > 0) //有小數點(浮點數)
							if(jn.isIntegral()) //JsonNumber有提供API:boolean isIntegral()
								number = jn.longValue();
							else
								number = jn.doubleValue();
							subMap.put(key, number);
							break;
						case TRUE:
							subMap.put(key, JsonValue.TRUE);
							break;
						case FALSE:
							subMap.put(key, JsonValue.FALSE);
							break;
						case NULL:
							subMap.put(key, JsonValue.NULL);
							break;
						default:
							System.err.println("Other is " + jv.getValueType());
							break;
						}
					}
					tm.put(_idKey, subMap);
					
					countOfStObj ++;
				} catch(javax.json.stream.JsonParsingException jpse) {
					System.err.println("JsonParsingException ====> Input String =" +
							inputStr + "=字串有錯(不是空字串，就是非JSon Type)。");
					//jpse.printStackTrace();
					//throw jpse;
				}
			} while((inputStr = br.readLine()) != null);
			long t3 = System.currentTimeMillis();
			/*while(jsonParser.hasNext()) {
				Event event = jsonParser.next();
				System.out.println(countOfStObj + " ==== " + event);
				if(event == Event.START_OBJECT) {
					countOfStObj ++;
				}
			}*/
			System.out.println("START_OBJECT counter = " + countOfStObj + " tm.size() = " + tm.size());
			System.out.println("初始化 BufferedReader(t2 - t1) = " + (t2 - t1) + " ms.");
			System.out.println("執行[" + countOfStObj + "筆]JSON Data(t3 - t2) = " + (t3 - t2) + " ms.");
		} finally {
			if(jsonReader != null)
				jsonReader.close();
			//if(fis != null)
			//	fis.close();
			if(br != null)
				br.close();
		}
		return tm;
	}
	
	/**
	 * Insert into MongoDB
	 * @param tm : Data Source
	 */
	public void insertIntoDB(Map<String, Map<String, Object>> tm) {
		System.out.println("listTmData tm.size() = " + tm.size());
		int breakInt = 0;
		for(Iterator<Map.Entry<String, Map<String, Object>>> it = tm.entrySet().iterator();
				it.hasNext();) {
			if(breakInt == 1) {
				break;
			}
			breakInt++;
			
			Map.Entry<String, Map<String, Object>> me = it.next();
			//String key = me.getKey(); //最外層的這個key無用
			Map<String, Object> value = me.getValue();
			//System.out.println("key = " + key + " value = " + value);
			
			//Create 要塞入DB的容器(這邊用簡單的Map)
			Map<String, Object> documentMap = new HashMap<String, Object>();
			//蒐集資料集
			for(Iterator<Map.Entry<String, Object>> it2 = value.entrySet().iterator();
					it2.hasNext();) {
				Map.Entry<String, Object> data = it2.next();
				String key2 = data.getKey();
				Object value2 = data.getValue();
				StringBuffer show = new StringBuffer();
				//id=loc,value=Object[]
				//System.out.println("type = " + value2.getClass().getSimpleName());
				//id=loc,value2 is Object[] (資料源頭沒有特別處理，只有轉成Array)
				//System.out.println("type = " + (value2 instanceof Object[]));
				if(value2 instanceof Object[]) {
					BasicDBList tmpList = new BasicDBList();
					Object[] aryOfValue2 = (Object[])value2;
					show.append("[");
					for(int i=0;i<aryOfValue2.length;i++) {
						Object item = aryOfValue2[i];
						show.append(item.toString());
						if(i < (aryOfValue2.length - 1))
							show.append(",");
						//db container object
						tmpList.add(item);
					}
					show.append("]");
					//db container object
					documentMap.put(key2, tmpList);
				} else {
					show.append( value2.toString() );
					//db container object
					documentMap.put(key2, value2);
				}
				System.out.print(key2 + ":" + show + ",");
			}
			System.out.println();
			//塞入DB
			collection.insert(new BasicDBObject(documentMap));
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String fileName = "zips.json";
		try {
			ConvertJsonDataIntoDocument convertObj = new ConvertJsonDataIntoDocument(fileName);
			
			Map<String, Map<String, Object>> tm = convertObj.convertJsonData();
			convertObj.insertIntoDB(tm);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
