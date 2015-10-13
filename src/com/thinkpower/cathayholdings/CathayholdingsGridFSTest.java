/**
 * 
 */
package com.thinkpower.cathayholdings;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.mongodb.BasicDBList;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.gridfs.GridFS;

/**
 * 國泰-GridFS測試Code<br>
 * 1.DB Information-DB:gridFSDB。Collection:
 * 		pocfiles_meta(一般Collection)，
 * 		pocfiles.files、pocfiles.chunks(GridFS Collection)<br>
 * 2.args = [DB URL, IMG path, for-loops_numbers, insertAll]<br>
 * 3.args - insertAll(true/false):針對只塞pocfiles_meta一般Collection(false)還是
 * 			連同GridFS的Collections一起塞資料(true)。<br>
 * 		✪邏輯：資料來源都是IMG path的相關資訊，但為了配合RangeOfShardKey，所以_id與files_id都要經過設計。<br>
 * 		✪_id/files_id均為數值。shard1 = 0~5000。shard2 = 5000~9999(用Random來產生)<br>
 * 		✪當有搭配塞入GridFS時，圖檔的名稱順便加上Random出來的資料，以便視別and不易重覆<br>
 * 		✪pocfiles_meta欄位：_id、name、date<br>
 * 		✪當塞完資料時，驗証資料有塞對的Shard(檢查sh.status()中的Tag與資料是否相同)<br>
 * @author jeff
 * @date 2015/10/05
 */
public class CathayholdingsGridFSTest {
	private static Mongo mongo = null;
	private static DB db = null;
	private static DBCollection collection = null;
	private static GridFS gridfs = null;
	
	private static String dbName = "gridFSTest";
	private static String collectionName = "pocfiles_meta";	//mongodb meta collection name
	private static String gridfsName = "pocfiles";	//gridfs collection name
	
	/**
	 * List 排列：IP、tags、ns、fields、min value、max value<br>
	 * 存放Shard and tags + IP<br>
	 */
	private static Map<String, List<String>> map = new HashMap<String, List<String>>();
	
	/**
	 * 
	 */
	public CathayholdingsGridFSTest() {
		
	}
	
	
	/**
	 * 當塞完資料時，驗証資料有塞對的Shard(檢查sh.status()中的Tag與資料是否相同)
	 */
	private static void collectCheckDataInShard() {
		try {
			//List 排列：IP、tags、ns、fields、min value、max value
			//存放Shard and tags + IP
			//Map<String, List<String>> map = new HashMap<String, List<String>>();
			
			db = mongo.getDB("admin");
			CommandResult result = db.command( "listShards" );
			System.out.println("OK is " + result.ok());
			if(result.ok()) {
				BasicDBList list = (BasicDBList)result.get("shards");
				for(int i=0;i<list.size();i++) {
					DBObject obj = (DBObject)list.get(i);
					//0=IP, 1=tags...
					List<String> _dataList = new ArrayList<String>();
					String _id = (String)obj.get("_id");
					String host = (String)obj.get("host");
					System.out.println("_id : " + obj.get("_id"));
					System.out.println("host : " + obj.get("host"));
					System.out.println("tags : " + obj.get("tags"));
					//解析host，取得其中一組的IP
					String[] ary1 = host.split("/");
					String _ip = ary1[1];
					if(_ip.indexOf(",") > -1) {
						_ip = _ip.split(",")[0];
					}
					System.out.println("_ip : " + _ip);
					//IP
					_dataList.add(_ip);
					
					BasicDBList aryOfStr = (BasicDBList)obj.get("tags");
					String obj2 = (String)aryOfStr.get(0); //測試--只有一組
					System.out.println("tags obj2 : " + obj2);
					//tags
					_dataList.add(obj2);
					//以tags當Key，這樣下面也比較好做
					//map.put(_id, _dataList);
					map.put(obj2, _dataList);
				}
				//上面取得IP與Tags，接下來從config DB中取得Collection、Tags and RangeOfShardKey以供判斷
				//存放Collection、Tags and RangeOfShardKey
				Map<String, List<String>> mapOfTags = new HashMap<String, List<String>>();
				db = mongo.getDB("config");
				DBCollection tagsColl = db.getCollection("tags");
				DBCursor cursor = tagsColl.find();
				while( cursor.hasNext() ){
					DBObject obj = cursor.next();
					String _ns = (String)obj.get("ns");
					String _tag = (String)obj.get("tag");
					DBObject _min = (DBObject)obj.get("min");
					DBObject _max = (DBObject)obj.get("max");
					String _field_id = "_id";
					double _min_dou = 0;
					double _max_dou = 0;
					if(_min.get(_field_id) == null && _max.get(_field_id) == null) {
						_field_id = "files_id";
						_min_dou = (double)_min.get(_field_id);
						_max_dou = (double)_max.get(_field_id);
					} else {
						_min_dou = (double)_min.get(_field_id);
						_max_dou = (double)_max.get(_field_id);
					}
					System.out.println("ns : " + _ns);
					System.out.println("tag : " + _tag);
					System.out.println("min : " + _min_dou);
					System.out.println("max : " + _max_dou);
					//List 排列：IP、tags、ns、fields、min value、max value
					List<String> _l = map.get(_tag);
					map.remove(_tag);
					_l.add(_ns); //ns
					_l.add(_field_id); //fields
					_l.add("" + _min_dou); //min value
					_l.add("" + _max_dou); //max value
					map.put(_tag, _l);
				}
			} else {
				System.err.println(result.getErrorMessage());
			}
		} catch(Exception e) {
			e.printStackTrace(System.err);
		}
	}
	
	/**
	 * 檢查
	 * @throws UnknownHostException 
	 */
	private static void doubleCheckDataInShard() {
		long t1 = System.currentTimeMillis();
		//Map<String, List<String>> map
		for(Iterator<Map.Entry<String, List<String>>> it = map.entrySet().iterator();it.hasNext();) {
			long t2 = System.currentTimeMillis();
			Map.Entry<String, List<String>> me = it.next();
			String key = me.getKey(); //tags
			List<String> value = me.getValue();
			//System.out.println("key is " + key);
			int _i = 0;
			String serverIP = null;
			String[] aryOfData = new String[5]; //存放：DB collection fields min max
			//組出要的設定檔
			for(int i=0;i<value.size();i++) {
				String item = value.get(i);
				if(i == 0) { //server IP
					serverIP = item;
				} else if(i == 1) { //tags
				} else { //collection fields min max (四組一遁環)
					if(_i <= 3) {
						if(_i == 0) { //db & collection
							//System.out.println("item = " + item);
							String[] aryOfTmp = item.split("\\.");
							//System.out.println("aryOfTmp = " + item.indexOf("."));
							aryOfData[0] = aryOfTmp[0];
							String _coll = "";
							for(int j=1;j<aryOfTmp.length;j++) {
								_coll += aryOfTmp[j];
								if(j < aryOfTmp.length - 1)
									_coll += ".";
							}
							aryOfData[1] = _coll;
							//System.out.println("DB:" + aryOfData[0] + " Collection:" + aryOfData[1]);
						} else if(_i == 1) { //fields
							aryOfData[2] = item;
						} else if(_i == 2) { //min
							aryOfData[3] = item;
						} else if(_i == 3) { //max
							aryOfData[4] = item;
						}
					}
					_i++;
					if(_i > 3)
						_i = 0;
				}
				System.out.print(item + "    ");
			}
			System.out.println();
			long t3 = System.currentTimeMillis();
			System.out.println("parse [" + key + "] Setting is " + (t3 - t2) + " ms.");
			//進行資料比對
			int totalRows = 0;	//總筆數
			int isRightRows = 0;//對的筆數
			int isFailRows = 0; //錯的筆數
			try {
				Mongo _mongo = new Mongo(serverIP);
				DB _db = _mongo.getDB(aryOfData[0]);
				DBCollection _collection = _db.getCollection(aryOfData[1]);
				DBCursor cursor = _collection.find();
				while( cursor.hasNext() ) {
					DBObject obj = cursor.next();
					String _data1 = obj.get(aryOfData[2]).toString();
					
					totalRows++;
					try {
						int min = ((int)Double.parseDouble(aryOfData[3]));
						int max = ((int)Double.parseDouble(aryOfData[4]));
						int beCheckedData = Integer.parseInt(_data1);
						//System.out.println("beCheckedData = " + beCheckedData + " min~max:" + min + " ~ " + max);
						if(beCheckedData >= min && beCheckedData < max) {
							isRightRows++;
						} else {
							isFailRows++;
						}
					} catch(Exception e) {
						e.printStackTrace(System.err);
						isFailRows++;
					}
				}
				System.out.println();
				System.out.println("DB [" + serverIP + "] Tags [" + key + "] "
						+ "比對條件：min ~ max:" + aryOfData[3] + " ~ " + aryOfData[4]
						+ ". 總筆數： " + totalRows + " 筆. "
						+ "符合條件的共： " + isRightRows + " 筆. 未符合條件的共： " + isFailRows + " 筆.");
				
			} catch(UnknownHostException uhe) {
				uhe.printStackTrace(System.err);
			} catch(Exception e) {
				e.printStackTrace(System.err);
			}
		}
		System.out.println("doubleCheckDataInShard Total Time : " + (System.currentTimeMillis() - t1) + " ms.");
	}
	
	/**
	 * 
	 * @param args [DB URL, IMG path, for-loops_numbers, insertAll]
	 */
	public static void main(String[] args) {
		try {
			//1.取得參數[DB URL, IMG path, for-loops_numbers, insertAll]
			if(args.length != 4) {
				throw new Exception("參數個數須為4個[DB URL, IMG path, for-loops_numbers, insertAll]！");
			}
			long s1 = System.currentTimeMillis();
			String url = args[0];
			String filePath = args[1];
			int forLoops = Integer.parseInt(args[2]);
			boolean insertAll = Boolean.parseBoolean(args[3]);
			
			System.out.println("filePath is " + filePath + "  forLoops is " + forLoops);
			System.out.println("DB URL is " + url);
			try {
				//mongo = new Mongo("192.168.1.103", 27027);
				mongo = new Mongo(url);
			} catch (UnknownHostException e) {
				e.printStackTrace();
				throw e;
			}
			db = mongo.getDB(dbName);
			
			long s2 = System.currentTimeMillis();
			System.out.println("Connect DB [" + (s2 - s1) + " ms.]");
			
			
			
			//2._id/files_id均為數值。shard1 = 0~5000。shard2 = 5000~9999(用Random來產生)
			
			for(int i=0;i<10;i++) {
				double randomNum = Math.random();
				int ans = (int)(10000 * randomNum);
				System.out.println("randomNum = " + randomNum + " ans = " + ans);
			}
			
			
			int totalRows = 0;
			long successRows = 0;
			long failRows = 0;
			
			
			
			long s3 = System.currentTimeMillis();
			System.out.println("Total[" + totalRows + "]rows。Success[" + successRows + "]rows。"
					+ "Fails[" + failRows + "]rows。Time is [" + (s3 - s1) + " ms.]");
			//當塞完資料時，驗証資料有塞對的Shard(檢查sh.status()中的Tag與資料是否相同)
			collectCheckDataInShard();
			
			doubleCheckDataInShard();
			long s4 = System.currentTimeMillis();
			System.out.println("doubleCheckDataInShard Time : " + (s4 - s3));
		} catch(Exception e) {
			e.printStackTrace(System.err);
		}
	}
}
