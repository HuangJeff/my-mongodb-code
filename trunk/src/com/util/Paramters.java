package com.util;

import java.util.ArrayList;

/**
 * 記錄參數的對應
 * @author asus1
 */
public class Paramters {
	//固定資料區
	//DB
	/** MongoDB Name = tptrs */
	public static final String MONGO_DB = "tptrs";
	
	
	//變數區
	//DB
	public static final String mongo_ip = "mongo.IP";
	public static final String mongo_port = "mongo.Port";
	
	public static final String[] aryOfDB = {
		mongo_ip, mongo_port
	};
	
	//Account
	public static final String userName = "userName"; //帳號
	public static final String activity_id = "activity.id"; //活動ID
	
	public static final String[] aryOfAccount = {
		userName, activity_id
	};
	
	//Collections
	public static final String collection_name = "collection.name"; //Collection Name
	public static final String data_type = "data.type"; //資料型態(A/B)
	
	public static final String[] aryOfCollections = {
		collection_name, data_type
	};
	
	//CRUD
	public static final String saveDataRange = "saveDataRange"; //資料條件(保留多久資料)(單位:月)
	public static final String crud_action = "CRUD"; //對Collection的動作(CRUD)
	
	public static final String[] aryOfCRUD = {
		saveDataRange, crud_action
	};
	
	
	//必填欄位
	public static ArrayList<String> getRequiredField() {
		ArrayList<String> rtnList = new ArrayList<String>();
		rtnList.add(mongo_ip);
		rtnList.add(userName);
		rtnList.add(activity_id);
		return rtnList;
	}
	
	//所有欄位
	public static ArrayList<String> getAllField() {
		ArrayList<String> rtnList = new ArrayList<String>();
		for(String item : aryOfDB) {
			rtnList.add(item);
		}
		for(String item : aryOfAccount) {
			rtnList.add(item);
		}
		for(String item : aryOfCollections) {
			rtnList.add(item);
		}
		for(String item : aryOfCRUD) {
			rtnList.add(item);
		}
		return rtnList;
	}
	
	
}
