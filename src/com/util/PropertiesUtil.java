/**
 * 
 */
package com.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * @author asus1
 *
 */
public class PropertiesUtil {
	private String errorMsg = null;
	
	private Map<String, String> propertyMap = null;
	
	/**
	 * 
	 */
	public PropertiesUtil() throws Exception {
		//載入設定資訊
		this.beCheckedProperty();
		
		
	}
	
	/**
	 * 取得設定資訊
	 * @return
	 * @throws Exception
	 */
	public boolean beCheckedProperty() throws Exception {
		Properties prop = null;
		try {
			prop = this.loadFile();
		} catch(FileNotFoundException fnfe) {
			fnfe.printStackTrace();
			throw fnfe;
		} catch(IOException ioe) {
			ioe.printStackTrace();
			throw ioe;
		}
		boolean flag = this.parserSetting(prop);
		System.out.println("====FOr Test Print====");
		for(Iterator<Map.Entry<String, String>> it = propertyMap.entrySet().iterator();it.hasNext();) {
			Map.Entry<String, String> me = it.next();
			System.out.println(me.getKey() + " --- " + me.getValue());
		}
		return flag;
	}
	
	/**
	 * 載入properties file，並解析設定值
	 * @throws Exception
	 */
	private Properties loadFile() throws FileNotFoundException, IOException {
		Properties mainProperties = new Properties();
	    FileInputStream file = null;
	    try {
		    String path = "./";
		    //測試用
		    path = "D:/9.tmp/TPRS/ForTestJAR/";
		    
		    String propertyName = "AppConfig.properties";
		    
		    path = path + propertyName;
		    System.out.println("path = " + path);
		    
		    file = new FileInputStream(path);
		    mainProperties.load(file);
	    } finally {
	    	if(file != null)
	    		file.close();
	    }
	    return mainProperties;
	}
	
	/**
	 * 解析 Properties Setting value
	 * @param mainProperties
	 * @return
	 */
	private boolean parserSetting(Properties mainProperties) {
		boolean flag = true;
		//檢查各屬性值未輸入的例外情形
	    /*System.out.println("=======" + mainProperties.getProperty("mongo.IP"));
	    System.out.println("=======" + mainProperties.getProperty("userName"));
	    System.out.println("=======" + mainProperties.getProperty("activity.id"));
	    System.out.println("=======" + mainProperties.getProperty("generalData.type"));
	    System.out.println("=======" + mainProperties.getProperty("saveDataRange"));*/
	    
		
	    
	    
	    //現在先當都有輸入--塞值
	    if(flag) {
	    	propertyMap = new HashMap<String, String>();
	    	for(String key : Paramters.getAllField()) {
	    		try {
	    			//把有設定的(File:AppConfig.properties)有的，才設進Map裡
	    			if(mainProperties.containsKey(key)) {
	    				propertyMap.put(key, mainProperties.getProperty(key));
	    			}
	    		} catch(Exception e) {
	    			System.err.println(e.getMessage());
	    		}
	    	}
	    }
	    return flag;
	}
	
	public String getErrorMsg() {
		return errorMsg;
	}
	
	public Map<String, String> getPropertyMap() {
		return propertyMap;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			new PropertiesUtil();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
