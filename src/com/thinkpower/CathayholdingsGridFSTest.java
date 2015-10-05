/**
 * 
 */
package com.thinkpower;

/**
 * 國泰-GridFS測試Code<br>
 * 1.DB Information-DB:gridFSDB。Collection:
 * 		pocfiles_meta(一般Collection)，
 * 		pocfiles.files、pocfiles.chunks(GridFS Collection)<br>
 * 2.args = [DB URL, IMG path, for-loops_numbers, insertAll]<br>
 * 3.args - insertAll(true/false):針對只塞pocfiles_meta一般Collection(false)還是
 * 			連同GridFS的Collections一起塞資料(true)。<br>
 * 		邏輯：資料來源都是IMG path的相關資訊，但為了配合RangeOfShardKey，所以_id與files_id都要經過設計。<br>
 * 		_id/files_id均為數值。shard1 = 0~1500。shard2 = 1500~3000(用Random來產生)<br>
 * 		當有搭配塞入GridFS時，圖檔的名稱順便加上Random出來的資料，以便視別and不易重覆<br>
 * 		pocfiles_meta欄位：_id、name、date<br>
 * @author jeff
 * @date 2015/10/05
 */
public class CathayholdingsGridFSTest {
	
	/**
	 * 
	 */
	public CathayholdingsGridFSTest() {
		
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
	}
}
