/**
 * 
 */
package com.mimport;

import java.util.concurrent.TimeoutException;

/**
 * @author asus1
 *
 */
public class ImportData1 {
	String mongohome = null; //mongo.home
    String host = null; //import mongo.host
    String port = null; //import mongo.port
    String db = null; //import mongo.db
	private void initConfig() {
		mongohome = "D:/trs-standalone/mongodb-win32-x86_64-2.2.2"; //mongo.home
	    //host = "localhost"; //import mongo.host
	    host = "192.168.1.69";
	    port = null; //import mongo.port
	    db = "tptrs"; //import mongo.db
	}
	
	/**
	 * 
	 */
	public ImportData1() throws Exception {
		this.initConfig();
		
		this.doWork01();
	}
	
	/**
	 * 將JSON格式的資料匯入"檔名"的Collection中
	 * @throws Exception
	 */
	private void doWork01() throws Exception {
		String collection = "Activity_root_A_1392359197344";
	    String inputlocation = "D:/mongoExportData/Activity_root_A_1392359197344.txt"; //needs to be asigned a file name
	    
	    String command = String.format(mongohome + "/bin/mongoimport " +
	            "--host %s " +
	            //"--port %s " +
	            "--db %s " +  
	            "--collection %s " +  
	            //"--fields _id,name,note,sDate,eDate " +  
	            "%s " +  
	            "-vvvvv",
	            //host,port,db,collection,outputlocation);
	            host,db,collection,inputlocation);
	    
	    //logger.info(command);
	    System.out.println(command);
	    try{            
	            Runtime rt = Runtime.getRuntime();              
	            Process pr = rt.exec(command);
	            //StreamGobbler errorGobbler = new StreamGobbler(pr.getErrorStream(),"ERROR",logger);
	            //StreamGobbler outputGobbler = new StreamGobbler(pr.getInputStream(),"OUTPUT",logger);
	            //errorGobbler.start();
	            //outputGobbler.start();
	            
	            //int exitVal = pr.waitFor();
	            int exitVal = -1;
	            long timeout = 30000; //long millis
	            Worker worker = new Worker(pr);
	            worker.start();
	            try {
	            	worker.join(timeout);
	            	if(worker.exit != null)
	            		exitVal = worker.exit;
	            	else
	            		//throw new TimeoutException();
	            		System.out.println("exitVal = " + exitVal + " worker = " + worker);
	            	System.out.println("exitVal = " + exitVal + " worker.exit = " + worker.exit + " " + pr.exitValue());
	            } catch(InterruptedException ex) {
	            	worker.interrupt();
	                Thread.currentThread().interrupt();
	                throw ex;
	            } finally {
	            	pr.destroy();
	            	//pr.waitFor(timeout, unit);
	            }
	            
	            
	            //logger.info(String.format("Process executed with exit code %d",exitVal));
	            System.out.println(String.format("Process executed with exit code %d", exitVal));
	    }catch(Exception e){
	        //logger.error(String.format("Error running task. Exception %s", e.toString()));
	    	throw e;
	    }
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			new ImportData1();
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	private static class Worker extends Thread {
		private final Process process;
		private Integer exit;
		
		private Worker(Process process) {
			this.process = process;
		}
		@Override
		public void run() {
			try {
				exit = process.waitFor();
			} catch(InterruptedException ignore) {
				return;
			}
		}
	}
}
