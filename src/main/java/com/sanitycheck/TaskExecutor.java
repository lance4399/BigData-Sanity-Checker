package com.sanitycheck;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.log4j.Logger;

import com.helper.ConfigHelper;

public class TaskExecutor {
	private static  Set<String> msgs; //messageHelper message set
	private static final Logger logger = Logger.getLogger(TaskExecutor.class);
	
	private  List<BasicQueryTool> taskList;
	private  File fileName;
	public TaskExecutor(Set<String> msgs) {
		TaskExecutor.msgs = msgs;
	}

//	 public static void main(String[] args) throws Exception {
//	 uuid = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss SSS").format(new Date());
//	 new TaskExecutor(uuid).executeTasks();
//	 }

	public void executeTasks() throws Exception {
		long startTime = System.currentTimeMillis();
		Map<String,String> map = new HashMap<String,String>();
		for(String msg: msgs){
			map.put(msg.split("-")[0], msg);
		}
//		System.out.println("the map keySet() is : "+ map.keySet());
//		System.out.println("the size of keySet is: "+map.keySet().size());
		
		QueryKafkaTool  kafkaTool=new QueryKafkaTool(map);
		QueryCassandraTool  cassandraTool=new QueryCassandraTool(map);
		QueryElasticsearchTool  elasticsearchTool=new QueryElasticsearchTool(map);
		QueryHBaseTool  hbaseTool=new QueryHBaseTool(map);
		taskList = new ArrayList<BasicQueryTool>();
		taskList.add(kafkaTool);
		taskList.add(cassandraTool);
		taskList.add(elasticsearchTool);
		taskList.add(hbaseTool);

//		System.out.println("Getting ready to execute tasks...");
		fileName = new File("/app/rt/rtc-processing/logs/rtc-sanity-check/errorLog.log");
		ExecutorService executorPool = Executors.newCachedThreadPool();
//		fileName = new File("C:/monitor_tool_error_log/error.txt");

		System.out.println("The sent message in MsgHelper is:" + msgs);
		for (BasicQueryTool task : taskList) {
			executorPool.execute(new Runnable() {
				@Override
				public void run() {
					try {
						int retryTime = 0;
						Set<String> uuids = task.execute();
						while ( uuids.size() > 0 && retryTime < ConfigHelper.ReconnectThreshold) {		
							Thread.sleep(retryTime * 1000 << 1);
							retryTime++;
							long queryCostTime = System.currentTimeMillis() - startTime;
							logger.info("Query Cost:" + queryCostTime + "ms, " + " could not get the "
									+ task.getClass().getName() + " result, reconnecting time:" + retryTime);
							uuids = task.execute();										
						}																
						if( uuids.size()>0 ){
							postExecute(task,fileName,uuids);
						}else{
							System.out.println("There is no lost msgs in "+task.getClass().getName());
						}
					} catch (Exception e) {
						logger.error(e.getStackTrace().toString());
						e.getStackTrace();
					}
				}
			});
		}
		executorPool.shutdown();
		while (true) {
			if (executorPool.isTerminated()) {
				long monitorTime = System.currentTimeMillis() - startTime;
				System.out.println("Monitor finished normally.The whole monitor time is: "
						+ String.valueOf(monitorTime + "ms."));
				break;
			}
		}
	}

	public static void postExecute(BasicQueryTool task, File fileName, Set<String> uuids) throws Exception {
		logger.info("Error log appending...");
//		System.out.println("Error log appending...");
		for (String uuid : uuids) {
			FileWriter writer = null;
			try {
				String content = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss SSS").format(new Date())
						+ " The lost message set of " + task.getClass().getName() + " is :" + uuid + "\n";
				writer = new FileWriter(fileName, true);
				// System.out.println("the lost message in
				// "+task.getClass().getName()+"is :"+map.getValue());
				writer.write(content);
			} catch (Exception e) {
			} finally {
				try {
					if (writer != null) {
						writer.close();
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}
