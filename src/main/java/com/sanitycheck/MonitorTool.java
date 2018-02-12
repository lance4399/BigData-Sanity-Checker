package com.sanitycheck;

import java.util.Timer;
import java.util.TimerTask;
import org.apache.log4j.Logger;

import com.helper.KafkaAvroConsumerHelper;
import com.helper.MessageHelper;

/**
 * @author liangxi.lance 2018-01-20 14:00
 */
public class MonitorTool {
	private static final Logger logger = Logger.getLogger(MonitorTool.class);

	public static void main(String[] args) throws Exception {

		 new MonitorTool().startMonitor();
	}
 
	public void startMonitor() throws Exception {
		logger.info("MonitorTool starts working now...");
		MessageHelper msgHelper = new MessageHelper();	
		KafkaAvroConsumerHelper.start();
		new Timer().scheduleAtFixedRate(new TimerTask() {
			@Override
			public void run() {
				try {
						new TaskExecutor(msgHelper.sendMessage()).executeTasks();	
				} catch (Exception e) {
					e.getStackTrace();
				}
			}
		}, 0, 10000);
		
	}	
}
