package com.helper;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.apache.log4j.Logger;

public class MessageHelper {
	private static Logger logger = Logger.getLogger(MessageHelper.class);
	private int count = 1;
	private Set<String> msgList;
	// public static void main(String[] args) throws Exception {
	//     new MessageHelper().sendMessage();
	// }
	
	// rtc_sanity_check|uuid|record_time|event_time|ip
	public Set<String> sendMessage() throws Exception {
		Socket s = null;
		PrintWriter pw = null;
		msgList = new HashSet<String>();
		String flumeHost=ConfigHelper.Flume_HOST;
		System.out.println(flumeHost);
		String[] flumeList =null;
		if(flumeHost.contains(",")){
			flumeList= ConfigHelper.Flume_HOST.split(",");
//			System.out.println("the msg list are: ");
			for(String i: flumeList){
				System.out.print(i+"	");
			}
			System.out.println("########################");
			for(int i=0;i<flumeList.length;i++){
				try {
					String uuid = UUID.randomUUID().toString().replace("-", "");
					long start = System.currentTimeMillis();
					s = new Socket(flumeList[i], Integer.parseInt(ConfigHelper.Flume_PORT));
					pw = new PrintWriter(s.getOutputStream(), true);	
					String content =new StringBuilder().append(ConfigHelper.TABLE_NAME_rtc_sanity_check).append("|").append(uuid).append("|")
							.append(new SimpleDateFormat("YYYY-MM-dd HH:mm:ss SSS").format(new Date())).append("|")
							.append(new SimpleDateFormat("YYYY-MM-dd HH:mm:ss").format(new Date())).append("|").append(flumeList[i]).append("\n").toString();
					pw.write(content);
					msgList.add(uuid+"-"+flumeList[i]);
					long end = System.currentTimeMillis() - start;
					logger.info("This is message "+ count++ +" of flume:" + flumeList[i]+ ".The content is: " + content + ".It took " + end + "ms.");
				} catch (Exception e) {
					e.printStackTrace();
				} finally {
					pw.flush();
					pw.close();
					s.close();
				}	
			}
//		}else{
//			int n = 0;
//			while (n < 5) {
//				try {
//					String uuid = UUID.randomUUID().toString().replace("-", "");
//					long start = System.currentTimeMillis();
//					s = new Socket(flumeHost, Integer.parseInt(ConfigHelper.Flume_PORT));
//					pw = new PrintWriter(s.getOutputStream(), true);
//					String content = new StringBuilder().append("rtc_sanity_check|").append(uuid).append("|")
//							.append(new SimpleDateFormat("YYYY-MM-dd HH:mm:ss SSS").format(new Date())).append("|")
//							.append(new SimpleDateFormat("YYYY-MM-dd HH:mm:ss").format(new Date())).append("|")
//							.append(flumeHost).append("\n").toString();
//					pw.write(content);
//					msgList.add(uuid+"-"+flumeHost);
//					long end = System.currentTimeMillis() - start;
//					logger.info("This is message " + count++ + " of flume:" + flumeHost + ".The content is: " + content
//							+ ".It took " + end + "ms.");
//					n++;
//				} catch (Exception e) {
//					e.printStackTrace();
//				} finally {
//					pw.flush();
//					pw.close();
//					s.close();
//				}
//			}
		}
//		System.out.println(msgList);
		return msgList;
	}
}
