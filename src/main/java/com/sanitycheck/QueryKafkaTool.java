package com.sanitycheck;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.log4j.Logger;

import com.facebook.presto.jdbc.internal.guava.collect.ImmutableMap;
import com.facebook.presto.jdbc.internal.guava.collect.MapDifference;
import com.facebook.presto.jdbc.internal.guava.collect.Maps;
import com.helper.ConfigHelper;
import com.helper.KafkaAvroConsumerHelper;

import net.sf.json.JSONObject;

public class QueryKafkaTool implements BasicQueryTool {
	private static final Logger logger = Logger.getLogger(QueryKafkaTool.class);
//	private Set<String> uuids;
	private Map<String, String> map;
//	private Map<String, JSONObject> maps;
	public QueryKafkaTool(Map<String, String> map) {
		this.map = map;
	}
	
	@Override
	public Set<String> execute() throws Exception {
		Set<String> result_uuids = new HashSet<String>();
		Map<String, String> kafkaMap=ImmutableMap.copyOf(KafkaAvroConsumerHelper.map);
		Map<String, String> msgMap=ImmutableMap.copyOf(map);
		MapDifference<String, String> diffHadle=Maps.difference(msgMap,kafkaMap); 
		Map<String, String> commonMap=diffHadle.entriesInCommon();
//		System.out.println("QueryKafkaTool.......map:" +map);
		if( commonMap.entrySet().size()== 0 ){
			insertInto_rtc_import_latency(kafkaMap);
		}else{
			for(Map.Entry<String, String> i: commonMap.entrySet()){
				if(result_uuids.contains(i.getValue())){
					result_uuids.remove(i.getValue());
				}
				result_uuids.add(i.getValue());
			}
		}
//		System.out.println("the result_uuids of Kafka: "+result_uuids);
		return result_uuids;
	}

	private void insertInto_rtc_import_latency(Map<String, String> maps) throws Exception {
		for(Map.Entry<String, String> entry :maps.entrySet()){
			String source  = entry.getValue();
			JSONObject amessage  = JSONObject.fromObject(source);
			String auuid = (String) amessage.get("uuid");
			String ip = (String) amessage.get("ip");
			int latency = (int) amessage.get("latency");
			String event_time = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss,SSSZ").format(new Date());
			String content = ConfigHelper.TABLE_NAME_rtc_import_latency + "|" + auuid + "|" + ip + "|" + latency + "|"
					+ event_time + "\n"; 
			Socket s = null;
			PrintWriter pw = null;
			try {
				s = new Socket(ip, Integer.parseInt(ConfigHelper.Flume_PORT));
				pw = new PrintWriter(s.getOutputStream(), true);
				pw.write(content);
			} catch (Exception e) {
				logger.error(content + " failed  to insert into table rtc_import_latency!");
				e.printStackTrace();
			} finally {
				pw.flush();
				pw.close();
				s.close();
			}
			logger.info(content + " sucessfully written into table rtc_import_latency!");
		}
		
	}

//	public static void insertInto_rtc_import_latency(JSONObject amessage,String flumeIp) throws Exception {
//		String auuid = (String) amessage.get("uuid");
//		String ip = (String) amessage.get("ip");
//		int latency = (int) amessage.get("latency");
//		String event_time = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss,SSSZ").format(new Date());
//		String content = ConfigHelper.TABLE_NAME_rtc_import_latency + "|" + auuid + "|" + ip + "|" + latency + "|"
//				+ event_time + "\n"; 
//		Socket s = null;
//		PrintWriter pw = null;
//		try {
//			s = new Socket(flumeIp, Integer.parseInt(ConfigHelper.Flume_PORT));
//			pw = new PrintWriter(s.getOutputStream(), true);
//			pw.write(content);
//		} catch (Exception e) {
//			logger.error(content + " failed  to insert into table rtc_import_latency!");
//			e.printStackTrace();
//		} finally {
//			pw.flush();
//			pw.close();
//			s.close();
//		}
//		logger.info(content + " sucessfully written into table rtc_import_latency!");
////		System.out.println(content + " sucessfully written into table rtc_import_latency!");
//	}

}