package com.helper;
import java.text.SimpleDateFormat;
import java.util.*;
import net.sf.json.JSONObject;

public class CacheHelper<K,V> extends LinkedHashMap<K,V>{
	private static final long serialVersionUID = 1L;
	
	public CacheHelper(){
		super(16, 0.75f, true);  
	}

	@Override
	public boolean removeEldestEntry(Map.Entry<K, V> eldest) {
		long removeThreshold = 2 * 24 * 60 * 60 * 1000;//2 days
		try {
			String source = (String) eldest.getValue();	
			JSONObject message =JSONObject.fromObject(source);
			String record_timeJsonElement = (String) message.get("record_time");
			Calendar calendar = Calendar.getInstance();
			calendar.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS").parse(record_timeJsonElement) );
			long record_time = calendar.getTimeInMillis();
			if (System.currentTimeMillis() - record_time > removeThreshold) {
				return true;
			}
		} catch (Exception e) {

		}
		return false;
	}

}
