package com.sanitycheck;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.helper.ConfigHelper;
import com.helper.HttpHelper;

import net.sf.json.JSONObject;

public class QueryElasticsearchTool implements BasicQueryTool {
	private Map<String, String> map;
	
	public QueryElasticsearchTool(Map<String, String> map) {
		this.map = map;
	}

	 public static void main(String[] args) throws Exception {
	 String uuid = "759d8bd3bad3402baa963b46442edb54";
	 Map<String, String> uus =new HashMap<String, String>();
	 uus.put(uuid,"");
	 //1a8d3e9dba6442689a42c3562b25e7af
	 System.out.println(new QueryElasticsearchTool(uus).execute());
	 }

	@Override
	public Set<String> execute() throws Exception {
		Set<String> result_uuids =new HashSet<String>();
//		System.out.println("QueryElasticsearchTool.......map:" +map);
		for(Map.Entry<String, String> entry : map.entrySet()){
			if(entry.getKey() != null){
				String url = "http://" + ConfigHelper.Elasticsearch_HOST + ":" + ConfigHelper.Elasticsearch_PORT + "/_sql?sql=";
				String sql = "select * from " + ConfigHelper.TABLE_NAME_rtc_sanity_check + " where uuid='" + entry.getKey() + "'";
				String result = HttpHelper.httpGet(url + URLEncoder.encode(sql, "UTF-8"));
				JSONObject jsonObject = (JSONObject) JSONObject.fromObject(result).get("hits");
				int num = (int) jsonObject.get("total");
				if (0 == num) {
					result_uuids.add(entry.getValue());
				}else if(0 != num && result_uuids.contains(entry.getValue())){
					result_uuids.remove(entry.getValue());
				}
			}
		}
//		System.out.println("the result_uuids of Elastisearch: "+result_uuids);
		return result_uuids;
	}
	
}