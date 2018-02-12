package com.sanitycheck;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import com.facebook.presto.jdbc.PrestoResultSet;
import com.helper.ConfigHelper;

public class QueryCassandraTool implements BasicQueryTool {
	private Map<String,String> map;
	
	public QueryCassandraTool(Map<String,String> map) {
		this.map = map;
	}

	@Override
	public Set<String> execute() throws Exception {
//		CopyOnWriteArraySet<String > set = new 
		Set<String> result_uuids =new HashSet<String>();
//		System.out.println("QueryCassandraTool.......map:" +map);
		for(Map.Entry<String, String> entry : map.entrySet()){
			Connection connection = null;
			Statement stmt = null ;
			try {
				Class.forName(ConfigHelper.Presto_Driver);
				connection = DriverManager.getConnection("jdbc:presto://" + ConfigHelper.Cassandra_Presto_HOST + ":"
						+ ConfigHelper.Cassandra_Presto_PORT + "/cassandra/rtc", ConfigHelper.TABLE_NAME_rtc_sanity_check,
						null);
				stmt = connection.createStatement();
				String sql = "select * from " + ConfigHelper.TABLE_NAME_rtc_sanity_check + " where uuid='" + entry.getKey() + "'";
				PrestoResultSet rs = (PrestoResultSet) stmt.executeQuery(sql);
	
				if (!rs.next()) {
					result_uuids.add(entry.getValue());
				}else if( rs.next() && result_uuids.contains(entry.getValue())){
					result_uuids.remove(entry.getValue());
				}
				stmt.close();
				connection.close();
			} catch (Exception e) {
				System.out.println(e);
			}finally{
				stmt.close();
				connection.close();
			}
		}
//		System.out.println("the result_uuids of Cassandra: "+result_uuids);
		return result_uuids;
	}
	
}
