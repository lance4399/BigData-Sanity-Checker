package com.sanitycheck;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.helper.ConfigHelper;

public class QueryHBaseTool implements BasicQueryTool {
	private Map<String,String> map;
	private static Configuration conf;
	
	public QueryHBaseTool(Map<String,String> map) {
		this.map = map;
	}

	static {
		conf = HBaseConfiguration.create();
		conf.set(ConfigHelper.HBase_Zookeeper_QUORUM, ConfigHelper.HBase_Zookeeper_QUORUM_HOST);
		conf.set(ConfigHelper.HBase_Zookeeper_CLIENTPORT, ConfigHelper.HBase_ZOOKEEPER_PORT);
	}

	public static void main(String[] args) throws Exception {
		 Map<String, String> uuids =new HashMap<String, String>();
		 uuids.put(" ","");
		System.out.println(new QueryHBaseTool(uuids).execute());
	}

	@Override
	public Set<String> execute() throws Exception {
		Set<String> result_uuids =new HashSet<String>();
		HBaseAdmin admin = new HBaseAdmin(conf);
		HTable table = null;
		boolean tableFlag = admin.tableExists(ConfigHelper.TABLE_NAME_rtc_sanity_check);
		if (tableFlag) {
			table = new HTable(conf, ConfigHelper.TABLE_NAME_rtc_sanity_check);
//			System.out.println("QueryHBaseTool.......map:" +map);

			for(Map.Entry<String, String> entry : map.entrySet()){
				Get get = new Get(Bytes.toBytes(entry.getKey()));

				Result result = table.get(get);
			// KeyValue[] kv = result.raw(); long hbase_timestamp = 0;
			// for(KeyValue v :kv){
			// hbase_timestamp = v.getTimestamp();
			// }
				if (null == result.getRow()) {
					result_uuids.add(entry.getValue());
				}else if(null != result.getRow() && result_uuids.contains(entry.getValue())){
					result_uuids.remove(entry.getValue());
				}
			}
					
			table.close();
			admin.close();
//			System.out.println("the result_uuids of HBase: "+result_uuids);
			return result_uuids;
		} else {
			admin.close();
			return result_uuids;
		}
	}

}
