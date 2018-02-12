package com.helper;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;

import net.sf.json.JSONObject;

public class SchemaHelper {

	public static Schema getSchema(String tableName) throws Exception {
		String SchemaResult = HttpHelper.httpGet("http://10.128.74.83:8081/subjects/" + tableName + "/versions/latest");
		String schemaString = (String) JSONObject.fromObject(SchemaResult).get("schema");
		Schema schema = new Parser().parse(schemaString);
		return schema;
	}

}
