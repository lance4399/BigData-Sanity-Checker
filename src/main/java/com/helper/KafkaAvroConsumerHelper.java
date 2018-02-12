package com.helper;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;

import net.sf.json.JSONObject;

public class KafkaAvroConsumerHelper{
	private static final Logger logger = Logger.getLogger(KafkaAvroConsumerHelper.class);
	public static Map<String, String> map = new CacheHelper<String, String>();
	public static Map<String, SpecificDatumReader<GenericRecord>> datumMap = new HashMap<String, SpecificDatumReader<GenericRecord>>();
	
	public static void start() throws Exception{
		Properties props = new Properties();
		props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, ConfigHelper.Kafka_HOST + ":" + ConfigHelper.Kafka_PORT);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "test_lance20180125");
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10000);
		Schema schema = SchemaHelper.getSchema(ConfigHelper.TABLE_NAME_rtc_sanity_check);	
		Consumer<?, ?> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(ConfigHelper.TABLE_NAME_rtc_sanity_check));	
		new Thread(new Runnable() {
			@Override
			public void run() {			
				try {
					while (true) {
						processMessage(schema, consumer.poll(500));
					}
				} catch (ParseException e) {
					e.printStackTrace();
				} finally {
					consumer.close();
				}
				
			}
		}).start();
	}
 
	private static void processMessage(Schema schema, ConsumerRecords<?, ?> consumerRecords) throws ParseException {
		for (ConsumerRecord<?, ?> record : consumerRecords) {
			long timestamp = record.timestamp();
			String source = null;
			try {
				byte[] buffer = (byte[]) record.value();
				BinaryDecoder binaryEncoder = DecoderFactory.get().binaryDecoder(buffer, null);
				GenericRecord gr = null;
				SpecificDatumReader<GenericRecord> reader = null;
				if(null != datumMap.get("datumReader")){
					reader = datumMap.get("datumReader");
				}else{
					reader = new SpecificDatumReader<GenericRecord>(schema);
					datumMap.put("datumReader", reader);
				}				
				source = reader.read(gr, binaryEncoder).toString();
				JSONObject message;
				message = JSONObject.fromObject(source);
//				System.out.println("The message in KafkaAvroConsumerHelper is: " + message);
				logger.info("The message in KafkaAvroConsumerHelper is: " + message);
				String record_timeJsonElement = (String) message.get("record_time");
				Calendar calendar = Calendar.getInstance();
				calendar.setTime(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS").parse(record_timeJsonElement));
				long record_time = calendar.getTimeInMillis();
				long latency = timestamp - record_time;
				message.put("latency", latency);
				map.put(message.getString("uuid"), message.toString());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

//	public Map<String, JSONObject> getKafkaCache() {
//		return map;
//	}
	
}