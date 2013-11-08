package com.vipshop.flume;

import java.io.IOException;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import org.apache.flume.Context;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaUtil {
	private static final Logger log = LoggerFactory.getLogger(KafkaUtil.class);

	public static String getTopic(Context context) {
		return context.getString(KafkaConstants.CONFIG_TOPIC);
	}
	public static String getBatchSize(Context context) {
		return context.getString(KafkaConstants.CONFIG_BATCH_SIZE, "200");
	}

	public static String getZkConnect(Context context) {
		return context.getString(KafkaConstants.CONFIG_ZK_CONNECT);
	}
	public static String getGroup(Context context) {
		return context.getString(KafkaConstants.CONFIG_GROUP);
	}
	public static String getResetOffset(Context context) {
		return context.getString(KafkaConstants.CONFIG_RESET_OFFSET, "no");
	}
	public static String getZKTimeout(Context context) {
		return context.getString(KafkaConstants.CONFIG_ZK_TIMEOUT, "15000");
	}
	public static String getAutoCommitEnable(Context context) {
		return context.getString(KafkaConstants.CONFIG_AUTOCOMMIT_ENABLE, "false");
	}
	public static String getAutoOffsetReset(Context context) {
		return context.getString(KafkaConstants.CONFIG_AUTOOFFSET, "largest");
	}
	public static String getAutoCommitInterval(Context context) {
		return context.getString(KafkaConstants.CONFIG_AUTOCOMMIT_INTERVAL, "10000");
	}
	public static String getConsumerTimeout(Context context) {
		return context.getString(KafkaConstants.CONFIG_CONSUMER_TIMEOUT, "-1");
	}
	public static Producer<String, String> getProducer(Context context) {
		Producer<String, String> producer;
		Properties props = new Properties();
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("zk.connect", getZkConnect(context));
		props.put("producer.type", "async");
		props.put("batch.size", getBatchSize(context));
		props.put("zk.sessiontimeout.ms", "15000");
		props.put("queue.size", "1000000");

		producer = new Producer<String, String>(new ProducerConfig(props));
		log.debug("-----------return producer");
		return producer;
	}
	public static ConsumerConnector getConsumer(Context context) throws IOException, KeeperException, InterruptedException {
		Properties props = new Properties();
		props.put("zk.connect", getZkConnect(context));
		props.put("zk.sessiontimeout.ms", "60000");
		props.put("groupid", getGroup(context));
		props.put("autocommit.enable", getAutoCommitEnable(context));
		props.put("autocommit.interval.ms", getAutoCommitInterval(context));
		props.put("queuedchunks.max", "1000");
		props.put("batch.size", getBatchSize(context));
		props.put("autooffset.reset", getAutoOffsetReset(context));
		props.put("socket.buffersize", "10240000");
		props.put("socket.timeout.ms", "60000");
		props.put("consumer.timeout.ms", getConsumerTimeout(context));
		ConsumerConfig consumerConfig = new ConsumerConfig(props);
		ConsumerConnector consumer = Consumer.createJavaConsumerConnector(consumerConfig);
		log.debug("-----------return consumer");
		return consumer;
	}
}

