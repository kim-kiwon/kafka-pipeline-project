package com.pipeline.config;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;

public class ElasticSearchSinkConnectorConfig extends AbstractConfig {
	public static final String ES_CLUSTER_HOST = "es.host";
	private static final String ES_CLUSTER_HOST_DEFAULT_VALUE = "localhost";
	private static final String ES_CLUSTER_HOST_DOC = "ES 호스트 입력";

	public static final String ES_CLUSTER_PORT = "es.port";
	private static final String ES_CLUSTER_PORT_DEFAULT_VALUE = "9200";
	private static final String ES_CLUSTER_PORT_DOC = "ES 포트 입력";


	public static final String ES_INDEX = "es.index";
	private static final String ES_INDEX_DEFAULT_VALUE = "kafka-connector-index";
	private static final String ES_INDEX_DOC = "ES 인덱스 입력";

	public static ConfigDef CONFIG = new ConfigDef().define(ES_CLUSTER_HOST, Type.STRING, ES_CLUSTER_HOST_DEFAULT_VALUE, ConfigDef.Importance.HIGH, ES_CLUSTER_HOST_DOC)
		.define(ES_CLUSTER_PORT, Type.INT, ES_CLUSTER_PORT_DEFAULT_VALUE, ConfigDef.Importance.HIGH, ES_CLUSTER_PORT_DOC)
		.define(ES_INDEX, Type.STRING, ES_INDEX_DEFAULT_VALUE, ConfigDef.Importance.HIGH, ES_INDEX_DOC);

	public ElasticSearchSinkConnectorConfig(Map<String, String> props) {
		super(CONFIG, props);
	}
}
