package com.chudichen.common.constant

/**
 * 常量类
 *
 * @author chudichen
 * @since 2020-11-19
 */
object PropertiesConstants {

  val CHUDICHEN = "CHUDICHEN"
  val KAFKA_BROKERS = "kafka.brokers"
  val DEFAULT_KAFKA_BROKERS = "localhost:9092"
  val KAFKA_ZOOKEEPER_CONNECT = "kafka.zookeeper.connect"
  val DEFAULT_KAFKA_ZOOKEEPER_CONNECT = "localhost:2181"
  val METRICS_TOPIC = "metrics.topic"
  val CONSUMER_FROM_TIME = "consumer.from.time"
  val KAFKA_GROUP_ID = "kafka.group.id"
  val DEFAULT_KAFKA_GROUP_ID = "chudichen"
  val STREAM_PARALLELISM = "stream.parallelism"
  val STREAM_SINK_PARALLELISM = "stream.sink.parallelism"
  val STREAM_DEFAULT_PARALLELISM = "stream.default.parallelism"
  val STREAM_CHECKPOINT_ENABLE = "stream.checkpoint.enable"
  val STREAM_CHECKPOINT_DIR = "stream.checkpoint.dir"
  val STREAM_CHECKPOINT_TYPE = "stream.checkpoint.type"
  val STREAM_CHECKPOINT_INTERVAL = "stream.checkpoint.interval"
  val PROPERTIES_FILE_NAME = "/application.properties"
  val CHECKPOINT_MEMORY = "memory"
  val CHECKPOINT_FS = "fs"
  val CHECKPOINT_ROCKETSDB = "rocksdb"

  // es config
  val ELASTICSEARCH_BULK_FLUSH_MAX_ACTIONS = "elasticsearch.bulk.flush.max.actions"
  val ELASTICSEARCH_HOSTS = "elasticsearch.hosts"

  // mysql
  val MYSQL_DATABASE = "mysql.database"
  val MYSQL_HOST = "mysql.host"
  val MYSQL_PASSWORD = "mysql.password"
  val MYSQL_PORT = "mysql.port"
  val MYSQL_USERNAME = "mysql.username"
}
