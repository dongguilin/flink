package org.example.flink._1_9.ddl.watermark

import org.example.flink._1_9.streamEnv

/**
 * 扩展create table语法，支持watermark
 *
 * {@link Demo_Table_Watermark}
 */
object Demo_Sql_Watermark {

  def main(args: Array[String]): Unit = {
    val sourceTopic = "login"
    val sinkTopic = "test"

    val (sEnv, tEnv) = streamEnv

    /**
     * create table (source)
     *
     * 定义watermark
     *
     * 数据:  source/login.csv
     */
    tEnv.sqlUpdate(
      s"""
         |CREATE TABLE kafka_source_table (
         |    log_id                   VARCHAR,
         |    event_time               TIMESTAMP(3),
         |    user_no                  VARCHAR,
         |    real_ip                  VARCHAR,
         |    login_status             BOOLEAN,
         |    login_code               VARCHAR,
         |    WATERMARK FOR event_time AS event_time - INTERVAL  '30' SECOND
         |) WITH (
         |  'connector.type' = 'kafka',
         |  'connector.version' = '0.11',
         |  'connector.topic' = '${sourceTopic}',
         |  'connector.properties.0.key' = 'bootstrap.servers',
         |  'connector.properties.0.value' = 'localhost:9092',
         |  'connector.properties.1.key' = 'group.id',
         |  'connector.properties.1.value' = 'testGroup',
         |  'connector.startup-mode' = 'earliest-offset',
         |
         |  'format.type' = 'csv',
         |  'format.derive-schema' = 'true',        -- or use the table's schema
         |
         |  'update-mode' = 'append'
         |
         |)
         |""".stripMargin)

    /**
     * create table (sink)
     */
    tEnv.sqlUpdate(
      s"""
         |CREATE TABLE kafka_sink_table (
         |    hop_start                      TIMESTAMP(3),
         |    hop_end                        TIMESTAMP(3),
         |    user_no                        VARCHAR,
         |    error_times                    BIGINT
         |) WITH (
         |  'connector.type' = 'kafka',
         |  'connector.version' = '0.11',
         |  'connector.topic' = '${sinkTopic}',
         |  'connector.properties.0.key' = 'bootstrap.servers',
         |  'connector.properties.0.value' = 'localhost:9092',
         |
         |  'format.type' = 'csv',
         |  'format.derive-schema' = 'true',        -- or use the table's schema
         |
         |  'update-mode' = 'append'
         |)
         |""".stripMargin)

    /**
     * rich insert
     */
    tEnv.sqlUpdate(
      """
        |INSERT INTO kafka_sink_table
        |SELECT
        |   HOP_START(event_time, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE) AS hop_start,
        |   HOP_END(event_time, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE) AS hop_end,
        |   user_no,
        |   COUNT(*) AS error_times
        |FROM
        |   kafka_source_table
        |WHERE
        |   login_status IS FALSE
        |GROUP BY
        |   HOP(event_time, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE),
        |   user_no
        |""".stripMargin)

    println(sEnv.getExecutionPlan)

    tEnv.execute(this.getClass.getName)

  }

}
