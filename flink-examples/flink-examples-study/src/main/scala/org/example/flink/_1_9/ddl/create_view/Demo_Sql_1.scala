package org.example.flink._1_9.ddl.create_view

import org.example.flink._1_9.streamEnv

/**
 *
 * <p>作业计划</p>
 * kafka_source_table (watermark)
 * ......=> wide_view (where login_status IS FALSE)
 * .............=> kafka_sink_view (hop window)
 * ....................=> kafka_sink_table
 *
 */
object Demo_Sql_1 {

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
     * create view
     *
     * kafka_source_table -> wide_view
     */
    tEnv.sqlUpdate(
      """
        |CREATE VIEW wide_view AS
        |SELECT
        |    log_id,
        |    event_time,
        |    user_no
        |FROM
        |   kafka_source_table
        |WHERE
        |   login_status IS FALSE
        |""".stripMargin)

    /**
     * create view
     *
     * wide_view -> kafka_sink_view
     */
    tEnv.sqlUpdate(
      """
        |CREATE VIEW kafka_sink_view AS
        |SELECT
        |   HOP_START(event_time, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE) AS hop_start,
        |   HOP_END(event_time, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE) AS hop_end,
        |   user_no,
        |   COUNT(*) AS error_times
        |FROM
        |   wide_view
        |GROUP BY
        |   HOP(event_time, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE),
        |   user_no
        |""".stripMargin)

    /**
     * create table (sink)
     *
     * kafka_sink_table
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
     *
     * kafka_sink_view -> kafka_sink_table
     */
    tEnv.sqlUpdate(
      """
        |INSERT INTO kafka_sink_table
        |SELECT
        |   *
        |FROM
        |   kafka_sink_view
        |""".stripMargin)

    println(sEnv.getExecutionPlan)

    tEnv.execute(this.getClass.getName)

  }

}
