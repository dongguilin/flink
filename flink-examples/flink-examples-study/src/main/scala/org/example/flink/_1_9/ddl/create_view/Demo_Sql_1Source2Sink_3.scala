package org.example.flink._1_9.ddl.create_view

import org.example.flink._1_9.streamEnv

import java.io.File
import java.util.UUID

/**
 * TODO ? 存在的问题: 多sink同source，source没有得到共用
 *
 * <p>作业计划</p>
 * kafka_source_table (watermark)
 * ......=> wide_view
 * .............=> file_sink_view (where user_no <> '0001')
 * ....................=> file_sink_table
 * .............=> kafka_sink_view (hop window)
 * ....................=> kafka_sink_table
 *
 * <p>最终执行计划</p>
 * Task2: kafka_source_table ==> wide_view ==> file_sink_view ==> file_sink_table
 * Task1: kafka_source_table ==> wide_view ==> kafka_sink_view ==> kafka_sink_table
 */
object Demo_Sql_1Source2Sink_3 {

  def main(args: Array[String]): Unit = {
    val sourceTopic = "login"
    val sinkTopic = "test"

    val outFile = System.getProperty("java.io.tmpdir") + File.separator + UUID.randomUUID().toString + ".csv"
    println(outFile)

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
        |    user_no,
        |    real_ip,
        |    IF(login_status, '成功', '失败') AS login_status
        |FROM
        |   kafka_source_table
        |""".stripMargin)

    /**
     * create view
     *
     * wide_view -> file_sink_view
     */
    tEnv.sqlUpdate(
      """
        |CREATE VIEW file_sink_view AS
        |SELECT
        |    log_id,
        |    event_time,
        |    user_no
        |FROM
        |   wide_view
        |WHERE
        |   user_no <> '0001'
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
        |   login_status,
        |   COUNT(*) AS countAll
        |FROM
        |   wide_view
        |WHERE
        |   user_no <> '0001'
        |GROUP BY
        |   HOP(event_time, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE),
        |   login_status
        |""".stripMargin)

    /**
     * create table (sink)
     *
     * file_sink_table
     */
    tEnv.sqlUpdate(
      s"""
         |CREATE TABLE file_sink_table (
         |    log_id                      VARCHAR,
         |    event_time                  TIMESTAMP(3),
         |    user_no                     VARCHAR
         |) WITH (
         |  'connector.type' = 'filesystem',
         |  'connector.path' = 'file://${outFile}',
         |
         |  'format.type' = 'csv',
         |  'format.fields.0.name' = 'log_id',
         |  'format.fields.0.type' = 'VARCHAR',
         |  'format.fields.1.name' = 'event_time',
         |  'format.fields.1.type' = 'TIMESTAMP',
         |  'format.fields.2.name' = 'user_no',
         |  'format.fields.2.type' = 'VARCHAR',
         |
         |  'update-mode' = 'append'
         |)
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
         |    login_status                   VARCHAR,
         |    countAll                       BIGINT
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
     * file_sink_view -> file_sink_table
     */
    tEnv.sqlUpdate(
      """
        |INSERT INTO file_sink_table
        |SELECT
        |   *
        |FROM
        |   file_sink_view
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
