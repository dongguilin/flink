package org.example.flink._1_9.ddl.create_view

import org.example.flink._1_9.streamEnv

import java.io.File
import java.util.UUID

/**
 *
 * TODO ? 存在的问题: 多sink同source，source没有得到共用
 *
 * <p>作业计划</p>
 * kafka_source_table (watermark)
 * ......=> wide_view (where user_no = '0001')
 * .............=> sink_view (hop window)
 * ....................=> file_sink_table
 * ....................=> kafka_sink_table
 *
 * <p>最终执行计划</p>
 * Task2: kafka_source_table ==> wide_view ==> sink_view ==> file_sink_table
 * Task1: kafka_source_table ==> wide_view ==> sink_view ==> kafka_sink_table
 *
 */
object Demo_Sql_1Source2Sink_2 {

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
        |    real_ip
        |FROM
        |   kafka_source_table
        |WHERE
        |   user_no = '0001'
        |""".stripMargin)

    /**
     * create view
     *
     * wide_view -> sink_view
     */
    tEnv.sqlUpdate(
      """
        |CREATE VIEW sink_view AS
        |SELECT
        |   HOP_START(event_time, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE) AS hop_start,
        |   HOP_END(event_time, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE) AS hop_end,
        |   real_ip,
        |   COUNT(*) AS countAll
        |FROM
        |   wide_view
        |GROUP BY
        |   HOP(event_time, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE),
        |   real_ip
        |""".stripMargin)

    /**
     * create table (sink)
     *
     * file_sink_table
     */
    tEnv.sqlUpdate(
      s"""
         |CREATE TABLE file_sink_table (
         |    hop_start                      TIMESTAMP(3),
         |    hop_end                        TIMESTAMP(3),
         |    real_ip                        VARCHAR,
         |    countAll                       BIGINT
         |) WITH (
         |  'connector.type' = 'filesystem',
         |  'connector.path' = 'file://${outFile}',
         |
         |  'format.type' = 'csv',
         |  'format.fields.0.name' = 'hop_start',
         |  'format.fields.0.type' = 'TIMESTAMP',
         |  'format.fields.1.name' = 'hop_end',
         |  'format.fields.1.type' = 'TIMESTAMP',
         |  'format.fields.2.name' = 'real_ip',
         |  'format.fields.2.type' = 'VARCHAR',
         |  'format.fields.3.name' = 'countAll',
         |  'format.fields.3.type' = 'BIGINT',
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
         |    real_ip                        VARCHAR,
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
     * sink_view -> file_sink_table
     */
    tEnv.sqlUpdate(
      """
        |INSERT INTO file_sink_table
        |SELECT
        |   *
        |FROM
        |   sink_view
        |""".stripMargin)

    /**
     * rich insert
     *
     * sink_view -> kafka_sink_table
     */
    tEnv.sqlUpdate(
      """
        |INSERT INTO kafka_sink_table
        |SELECT
        |   *
        |FROM
        |   sink_view
        |""".stripMargin)

    println(sEnv.getExecutionPlan)

    tEnv.execute(this.getClass.getName)

  }

}
