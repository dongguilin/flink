package org.example.flink._1_11.ddl.create_view

import org.apache.flink.table.api.ExplainDetail
import org.example.flink._1_11.streamEnv

import java.io.File
import java.util.UUID

/**
 *
 * 1) 作业描述
 * 测试多sink同source，source是否得到共用问题
 *
 *
 * <p>作业计划</p>
 * kafka_source_table (watermark)
 * ......=> wide_view
 * .............=> file_sink_view (where user_no <> '0001')
 * ....................=> file_sink_table
 * .............=> kafka_sink_view (hop window)
 * ....................=> kafka_sink_table
 *
 * 2) 结论
 * 1.9、1.10版本多sink同source情况下，source是得不到共用的，会形成两条没有相交的DAG
 * 该问题在flink1.11版本解决, use {@link TableEnvironment# createStatementSet ( )} for multiple DML statements
 *
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
    tEnv.executeSql(
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
         |  'connector' = 'kafka',
         |  'topic' = '${sourceTopic}',
         |  'properties.bootstrap.servers' = 'localhost:9092',
         |  'properties.group.id' = 'testGroup',
         |  'scan.startup.mode' = 'earliest-offset',
         |
         |  'format' = 'csv'
         |
         |)
         |""".stripMargin)

    /**
     * create view
     *
     * kafka_source_table -> wide_view
     */
    tEnv.executeSql(
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
    tEnv.executeSql(
      """
        |CREATE VIEW file_sink_view AS
        |SELECT
        |    log_id,
        |    CAST(event_time AS VARCHAR) AS event_time,
        |    user_no,
        |    login_status
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
    tEnv.executeSql(
      """
        |CREATE VIEW kafka_sink_view AS
        |SELECT
        |   CAST(HOP_START(event_time, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE) AS VARCHAR) AS hop_start,
        |   CAST(HOP_END(event_time, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE) AS VARCHAR) AS hop_end,
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
    tEnv.executeSql(
      s"""
         |CREATE TABLE file_sink_table (
         |    log_id                      VARCHAR,
         |    event_time                  VARCHAR,
         |    user_no                     VARCHAR,
         |    login_status                VARCHAR
         |) WITH (
         |  'connector' = 'filesystem',
         |  'path' = 'file://${outFile}',
         |  'format' = 'csv'
         |)
         |""".stripMargin)

    /**
     * create table (sink)
     *
     * kafka_sink_table
     */
    tEnv.executeSql(
      s"""
         |CREATE TABLE kafka_sink_table (
         |    hop_start                      VARCHAR,
         |    hop_end                        VARCHAR,
         |    login_status                   VARCHAR,
         |    countAll                       BIGINT
         |) WITH (
         |  'connector' = 'kafka',
         |  'topic' = '${sinkTopic}',
         |  'properties.bootstrap.servers' = 'localhost:9092',
         |  'format' = 'csv'
         |)
         |""".stripMargin)

    /**
     * use {@link TableEnvironment# createStatementSet ( )} for multiple DML statements
     */
    val sset = tEnv.createStatementSet()
    sset.addInsertSql(
      """
        |INSERT INTO file_sink_table
        |SELECT
        |   *
        |FROM
        |   file_sink_view
        |""".stripMargin)

    sset.addInsertSql(
      """
        |INSERT INTO kafka_sink_table
        |SELECT
        |   *
        |FROM
        |   kafka_sink_view
        |""".stripMargin)

    println(s"explain: |+\n${sset.explain(ExplainDetail.ESTIMATED_COST, ExplainDetail.CHANGELOG_MODE)}")

    sset.execute()

  }

}
