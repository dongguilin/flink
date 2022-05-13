package org.example.flink._1_13.join.table_function

import org.apache.flink.table.api.ExplainDetail
import org.example.flink._1_13.join.table_function.udtf.UserUdtf
import org.example.flink._1_13.streamEnv

/**
 * 作业计划:
 *
 * source(kafka, json格式)
 * ..........===>  (LEFT JOIN LATERAL TABLE (user_udtf(o.log_id, o.user_no)), group by hop window)
 * .................===> sink(kafka, json格式)
 *
 */
object Demo_LeftJoin_1 {

  def main(args: Array[String]): Unit = {
    val sourceTopic = "login-json-2"
    val sinkTopic = "test"

    val (sEnv, tEnv) = streamEnv

    sEnv.disableOperatorChaining()

    /**
     * source | kafka | json
     *
     * 数据:  source/login.json
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
         |    proctime AS PROCTIME()
         |) WITH (
         |  'connector' = 'kafka',
         |  'topic' = '${sourceTopic}',
         |  'properties.bootstrap.servers' = 'localhost:9092',
         |  'properties.group.id' = 'testGroup',
         |  'scan.startup.mode' = 'earliest-offset',
         |
         |  'format' = 'json',
         |  'json.fail-on-missing-field' = 'true',
         |  'json.ignore-parse-errors' = 'false'
         |
         |)
         |""".stripMargin)

    /**
     * UDTF
     */
    tEnv.createTemporarySystemFunction("user_udtf", new UserUdtf)

    /**
     * sink | kafka | json
     *
     * 输出:
     * {"hop_start":"2022-05-13 15:55:00.000","hop_end":"2022-05-13 16:05:00.000","hop_proctime":"2022-05-13 16:05:00.081","user_no":"0001","user_name":"小伊","gender":"女","age":20,"city":"郑州","error_times":6}
     * {"hop_start":"2022-05-13 15:55:00.000","hop_end":"2022-05-13 16:05:00.000","hop_proctime":"2022-05-13 16:05:00.083","user_no":"0022","user_name":null,"gender":null,"age":null,"city":null,"error_times":2}
     */
    tEnv.executeSql(
      s"""
         |CREATE TABLE kafka_sink_table (
         |    hop_start                      VARCHAR,
         |    hop_end                        VARCHAR,
         |    hop_proctime                   VARCHAR,
         |    user_no                        VARCHAR,
         |    user_name                      VARCHAR,
         |    gender                         VARCHAR,
         |    age                            INT,
         |    city                           VARCHAR,
         |    error_times                    BIGINT
         |) WITH (
         |  'connector' = 'kafka',
         |  'topic' = '${sinkTopic}',
         |  'properties.bootstrap.servers' = 'localhost:9092',
         |
         |  'format' = 'json'
         |)
         |""".stripMargin)

    val insertSql =
      """
        |INSERT INTO kafka_sink_table
        |SELECT
        |   CAST(HOP_START(o.proctime, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE) AS VARCHAR) AS hop_start,
        |   CAST(HOP_END(o.proctime, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE) AS VARCHAR) AS hop_end,
        |   CAST(HOP_PROCTIME(o.proctime, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE) AS VARCHAR) AS hop_proctime,
        |   o.user_no,
        |   T.user_name,
        |   T.gender,
        |   T.age,
        |   T.city,
        |   COUNT(*) AS error_times
        |FROM
        |   kafka_source_table o
        |LEFT JOIN
        |   LATERAL TABLE (user_udtf(o.log_id, o.user_no)) AS T(user_name,gender,age,city) ON TRUE
        |WHERE
        |   o.login_status IS FALSE
        |GROUP BY
        |   HOP(o.proctime, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE),
        |   o.user_no,
        |   T.user_name,
        |   T.gender,
        |   T.age,
        |   T.city
        |HAVING
        |   COUNT(*) > 1
        |""".stripMargin

    tEnv.executeSql(insertSql)

    println(s"explain: |+\n${tEnv.explainSql(insertSql, ExplainDetail.ESTIMATED_COST, ExplainDetail.CHANGELOG_MODE, ExplainDetail.JSON_EXECUTION_PLAN)}")

  }

}
