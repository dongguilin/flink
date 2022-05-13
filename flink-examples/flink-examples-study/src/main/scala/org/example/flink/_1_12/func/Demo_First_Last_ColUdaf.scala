package org.example.flink._1_12.func

import org.apache.flink.table.api.ExplainDetail
import org.example.flink._1_12.streamEnv

/**
 * 测试FirstColUdaf/LastColUdaf函数
 */
object Demo_First_Last_ColUdaf {

  def main(args: Array[String]): Unit = {

    val sourceTopic = "workflow"
    val sinkTopic = "test"

    val (sEnv, tEnv) = streamEnv
    sEnv.disableOperatorChaining()

    tEnv.createFunction("firstcol_udtf", classOf[FirstColUdaf], true)
    tEnv.createFunction("lastcol_udtf", classOf[LastColUdaf], true)

    /**
     * source | kafka | csv
     *
     * 数据:  source/workflow.csv
     */
    tEnv.executeSql(
      s"""
         |CREATE TABLE kafka_source_table (
         |    log_id                   VARCHAR COMMENT '事件ID',
         |    event_time               TIMESTAMP(3) COMMENT '事件发生时间',
         |    event_type               VARCHAR COMMENT '事件类型',
         |    flow_id                  VARCHAR COMMENT '流程ID',
         |    stage_id                 VARCHAR COMMENT '流程所处阶段标识',
         |    user_no                  VARCHAR COMMENT '发起流程的用户',
         |    approver                 VARCHAR COMMENT '流程审批人',
         |    tag                      VARCHAR COMMENT '测试数据结果标识',
         |    WATERMARK FOR event_time AS event_time - INTERVAL  '3' MINUTE
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
     * sink | kafka | csv
     *
     */
    tEnv.executeSql(
      s"""
         |CREATE TABLE kafka_sink_table (
         |    flow_id                     VARCHAR,
         |    f_log_id                    VARCHAR,
         |    event_type                  VARCHAR,
         |    diff_minute                 INT,
         |    stage_id                    VARCHAR,
         |    PRIMARY KEY (flow_id) NOT ENFORCED
         |) WITH (
         |  'connector' = 'upsert-kafka',
         |  'topic' = '${sinkTopic}',
         |  'properties.bootstrap.servers' = 'localhost:9092',
         |  'key.format' = 'csv',
         |  'value.format' = 'csv'
         |)
         |""".stripMargin)

    tEnv.executeSql(
      """
        |CREATE VIEW myview AS
        |SELECT
        |   firstcol_udtf(log_id, event_time)       AS  f_log_id,
        |   lastcol_udtf(log_id, event_time)        AS  l_log_id,
        |   TIMESTAMPDIFF(MINUTE, CAST(firstcol_udtf(event_time, event_time) AS TIMESTAMP), CAST(lastcol_udtf(event_time, event_time) AS TIMESTAMP)) AS diff_minute,
        |   flow_id,
        |   lastcol_udtf(stage_id, event_time)      AS  stage_id,
        |   lastcol_udtf(user_no, event_time)       AS  user_no,
        |   lastcol_udtf(approver, event_time)      AS  approver
        |FROM
        |   kafka_source_table
        |WHERE
        |   event_type = 'workflow'
        |GROUP BY
        |   flow_id
        |""".stripMargin)

    val sset = tEnv.createStatementSet()

    /**
     * 输出:
     * flow_002,log_id_000004,log_id_000009,61,stage_2
     */
    sset.addInsertSql(
      """
        |INSERT INTO kafka_sink_table
        |SELECT
        |   flow_id,
        |   f_log_id,
        |   l_log_id,
        |   diff_minute,
        |   stage_id
        |FROM
        |   myview
        |WHERE
        |   diff_minute > 60
        |""".stripMargin)

    println(sset.explain(ExplainDetail.ESTIMATED_COST, ExplainDetail.CHANGELOG_MODE))

    sset.execute()

  }

}
