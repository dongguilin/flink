package org.example.flink._1_12.cep.workflow

import org.apache.flink.table.api.ExplainDetail
import org.example.flink._1_12.streamEnv

/**
 *
 *
 * 1）业务描述
 * 统计流程超时的记录，告警
 * 规则：从流程发起到结束，耗时>60分钟，告警
 *
 * 2) 目前问题
 * create view中有MATCH_RECOGNIZE会有异常
 *
 * Exception in thread "main" java.lang.ClassCastException: class org.apache.calcite.sql.SqlNodeList cannot be cast to class org.apache.calcite.sql.SqlLiteral (org.apache.calcite.sql.SqlNodeList and org.apache.calcite.sql.SqlLiteral are in unnamed module of loader 'app')
 *
 * 参见 {@link Demo_Sql_Timeout} 正确写法
 */
object Demo_Sql_CreateView_error {

  def main(args: Array[String]): Unit = {

    val sourceTopic = "workflow"
    val sinkTopic = "test"

    val (sEnv, tEnv) = streamEnv
    sEnv.disableOperatorChaining()

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
         |    tag                           VARCHAR,
         |    s_log_id                      VARCHAR,
         |    s_event_time                  VARCHAR,
         |    s_flow_id                     VARCHAR,
         |    user_no                       VARCHAR,
         |    ex_log_id                     VARCHAR,
         |    ex_event_time                 VARCHAR,
         |    ex_stage_id                   VARCHAR,
         |    ex_approver                   VARCHAR
         |) WITH (
         |  'connector' = 'kafka',
         |  'topic' = '${sinkTopic}',
         |  'properties.bootstrap.servers' = 'localhost:9092',
         |  'format' = 'csv'
         |)
         |""".stripMargin)

    /**
     * 抛异常
     */
    tEnv.executeSql(
      """
        |CREATE VIEW kafka_sink_view AS
        |SELECT
        |   IF(TIMESTAMPDIFF(MINUTE, s_event_time, ex_event_time) > 60, '超时', '正常') AS tag,
        |   s_log_id,
        |   DATE_FORMAT(s_event_time, 'yyyy-MM-dd HH:mm:ss') AS s_event_time,
        |   s_flow_id,
        |   user_no,
        |   ex_log_id,
        |   DATE_FORMAT(ex_event_time, 'yyyy-MM-dd HH:mm:ss') AS ex_event_time,
        |   ex_stage_id,
        |   ex_approver
        |FROM
        |   (SELECT * FROM kafka_source_table WHERE event_type = 'workflow')
        |MATCH_RECOGNIZE (
        |   PARTITION BY  flow_id
        |   ORDER BY  event_time
        |   MEASURES
        |       A.log_id                    AS  s_log_id,
        |       A.event_time                AS  s_event_time,
        |       A.flow_id                   AS  s_flow_id,
        |       A.user_no                   AS  user_no,
        |       LAST(C.log_id)                AS  ex_log_id,
        |       LAST(C.event_time)            AS  ex_event_time,
        |       LAST(C.stage_id)              AS  ex_stage_id,
        |       LAST(C.approver)              AS  ex_approver
        |   ONE ROW PER MATCH
        |   AFTER MATCH SKIP PAST LAST ROW
        |   PATTERN (A B* C) WITHIN INTERVAL '61' MINUTE        -- 超时时长定义, 61 > 60, 是为了计算流程耗时刚好=60的情况
        |   DEFINE
        |       A AS A.stage_id = 'stage_start',
        |       B AS
        |           B.stage_id <> 'stage_start'
        |           AND B.stage_id <> 'stage_end'
        |           AND TIMESTAMPDIFF(MINUTE, A.event_time, B.event_time)  < 60,
        |       C AS
        |           (C.stage_id <> 'stage_start' AND TIMESTAMPDIFF(MINUTE, A.event_time, C.event_time) > 60)   -- 超时流程
        |           OR
        |           (C.stage_id = 'stage_end' AND TIMESTAMPDIFF(MINUTE, A.event_time, C.event_time)  <= 60)    -- 流程正常结束(未超时)
        |)
        |""".stripMargin)

    val sset = tEnv.createStatementSet()
    sset.addInsertSql(
      """
        |INSERT INTO kafka_sink_table
        |SELECT
        |   *
        |FROM
        |   kafka_sink_view
        |""".stripMargin)

    println(sset.explain(ExplainDetail.ESTIMATED_COST, ExplainDetail.CHANGELOG_MODE))

    sset.execute()


  }

}
