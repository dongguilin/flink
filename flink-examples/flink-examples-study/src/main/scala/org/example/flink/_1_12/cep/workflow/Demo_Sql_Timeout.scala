package org.example.flink._1_12.cep.workflow

import org.apache.flink.table.api.ExplainDetail
import org.example.flink._1_12.streamEnv

/**
 * 1）业务描述
 * 统计流程超时的记录，告警
 * 规则：从流程发起到结束，耗时>60分钟，告警
 *
 * 2) 作业描述
 * 使用 CEP SQL
 *
 * pattern -> A B* C
 *
 * A代表流程开始事件，B是流程中间的审批事件，C是流程结束事件
 *
 * 3) 目前问题
 * 当前版本CEP SQL不支持超时事件下发
 *
 * 参见 {@link Demo_Code_Timeout}, 使用CEP code的OutputTag可捕获超时数据
 */
object Demo_Sql_Timeout {

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
     * 输出：
     * 正常,log_id_000002,"2021-12-11 11:00:10",flow_001,user_001,log_id_000008,"2021-12-11 11:07:10",stage_end,李总
     * 正常,log_id_000015,"2021-12-11 13:17:10",flow_005,user_005,log_id_000016,"2021-12-11 14:17:10",stage_end,李总
     * 正常,log_id_000011,"2021-12-11 12:05:10",flow_003,user_003,log_id_000013,"2021-12-11 13:04:10",stage_end,李总
     *
     * PATTERN 超时设置的61分钟，超时的数据会被丢弃，导致超时流程数据不能输出
     */
    val sset = tEnv.createStatementSet()
    sset.addInsertSql(
      """
        |INSERT INTO kafka_sink_table
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

    println(s"explain: |+\n${sset.explain(ExplainDetail.ESTIMATED_COST, ExplainDetail.CHANGELOG_MODE)}")

    sset.execute()

  }

}
