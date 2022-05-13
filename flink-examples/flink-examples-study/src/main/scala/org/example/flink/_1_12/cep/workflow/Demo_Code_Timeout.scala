package org.example.flink._1_12.cep.workflow

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.example.flink._1_12.streamEnv

import scala.collection.Map

/**
 * 1）业务描述
 * 统计流程超时的记录，告警
 * 规则：从流程发起到结束，耗时>60分钟，告警
 *
 * 2) 作业描述
 * 使用 CEP code
 *
 * pattern -> A B* C
 * A (next B)oneOrMore.optional.greedy (next C)
 *
 * A代表流程开始事件，B是流程中间的审批事件，C是流程结束事件
 *
 * 3) 目前问题
 * 超时时，pattern匹配完的数据也会输出
 *
 */
object Demo_Code_Timeout {

  def main(args: Array[String]): Unit = {

    val sourceTopic = "workflow"

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

    import org.apache.flink.api.scala._

    val typeInfo = Types.ROW(
      Types.STRING, Types.SQL_TIMESTAMP, Types.STRING, Types.STRING, Types.STRING,
      Types.STRING, Types.STRING, Types.STRING)

    val (event_type_index, flow_id_index, stage_id_index) = (2, 3, 4)

    val keyedDs = tEnv.toAppendStream(tEnv.from("kafka_source_table"))(typeInfo)
      .filter(_.getField(event_type_index) == "workflow")
      .keyBy(_.getField(flow_id_index))

    /**
     * 捕获超时事件
     */
    val outputTag = new OutputTag[Row]("timeout-output")

    /**
     * CEP
     */
    val patternStream: PatternStream[Row] = CEP.pattern(keyedDs,
      Pattern
        .begin("A").where((row: Row) => row.getField(4).toString == "stage_start")
        .next("B")
        .where(row => row.getField(stage_id_index).toString != "stage_start" && row.getField(stage_id_index).toString != "stage_end")
        .oneOrMore.optional.greedy // 期望出现0到多次，并且尽可能的重复次数多
        .next("C").where(row => row.getField(stage_id_index).toString == "stage_end")
        .within(Time.minutes(61))
    )

    def patternFlatTimeoutFunction(pattern: Map[String, Iterable[Row]], timeoutTs: Long, collector: Collector[Row]): Unit = {
      val start: Iterable[Row] = pattern.get("A").get
      val startRow = start.head

      val timeoutStr = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss").format(timeoutTs)
      collector.collect(Row.join(Row.of("A", timeoutStr), startRow))

      pattern.get("B").map(iterable =>
        collector.collect(Row.join(Row.of("B", timeoutStr), iterable.tail.head))
      )
    }

    def patternFlatSelectFunction(pattern: Map[String, Iterable[Row]], collector: Collector[Row]): Unit = {
      val start: Iterable[Row] = pattern.get("A").get
      val end: Iterable[Row] = pattern.get("C").get
      val startRow = start.head
      val endRow = end.head

      collector.collect(Row.join(Row.of("A"), startRow))
      collector.collect(Row.join(Row.of("C"), endRow))
    }

    val outDs = patternStream.flatSelect(outputTag)(patternFlatTimeoutFunction)(patternFlatSelectFunction)

    /**
     * 输出:
     *
     * A,log_id_000002,2021-12-11 11:00:10.0,workflow,flow_001,stage_start,user_001,小文,_
     * C,log_id_000008,2021-12-11 11:07:10.0,workflow,flow_001,stage_end,user_001,李总,flow_001_正常结束_02_06_07_08
     * A,log_id_000015,2021-12-11 13:17:10.0,workflow,flow_005,stage_start,user_005,小波,_
     * C,log_id_000016,2021-12-11 14:17:10.0,workflow,flow_005,stage_end,user_005,李总,flow_005_正常结束_15_16
     * A,log_id_000011,2021-12-11 12:05:10.0,workflow,flow_003,stage_start,user_003,小花,_
     * C,log_id_000013,2021-12-11 13:04:10.0,workflow,flow_003,stage_end,user_003,李总,flow_003_正常结束_11_13
     *
     */
    outDs.addSink(row => {
      println(row)
    }).name("normal")

    /**
     * 输出:
     *
     * timeout -> A,2021-12-11 20:01:10,log_id_000002,2021-12-11 11:00:10.0,workflow,flow_001,stage_start,user_001,小文,_
     * timeout -> B,2021-12-11 20:01:10,log_id_000007,2021-12-11 11:06:10.0,workflow,flow_001,stage_3,user_001,张二哥,_
     * timeout -> A,2021-12-11 21:07:10,log_id_000012,2021-12-11 12:06:10.0,workflow,flow_004,stage_start,user_004,小刚,_
     * timeout -> A,2021-12-11 20:04:10,log_id_000004,2021-12-11 11:03:10.0,workflow,flow_002,stage_start,user_002,小明,_
     */
    outDs.getSideOutput(outputTag).addSink(row => {
      println(s"timeout -> ${row}")
    }).name("timeout")

    println(s"ExecutionPlan: |+\n${sEnv.getExecutionPlan}")

    sEnv.execute()

  }

}
