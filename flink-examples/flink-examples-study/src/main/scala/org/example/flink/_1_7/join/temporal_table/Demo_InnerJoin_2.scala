package org.example.flink._1_7.join.temporal_table

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.table.descriptors.{Json, Kafka, Schema}
import org.example.flink._1_7.{prettyJson, streamEnv}

import scala.collection.mutable

/**
 * 使用Join with Temporal Table来做维表join
 *
 * 1) 使用API:
 *
 * tEnv.connect
 * sEnv.fromCollection().toTable
 * {Table}.createTemporalTableFunction
 * tEnv.registerFunction
 * tEnv.sqlQuery
 * tEnv.toAppendStream().print
 * tEnv.registerTableSink
 * tEnv.sqlUpdate
 * sEnv.disableOperatorChaining
 *
 * 2) 作业计划:
 *
 * source(kafka, json格式)
 * ..........===> Temporal Table join -> LATERAL TABLE (UserHistory(o.proctime)) -> 简单select
 * .................===> sink(print)
 * ..........===> Temporal Table join -> LATERAL TABLE (UserHistory(o.proctime)) -> group by hop window
 * .................===> sink(kafka, json格式)
 *
 * 3) 最终执行计划:
 *
 * 1 Task Managers
 * 1 Task Slots
 * 6 Tasks
 *
 * 4) 目前存在的问题:
 *
 * DAG中kafka source没有得到共用
 * DAG中fromCollection的source得到了共用
 *
 * 5) 目前版本限制:
 *
 * 仅支持proctime
 * 仅支持inner join
 *
 */
object Demo_InnerJoin_2 {

  def main(args: Array[String]): Unit = {
    val sourceTopic = "login-json"
    val sinkTopic = "test"

    val (sEnv, tEnv) = streamEnv

    /**
     * source | kafka | json
     *
     * 数据:  source/login.json
     */
    tEnv.connect(
      new Kafka()
        .version("0.11")
        .topic(sourceTopic)
        .property("bootstrap.servers", "localhost:9092")
        .property("group.id", "testGroup")
        .startFromEarliest()
    ).withFormat(
      new Json()
        .failOnMissingField(true)
        .jsonSchema(
          """
            |{
            |  "type": "object",
            |  "properties": {
            |    "log_id": {
            |      "type": "string"
            |    },
            |    "event_time": {
            |      "type": "string",
            |      "format": "date-time"
            |    },
            |    "user_no": {
            |      "type": "string"
            |    },
            |    "real_ip": {
            |      "type": "string"
            |    },
            |    "login_status": {
            |      "type": "boolean"
            |    },
            |    "login_code": {
            |      "type": "string"
            |    }
            |  }
            |}
            |""".stripMargin)
    ).withSchema(
      new Schema()
        .field("log_id", Types.STRING)
        .field("event_time", Types.SQL_TIMESTAMP)
        .field("user_no", Types.STRING)
        .field("real_ip", Types.STRING)
        .field("login_status", Types.BOOLEAN)
        .field("proctime", Types.SQL_TIMESTAMP)
        .proctime()
    ).inAppendMode().registerTableSource("kafka_source_table")

    /**
     * 维表
     * 用户信息表
     */
    val userData = new mutable.MutableList[(String, String)]
    userData.+=(("0001", "小伊"))
    userData.+=(("0002", "小二"))
    userData.+=(("0003", "张三"))
    userData.+=(("0004", "李四"))
    userData.+=(("0005", "王五"))
    userData.+=(("0006", "赵六"))

    import org.apache.flink.api.scala._
    import org.apache.flink.table.api.scala._
    val userInfo = sEnv.fromCollection(userData).toTable(tEnv, 'u_user_no, 'u_user_name, 'u_proctime.proctime)
    tEnv.registerFunction("UserHistory", userInfo.createTemporalTableFunction('u_proctime, 'u_user_no))

    val printTable = tEnv.sqlQuery(
      """
        |SELECT
        |   o.user_no,
        |   d.u_user_name
        |FROM
        |   kafka_source_table as o,
        |   LATERAL TABLE (UserHistory(o.proctime)) AS d
        |WHERE
        |  o.user_no = d.u_user_no
        |""".stripMargin)
    tEnv.toAppendStream(printTable)(Types.ROW(Types.STRING, Types.STRING)).print()

    /**
     * sink | kafka | json
     */
    tEnv.connect(
      new Kafka()
        .version("0.11")
        .topic(sinkTopic)
        .property("bootstrap.servers", "localhost:9092")
    ).withFormat(
      new Json()
        .failOnMissingField(true)
        .schema(
          Types.ROW(
            Types.SQL_TIMESTAMP,
            Types.SQL_TIMESTAMP,
            Types.STRING,
            Types.STRING,
            Types.STRING,
            Types.LONG)
        ).deriveSchema()
    ).withSchema(
      new Schema()
        .field("hop_start", Types.SQL_TIMESTAMP)
        .field("hop_end", Types.SQL_TIMESTAMP)
        .field("hop_proctime", Types.STRING)
        .field("user_no", Types.STRING)
        .field("user_name", Types.STRING)
        .field("error_times", Types.LONG)
    ).inAppendMode().registerTableSink("kafka_sink_table")

    tEnv.sqlUpdate(
      """
        |INSERT INTO kafka_sink_table
        |SELECT
        |   HOP_START(o.proctime, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE) AS hop_start,
        |   HOP_END(o.proctime, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE) AS hop_end,
        |   CAST(HOP_PROCTIME(o.proctime, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE) AS VARCHAR) AS hop_proctime,
        |   o.user_no,
        |   d.u_user_name AS user_name,
        |   COUNT(*) AS error_times
        |FROM
        |   kafka_source_table o,
        |   LATERAL TABLE (UserHistory(o.proctime)) AS d
        |WHERE
        |   o.user_no = d.u_user_no AND
        |   o.login_status IS FALSE
        |GROUP BY
        |   HOP(o.proctime, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE),
        |   o.user_no,
        |   d.u_user_name
        |""".stripMargin)

    //    sEnv.disableOperatorChaining()
    println(s"ExecutionPlan: |+\n${prettyJson(sEnv.getExecutionPlan)}")

    sEnv.execute(this.getClass.getName)

  }

}
