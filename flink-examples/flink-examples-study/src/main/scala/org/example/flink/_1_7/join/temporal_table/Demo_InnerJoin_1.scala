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
 * sEnv.disableOperatorChaining
 *
 * 2) 作业计划:
 *
 * source(kafka, json格式)
 * ..........===> Temporal Table join -> LATERAL TABLE (UserHistory(o.proctime)) -> 简单select
 * .................===> sink(print)
 *
 * 3) 最终执行计划:
 *
 * 1 Task Managers
 * 1 Task Slots
 * 9 Tasks
 *
 * DAG: Y型
 *
 * 4) 目前存在的问题:
 *
 * DAG中kafka source没有得到共用
 * fromCollection的source得到了共用
 *
 * 5) 目前版本限制:
 *
 * 仅支持proctime
 * 仅支持inner join
 */
object Demo_InnerJoin_1 {

  def main(args: Array[String]): Unit = {
    val sourceTopic = "login-json"

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

    sEnv.disableOperatorChaining()
    println(s"ExecutionPlan: |+\n${prettyJson(sEnv.getExecutionPlan)}")

    sEnv.execute(this.getClass.getName)

  }

}
