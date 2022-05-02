package org.example.flink._1_7.join.table_function

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.table.descriptors.{Json, Kafka, Schema}
import org.example.flink._1_7.join.table_function.udtf.{EducationUdtf, UserUdtf}
import org.example.flink._1_7.{prettyJson, streamEnv}

/**
 * 1) 使用API:
 *
 * tEnv.connect
 * tEnv.registerFunction
 * tEnv.registerTableSink
 * tEnv.sqlUpdate
 * sEnv.disableOperatorChaining
 *
 * 2) 作业计划:
 *
 * source(kafka, json格式)
 * ..........===>  inner join LATERAL TABLE (education_udtf(o.user_no))
 * ..........===>  LEFT JOIN LATERAL TABLE (user_udtf(o.user_no))
 * ..........===>  group by hop window
 * .................===> sink(kafka, json格式)
 *
 * 3) 最终执行计划:
 *
 * 1 Task Managers
 * 1 Task Slots
 * 10 Tasks
 *
 * DAG: 一条直线
 *
 * 4) 目前版本限制:
 *
 * 仅支持proctime
 */
object Demo_Join_1 {

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
     * UDTF
     */
    import org.apache.flink.api.scala._
    tEnv.registerFunction("user_udtf", new UserUdtf)
    tEnv.registerFunction("education_udtf", new EducationUdtf)

    /**
     * sink | kafka | json
     *
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
            Types.INT,
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
        .field("gender", Types.STRING)
        .field("age", Types.INT)
        .field("city", Types.STRING)
        .field("education", Types.STRING)
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
        |   T2.user_name,
        |   T2.gender,
        |   T2.age,
        |   T2.city,
        |   T1.education,
        |   COUNT(*) AS error_times
        |FROM
        |   kafka_source_table o, LATERAL TABLE (education_udtf(o.user_no)) AS T1(education)
        |LEFT JOIN
        |   LATERAL TABLE (user_udtf(o.user_no)) AS T2(user_name,gender,age,city) ON TRUE
        |WHERE
        |   o.login_status IS FALSE
        |GROUP BY
        |   HOP(o.proctime, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE),
        |   o.user_no,
        |   T1.education,
        |   T2.user_name,
        |   T2.gender,
        |   T2.age,
        |   T2.city
        |HAVING
        |   COUNT(*) > 1
        |""".stripMargin)

    sEnv.disableOperatorChaining()

    println(s"ExecutionPlan: |+\n${prettyJson(sEnv.getExecutionPlan)}")

    sEnv.execute(this.getClass.getName)

  }

}
