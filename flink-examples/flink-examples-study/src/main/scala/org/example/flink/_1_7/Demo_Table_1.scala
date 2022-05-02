package org.example.flink._1_7

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.descriptors.{Json, Kafka, Rowtime, Schema}
import org.apache.flink.table.sinks.CsvTableSink
import org.apache.flink.table.sources.wmstrategies.PunctuatedWatermarkAssigner
import org.apache.flink.types.Row

import java.io.File
import java.util.UUID

/**
 *
 * 1) 使用API:
 *
 * tEnv.connect
 * tEnv.registerTableSink
 * tEnv.sqlQuery
 * {Table}.insertInto
 *
 * 2) 作业计划:
 *
 * source(kafka, json格式)
 * ..........===> 简单select
 * .................===> sink(file, csv格式)
 * ..........===> group by hop window
 * .................===> sink(kafka, json格式)
 *
 * 3) 最终执行计划:
 *
 * 1 Task Managers
 * 1 Task Slots
 * 3 Tasks
 *
 * DAG1：Task1(Source, file sink)
 * DAG2: Task2(Source) + Task3(group by, kafka sink)
 *
 * 4) 目前存在的问题:
 *
 * DAG中source没有得到共用
 *
 */
object Demo_Table_1 {

  def main(args: Array[String]): Unit = {
    val sourceTopic = "login-json"
    val sinkTopic = "test"

    val outFile = System.getProperty("java.io.tmpdir") + File.separator + UUID.randomUUID().toString + ".csv"
    println(outFile)

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
        .field("row_time", Types.SQL_TIMESTAMP)
        .rowtime(Rowtime()
          .timestampsFromField("event_time")
          .watermarksFromStrategy(new PunctuatedWatermarkAssigner {
            override def getWatermark(row: Row, timestamp: Long): Watermark = {
              new Watermark(timestamp - 30 * 1000)
            }
          }))
    ).inAppendMode().registerTableSource("kafka_source_table")

    /**
     * sink | file | csv
     */
    tEnv.registerTableSink(
      "file_sink_table",
      new CsvTableSink(
        outFile,
        fieldDelim = "|",
        numFiles = 1,
        writeMode = WriteMode.OVERWRITE)
        .configure(
          Array[String]("log_id", "event_time", "row_time", "user_no", "real_ip", "login_status"),
          Array[TypeInformation[_]](Types.STRING, Types.SQL_TIMESTAMP, Types.SQL_TIMESTAMP, Types.STRING, Types.STRING, Types.BOOLEAN)
        )
    )

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
        .schema(Types.ROW(Types.SQL_TIMESTAMP, Types.SQL_TIMESTAMP, Types.STRING, Types.STRING, Types.LONG))
        .deriveSchema()
    ).withSchema(
      new Schema()
        .field("hop_start", Types.SQL_TIMESTAMP)
        .field("hop_end", Types.SQL_TIMESTAMP)
        .field("hop_rowtime", Types.STRING)
        .field("user_no", Types.STRING)
        .field("error_times", Types.LONG)
    ).inAppendMode().registerTableSink("kafka_sink_table")

    val fileTableQuery = tEnv.sqlQuery(
      """
        |SELECT
        |   log_id,
        |   event_time,
        |   row_time,
        |   user_no,
        |   real_ip,
        |   login_status
        |FROM
        |   kafka_source_table
        |""".stripMargin)
    fileTableQuery.insertInto("file_sink_table")

    val kafkaTableQuery = tEnv.sqlQuery(
      """
        |SELECT
        |   HOP_START(row_time, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE) AS hop_start,
        |   HOP_END(row_time, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE) AS hop_end,
        |   CAST(HOP_ROWTIME(row_time, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE) AS VARCHAR) AS hop_rowtime,
        |   user_no,
        |   COUNT(*) AS error_times
        |FROM
        |   kafka_source_table
        |WHERE
        |   login_status IS FALSE
        |GROUP BY
        |   HOP(row_time, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE),
        |   user_no
        |""".stripMargin)
    kafkaTableQuery.insertInto("kafka_sink_table")

    println(s"ExecutionPlan: |+\n${prettyJson(sEnv.getExecutionPlan)}")

    sEnv.execute(this.getClass.getName)

  }

}
