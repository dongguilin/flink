package org.example.flink._1_9.ddl.watermark

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.table.descriptors.{Csv, Kafka, Rowtime, Schema}
import org.apache.flink.table.sources.wmstrategies.PunctuatedWatermarkAssigner
import org.apache.flink.types.Row
import org.example.flink._1_9.streamEnv

/**
 *
 * 该类的kafka source表使用table api注册，看看框架做了什么处理
 *
 */
object Demo_Table_Watermark {

  def main(args: Array[String]): Unit = {
    val sourceTopic = "login"
    val sinkTopic = "test"

    val (sEnv, tEnv) = streamEnv

    /**
     * 使用table api创建source表
     *
     * 并定义watermark
     *
     * 数据:  source/login.csv
     */
    tEnv
      .connect(
        new Kafka()
          .version("0.11")
          .topic(sourceTopic)
          .property("bootstrap.servers", "localhost:9092")
          .property("group.id", "testGroup")
          .startFromEarliest())
      .withFormat(
        new Csv()
          .schema(
            Types.ROW(Array[String]("log_id", "event_time", "user_no", "real_ip", "login_status", "login_code"),
              Array[TypeInformation[_]](Types.STRING, Types.SQL_TIMESTAMP, Types.STRING, Types.STRING, Types.BOOLEAN, Types.STRING
              )
            )
          )
      )
      .withSchema(new Schema()
        .field("log_id", Types.STRING)
        .field("event_time", Types.SQL_TIMESTAMP)
        .field("user_no", Types.STRING)
        .field("real_ip", Types.STRING)
        .field("login_status", Types.BOOLEAN)
        .field("login_code", Types.STRING)
        .field("rowtime", Types.SQL_TIMESTAMP)
        .rowtime(new Rowtime()
          .timestampsFromField("event_time")
          .watermarksFromStrategy(new PunctuatedWatermarkAssigner() {
            override def getWatermark(row: Row, timestamp: Long): Watermark = {
              new Watermark(timestamp - 30 * 1000)
            }
          }))
      )
      .inAppendMode()
      .registerTableSource("kafka_source_table")

    /**
     * create table (sink)
     */
    tEnv.sqlUpdate(
      s"""
         |CREATE TABLE kafka_sink_table (
         |    hop_start                      TIMESTAMP(3),
         |    hop_end                        TIMESTAMP(3),
         |    user_no                        VARCHAR,
         |    error_times                    BIGINT
         |) WITH (
         |  'connector.type' = 'kafka',
         |  'connector.version' = '0.11',
         |  'connector.topic' = '${sinkTopic}',
         |  'connector.properties.0.key' = 'bootstrap.servers',
         |  'connector.properties.0.value' = 'localhost:9092',
         |
         |  'format.type' = 'csv',
         |  'format.derive-schema' = 'true',        -- or use the table's schema
         |
         |  'update-mode' = 'append'
         |)
         |""".stripMargin)

    /**
     * rich insert
     */
    tEnv.sqlUpdate(
      """
        |INSERT INTO kafka_sink_table
        |SELECT
        |   HOP_START(rowtime, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE) AS hop_start,
        |   HOP_END(rowtime, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE) AS hop_end,
        |   user_no,
        |   COUNT(*) AS error_times
        |FROM
        |   kafka_source_table
        |WHERE
        |   login_status IS FALSE
        |GROUP BY
        |   HOP(rowtime, INTERVAL '1' MINUTE, INTERVAL '10' MINUTE),
        |   user_no
        |""".stripMargin)

    println(sEnv.getExecutionPlan)

    tEnv.execute(this.getClass.getName)

  }

}
