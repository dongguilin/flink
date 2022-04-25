package org.example.flink._1_10.ddl.create_view

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}
import org.example.flink._1_10.streamEnv

/**
 *
 * TODO 存在的问题: 多sink同source，source没有得到共用
 *
 * <p>作业计划</p>
 * kafka_source_table
 * .....==> kafka_sink_table1
 * .....==> kafka_sink_table2
 * <p>最终执行计划</p>
 * kafka_source_table ==> kafka_sink_table1
 * kafka_source_table ==> kafka_sink_table2
 *
 * 问题:
 *
 * 目前两个Task都有自己单独的FlinkKafkaConsumer011实例和KafkaFetcher实例
 * 执行计划待优化，应该合为一个consumer和fetcher实例，仅需读一次将数据发往下游
 *
 * 解决:
 * flink1.11版本解决, use {@link TableEnvironment# createStatementSet ( )} for multiple DML statements
 *
 */
object Demo_Table_1Source2Sink {

  def main(args: Array[String]): Unit = {
    val sourceTopic = "login"
    val sinkTopic1 = "test"
    val sinkTopic2 = "test2"

    val (sEnv, tEnv) = streamEnv

    /**
     * create table (source)
     *
     * 定义watermark
     *
     * 数据:  source/login.csv
     */
    tEnv
      .connect(
        new Kafka()
          .version("0.11")
          .topic(sourceTopic)
          .property("zookeeper.connect", "localhost")
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
        .field("log_id", DataTypes.STRING)
        .field("event_time", DataTypes.TIMESTAMP(3))
        .field("user_no", DataTypes.STRING)
        .field("real_ip", DataTypes.STRING)
        .field("login_status", DataTypes.BOOLEAN)
        .field("login_code", DataTypes.STRING)
      )
      .inAppendMode()
      .createTemporaryTable("kafka_source_table")


    /**
     * create table (sink)
     *
     * kafka_sink_table1
     */
    tEnv
      .connect(
        new Kafka()
          .version("0.11")
          .topic(sinkTopic1)
          .property("bootstrap.servers", "localhost:9092")
          .startFromEarliest())
      .withFormat(
        new Csv()
          .schema(
            Types.ROW(Array[String]("log_id", "event_time", "user_no", "login_status"),
              Array[TypeInformation[_]](Types.STRING, Types.SQL_TIMESTAMP, Types.STRING, Types.BOOLEAN
              )
            )
          )
      )
      .withSchema(new Schema()
        .field("log_id", DataTypes.STRING)
        .field("event_time", DataTypes.TIMESTAMP(3))
        .field("user_no", DataTypes.STRING)
        .field("login_status", DataTypes.BOOLEAN)
      )
      .inAppendMode()
      .createTemporaryTable("kafka_sink_table1")


    /**
     * create table (sink)
     *
     * kafka_sink_table2
     */
    tEnv
      .connect(
        new Kafka()
          .version("0.11")
          .topic(sinkTopic2)
          .property("bootstrap.servers", "localhost:9092")
          .startFromEarliest())
      .withFormat(
        new Csv()
          .schema(
            Types.ROW(Array[String]("log_id", "event_time", "user_no", "login_status"),
              Array[TypeInformation[_]](Types.STRING, Types.SQL_TIMESTAMP, Types.STRING, Types.BOOLEAN
              )
            )
          )
      )
      .withSchema(new Schema()
        .field("log_id", DataTypes.STRING)
        .field("event_time", DataTypes.TIMESTAMP(3))
        .field("user_no", DataTypes.STRING)
        .field("login_status", DataTypes.BOOLEAN)
      )
      .inAppendMode()
      .createTemporaryTable("kafka_sink_table2")

    /**
     * rich insert
     *
     * kafka_source_table -> kafka_sink_table1
     */
    tEnv.sqlUpdate(
      """
        |INSERT INTO kafka_sink_table1
        |SELECT
        |    log_id,
        |    event_time,
        |    user_no,
        |    login_status
        |FROM
        |   kafka_source_table
        |WHERE
        |   user_no = '0001'
        |""".stripMargin)


    /**
     * rich insert
     *
     * kafka_source_table -> kafka_sink_table2
     */
    tEnv.sqlUpdate(
      """
        |INSERT INTO kafka_sink_table2
        |SELECT
        |    log_id,
        |    event_time,
        |    user_no,
        |    login_status
        |FROM
        |   kafka_source_table
        |WHERE
        |   user_no <> '0001'
        |""".stripMargin)

    println(sEnv.getExecutionPlan)

    tEnv.execute(this.getClass.getName)

  }

}
