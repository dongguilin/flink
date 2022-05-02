package org.example.flink

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment

package object _1_7 {
  val mapper = new ObjectMapper()

  def streamEnv(): (StreamExecutionEnvironment, StreamTableEnvironment) = {
    val sEnv = StreamExecutionEnvironment.getExecutionEnvironment

    val tEnv = TableEnvironment.getTableEnvironment(sEnv)

    sEnv.setParallelism(1)
    sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val ckpath = s"file://${System.getProperty("user.home")}/flink_checkpoint"
    sEnv.setStateBackend(new RocksDBStateBackend(ckpath, true))
    sEnv.getCheckpointConfig.setCheckpointInterval(30000)
    sEnv.getCheckpointConfig.setCheckpointTimeout(2 * 60000)
    sEnv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)

    (sEnv, tEnv)
  }

  def prettyJson(str: String): String = {
    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(mapper.readTree(str))
  }

}
