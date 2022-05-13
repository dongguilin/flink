package org.example.flink

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

package object _1_13 {

  val mapper = new ObjectMapper()

  def streamEnv(): (StreamExecutionEnvironment, StreamTableEnvironment) = {
    val sEnv = StreamExecutionEnvironment.getExecutionEnvironment

    val tEnv = StreamTableEnvironment.create(sEnv, EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build())

    sEnv.setParallelism(1)
    sEnv.getCheckpointConfig.setCheckpointInterval(30000)
    sEnv.getCheckpointConfig.setCheckpointTimeout(2 * 60000)
    sEnv.setStateBackend(new EmbeddedRocksDBStateBackend)
    val ckpath = s"file://${System.getProperty("user.home")}/flink_checkpoint"
    sEnv.getCheckpointConfig.setCheckpointStorage(ckpath)

    (sEnv, tEnv)
  }

  def prettyJson(str: String): String = {
    mapper.writerWithDefaultPrettyPrinter().writeValueAsString(mapper.readTree(str))
  }

}
