package org.example.flink

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

package object _1_11 {

  def streamEnv(implicit useBlink: Boolean = true): (StreamExecutionEnvironment, StreamTableEnvironment) = {
    val sEnv = StreamExecutionEnvironment.getExecutionEnvironment

    val tEnv = if (useBlink) {
      StreamTableEnvironment.create(sEnv, EnvironmentSettings.newInstance()
        .inStreamingMode()
        .useBlinkPlanner()
        .build())
    } else {
      StreamTableEnvironment.create(sEnv, EnvironmentSettings.newInstance()
        .useOldPlanner()
        .inStreamingMode()
        .build())
    }

    sEnv.setParallelism(1)
    sEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val ckpath = s"file://${System.getProperty("user.home")}/flink_checkpoint"
    sEnv.setStateBackend(new RocksDBStateBackend(ckpath, true))
    sEnv.getCheckpointConfig.setCheckpointInterval(30000)
    sEnv.getCheckpointConfig.setCheckpointTimeout(2 * 60000)
    sEnv.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)

    (sEnv, tEnv)
  }

}
