package org.example.flink

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment

package object _1_10 {

  def streamEnv(implicit useBlink: Boolean = true): (StreamExecutionEnvironment, StreamTableEnvironment) = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val tEnv = if (useBlink) {
      StreamTableEnvironment.create(env,
        EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build())
    } else {
      StreamTableEnvironment.create(env,
        EnvironmentSettings.newInstance().inStreamingMode().useOldPlanner().build())
    }

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val ckpath = s"file://${System.getProperty("user.home")}/flink_checkpoint"
    env.setStateBackend(new RocksDBStateBackend(ckpath, true))
    env.getCheckpointConfig.setCheckpointInterval(30000)
    env.getCheckpointConfig.setCheckpointTimeout(2 * 60000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)

    (env, tEnv)
  }

}
