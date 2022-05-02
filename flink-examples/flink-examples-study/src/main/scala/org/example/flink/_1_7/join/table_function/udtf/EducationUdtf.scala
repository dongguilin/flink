package org.example.flink._1_7.join.table_function.udtf

import org.apache.flink.table.functions.TableFunction

class EducationUdtf extends TableFunction[String] {

  val data = Map(
    "0001" -> "本科",
    "0002" -> "高中",
    "0003" -> "硕士",
    "0004" -> "本科",
    "0005" -> "本科",
    "0006" -> "硕士",
    "0007" -> "本科",
    "0022" -> "本科"
  )

  def eval(userNo: String): Unit = {
    data.get(userNo).map(collect(_))
  }

}
