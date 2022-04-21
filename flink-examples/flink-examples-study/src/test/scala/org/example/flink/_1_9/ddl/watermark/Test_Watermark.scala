package org.example.flink._1_9.ddl.watermark

import org.scalatest.funsuite.AnyFunSuite

import java.util.regex.Pattern

class Test_Watermark extends AnyFunSuite {

  test("parse watermark expr") {
    val str = "`event_time` - INTERVAL '30' SECOND"
    val pattern = Pattern.compile("`(?<field>\\w+)`(?:\\s-\\sINTERVAL\\s)'(?<num>\\d+)'\\s(?<span>SECOND|MINUTE|HOUR)")
    val matcher = pattern.matcher(str)
    if (!matcher.matches()) fail()
    println(matcher.group("field"))
    println(matcher.group("num"))
    println(matcher.group("span"))
  }

}
