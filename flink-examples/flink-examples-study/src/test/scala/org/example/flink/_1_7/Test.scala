package org.example.flink._1_7

import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml
import org.scalatest.funsuite.AnyFunSuite

import java.io.FileInputStream
import java.util.{Map => JavaMap}


class Test extends AnyFunSuite {

  test("yaml") {
    val in = new FileInputStream(this.getClass.getClassLoader.getResource("org/example/flink/_1_7/Demo_Table_1.yaml").getFile)
    val root = new Yaml().loadAs(in, classOf[JavaMap[String, String]])

    import collection.JavaConverters._
    root.asScala.map(_._2).foreach(println)
  }

}
