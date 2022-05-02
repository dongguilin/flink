package org.example.flink._1_7.join.table_function.udtf

import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.table.functions.{FunctionContext, TableFunction}
import org.example.flink._1_7.join.table_function.udtf.UserUdtf.{data, runtimeContext}

class UserUdtf extends TableFunction[User] {

  /**
   * 加载维表数据
   *
   * @param context
   */
  override def open(context: FunctionContext): Unit = {
    runtimeContext = context.getRuntimeContext
    println("open UserUdtf", runtimeContext.getTaskNameWithSubtasks, runtimeContext.getUserCodeClassLoader.getClass)
    UserUdtf.synchronized {
      if (data == null) {
        println("init data...")
        data = Map(
          "0001" -> User("小伊", "女", 20, "郑州"),
          "0002" -> User("小二", "女", 22, "郑州"),
          "0003" -> User("张三", "男", 26, "洛阳"),
          "0004" -> User("李四", "男", 22, "信阳"),
          "0005" -> User("王五", "男", 36, "洛阳"),
          "0006" -> User("赵六", "男", 21, "新乡"),
          "0007" -> User("小七", "女", 18, "郑州")
        )
      }
    }
  }

  def eval(userNo: String): Unit = {
    data.get(userNo).map(collect(_))
  }

  /**
   * for test
   *
   * @param logId
   * @param userNo
   */
  def eval(logId: String, userNo: String): Unit = {
    println(runtimeContext.getTaskNameWithSubtasks, logId, userNo)
    data.get(userNo).map(collect(_))
  }

}

object UserUdtf {
  var data: Map[String, User] = _
  var runtimeContext: RuntimeContext = _
}

case class User(userName: String, gender: String, age: Int, city: String)
