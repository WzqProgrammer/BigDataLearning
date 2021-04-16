package com.atguigu.spark.core.socketTest

class TaskPool extends Serializable {

  val data = List(1, 2, 3, 4)

  val logic = (nums: Int) => {
    nums * 2
  }

  def compute():List[Int]={
      data.map(logic)
  }

}
