package com.atguigu.spark.core.socketTest

class SubTask extends Serializable {

  var data:List[Int] = _
  var logic:Int=>Int = _

  def compute():List[Int]={
    data.map(logic)
  }

}
