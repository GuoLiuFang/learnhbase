package com.funcoming.test

import scala.reflect.runtime.universe._

abstract class testReflect[T: TypeTag] {

  private def tag: TypeTag[T] = typeTag[T]

  override def toString: String = {
    val tpe = tag.tpe
    println("------测试方法调用------" + tpe)
    ""
  }

}
