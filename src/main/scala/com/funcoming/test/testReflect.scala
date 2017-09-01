package com.funcoming.test

import scala.reflect.runtime.universe._

abstract class testReflect[T: TypeTag] {

  private def tag: TypeTag[T] = typeTag[T]

  override def toString: String = {
    val tpe = tag.tpe
    println("------测试方法调用------" + tpe)
    //    collect按照条件进行过滤
    //    a field of a case class
    val allAccessors = tpe.declarations.collect {
      case m: MethodSymbol if m.isCaseAccessor => m
    }
    allAccessors foreach println
    println("-----查看区别-----")
    println(tpe.declarations)
    val mirror = runtimeMirror(getClass.getClassLoader)
    val instanceMirror = mirror.reflect(this)
    allAccessors.map {
      f =>
        val paramName = f.name.toString
        val fieldMirror = instanceMirror.reflectField(f)
        val paramValue = fieldMirror.get
        s"  $paramName:\t$paramValue"

    }.mkString("{\n", ",\n", "\n}")
  }

}
