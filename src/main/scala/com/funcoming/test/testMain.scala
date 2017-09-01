package com.funcoming.test

import org.apache.spark.examples.mllib.AbstractParams
import scala.reflect.api.TypeCreator

object testMain {

  case class Person(name: String = "nidaye") extends testReflect[Person]

  def main(args: Array[String]): Unit = {
    val p = Person()
    println(p.toString)

  }

}
