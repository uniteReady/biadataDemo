package com.bigdata.spark_core

object implictDemo {
  def main(args: Array[String]): Unit = {

    test()
  }

  class Person
  class User extends Person
  class Child extends User

  def test[User](user: User ): Unit ={
    print(user)
  }


}
