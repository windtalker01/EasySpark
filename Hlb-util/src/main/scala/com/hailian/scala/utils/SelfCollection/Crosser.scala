package com.hailian.scala.utils.SelfCollection

/**
  * Copyright (C), 2015-2019, 乐信云科技有限公司
  * FileName: Crosser
  * Author: Luyj
  * Date: 2020/3/10 下午3:01
  * Description:
  * History:
  * <author>          <time>          <version>          <desc>
  * 作者姓名           修改时间           版本号              描述
  * Luyj              2020/3/10 下午3:01    v1.0              创建 
  */
trait Crosser[A, B, C] {
  def cross(as:Traversable[A], bs: Traversable[B]): Traversable[C]
}
trait LowPriorityCrosserImplicits {
  private type T[X] = Traversable[X]

  //两个集合
  implicit def crosser2[A, B] = new Crosser[A, B, (A, B)] {
    def cross(as: T[A], bs: T[B]): T[(A, B)] = {
      for(a <- as; b <- bs) yield (a, b)
    }
  }
}
object Crosser extends LowPriorityCrosserImplicits{
  private type T[X] = Traversable[X]

  //三个集合
  implicit def crosser3[A, B, C] =new Crosser[(A, B), C, (A, B, C)] {
    def cross(as: T[(A, B)], cs: T[C]): T[(A, B, C)] = {
      for((a, b) <- as; c <- cs) yield (a, b, c)
    }
  }
  //四个集合
  implicit def crosser4[A,B,C,D] = new Crosser[(A,B,C),D,(A,B,C,D)] {
    def cross( abcs: T[(A,B,C)], ds: T[D] ): T[(A,B,C,D)] = for { (a,b,c) <- abcs; d <- ds } yield (a, b, c, d)
  }
  //and so on

}
