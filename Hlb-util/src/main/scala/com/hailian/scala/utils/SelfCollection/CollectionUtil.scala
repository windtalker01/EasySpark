package com.hailian.scala.utils.SelfCollection

/**
  * Copyright (C), 2015-2019, 乐信云科技有限公司
  * FileName: CollectionUtil
  * Author: Luyj
  * Date: 2020/3/10 下午3:32
  * Description:
  * History:
  * <author>          <time>          <version>          <desc>
  * 作者姓名           修改时间           版本号              描述
  * Luyj              2020/3/10 下午3:32    v1.0              创建 
  */
object CollectionUtil {
  implicit class Crossable[A](xs: Traversable[A]){
    def cross[B, C](ys: Traversable[B])(implicit crosser: Crosser[A, B, C]):Traversable[C] = crosser.cross(xs, ys)
  }
}
