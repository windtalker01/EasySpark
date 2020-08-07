package com.hailian.scala.utils

import breeze.linalg.DenseMatrix
import net.sourceforge.pinyin4j.PinyinHelper
import net.sourceforge.pinyin4j.format.{HanyuPinyinOutputFormat, HanyuPinyinToneType, HanyuPinyinVCharType}

/**
  * 海联软件大数据
  * FileName: ScalaStringUtils
  * Author: Luyj
  * Date: 2020/4/24 下午5:29
  * Description:
  * History:
  * <author>          <time>          <version>          <desc>
  * 作者姓名           修改时间           版本号              描述
  * Luyj              2020/4/24 下午5:29    v1.0              创建 
  */
object StringUtils {

  /***
    * @@Param [s1, s2] //参数
    * @[Java]Param [s1, s2]//java参数
    * @@description 计算两个字符串的相似度   //描述
    * @author luyj //作者
    * @@date 2020/3/4 下午8:35 //时间
    * @return Int  //返回值
    * @[java]return int //java返回值
    */
  def editDist(s1: String, s2: String): Int = {
    val s1_length = s1.length + 1
    val s2_length = s2.length + 1

    val matrix = DenseMatrix.zeros[Int](s1_length, s2_length)
    for (i <- 1.until(s1_length)) {
      matrix(i, 0) = matrix(i - 1, 0) + 1
    }

    for (j <- 1.until(s2_length)) {
      matrix(0, j) = matrix(0, j - 1) + 1
    }

    var cost = 0
    for (j <- 1.until(s2_length)) {
      for (i <- 1.until(s1_length)) {
        if (s1.charAt(i - 1) == s2.charAt(j - 1)) {
          cost = 0
        } else {
          cost = 1
        }
        matrix(i, j) = math.min(math.min(matrix(i - 1, j) + 1, matrix(i, j - 1) + 1), matrix(i - 1, j - 1) + cost)
      }
    }
    matrix(s1_length - 1, s2_length - 1)
  }

  /***
    * @@Param [s1, s2] //参数
    * @[Java]Param [s1, s2]//java参数
    * @@description 比较两个字符的拼音是否一样  //描述
    * @author luyj //作者
    * @@date 2020/3/4 下午8:37 //时间
    * @return Boolean  //返回值
    * @[java]return boolean //java返回值
    */
  def pinYinEqual(s1: String, s2: String): Boolean = {
    var res: Boolean = false
    var cnt: Int = 0
    if (s1 != null && s2 != null && s1.length == s2.length) {
      for (i <- 0 until s1.length if s1(i) != null && s2(i) != null) {
        val s1Pinyin = string2Pinyin(s1(i).toString)
        val s2Pinyin = string2Pinyin(s2(i).toString)
        if (s1(i) == s2(i) || (s1Pinyin != null && s2Pinyin != null && s1Pinyin == s2Pinyin)) {
          cnt = cnt + 1
        }
      }

      if (cnt == s1.length) res = true
    }
    res
  }

  /***
    * @@Param [inString] //参数
    * @[Java]Param [inString]//java参数
    * @@description 字符转拼音  //描述
    * @author luyj //作者
    * @@date 2020/3/4 下午8:37 //时间
    * @return _root_.scala.Predef.String  //返回值
    * @[java]return java.lang.String //java返回值
    */
  def string2Pinyin(inString: String): String = {
    //    val outString = new StringBuilder
    var outString:String = null
    val pinyinFormat = new HanyuPinyinOutputFormat()
    pinyinFormat.setToneType(HanyuPinyinToneType.WITHOUT_TONE) //没有声调
    pinyinFormat.setVCharType(HanyuPinyinVCharType.WITH_U_UNICODE) //UNICODE编码集
    for (inChar <- inString) {
      val pinyin = PinyinHelper.toHanyuPinyinStringArray(inChar, pinyinFormat)
      // 如果c不是汉字，toHanyuPinyinStringArray会返回null;如果是多音字，仅取第一个发音
      //      outString.append(if (pinyin == null) null else pinyin(0))
      outString = (if (pinyin == null) null else pinyin(0))
    }
    return outString
  }
}
