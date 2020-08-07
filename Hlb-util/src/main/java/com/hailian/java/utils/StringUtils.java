package com.hailian.java.utils;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.UUID;

/**
 * 字符串工具类
 *
 * @author Administrator
 */
public class StringUtils extends Logging {


    public synchronized  static String getPhone(String phone){

        // '0123456789','OIUYTREWQA'138TUOU2317
        if (phone != null) {
            if (phone.contains("O")) {
                phone = phone.replace("O", "0");
            }
            if (phone.contains("I")) {
                phone = phone.replace("I", "1");
            }
            if (phone.contains("U")) {
                phone = phone.replace("U", "2");
            }
            if (phone.contains("Y")) {
                phone = phone.replace("Y", "3");
            }
            if (phone.contains("T")) {
                phone = phone.replace("T", "4");
            }
            if (phone.contains("R")) {
                phone = phone.replace("R", "5");
            }
            if (phone.contains("E")) {
                phone = phone.replace("E", "6");
            }
            if (phone.contains("W")) {
                phone = phone.replace("W", "7");
            }
            if (phone.contains("Q")) {
                phone = phone.replace("Q", "8");
            }
            if (phone.contains("A")) {
                phone = phone.replace("A", "9");
            }
        }else{
            phone = "";
        }


        return phone;

    }
    /**
     * MD5方法
     *
     * @param text 明文
     * @return 密文
     * @throws Exception
     */
    public static String md5(String text) throws Exception {
        //加密后的字符串
        String encodeStr= DigestUtils.md5Hex(text );
        debug("MD5加密后的字符串为:encodeStr="+encodeStr);
        return encodeStr;
    }
    public static Long uuid(String text) throws  Exception{
        return UUID.nameUUIDFromBytes(Bytes.toBytes(text)).getLeastSignificantBits();
    }
    public static String uniqualUUID(){
        long onlyID = UUID.randomUUID().getLeastSignificantBits();
        if (onlyID <= 0) {
            onlyID = -onlyID;
        }
        return String.valueOf(onlyID);
    }

    /**
     * MD5验证方法
     *
     * @param text 明文
     * @param md5 密文
     * @return true/false
     * @throws Exception
     */
    public static boolean verify(String text,String md5) throws Exception {
        //根据传入的密钥进行验证
        String md5Text = md5(text);
        if(md5Text.equalsIgnoreCase(md5))
        {
            System.out.println("MD5验证通过");
            return true;
        }

        return false;
    }

    public static String subString(String subData) {
        return subData.substring(0, 9);
    }

    /**
     * 判断字符串是否为空
     *
     * @param str 字符串
     * @return 是否为空
     */
    public static boolean isEmpty(String str) {
        return str == null || "".equals(str);
    }

    /**
     * 判断字符串是否不为空
     *
     * @param str 字符串
     * @return 是否不为空
     */
    public static boolean isNotEmpty(String str) {
        return str != null && !"".equals(str);
    }

    /**
     * 截断字符串两侧的逗号
     *
     * @param str 字符串
     * @return 字符串
     */
    public static String trimComma(String str) {
        if (str.startsWith(",")) {
            str = str.substring(1);
        }
        if (str.endsWith(",")) {
            str = str.substring(0, str.length() - 1);
        }
        return str;
    }

    /**
     * 补全两位数字
     *
     * @param str
     * @return
     */
    public static String fulfuill(String str) {
        if (str.length() == 2) {
            return str;
        } else {
            return "0" + str;
        }
    }

    /**
     * 从拼接的字符串中提取字段
     *
     * @param str       传入的拼接字符串
     * @param delimiter 分隔符
     * @param field     指定要提取的字段，一般不使用硬编码
     * @return 字段值
     */
    public static String getFieldFromConcatString(
            String str,
            String delimiter,
            String field
    ) {
        try {
            String[] fields = str.split(delimiter);
            for (String concatField : fields) {
                // searchKeywords=|clickCategoryIds=1,2,3
                // 判断传入的数据以 = 进行切割后，长度是否为 2
                if (concatField.split("=").length == 2) {
                    String fieldName = concatField.split("=")[0];
                    String fieldValue = concatField.split("=")[1];
                    if (fieldName.equals(field)) {
                        return fieldValue;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 从拼接的字符串中给字段设置值
     *
     * @param str           传入的源字符串
     * @param delimiter     原数据分隔符
     * @param field         字段名
     * @param newFieldValue 新的 field 值
     * @return 字段值
     */
    public static String setFieldInConcatString(
            String str,
            String delimiter,
            String field,
            String newFieldValue
    ) {

        // 将原数据的字符串使用传入的指定分隔符进行分割
        String[] fields = str.split(delimiter);

        // 遍历全部的分割后的字符串
        for (int i = 0; i < fields.length; i++) {
            // 获取序列为 0 的对应 key=value 对的 key
            String fieldName = fields[i].split("=")[0];
            if (fieldName.equals(field)) {
                // 重新将传入的值进行拼接，重新拼接成 key=value 对的字符串
                String concatField = fieldName + "=" + newFieldValue;
                // 将新的 key=value 对放在指定的数组中
                fields[i] = concatField;
                // 结束循环，跳出循环
                break;
            }
        }

        // 定义一个可修改的字符串
        StringBuffer buffer = new StringBuffer("");
        // 遍历单组的 key=value 对，组合成一组 key=value 对
        for (int i = 0; i < fields.length; i++) {
            // 将一组一组的 key=value 对赋值给
            buffer.append(fields[i]);
            if (i < fields.length - 1) {
                // 对单个 key=value 对进行拼接成组合 key=value 对
                buffer.append("|");
            }
        }

        // 将可变的字符串转换为不可变字符串类型然后返回。
        return buffer.toString();
    }

    /**
     * 将传入的 String 类型的字符串转换为 Int 类型
     *
     * @param stringToInt
     * @return
     */
    public static int stringToInt(String stringToInt) {
        int b = 0;
        try {
            b = Integer.valueOf(stringToInt).intValue();
        } catch (NumberFormatException e) {
            e.printStackTrace();
        }

        return b;
    }

    /**
     * 判断一个字符串是否在另一个字符串中
     *
     * @param stringdata 要查找的全量的字符串
     * @return
     */
    public static String stringFindString(String stringdata) {

        String s = "";

        if (stringdata != null) {
                s = stringdata.replaceAll("\r|\n", "\\\\t");

        } else {
            s = stringdata;
        }

        return s;
    }

}
