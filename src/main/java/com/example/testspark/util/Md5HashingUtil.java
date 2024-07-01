package com.example.testspark.util;

import org.apache.hadoop.shaded.javax.xml.bind.DatatypeConverter;

import java.security.MessageDigest;

public class Md5HashingUtil {

    /**
     * Метод хеширует строку по алгоритму MD5
     * Если при выполнении возникло исключение возвращает пустую строку
     * @param arg1 строка хеш которой требуется вычислить
     * @return хеш строки либо (при возникновении исключения) пустую строку
     */
    public static String getMd5Hash(String arg1) {
        String resultHash = "";
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(arg1.getBytes());
            byte[] digest = md.digest();
            resultHash = DatatypeConverter.printHexBinary(digest);

        }catch (Exception e) {
            e.printStackTrace();
        }
        return resultHash;
    }
}
