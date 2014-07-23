package com.elex.ba.util;

/**
 * Author: liqiang
 * Date: 14-7-23
 * Time: 下午5:05
 */
public class Utils {

    public static long transformerUID(byte[] hashUID){
        int offset = 5;
        byte[] newBytes = new byte[offset];
        System.arraycopy(hashUID, 1, newBytes, 0, offset);
        long samplingUid = 0;
        for (int i = 0; i < offset; i++) {
            samplingUid <<= 8;
            samplingUid ^= newBytes[i] & 0xFF;
        }
        return samplingUid;
    }

}
