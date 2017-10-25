package com.wade.ops.harmonius;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

/**
 * Copyright: (c) 2017 Asiainfo
 *
 * @desc:
 * @auth: steven.zhou
 * @date: 2017/09/01
 */
public final class TripleDES {

    private static final String ALGORITHM = "DESede";

    static final byte[] keyBytes = {
            (byte) 0x11, (byte) 0x22, (byte) 0x4F, (byte) 0x58, (byte) 0x88, (byte) 0x10,
            (byte) 0x40, (byte) 0x38, (byte) 0x28, (byte) 0x25, (byte) 0x79, (byte) 0x51,
            (byte) 0xCB, (byte) 0xDD, (byte) 0x55, (byte) 0x66, (byte) 0x77, (byte) 0x29,
            (byte) 0x74, (byte) 0x98, (byte) 0x30, (byte) 0x40, (byte) 0x36, (byte) 0xE2
    };

    /**
     * 3DES加密
     *
     * @param secretKeyBytes 密钥
     * @param clearText      明文
     * @return
     */
    public static final byte[] encrypt(byte[] secretKeyBytes, byte[] clearText) {

        try {

            SecretKey deskey = new SecretKeySpec(secretKeyBytes, ALGORITHM);
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            cipher.init(Cipher.ENCRYPT_MODE, deskey);

            return cipher.doFinal(clearText);

        } catch (java.security.NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (javax.crypto.NoSuchPaddingException e) {
            e.printStackTrace();
        } catch (java.lang.Exception e) {
            e.printStackTrace();
        }

        return null;

    }

    /**
     * 3DES加密，密文转成BASE64串
     *
     * @param clearText
     * @return
     */
    public static final String encrypt(String clearText) {
        byte[] bytes = encrypt(keyBytes, clearText.getBytes());
        return encodeBase64(bytes);
    }

    /**
     * 3DES解密
     *
     * @param secretKeyBytes 密钥
     * @param encryptedBytes 密文
     * @return
     */
    public static final byte[] decrypt(byte[] secretKeyBytes, byte[] encryptedBytes) {

        try {

            SecretKey deskey = new SecretKeySpec(secretKeyBytes, ALGORITHM);
            Cipher c1 = Cipher.getInstance(ALGORITHM);
            c1.init(Cipher.DECRYPT_MODE, deskey);

            return c1.doFinal(encryptedBytes);

        } catch (java.security.NoSuchAlgorithmException e) {
            e.printStackTrace();
        } catch (javax.crypto.NoSuchPaddingException e) {
            e.printStackTrace();
        } catch (java.lang.Exception e) {
            e.printStackTrace();
        }

        return null;

    }

    public static final String decrypt(String base64) {
        byte[] bytes = decodeBase64(base64);
        return new String(decrypt(keyBytes, bytes));
    }

    /**
     * 字节数组转base64编码
     */
    private static final String encodeBase64(byte[] a) {

        int aLen = a.length;
        int numFullGroups = aLen / 3;
        int numBytesInPartialGroup = aLen - 3 * numFullGroups;
        int resultLen = 4 * ((aLen + 2) / 3);

        StringBuffer result = new StringBuffer(resultLen);

        int inCursor = 0;
        for (int i = 0; i < numFullGroups; i++) {
            int byte0 = a[inCursor++] & 0xff;
            int byte1 = a[inCursor++] & 0xff;
            int byte2 = a[inCursor++] & 0xff;
            result.append(intToBase64[byte0 >> 2]);
            result.append(intToBase64[(byte0 << 4) & 0x3f | (byte1 >> 4)]);
            result.append(intToBase64[(byte1 << 2) & 0x3f | (byte2 >> 6)]);
            result.append(intToBase64[byte2 & 0x3f]);
        }

        if (numBytesInPartialGroup != 0) {
            int byte0 = a[inCursor++] & 0xff;
            result.append(intToBase64[byte0 >> 2]);

            if (numBytesInPartialGroup == 1) {
                result.append(intToBase64[(byte0 << 4) & 0x3f]);
                result.append("==");
            } else {
                int byte1 = a[inCursor++] & 0xff;
                result.append(intToBase64[(byte0 << 4) & 0x3f | (byte1 >> 4)]);
                result.append(intToBase64[(byte1 << 2) & 0x3f]);
                result.append('=');
            }
        }

        return result.toString();
    }

    private static final char intToBase64[] = {
            'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '/'
    };

    /**
     * base64编码转byte数组
     *
     * @param s
     * @return
     */
    private static final byte[] decodeBase64(String s) {

        int sLen = s.length();
        int numGroups = sLen / 4;
        if (4 * numGroups != sLen) {
            throw new IllegalArgumentException("String length must be a multiple of four.");
        }
        int missingBytesInLastGroup = 0;
        int numFullGroups = numGroups;
        if (sLen != 0) {
            if (s.charAt(sLen - 1) == '=') {
                missingBytesInLastGroup++;
                numFullGroups--;
            }
            if (s.charAt(sLen - 2) == '=') {
                missingBytesInLastGroup++;
            }
        }
        byte[] result = new byte[3 * numGroups - missingBytesInLastGroup];

        int inCursor = 0, outCursor = 0;
        for (int i = 0; i < numFullGroups; i++) {
            int ch0 = base64toInt(s.charAt(inCursor++), base64ToInt);
            int ch1 = base64toInt(s.charAt(inCursor++), base64ToInt);
            int ch2 = base64toInt(s.charAt(inCursor++), base64ToInt);
            int ch3 = base64toInt(s.charAt(inCursor++), base64ToInt);
            result[outCursor++] = (byte) ((ch0 << 2) | (ch1 >> 4));
            result[outCursor++] = (byte) ((ch1 << 4) | (ch2 >> 2));
            result[outCursor++] = (byte) ((ch2 << 6) | ch3);
        }

        if (missingBytesInLastGroup != 0) {
            int ch0 = base64toInt(s.charAt(inCursor++), base64ToInt);
            int ch1 = base64toInt(s.charAt(inCursor++), base64ToInt);
            result[outCursor++] = (byte) ((ch0 << 2) | (ch1 >> 4));

            if (missingBytesInLastGroup == 1) {
                int ch2 = base64toInt(s.charAt(inCursor++), base64ToInt);
                result[outCursor++] = (byte) ((ch1 << 4) | (ch2 >> 2));
            }
        }

        return result;
    }

    private static final int base64toInt(char c, byte[] alphaToInt) {
        int result = alphaToInt[c];
        if (result < 0) {
            throw new IllegalArgumentException("Illegal character " + c);
        }
        return result;
    }

    private static final byte base64ToInt[] = {
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
            -1, -1, -1, -1, -1, -1, -1, -1, -1, 62, -1, -1, -1, 63, 52, 53, 54,
            55, 56, 57, 58, 59, 60, 61, -1, -1, -1, -1, -1, -1, -1, 0, 1, 2, 3,
            4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21,
            22, 23, 24, 25, -1, -1, -1, -1, -1, -1, 26, 27, 28, 29, 30, 31, 32,
            33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51
    };

    public static void main(String[] args) {
        String passwd = "123";
        String enpwd = encrypt(passwd);
        System.out.println("密文: " + enpwd);
        System.out.println("明文: " + decrypt(enpwd));
    }
    
}