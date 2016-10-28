package org.apache.nifi.processors.gettcp;

import java.math.BigInteger;

/**
 * Created by ddewaele on 27/10/16.
 */
public class Test {

    public static void main(String[] args) {
        System.out.println("testing- " + byteArrayToHex("testing \r\n".getBytes()));
    }

    private static String toHex(String msg) {
        try {
            //return Integer.toHexString(Integer.parseInt(msg));

            return String.format("%4x", new BigInteger(1, msg.getBytes("UTF-8")));
        } catch (Exception ex) {
            ex.printStackTrace();
            return msg + " - " + ex.getMessage();
        }
    }

    final protected static char[] hexArray = "0123456789ABCDEF".toCharArray();


    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for ( int j = 0; j < bytes.length; j++ ) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    public static String byteArrayToHex(byte[] a) {
        StringBuilder sb = new StringBuilder(a.length * 2);
        for(byte b: a)
            sb.append(String.format(" [%02x]", b & 0xff));
        return sb.toString();
    }

}
