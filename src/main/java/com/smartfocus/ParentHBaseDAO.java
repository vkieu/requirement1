package com.smartfocus;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.hbase.util.Bytes;

public abstract class ParentHBaseDAO {

    private static final String DB_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss.SSSZ";

    protected static byte[] joinBytes(byte[] a, byte[] b) {
        return Bytes.add(a, b);
    }

    protected static byte[] joinBytes(byte[] a, byte[] b, byte[] c) {
        return Bytes.add(a, b, c);
    }

    protected static byte[] joinBytes(byte[] a, byte[] b, byte[] c, byte[] d) {
        return Bytes.add(Bytes.add(a, b, c), d);
    }

    protected static byte[] joinBytes(byte[] a, byte[] b, byte[] c, byte[] d, byte[] e) {
        return Bytes.add(Bytes.add(a, b, c), Bytes.add(d, e));
    }

    protected static byte[] joinBytes(byte[] a, byte[] b, byte[] c, byte[] d, byte[] e, byte[] f) {
        return Bytes.add(Bytes.add(a, b, c), Bytes.add(d, e, f));
    }


    protected static byte[] toBytes(String val) {
        return (null == val) ? null : Bytes.toBytes(val);
    }

    protected static byte[] toBytesUpper(String val) {
        return (null == val) ? null : Bytes.toBytes(val.toUpperCase());
    }

    public static byte[] toBytes(long val) {
        return Bytes.toBytes(val);
    }

    protected static byte[] toBytes(int val) {
        return Bytes.toBytes(val);
    }

    protected static byte[] toBytes(boolean val) {
        return Bytes.toBytes(val);
    }

    public static String convertCharToString(char chr) {
        return Character.toString(chr);
    }

    protected static byte[] toBytes(char val) {
        return toBytes(convertCharToString(val));
    }

    protected static byte[] toBytes(Date val) {
        return (null == val) ? null : toBytes(new SimpleDateFormat(DB_DATE_FORMAT).format(val));
    }

    protected static byte[] toBytes(BigDecimal val) {
        return (null == val) ? null : Bytes.toBytes(val);
    }

    protected static byte[] toBytes(byte val) {
        return new byte[]{val};
    }

    protected static byte[] toBytes(Float val) {
        return null == val ? null : Bytes.toBytes(val);
    }
}
