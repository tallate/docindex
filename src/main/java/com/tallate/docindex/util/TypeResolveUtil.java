package com.tallate.docindex.util;


import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * 将对象转换为目标类型
 * 1. 需要考虑兼容性，比如Integer可以转换为Double或BigDecimal
 * 2. 考虑泛化的情况，比如ArrayList可以被转换为List
 */
public class TypeResolveUtil {

    private static BigDecimal resolveBigDecimal(Object originObj) throws UtilException {
        if (originObj == null) {
            return null;
        }
        if (originObj instanceof BigDecimal) {
            return (BigDecimal) originObj;
        } else if (originObj instanceof Integer
                || originObj instanceof Long
                || originObj instanceof Float
                || originObj instanceof Double) {
            // 使用自动拆装箱能减少一些代码，但要注意参数为null的话拆箱会报空指针
            return new BigDecimal((double) originObj);
        } else if (originObj instanceof String) {
            return new BigDecimal((String) originObj);
        } else {
            throw new UtilException("不支持的类型转换：" + originObj.getClass().getName() + " -> BigDecimal");
        }
    }

    private static <T> T resolveComplex(Map<String, Object> originObj, Class<T> targetType) throws UtilException {
        try {
            T tarObj = targetType.newInstance();
            for (Field field : targetType.getDeclaredFields()) {
                FieldResolveUtil.setFieldValue(tarObj, field.getName(), originObj.get(field.getName()));
            }
            return tarObj;
        } catch (InstantiationException | IllegalAccessException e) {
            throw new UtilException("实例化目标类型失败", e);
        }
    }

    private static Long resolveLong(Object originObj) throws UtilException {
        if (originObj == null) {
            return null;
        }
        if (originObj instanceof Integer
                || originObj instanceof Long) {
            return (long) originObj;
        } else {
            throw new UtilException("不支持的类型转换：" + originObj.getClass().getName() + " -> Long");
        }
    }

    private static Float resolveFloat(Object originObj) throws UtilException {
        if (originObj == null) {
            return null;
        }
        // 没有考虑Double -> Float的情况，这个应该不会出现、也不合理
        if (originObj instanceof Integer
                || originObj instanceof Long
                || originObj instanceof Float) {
            return (float) originObj;
        } else {
            throw new UtilException("不支持的类型转换：" + originObj.getClass().getName() + " -> Float");
        }
    }

    private static Double resolveDouble(Object originObj) throws UtilException {
        if (originObj == null) {
            return null;
        }
        if (originObj instanceof Integer
                || originObj instanceof Long
                || originObj instanceof Float
                || originObj instanceof Double) {
            return (double) originObj;
        } else {
            throw new UtilException("不支持的类型转换：" + originObj.getClass().getName() + " -> Double");
        }
    }

    private static Date resolveDate(Object originObj) throws UtilException {
        if (originObj == null) {
            return null;
        }
        if (originObj instanceof Date) {
            return (Date) originObj;
        } else if (originObj instanceof String) {
            return FormatUtil.parse((String) originObj);
        } else {
            throw new UtilException("不支持的类型转换：" + originObj.getClass().getName() + " -> Date");
        }
    }

    private static Timestamp resolveTimestamp(Object originObj) throws UtilException {
        if (originObj == null) {
            return null;
        }
        if (originObj instanceof Timestamp) {
            return (Timestamp) originObj;
        } else if (originObj instanceof Integer
                || originObj instanceof Long) {
            return new Timestamp((long) originObj);
        } else if (originObj instanceof Date) {
            return new Timestamp(((Date) originObj).getTime());
        } else if (originObj instanceof String) {
            return new Timestamp(FormatUtil.parse((String) originObj).getTime());
        } else {
            throw new UtilException("不支持的类型转换：" + originObj.getClass().getName() + " -> Timestamp");
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T resolve(Object originObj, Class<T> targetType) throws UtilException {
        if (originObj == null) {
            return null;
        }

        // 需要考虑类型的兼容问题
        if (targetType == Long.class) {
            return (T) resolveLong(originObj);
        } else if (targetType == Float.class) {
            return (T) resolveFloat(originObj);
        } else if (targetType == Double.class) {
            return (T) resolveDouble(originObj);
        } else if (targetType == Date.class) {
            return (T) resolveDate(originObj);
        } else if (targetType == BigDecimal.class) {
            return (T) resolveBigDecimal(originObj);
        } else if (targetType == Timestamp.class) {
            return (T) resolveTimestamp(originObj);
        }

        if (targetType == String.class
                || targetType == Boolean.class
                || targetType == Byte.class
                || targetType == Short.class
                || targetType == Integer.class
                || targetType == Character.class) {
            return (T) originObj;
        } else if (targetType == BigDecimal.class) {
            return (T) resolveBigDecimal(originObj);
        } else if (Map.class.isAssignableFrom(originObj.getClass())) {
            // 此时targetType必须为自定义类型
            return resolveComplex((Map<String, Object>) originObj, targetType);
        } else if (List.class.isAssignableFrom(targetType) && targetType.isAssignableFrom(originObj.getClass())) {
            return (T) originObj;
        } else {
            throw new UtilException("不支持的类型转换：" + originObj.getClass().getName() + " -> " + targetType.getName());
        }
    }

}
