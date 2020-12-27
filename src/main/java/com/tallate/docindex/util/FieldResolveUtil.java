package com.tallate.docindex.util;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.*;

@Slf4j
public class FieldResolveUtil {

    /**
     * 获取字段元数据
     */
    public static Field getField(Object target, String fieldName) throws UtilException {
        try {
            return target.getClass().getDeclaredField(fieldName);
        } catch (NoSuchFieldException e) {
            throw new UtilException("Field [" + fieldName + "] not exists in Class [" + target.getClass().getName() + "]", e);
        }
    }

    /**
     * 判断某字段是否存在
     */
    public static boolean containsFiled(Object target, String fieldName) {
        if (null == target) {
            return false;
        }
        // 根据是否抛出异常可以判断是否存在某字段
        try {
            target.getClass().getDeclaredField(fieldName);
            return true;
        } catch (NoSuchFieldException e) {
            return false;
        }
    }

    /**
     * 获取字段值
     */
    @SuppressWarnings("unchecked")
    public static <T> T getFieldValue(Object target, String fieldName) throws UtilException {
        Field field = getField(target, fieldName);
        try {
            field.setAccessible(true);
            return (T) field.get(target);
        } catch (IllegalAccessException e) {
            throw new UtilException("Field [" + fieldName + "] unAccessable in Class [" + target.getClass().getName() + "]", e);
        }
    }

    public static void setFieldValue(Object target, String fieldName, Object value) throws UtilException {
        Field field = getField(target, fieldName);
        try {
            // 设置private可访问
            field.setAccessible(true);
            // 设置final可修改
            Field modifierField = Field.class.getDeclaredField("modifiers");
            modifierField.setAccessible(true);
            modifierField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

            field.set(target, value);
        } catch (IllegalAccessException e) {
            throw new UtilException("Setting field [" + fieldName + "] for Class [" + target.getClass() + "] failed, cause: " + e.getMessage());
        } catch (NoSuchFieldException e) {
            throw new UtilException("cannot find field", e);
        }
    }

    /**
     * 获取被注解的字段
     */
    @SuppressWarnings("unchecked")
    public static List<Field> getFieldsWithAnnotation(Object target, Class annoClass) {
        List<Field> resultList = new ArrayList<>();
        Field[] fields = target.getClass().getDeclaredFields();
        for (Field field : fields) {
            if (field.getAnnotation(annoClass) != null) {
                resultList.add(field);
            }
        }
        return resultList;
    }

    /**
     * 获取对象一些字段的值并排除一部分字段
     */
    public static <T> Map<String, Object> getFieldMap(T target, Set<String> excludes) throws UtilException {
        Field[] fields = target.getClass().getDeclaredFields();
        Map<String, Object> fieldMap = new LinkedHashMap<>(fields.length);
        for (Field field : fields) {
            if (!excludes.contains(field.getName())) {
                // private的情况
                field.setAccessible(true);
                try {
                    fieldMap.put(field.getName(), field.get(target));
                } catch (IllegalAccessException e) {
                    throw new UtilException("Converting doc failed.cause:" + e.getMessage(), e);
                }
            }
        }
        return fieldMap;
    }

    /**
     * 拷贝对象属性
     * 1. int和Integer是不兼容的，需要在代码里统一，这里不处理
     * 2. 使用BeanUtils进行属性的拷贝：https://commons.apache.org/proper/commons-beanutils/
     *
     * @param source
     * @param target
     */
    public static void copyProperties(Object source, Object target) {
        BeanUtils.copyProperties(source, target);
    }

}
