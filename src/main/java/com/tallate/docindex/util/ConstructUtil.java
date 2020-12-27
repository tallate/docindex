package com.tallate.docindex.util;

import java.lang.reflect.Array;

/**
 * 反射构造实例工具
 *
 * @author hgc
 */
public class ConstructUtil {

    private ConstructUtil() {
        // 缺省构造方法
    }

    /**
     * 构造一个类型的实例。当前类必须有默认构造方法
     *
     * @param <T>     要构造对象的类型
     * @param voclass 要构造实例的Class
     * @return 构造完成的实例
     */
    public static <T> T construct(Class<T> voclass) throws UtilException {
        try {
            return voclass.newInstance();
        } catch (InstantiationException e) {
            throw new UtilException("Initializing failed", e);
        } catch (IllegalAccessException e) {
            throw new UtilException("Initializing failed, maybe the contructor method is private?", e);
        }
    }

    /**
     * 构造一个类型的数组实例，同时数组中的值已经初始化
     *
     * @param <T>     要构造数组的类型
     * @param voclass 数组的Class
     * @param size    数组的长度
     * @return 元素已经初始化的数组
     */
    public static <T> T[] construct(Class<T> voclass, int size) throws UtilException {
        T[] instances = declareArray(voclass, size);
        for (int i = 0; i < size; i++) {
            instances[i] = construct(voclass);
        }
        return instances;
    }

    /**
     * 构造一个类型的数组。数组中的元素没有初始化
     *
     * @param <T>     要构造数组的类型
     * @param voclass 数组的Class
     * @param size    要构造的数组长度
     * @return 元素没有初始化的数组
     */
    @SuppressWarnings("unchecked")
    public static <T> T[] declareArray(Class<T> voclass, int size) {
        return (T[]) Array.newInstance(voclass, size);
    }

    /**
     * 获取类的数组类型
     *
     * @param clazz 类
     * @return 类的数组类型
     */
    public static Class<?> getArrayClass(Class<?> clazz) throws UtilException {
        StringBuilder sb = new StringBuilder();
        sb.append("[L");
        sb.append(clazz.getName());
        sb.append(";");
        return load(sb.toString());
    }

    /**
     * 获取类的数组类型
     *
     * @param classname 类名
     * @return 类的数组类型
     */
    public static Class<?> getArrayClass(String classname) throws UtilException {
        StringBuilder sb = new StringBuilder();
        sb.append("[L");
        sb.append(classname);
        sb.append(";");
        return load(sb.toString());
    }

    /**
     * 获取类的数组名称
     *
     * @param clazz 类
     * @return 类的数组名称
     */
    public static String getArrayClassName(Class<?> clazz) {
        StringBuilder sb = new StringBuilder();
        sb.append("[L");
        sb.append(clazz.getName());
        sb.append(";");
        return sb.toString();
    }

    /**
     * 获取类的数组名称
     *
     * @param classname 类名
     * @return 类的数组名称
     */
    public static String getArrayClassName(String classname) {
        StringBuilder sb = new StringBuilder();
        sb.append("[L");
        sb.append(classname);
        sb.append(";");
        return sb.toString();
    }

    /**
     * 装载class类型
     *
     * @param name class名称
     * @return class类型
     */
    public static Class<?> load(String name) throws UtilException {
        Class<?> clazz = null;
        try {
            clazz = Class.forName(name);
        } catch (ClassNotFoundException e) {
            throw new UtilException(e);
        }
        return clazz;
    }

}
