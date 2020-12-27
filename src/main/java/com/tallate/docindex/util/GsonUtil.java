package com.tallate.docindex.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.reflect.TypeToken;

/**
 * Gson工具
 * <p>
 * https://www.jianshu.com/p/e740196225a4
 *
 * @author hgc
 */
public class GsonUtil {

    private static final String DEFAULT_DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

    /**
     * 默认对对象进行反序列化
     *
     * @param json
     * @param clazz
     */
    public static <T> T json2obj(String json, Class<T> clazz) {
        Gson gson = new Gson();
        return gson.fromJson(json, clazz);
    }

    /**
     * 反序列化，并使用TypeAdapter对部分属性特殊处理
     *
     * @param json
     * @param typeAdapter 类型适配器
     */
    public static <T> T json2obj(
            String json, TypeToken<T> typeToken, TypeAdapter typeAdapter) {
        Gson gson = new GsonBuilder()
                .registerTypeAdapter(typeToken.getType(), typeAdapter)
                .setDateFormat(DEFAULT_DATE_FORMAT).setPrettyPrinting()
                .create();
        return gson.fromJson(json, typeToken.getType());
    }

    public static <T> String obj2json(T obj) {
        Gson gson = new Gson();
        return gson.toJson(obj);
    }

}
