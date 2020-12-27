package com.tallate.docindex.es.convert;

import com.google.gson.TypeAdapter;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import com.tallate.docindex.util.EsException;
import com.tallate.docindex.util.GsonUtil;
import com.tallate.docindex.util.TypeResolveUtil;
import com.tallate.docindex.util.UtilException;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 将source字段转换为对应的Java对象
 *
 * @author hgc
 */
public class EsDoc2ObjUtil {

    private static final String PROPERTIES = "properties";

    public static <T> T genObject(Map<String, Object> fieldMap, Class<T> clazz) throws IllegalAccessException, InstantiationException, EsException, UtilException {
        // 然后取sourceMap中的属性为实例赋值
        Field[] fields = clazz.getDeclaredFields();
        T obj = clazz.newInstance();
        // TODO：这里选择遍历类属性，尽可能去map里取，不知道反过来会不会好一些，getField()的效率如何？
        for (Field field : fields) {
            Object value = fieldMap.get(field.getName());
            if (null != value) {
                field.setAccessible(true);
                // 类型转换，比如Integer应该被转换为Double类型才能赋给Double类型的字段
                value = TypeResolveUtil.resolve(value, field.getType());
                field.set(obj, value);
            }
        }
        return obj;
    }

    /**
     * 将Map格式的source转换为对应类型
     *
     * @param sourceMap {"msg": "hello"}
     * @param clazz     目标类型
     */
    public static <T> T convert(Map<String, Object> sourceMap, String[] includes, Class<T> clazz) throws IllegalAccessException, InstantiationException, UtilException, EsException {
        if (null == sourceMap) {
            return null;
        }
        // 先提取出sourceMap中包含于includes的字段
        Map<String, Object> includedMap;
        if (null == includes) {
            includedMap = sourceMap;
        } else {
            includedMap = new HashMap<>(sourceMap.size());
            for (String include : includes) {
                Object value = sourceMap.get(include);
                if (null != value) {
                    includedMap.put(include, value);
                }
            }
        }
        // 实例化对象
        return genObject(includedMap, clazz);
    }

    /**
     * 默认不排除任何字段，尽可能转换字段
     */
    public static <T> T convert(Map<String, Object> sourceMap, Class<T> clazz) throws InstantiationException, IllegalAccessException, EsException, UtilException {
        return convert(sourceMap, null, clazz);
    }

    /**
     * 将String类型的source转换为目标对象
     */
    public static <T> T convert(String source, Class<T> clazz) throws InstantiationException, IllegalAccessException, UtilException, EsException {
        Map<String, Object> fieldMap = GsonUtil.json2obj(source, Map.class);
        return genObject(fieldMap, clazz);
    }

    /**
     * 通过Gson的TypeAdapter来反序列化json
     *
     * @param source
     * @return
     */
    public static Object convert(String source) {
        return GsonUtil.json2obj(
                source, new TypeToken<Map<String, Object>>() {
                }, new MapTypeAdapter());
    }

    /**
     * 下面这个TypeAdapter暂时没什么用
     */
    public static class MapTypeAdapter extends TypeAdapter<Object> {
        boolean docStart = false;

        @Override
        public void write(JsonWriter jsonWriter, Object o) throws IOException {
        }

        @Override
        public Object read(JsonReader reader) throws IOException {
            JsonToken token = reader.peek();
            switch (token) {
                case BEGIN_ARRAY:
                    List<Object> list = new ArrayList<Object>();
                    reader.beginArray();
                    while (reader.hasNext()) {
                        list.add(read(reader));
                    }
                    reader.endArray();
                    return list;

                case BEGIN_OBJECT:
                    Map<String, Object> map = new HashMap<String, Object>();
                    reader.beginObject();
                    while (reader.hasNext()) {
                        map.put(reader.nextName(), read(reader));
                    }
                    reader.endObject();
                    return map;
                case STRING:
                    return reader.nextString();
                case NUMBER:
                    // 对数值的处理分整型和浮点型
                    double dbNum = reader.nextDouble();
                    // 数字超过long的最大值，直接返回double类型
                    if (dbNum > Long.MAX_VALUE) {
                        return dbNum;
                    }
                    // 判断是否可以转换为long
                    long lNum = (long) dbNum;
                    if (dbNum == lNum) {
                        return lNum;
                    }
                    return dbNum;
                case BOOLEAN:
                    return reader.nextBoolean();
                case NULL:
                    reader.nextNull();
                    return null;
                default:
                    throw new IOException("不支持的数据类型");

            }
        }
    }
}
