package com.tallate.docindex.es.convert;

import com.tallate.docindex.es.EsField;
import com.tallate.docindex.util.FieldResolveUtil;
import com.tallate.docindex.util.GsonUtil;
import com.tallate.docindex.util.UtilException;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * 将对象转换为elasticsearch文档
 *
 * @author hgc
 */
public class EsObj2DocUtil {

    /**
     * 转换，只转换有字段
     */
    public static <T> String convert(T target) throws UtilException {
        // 过滤出有注解的字段
        List<Field> fieldList = FieldResolveUtil.getFieldsWithAnnotation(target, EsField.class);
        Set<String> excludes = new TreeSet<>();
        for (Field field : fieldList) {
            if (!field.getAnnotation(EsField.class).include()) {
                excludes.add(field.getName());
            }
        }
        Map<String, Object> fieldMap = FieldResolveUtil.getFieldMap(target, excludes);
        return GsonUtil.obj2json(target);
    }

}
