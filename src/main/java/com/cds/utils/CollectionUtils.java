package com.cds.utils;

import java.util.Collection;
import java.util.Map;

/**
 * 集合类库
 */
public class CollectionUtils {

    /**
     * 判断集合是否为空
     *
     * @param collection
     * @return
     */
    public static boolean isEmpty(Collection<?> collection) {
        return (null == collection || collection.isEmpty());
    }

    public static boolean isEmpty(Map<?, ?> map) {
        return (null == map || map.isEmpty());
    }

}
