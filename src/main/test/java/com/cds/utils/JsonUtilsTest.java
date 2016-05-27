package com.cds.utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.*;

public class JsonUtilsTest {

    private static JsonUtils binder = JsonUtils.nonDefaultMapper();

    @Test
    public void toJson() {
        //Map
        Map<String,Object> map = Maps.newLinkedHashMap();
        map.put("name", "A");
        map.put("age", 2);
        String mapString = binder.toJson(map);
        System.out.println("Map:" + mapString);
        assertThat(mapString).isEqualTo("{\"name\":\"A\",\"age\":2}");

        //List
        List<String> stringList = Lists.newArrayList("A", "B", "C");
        String listString = binder.toJson(stringList);
        System.out.println("String List:" + listString);
        assertThat(listString).isEqualTo("[\"A\",\"B\",\"C\"]");
    }

    @SuppressWarnings("unchecked")
    @Test
    public void fromJson() throws IOException {
        // Map
        String mapString = "{\"name\":\"A\",\"age\":2}";
        Map<String, Object> map = binder.fromJson(mapString, HashMap.class);
        System.out.println("Map:");
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            System.out.println(entry.getKey() + " " + entry.getValue());
        }

        // List<String>
        String listString = "[\"A\",\"B\",\"C\"]";
        List<String> stringList = binder.getMapper().readValue(listString, List.class);
        System.out.println("String List:");
        for (String element : stringList) {
            System.out.println(element);
        }
    }

    @Test
    public void nullAndEmpty() {
        // Null/Empty String for List
        List nullListResult = binder.fromJson(null, List.class);
        assertThat(nullListResult).isNull();

        nullListResult = binder.fromJson("null", List.class);
        assertThat(nullListResult).isNull();

        nullListResult = binder.fromJson("[]", List.class);
        assertThat(nullListResult).isEmpty();
    }

    @Test
    public void putTest() {
        binder.put("key", "value");
        binder.put("int", 4234);
        binder.put("float", 1.0);
        binder.put("Long", 4234L);

        List<String> list = new ArrayList<>();
        list.add("value1");
        list.add("value2");
        binder.put("list", list);

        List<Map<String, String>> listMap = new ArrayList<>();
        Map<String, String> map1 = new HashMap<>();
        Map<String, String> map2 = new HashMap<>();

        map1.put("abc", "bcd");
        map1.put("cde", "def");
        map2.put("123", "jl");
        map2.put("jklj", "jlkj");
        listMap.add(map1);
        listMap.add(map2);
        binder.put("listmap", listMap);
        System.out.println(binder.toJson());
    }

}
