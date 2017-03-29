package com.cds.utils.common.timer;

import java.util.HashMap;
import java.util.Map;
import scala.Tuple4;

/**
 * Created by chendongsheng5 on 2017/3/29.
 */
public class IndexCreateTask {

  public void parseIndexColSchema(String indexColSchemaStr, Map<String, Tuple4<String, Boolean, Boolean, Boolean>> indexColSchema) {

    String[] colsStr = indexColSchemaStr.split("\\), |\\)}");

    for (String colStr : colsStr) {
      String[] tupleStr = colStr.split("=");
      String colName = tupleStr[0].replaceAll("\\{", "").trim();
      String elementsStr = tupleStr[1].replaceFirst("\\(|}", "");
      String[] elements = elementsStr.split(",");
      String type;
      boolean isStore;
      boolean isTokenized;
      boolean isToAll;
      if (5 == elements.length) {
        // 处理Decimal类型: Decimal(20,2)
        type = elements[0].trim() + "," + elements[1].trim();
        isStore = Boolean.parseBoolean(elements[2].trim());
        isTokenized = Boolean.parseBoolean(elements[3].trim());
        isToAll = Boolean.parseBoolean(elements[4].trim());
      } else {
        type = elements[0].trim();
        isStore = Boolean.parseBoolean(elements[1].trim());
        isTokenized = Boolean.parseBoolean(elements[2].trim());
        isToAll = Boolean.parseBoolean(elements[3].trim());
      }
      indexColSchema.put(colName, new Tuple4<>(type, isStore, isTokenized, isToAll));
      System.out.println(colStr);
    }
  }

  public static void main(String[] args) {
    Map<String, Tuple4<String, Boolean, Boolean,Boolean>> indexColSchema = new HashMap<>();

    indexColSchema.put("col1", new Tuple4<>("String", true, true,false));
    indexColSchema.put("col2", new Tuple4<>("Int", true, false,false));
    indexColSchema.put("col3", new Tuple4<>("Boolean", true, false,false));
    indexColSchema.put("col4", new Tuple4<>("Double", true, false,false));
    indexColSchema.put("col5", new Tuple4<>("Float", true, false,false));
    indexColSchema.put("col6", new Tuple4<>("Short", true, false,false));

    System.out.println(indexColSchema);
    IndexCreateTask task = new IndexCreateTask();

    Map<String, Tuple4<String, Boolean, Boolean,Boolean>> indexColSchema1 = new HashMap<>();
    task.parseIndexColSchema(indexColSchema.toString(), indexColSchema1);

    System.out.println(indexColSchema1);
  }
}
