package com.cds.utils;

import com.cds.utils.annotation.SuppressForbidden;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by chendongsheng5 on 2017/4/18.
 */
public class JavaVersionChecker {

  private static final List<Integer> JAVA_8 = Arrays.asList(1, 8);

  private JavaVersionChecker() {
  }

  public static void main(String[] args) {
    if (args.length != 0) {
      throw new IllegalArgumentException(
          "expected zero arguments but was: " + Arrays.toString(args));
    }

    final String javaSpecificationVersion = System.getProperty("java.specification.version");

    final List<Integer> current = parse(javaSpecificationVersion);

    if (compare(current, JAVA_8) < 0) {
      exit(1);
    }
    exit(0);

  }

  private static List<Integer> parse(final String value) {
    if (!value.matches("^0*[0-9]+(\\.[0-9]+)*$")) {
      throw new IllegalArgumentException(value);
    }

    final List<Integer> version = new ArrayList<Integer>();
    final String[] components = value.split("\\.");
    for (final String component : components) {
      version.add(Integer.valueOf(component));
    }
    return version;
  }

  private static int compare(final List<Integer> left, final List<Integer> right) {
    // lexicographically compare two lists, treating missing entries as zeros
    final int len = Math.max(left.size(), right.size());
    for (int i = 0; i < len; i++) {
      final int l = (i < left.size()) ? left.get(i) : 0;
      final int r = (i < right.size()) ? right.get(i) : 0;
      if (l < r) {
        return -1;
      }
      if (r < l) {
        return 1;
      }
    }
    return 0;
  }

  @SuppressForbidden(reason = "exit")
  private static void exit(final int status) {
    System.exit(status);
  }
}
