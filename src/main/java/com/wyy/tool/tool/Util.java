package com.wyy.tool.tool;

public class Util {

  public static String trimToObjectPath(String path) {
    if (path.startsWith("/")) {
      path = path.substring(1);
    }
    if (path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }
    return path;
  }
}
