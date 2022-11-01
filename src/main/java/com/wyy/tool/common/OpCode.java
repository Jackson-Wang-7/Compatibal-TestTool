package com.wyy.tool.common;

public enum OpCode {
  CREATE("create"),
  READ("read"),
  CHECK_STATUS("check"),
  REST_READ("rest-read"),
  DELETE("delete"),
  LOOP("loop");


  private String opValue;
  OpCode(String op) {
    opValue = op;
  }

  public String getOpValue() {
    return opValue;
  }
}
