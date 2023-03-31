package com.wyy.tool.common;

public enum OpCode {
  CREATE("create"),
  REST_CREATE("rest-create"),
  MP_CREATE("multipart-upload"),
  READ("read"),
  CHECK_STATUS("check"),
  REST_READ("rest-read"),
  DELETE("delete"),
  MIX("mix"),
  LOOP("loop");


  private String opValue;
  OpCode(String op) {
    opValue = op;
  }

  public String getOpValue() {
    return opValue;
  }
}
