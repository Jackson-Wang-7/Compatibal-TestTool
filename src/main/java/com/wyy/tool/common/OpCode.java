package com.wyy.tool.common;

public enum OpCode {
  CREATE("create"),
  S3_CREATE("s3-create"),
  MP_CREATE("multipart-upload"),
  READ("read"),
  CHECK_STATUS("check"),
  REST_READ("rest-read"),
  RANGE_READ("s3-range-read"),
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
