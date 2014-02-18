package com.mapr.hbase.support;


public class StringBufferEx {
  StringBuffer buff = new StringBuffer();

  public StringBufferEx append(Object... values) {
    return append(true, values);
  }

  public StringBufferEx append(boolean shouldAppend, Object... values) {
    if (shouldAppend && values != null && values.length > 0) {
      for (Object value : values) {
        buff.append(value);
      }
    }
    return this;
  }

  public StringBufferEx appendLine(Object... values) {
    return appendLine(true, values);
  }

  public StringBufferEx appendLine(Boolean shouldAppend, Object... values) {
    if (shouldAppend) { 
      if (values != null && values.length > 0) {
        for (Object value : values) {
          buff.append(value);
        }
      }
      buff.append("\n");
    }
    return this;
  }

  public StringBufferEx append(String value) {
    return append(true, value);
  }
  public StringBufferEx append(boolean shouldAppend, String value) {
    if (shouldAppend) {
      buff.append(value);
    }
    return this;
  }
  
  public StringBufferEx append(boolean value) {
    return append(true, value);
  }
  public StringBufferEx append(boolean shouldAppend, boolean value) {
    if (shouldAppend) {
      buff.append(value);
    }
    return this;
  }
  
  @Override
  public String toString() {
    return buff.toString();
  }
  public int length() {
    return buff.length();
  }
}
