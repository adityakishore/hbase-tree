package com.mapr.hbase.support;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class ConfReader {

  static Configuration conf_ = null;
  
  static {
    conf_ = HBaseConfiguration.create();
  }
  
  public static String get(String name, String defaultValue) {
    return conf_.get(name, defaultValue);
  }

  public static Configuration getConf() {
    return conf_;
  }

}
