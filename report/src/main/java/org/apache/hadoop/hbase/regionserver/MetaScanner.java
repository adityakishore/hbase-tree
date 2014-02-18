package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;

public class MetaScanner {
  Store store_;

  public MetaScanner() {
    Path basedir = null;
    HRegion region = null;
    HColumnDescriptor hcd = null;
    FileSystem fs = null;
    Configuration conf = null;
    try {
      store_ = new Store(basedir, region, hcd, fs, conf);
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

  public static Path getRoot(Configuration conf) {
    return new Path(conf.get("hbase.rootdir"));
  }

  public static Path getTablePath(Configuration conf, String tableName) {
    return new Path(getRoot(conf), tableName);
  }

  public static Path getRegionPath(Configuration conf,
      String tableName, String regionName) {
    return new Path(getTablePath(conf, tableName), regionName);
  }
}
