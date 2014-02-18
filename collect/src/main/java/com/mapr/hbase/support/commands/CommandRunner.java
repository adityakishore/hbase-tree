package com.mapr.hbase.support.commands;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.mapr.hbase.support.ConfReader;

public abstract class CommandRunner {

  public abstract boolean parseArgs(String[] args);
  public abstract int run() throws Exception;

  public static final String ROOT_TABLE_NAME = "-ROOT-";
  public static final String META_TABLE_NAME = ".META.";
  public static final String HBASE_VERSION_FILE = "hbase.version";

  protected String hbase_root_uri_;
  protected Path hbase_root_;
  protected FileSystem fs_;
  protected Configuration conf_;
  
  public void init() throws IOException {
    String uri = ConfReader.get("hbase.rootdir", null);
    if (uri == null) {
      throw new RuntimeException("Could not find 'hbase.rootdir' in hbase-site.xml");
    }
    hbase_root_ = new Path(uri);
    conf_ = ConfReader.getConf();
    fs_ = FileSystem.get(conf_);

    hbase_root_uri_ = fs_.getFileStatus(hbase_root_).getPath().toUri().toString();
    logMsg("HBase installed at '%s'", hbase_root_uri_);
  }
  
  protected void log(Level level, String msg, Object ... args) {
    String message = String.format(msg, args);
    Logger.getLogger(this.getClass()).log(level, message);
  }

  protected void logMsg(String msg, Object ... args) {

    log(Level.INFO, msg, args);
  }

  protected void logErr(String msg, Object ... args) {
    log(Level.ERROR, msg, args);
  }

  protected void logWarn(String msg, Object ... args) {
    log(Level.WARN, msg, args);
  }
}
