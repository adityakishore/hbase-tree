package com.mapr.hbase.support;

import static com.mapr.hbase.support.HFileSystemLoader.ITEM_TYPE;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.NumberFormat;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Device;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;

import com.mapr.hbase.support.HFileSystemLoader.TreeItemType;

public class Utils {
  static Device device_ = Display.getCurrent();
  static NumberFormat fileSizeNumberFormat_ = NumberFormat.getInstance();
  
  static {
    fileSizeNumberFormat_.setGroupingUsed(true);
    fileSizeNumberFormat_.setMaximumFractionDigits(0);
  }
  
  static TreeItem createTreeItem(Tree tree, TreeItemType type, String text, Object data) {
    TreeItem item = new TreeItem(tree, 0);
    item.setText(text);
    item.setData(data);
    item.setData(ITEM_TYPE, type);
    item.setForeground(getColor(SWT.COLOR_BLACK));
    return item;
  }


  static TreeItem createTreeItem(TreeItem parent, TreeItemType type, String text, Object data) {
    TreeItem item = new TreeItem(parent, 0);
    item.setText(text);
    item.setData(data);
    item.setData(ITEM_TYPE, type);
    item.setForeground(getColor(SWT.COLOR_BLACK));
    return item;
  }

  static Color getColor(int color) {
    return device_.getSystemColor(color);
  }

  static protected void cleanupDir(File tempDir) {
    cleanupTempDir(tempDir);
  }

  static protected void cleanupTempDir(File thisFile) {
    if (!thisFile.getAbsolutePath().toLowerCase().contains("temp"))
      return;
    if (thisFile.isDirectory()) {
      for (File file : thisFile.listFiles())
        cleanupTempDir(file);
    }
    thisFile.delete();
  }

  static void addFileOrFolder(TreeItem root, File otherFile, boolean showHidden) {
    if (!showHidden)
      return;
    TreeItem file = createTreeItem(root, TreeItemType.HUnknown, otherFile.getName(), null);
    file.setForeground(getColor(SWT.COLOR_GRAY));
    if (otherFile.isDirectory()) {
      for (File child : otherFile.listFiles()) {
        addFileOrFolder(file, child, showHidden);
      }
    }
  }

  public static ThreadPoolExecutor createThreadPool() {
    final String n = Thread.currentThread().getName();
    return new ThreadPoolExecutor(4, 4, 60, TimeUnit.SECONDS, 
      new LinkedBlockingQueue<Runnable>(),
      new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setName(n + "-poolExecutor-" + System.currentTimeMillis());
        return t;
      }
    });
  }

  public static String getFileSize(File file) throws IOException {
    String text = readAsString(file, 20);
    long size = 0;
    if (text.startsWith("DATABLK")) {
      size = file.length();
    } else {
      size = Long.parseLong(text);
    }
    return fileSizeNumberFormat_.format(size) + " bytes";
  }

  public static String readAsString(File file, long maxBytes) throws IOException {
    StringBuffer sb = new StringBuffer((int) file.length());
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new FileReader(file));
      while(reader.ready() && maxBytes-- > 0)
        sb.append((char)reader.read());
    } finally {
      if (reader != null)
        reader.close();
    }
    return sb.toString();
  }

  public static void disposeThreadPool(ThreadPoolExecutor poolExecutor) throws Exception {
    poolExecutor.shutdown();
    poolExecutor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
  }

}
