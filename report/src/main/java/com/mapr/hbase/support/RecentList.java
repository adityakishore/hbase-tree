package com.mapr.hbase.support;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;


public class RecentList {
  public static final String RECENT_FILE = ".hbasemap";
  static final int CAPACITY = 5;

  int tail = 0;
  int size = 0;
  String[] list = new String[CAPACITY];
  File recentFile = null;
  MenuItem mntmRecent_ = null;
  SelectionAdapter listener_ = null;

  public RecentList(MenuItem mntmRecent, SelectionAdapter listener) throws IOException {
    mntmRecent_ = mntmRecent;
    listener_ = listener;
    String userHome = System.getenv("USERPROFILE");
    if (userHome == null) {
      userHome = System.getProperty("user.home");
    }
    recentFile = new File(userHome, RECENT_FILE);
    readListFromFile();
  }

  public void addToRecentList(String fileName) throws IOException {
    list[tail] = fileName;
    tail = (tail+1) % CAPACITY;
    if (size < CAPACITY) ++size;
    writeListToFile();
  }

  private void writeListToFile() throws IOException {
    int index = tail;
    StringBuffer data = new StringBuffer();
    for (int i = 0; i < size; i++) {
      --index;
      if (index < 0) index = CAPACITY;
      data.append(list[index]).append('\n');
    }
    FileOutputStream out = new FileOutputStream(recentFile);
    out.write(data.toString().getBytes("UTF-8"));
    out.close();
  }

  private void readListFromFile() throws IOException {
    tail = size = 0;
    BufferedReader reader = null;
    try {
      reader = new BufferedReader(new FileReader(recentFile));
    } catch (FileNotFoundException e) {
      mntmRecent_.setEnabled(false);
      return;
    }
    Menu subMenu = new Menu(mntmRecent_);
    mntmRecent_.setMenu(subMenu);

    String line = reader.readLine();
    for (int i = 0; line != null && i < CAPACITY; i++) {
      list[tail] = line;
      MenuItem menuOpenFolder = new MenuItem(subMenu, SWT.NONE);
      menuOpenFolder.setText(line);
      menuOpenFolder.addSelectionListener(listener_);
      menuOpenFolder.setData("ID", HBaseClusterMap.MNTM_RECENT_FILE);
      size = ++tail;
      line = reader.readLine();
    }
    reader.close();
    mntmRecent_.setEnabled(true);
  }
}
