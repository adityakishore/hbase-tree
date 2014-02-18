package com.mapr.hbase.support;

import static com.mapr.hbase.support.HFileSystemLoader.ITEM_INDEX;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.eclipse.swt.SWT;
import org.eclipse.swt.widgets.TreeItem;

import com.mapr.hbase.support.HFileSystemLoader.TreeItemType;
import com.mapr.hbase.support.RegionInfo.RegionError;
import com.mapr.hbase.support.objects.MHRegionInfo;

public class TableLoader implements Runnable {

  private static final Logger LOG = Logger.getLogger(TableLoader.class);
  private TreeItem tableItem_;
  private File tableFile_;
  HMetaInfo metaInfo_;
  private boolean showHidden_;

  public TableLoader(TreeItem tableItem, File tableFile, 
      HMetaInfo metaInfo, boolean showHidden) {
    tableItem_ = tableItem;
    tableFile_ = tableFile;
    metaInfo_ = metaInfo;
    showHidden_ = showHidden;
  }

  @Override
  public void run() {
    try {
      loadTable(tableItem_, tableFile_);
    } catch (IOException e) {
      LOG.warn("Exception loading table : " + tableFile_.getName(), e);
    }
  }

  void loadTable(TreeItem table, File tableFile) throws IOException {
    if (!loadRegions(table, tableFile)) {
      table.setForeground(Utils.getColor(SWT.COLOR_RED));
    }
  }

  public static final String TABLE_INFO_FILE_PREFIX = ".tableinfo.";

  private boolean loadRegions(TreeItem table, File tableFile) throws IOException {
    File[] tableInfos = tableFile.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.startsWith(TABLE_INFO_FILE_PREFIX);
      }
    });
    
    if (tableInfos != null && tableInfos.length > 0) {
      File tableInfoFile = tableInfos[tableInfos.length-1];
      try {
        HTableDescriptor htd = new HTableDescriptor();
        DataInputStream in = new DataInputStream(new FileInputStream(tableInfoFile));
        htd.readFields(in);
        metaInfo_.setHTD(tableFile.getName(), htd);
        in.close();
      } catch (Exception e) {
        LOG.warn("Exception parsing tableInfo : " + tableInfoFile.getAbsolutePath(), e);
      }
    }

    File[] files = tableFile.listFiles();
    if (files == null)
      return false;
    boolean foundRegions = false;
    Arrays.sort(files, new Comparator<File>() {
      @Override
      public int compare(File r1, File r2) {
        boolean isR1 = metaInfo_.isRegion(r1);
        boolean isR2 = metaInfo_.isRegion(r2);
        if (!(isR1 || isR2))
          return r1.getName().compareTo(r2.getName());
        else if (!isR1)
          return 1;
        else if (!isR2)
          return -1;

        return compareStartKey(r1, r2);
      }

      private int compareStartKey(File r1, File r2) {
        try {
          RegionInfo hri1 = metaInfo_.getRegionInfo(r1);
          RegionInfo hri2 = metaInfo_.getRegionInfo(r2);
          return hri1.compareTo(hri2);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
    
    RegionInfo last = RegionInfo.EMPTY_REGION_INFO;
    RegionInfo current = null;
    int itemIndex = 0;
    for (int i = 0; i < files.length; i++) {
      File regionFile = files[i];
      if (metaInfo_.isRegion(regionFile)) {
        current = metaInfo_.getRegionInfo(regionFile);
        if (last.endKey_.length != 0 && Bytes.compareTo(last.endKey_, current.startKey_) < 0) {
          TreeItem region = createEmptyRegion(tableFile, last.endKey_, current.startKey_, table); 
          region.setData(ITEM_INDEX, itemIndex++);
        }
        last = current;
        foundRegions = true;
        TreeItem region = Utils.createTreeItem(table, TreeItemType.HRegion, regionFile.getName(), regionFile); 
        region.setData(ITEM_INDEX, itemIndex++);
        if (!loadCFs(region, regionFile)) {
          region.setForeground(Utils.getColor(SWT.COLOR_RED));
        }
      } else {
        Utils.addFileOrFolder(table, regionFile, showHidden_);
      }
    }
    if (current != null && current.endKey_.length != 0) {
      TreeItem region = createEmptyRegion(tableFile, current.startKey_, HConstants.EMPTY_BYTE_ARRAY, table); 
      region.setData(ITEM_INDEX, itemIndex++);
    }
    return foundRegions;
  }

  protected TreeItem createEmptyRegion(File tableFile, byte[] startKey, byte[] endKey, TreeItem table) {
    byte[] tableName = Bytes.toBytes(tableFile.getName());
    MHRegionInfo hri = new MHRegionInfo(tableName, startKey, endKey);
    RegionInfo ri = new RegionInfo(hri.getEncodedName());
    ri.copyFromHRegionInfo(hri);
    ri.setError(RegionError.ERROR_REGION_HOLE);
    metaInfo_.getEncodedRegionInfoMap().put(ri.encoded_, ri);
    TreeItem region = Utils.createTreeItem(table, TreeItemType.HRegion, ri.encoded_, null);
    region.setForeground(Utils.getColor(SWT.COLOR_BLUE));
    return region;
  }

  private boolean loadCFs(TreeItem region, File regionFile) {
    File[] files = regionFile.listFiles();
    if (files == null)
      return false;

    for (File cf : files) {
      loadHFiles(region, cf);
    }
    return true;
  }

  private boolean loadHFiles(TreeItem region, File cf) {
    File[] files = cf.listFiles();
    if (files == null)
      return true;
    TreeItem cf_item = Utils.createTreeItem(region, TreeItemType.HColumnFamily, cf.getName(), cf);
    boolean foundHFile = false;
    for (File hfile : files) {
      if (metaInfo_.isHFile(hfile)) {
        foundHFile = true;
        Utils.createTreeItem(cf_item, TreeItemType.HFile, hfile.getName(), hfile);
      } else {
        Utils.addFileOrFolder(cf_item, hfile, showHidden_);
      }
    }
    return foundHFile;
  }

}
