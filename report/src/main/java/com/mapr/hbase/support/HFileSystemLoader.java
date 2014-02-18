package com.mapr.hbase.support;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.log4j.Logger;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.StyleRange;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.widgets.DirectoryDialog;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.FileDialog;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableItem;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.mapr.hbase.support.RegionInfo.RegionError;

public class HFileSystemLoader extends SelectionAdapter implements Listener{

  static final Logger LOG = Logger.getLogger(HFileSystemLoader.class);

  public static final String ITEM_TYPE = "ITEM_TYPE";
  static final String ITEM_INDEX = "ITEM_INDEX";
  public static final String TREE_ITEM_INDEX = "TREE_ITEM_INDEX";

  private FileDialog fileDialog_;
  private DirectoryDialog directoryDialog_;
  private Shell shell_;
  private Tree treeTables_;
  private Table tableDetails_;
  ZipInputStream zipis_;
  private File hbaseRoot_ = null;
  private FileSystem fs_;
  private Configuration conf_;
  private StyleRange styleBold_ = new StyleRange();
  private StyledText txtDetails_;
  private LinkedHashSet<TreeItem> redItemSet_ = new LinkedHashSet<TreeItem>();
  private int treeItemIndex = 0;
  private   HMetaInfo metaInfo_;
  private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd, HH:mm:ss.SSS");
  private boolean showHidden_;
  private HBaseClusterMap hBaseClusterMap_;

  public HFileSystemLoader(HBaseClusterMap hBaseClusterMap, Shell shell, Tree treeTables, Table tableDetails, StyledText txtDetails)
      throws IOException 
      {
    hBaseClusterMap_ = hBaseClusterMap;
    shell_ = shell;
    treeTables_ = treeTables;
    tableDetails_ = tableDetails;
    txtDetails_ = txtDetails;
    fileDialog_ = new FileDialog(shell_, SWT.OPEN);
    directoryDialog_ = new DirectoryDialog(shell_);
    treeTables_.addListener(SWT.SELECTED, this);
    styleBold_.fontStyle = SWT.BOLD;
      }

  @Override
  public void widgetSelected(SelectionEvent e) {
    Object source = e.getSource();
    if (source instanceof MenuItem)
    {
      MenuItem menu = (MenuItem) source;
      boolean loadData = false;
      File selectedFile = null;
      try {
        int idData = (Integer) menu.getData("ID");
        switch (idData) {
        case HBaseClusterMap.MNTM_OPENZIPFILE:
          fileDialog_.setFileName("");
          fileDialog_.setOverwrite(true);
          String filePath = fileDialog_.open();
          if (filePath != null) {
            loadData = true;
            selectedFile = new File(filePath);
          }
          break;
        case HBaseClusterMap.MNTM_OPENFOLDER:
          directoryDialog_.setText("Select HBase root folder");
          String folderPath = directoryDialog_.open();
          if (folderPath != null) {
            loadData = true;
            selectedFile = new File(folderPath);
          }
          break;
        case HBaseClusterMap.MNTM_RECENT_FILE:
          selectedFile = new File(menu.getText());
          if (selectedFile.exists()) {
            loadData = true;
          }
          break;
        case HBaseClusterMap.MNTM_SHOWHIDDENFILES:
          showHidden_ = menu.getSelection();
          if (!showHidden_) {
            removeHidden(treeTables_.getItems()[0].getItems());
          }
          else if (hbaseRoot_ != null) {
            loadTables();
          }
          break;
        case HBaseClusterMap.MNTM_COLORMERED:
          colorMeRed(treeTables_.getItems()[0].getItems());
          break;
        case HBaseClusterMap.MNTM_NEXT_ISSUE:
          selectNextIssue();
          break;
        case HBaseClusterMap.MNTM_PREV_ISSUE:
          selectPrevIssue();
          break;
        case HBaseClusterMap.MNTM_SHOWREPORT:
          showReport();
          break;
        }

        if (loadData && selectedFile != null) {
          treeTables_.removeAll();
          if (selectedFile.isDirectory()) {
            selectedFile = findHBaseRoot(selectedFile, true);
            initHBaseFileSystem(selectedFile);
            loadTables();
          }
          else {
            directoryDialog_.setFilterPath(selectedFile.getParent());
            directoryDialog_.setText("Select the folder to extract the Zip file");
            String folderPath = directoryDialog_.open();
            if (folderPath != null) {
              loadData = true;
              File extractFolder = new File(folderPath);
              loadZip(extractFolder, selectedFile);
            }
          }
        }
      } catch (Exception e1) {
        LOG.error(e1.getMessage(), e1);
      }
    }
    else if (source instanceof Tree) {
      try {
        showSelectionInfo();
      } catch (IOException e1) {
        LOG.error(e1.getMessage(), e1);
      }
    }
  }

  private void showReport() throws IOException {
    ReportDialog dialog = new ReportDialog(shell_, 0);
    StringBufferEx text = new StringBufferEx();
    for (TreeItem item : redItemSet_) {
      TreeItem parent = item.getParentItem();
      int itemIndex = (Integer) item.getData(ITEM_INDEX);
      RegionInfo ri = metaInfo_.getRegionInfo(parent.getItem(itemIndex).getText());
      appendRegionInfo(text, ri, true);
    }
    dialog.open(text.toString());
  }

  public void showSelectionInfo() throws IOException {
    tableDetails_.removeAll();
    txtDetails_.setText("");
    TreeItem[] items = treeTables_.getSelection();
    //TreeItem item = (items == null || items.length == 0) ? null : items[0];
    ArrayList<TreeItem> regions = new ArrayList<TreeItem>();

    for (TreeItem item : items) {
      TreeItemType type = (TreeItemType) item.getData(ITEM_TYPE);
      if (type != null) {
        switch (type) {
        case HFile:
          File hfile = (File) item.getData();
          showFileInfo(tableDetails_, hfile);
        default:
          while (item != null && item.getData(ITEM_INDEX) == null) {
            item = item.getParentItem();
          }
        case HRegion:
          if (item != null && !regions.contains(item))
            regions.add(item);
          break;
        }
      }
    }

    StringBufferEx text = new StringBufferEx();
    if (regions.size() == 1) {
      int itemIndex = (Integer) regions.get(0).getData(ITEM_INDEX);
      TreeItem parent = regions.get(0).getParentItem();
      try {
        int start = itemIndex > 0 ? itemIndex-1 : itemIndex;   
        int end = itemIndex < parent.getItemCount()-1 ? itemIndex+1 : itemIndex;
        Range range = null;
        for (int i = start; i <= end; i++) {
          RegionInfo ri = metaInfo_.getRegionInfo(parent.getItem(i).getText());
          boolean currentItem = (itemIndex == i);
          if (currentItem) {
            range = appendRegionInfo(text, ri, currentItem);
          } else {
            appendRegionInfo(text, ri, false);
          }
        }
        styleBold_.start = range.start_;
        styleBold_.length = range.length_;
        txtDetails_.setText(text.toString());
        txtDetails_.setStyleRange(styleBold_);
      } catch (Throwable e1) {
        LOG.error(e1.getMessage(), e1);
      }
    }
    else {
      for (TreeItem item : regions) {
        RegionInfo ri = metaInfo_.getRegionInfo(item.getText());
        appendRegionInfo(text, ri, false);
      }
      txtDetails_.setText(text.toString());
    }
  }

  private void showFileInfo(Table table, File hfile) throws IOException {
    TableItem item = null;
    item = new TableItem (table, SWT.NONE);
    item.setText (0, "File Name");
    item.setText (1, hfile.getAbsolutePath());
    item = new TableItem (table, SWT.NONE);
    item.setText (0, "File size");
    item.setText (1, Utils.getFileSize(hfile));
    item = new TableItem (table, SWT.NONE);
    item.setText (0, "Created");
    item.setText (1, dateFormat.format(hfile.lastModified()));
    item = new TableItem (table, SWT.NONE);
  }

  class Range
  {
    int start_;
    int length_;
  }

  private Range appendRegionInfo(StringBufferEx text, RegionInfo ri, boolean detail) {
    Range range = null;
    if (ri != null) {
      range = new Range();
      range.start_ = text.length();
      text
      .appendLine("Name     => ", Bytes.toStringBinary(ri.regionName_))
      .appendLine("STARTKEY => ", Bytes.toStringBinary(ri.startKey_))
      .appendLine("ENDKEY   => ", Bytes.toStringBinary(ri.endKey_))
      .appendLine("Created  => ", dateFormat.format(ri.id_));
      if (detail) {
        text
        .appendLine((ri.server_ != null), "Server   => ", ri.server_, "-", ri.startCode_)
        .appendLine(ri.offLine_, "Offline? => true")
        .appendLine(ri.split_, "Split?   => true")
        .appendLine((ri.errors_ != 0), "Errors   => ", Arrays.toString(ri.getErrors()))
        .appendLine((ri.splitA_ != null), "SplitA   => ", ri.splitA_)
        .appendLine((ri.splitB_ != null), "SplitB   => ", ri.splitB_);
        range.length_ = text.length() - range.start_;
      }
      text.append("\n");
    }
    return range;
  }

  private void selectPrevIssue() throws IOException {
    TreeItem item = findSelectedRegion();
    Integer itemIndex = (Integer) item.getData(TREE_ITEM_INDEX);
    TreeItem last = null;
    for (TreeItem redItem : redItemSet_) {
      if (last == null) {
        last = redItem;
      }
      int redItemIndex = (Integer) redItem.getData(TREE_ITEM_INDEX);
      if (redItemIndex >= itemIndex) {
        break;
      }
      last = redItem;
    }
    treeTables_.setSelection(last);
    showSelectionInfo();
  }

  public TreeItem findSelectedRegion() {
    TreeItem[] items = treeTables_.getSelection();
    TreeItem item = null;
    item = (items == null || items.length == 0) ? treeTables_.getItem(0) : items[0];
    TreeItemType itemType = (TreeItemType) item.getData(ITEM_TYPE);
    while (itemType == null) {
      item = item.getParentItem();
      itemType = (TreeItemType) item.getData(ITEM_TYPE);
    }
    return item;
  }

  private void selectNextIssue() throws IOException {
    TreeItem item = findSelectedRegion();
    Integer itemIndex = (Integer) item.getData(TREE_ITEM_INDEX);
    for (TreeItem redItem : redItemSet_) {
      int redItemIndex = (Integer) redItem.getData(TREE_ITEM_INDEX);
      if (redItemIndex > itemIndex) {
        treeTables_.setSelection(redItem);
        showSelectionInfo();
        break;
      }
    }
  }

  private void colorMeRed(TreeItem[] items) throws IOException {
    redItemSet_.clear();
    for (TreeItem treeItem : items) {
      if (treeItem.getData(ITEM_TYPE) == TreeItemType.HTable) {
        colorTable(treeItem);
      }
    }
    hBaseClusterMap_.canShowReport(true);
  }

  private void colorTable(TreeItem treeItem) throws IOException {
    if (treeItem.getItemCount() <= 0)
      return;
    RegionInfo prevRegionInfo = metaInfo_.getRegionInfo(treeItem.getItem(0).getText());
    TreeItem prevTreeItem = treeItem.getItem(0);
    if (prevRegionInfo.startKey_.length != 0) {
      colorRegion(prevTreeItem, RegionError.ERROR_REGION_HOLE);
    }
    if (prevRegionInfo.errors_ != 0) {
      colorRegion(prevTreeItem, prevRegionInfo.getErrors()[0]);
    }

    int itemCount = treeItem.getItemCount();
    if (itemCount <= 0) return;
    RegionInfo thisRegionInfo = prevRegionInfo;
    TreeItem thisTreeItem = prevTreeItem;
    for (int i = 1; i < itemCount; i++) {
      thisTreeItem = treeItem.getItem(i);
      RegionInfo itemRI = metaInfo_.getRegionInfo(thisTreeItem.getText());
      if (itemRI == null) {
        if (thisTreeItem.getData(ITEM_TYPE) != TreeItemType.HUnknown)
          colorRegion(thisTreeItem, RegionError.ERROR_MISSING_IN_META);
        continue;
      }
      thisRegionInfo = itemRI;
      if (thisRegionInfo.split_) {
        colorRegion(thisTreeItem, RegionError.ERROR_NONE);
        continue;
      }
      if (prevRegionInfo.endKey_.length == 0 ||
          Bytes.compareTo(prevRegionInfo.endKey_, thisRegionInfo.startKey_) > 0) {
        colorRegion(prevTreeItem, RegionError.ERROR_REGION_OVERLAP);
        colorRegion(thisTreeItem, RegionError.ERROR_REGION_OVERLAP);
      }
      if ((prevRegionInfo.endKey_.length != 0 &&
          Bytes.compareTo(prevRegionInfo.endKey_, thisRegionInfo.endKey_) <= 0)
          || thisRegionInfo.endKey_.length == 0) {
        prevTreeItem = thisTreeItem;
        prevRegionInfo = thisRegionInfo;
      }
      if (thisRegionInfo.errors_ != 0) {
        colorRegion(thisTreeItem, thisRegionInfo.getErrors()[0]);
      }
    }

    // last region must have empty endKey
    if (thisRegionInfo.endKey_ != null && thisRegionInfo.endKey_.length != 0) {
      colorRegion(thisTreeItem, RegionError.ERROR_REGION_HOLE);
    }
  }

  private void colorRegion(TreeItem item, RegionError error) {
    if (redItemSet_.add(item)) {
      TreeItem current = item.getParentItem();
      while (current != null) {
        current.setExpanded(true);
        current = current.getParentItem();
      }
    }
    int color = (error == RegionError.ERROR_NONE) ? SWT.COLOR_GREEN : SWT.COLOR_RED;
    item.setForeground(Utils.getColor(color));
    RegionInfo ri = metaInfo_.getRegionInfo(item.getText());
    if (ri != null)
      ri.setError(error);
  }

  private void loadZip(File extractFolder, File selectedFile) throws Exception {
    extractFolder = File.createTempFile("hbk", "", extractFolder);
    extractFolder.delete();
    extractFolder.mkdirs();
    zipis_ = new ZipInputStream(new BufferedInputStream(new FileInputStream(selectedFile)));
    ZipEntry entry = null;
    while ((entry = zipis_.getNextEntry()) != null) {
      File file = new File(extractFolder, 
          entry.getName().replace(HMetaInfo.META_TABLENAME, HMetaInfo.META_TABLENAME_EX));
      if (entry.isDirectory()) {
        file.mkdirs();
      } else {
        file.getParentFile().mkdirs();
        BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(file));
        int ch = -1;
        while ((ch = zipis_.read()) != -1) {
          out.write(ch);
        }
        out.close();
      }
      file.setLastModified(entry.getTime());
      zipis_.closeEntry();
    }
    zipis_.close();
    File hbaseRoot = findHBaseRoot(extractFolder, true);
    updateMetaTable(hbaseRoot);
    initHBaseFileSystem(hbaseRoot);
    loadTables();
  }

  protected void updateMetaTable(File hbaseRoot) throws IOException {
    File metaTable = new File(hbaseRoot, HMetaInfo.META_TABLENAME_EX);
    File metaRegion = new File (metaTable, HMetaInfo.META_REGION_NAME);
    if (metaRegion.exists()) {
      File newMetaRegion = new File(metaTable, HMetaInfo.META_REGION_NAME_EX);
      metaRegion.renameTo(newMetaRegion);
      File metaRegionInfo = new File (newMetaRegion, HRegion.REGIONINFO_FILE);
      RandomAccessFile in = new RandomAccessFile(metaRegionInfo, "r");
      byte[] buffer = new byte[(int) in.length()];
      in.read(buffer);
      in.close();
      String text = new String(buffer, "ISO-8859-1");
      String newtext = text
          .replaceAll(HMetaInfo.META_TABLENAME_REGEX, HMetaInfo.META_TABLENAME_EX)
          .replaceAll(HMetaInfo.META_REGION_NAME, HMetaInfo.META_REGION_NAME_EX);
      buffer = newtext.getBytes("ISO-8859-1");
      metaRegionInfo.renameTo(new File(newMetaRegion, HRegion.REGIONINFO_FILE+".bak"));
      FileOutputStream out = new FileOutputStream(metaRegionInfo);
      out.write(buffer);
      out.close();
    }
  }

  private File findHBaseRoot(File base, boolean isRoot) throws IOException {
    File root = new File(base, "-ROOT-");
    if (root.exists()) {
      return base;
    }
    File[] children = base.listFiles();
    if (children != null) {
      for (File child: children) {
        if (child.isDirectory() && (child = findHBaseRoot(child, false)) != null) {
          return child;
        }
      }
    }
    if (isRoot)
      throw new FileNotFoundException("Could not find HBase root folder starting at " + base.getAbsolutePath());
    return null;
  }

  private void initHBaseFileSystem(File hbaseRoot) throws Exception {
    hBaseClusterMap_.setStatusText("");
    shell_.setText(HBaseClusterMap.APPLICATION_WND_TITLE);
    hbaseRoot_ = hbaseRoot;
    conf_ = HBaseConfiguration.create();
    String rootURI = hbaseRoot_.toURI().toString();
    conf_.set("hbase.rootdir", rootURI );
    fs_ = FileSystem.get(conf_);
    hBaseClusterMap_.canShowReport(false);
    metaInfo_ = new HMetaInfo(fs_, hbaseRoot_);
    metaInfo_.loadMeta();
    metaInfo_.loadRegionInfoFiles();
    shell_.setText(HBaseClusterMap.APPLICATION_WND_TITLE + " - " + hbaseRoot.getAbsolutePath());
  }

  private void loadTables() throws IOException {
    treeItemIndex = 0;
    txtDetails_.setText("");
    treeTables_.removeAll();
    tableDetails_.removeAll();
    TreeItem root = Utils.createTreeItem(treeTables_, TreeItemType.HRoot, "HBASE_ROOT", null);
    LOG.info("Loading tables.");
    for (File tableFile : hbaseRoot_.listFiles()) {
      if (metaInfo_.isTable(tableFile)) {
        TreeItem table = Utils.createTreeItem(root, TreeItemType.HTable, tableFile.getName(), tableFile);
        TableLoader tableLoader = new TableLoader(table, tableFile, metaInfo_, showHidden_); 
        tableLoader.loadTable(table, tableFile);
      } else {
        Utils.addFileOrFolder(root, tableFile, showHidden_);
      }
    }
    LOG.info("Finished loading tables.");
    LOG.info("Starting DFS walk.");
    TreeWalk.dfsWalk(treeTables_, new TreeItemVisitor() {
      @Override
      public boolean visit(TreeItem item) {
        item.setData(TREE_ITEM_INDEX, treeItemIndex++);
        return true;
      }
    });
    LOG.info("Finished DFS walk.");
    treeTables_.getItem(0).setExpanded(true);
    metaInfo_.getRootRegionInfo().clearError(RegionError.ERROR_MISSING_IN_META);
    metaInfo_.getMetaRegionInfo().clearError(RegionError.ERROR_MISSING_IN_META);
    colorMeRed(treeTables_.getItem(0).getItems());
  }

  private void removeHidden(TreeItem[] items) {
    if (items == null || items.length == 0)
      return;
    for (TreeItem treeItem : items) {
      if (treeItem.getData(ITEM_TYPE) == TreeItemType.HUnknown)
        treeItem.dispose();
      else
        removeHidden(treeItem.getItems());
    }
  }

  @Override
  public void handleEvent(Event event) {
    // TODO Auto-generated method stub

  }

  enum TreeItemType {
    HUnknown,
    HRoot,
    HTable,
    HRegion,
    HColumnFamily,
    HFile
  }

  public void exportMergeScript() throws IOException {
    //directoryDialog_.setText("Select the folder to export the merge script");
    fileDialog_.setText("XML file to export the merge script?");
    fileDialog_.setFileName("");
    fileDialog_.setOverwrite(false);
    String filePath = fileDialog_.open();
    if (filePath != null) {
      if (!filePath.toLowerCase().endsWith(".xml")) {
        filePath += ".xml";
      }
      File exportedFile = new File(filePath);

      try {
        TreeItem[] items = treeTables_.getSelection();
        if (items != null && items.length > 1) {
          RegionInfo riFirst = metaInfo_.getRegionInfo(items[0].getText());
          RegionInfo riLast = metaInfo_.getRegionInfo(items[items.length-1].getText());
          String tableName = riFirst.getTableNameStr();
          HTableDescriptor tableDesc = metaInfo_.getHTD(tableName);

          DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
          DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
          Document doc = docBuilder.newDocument();
          Element rootElement = doc.createElement("merge");
          doc.appendChild(rootElement);

          Element version = doc.createElement("version");
          version.setTextContent(String.valueOf(metaInfo_.getHbaseVersion()));
          rootElement.appendChild(version);

          Element startKey = doc.createElement("startKey");
          startKey.setTextContent(Bytes.toStringBinary(riFirst.startKey_));
          rootElement.appendChild(startKey);

          Element endKey = doc.createElement("endKey");
          endKey.setTextContent(Bytes.toStringBinary(riLast.endKey_));
          rootElement.appendChild(endKey);

          //  Add regions
          for (TreeItem regionItem : items) {
            Element region = doc.createElement("region");
            region.setTextContent(regionItem.getText());
            rootElement.appendChild(region);
          }

          if (tableDesc != null) {
            Element table = doc.createElement("table");
            rootElement.appendChild(table);
            DataOutputBuffer out = new DataOutputBuffer();
            tableDesc.write(out);
            String data = Base64.encodeBytes(out.getData(), 0, out.getLength());
            table.appendChild(doc.createCDATASection(data));
          }

          // write the content into xml file
          TransformerFactory transformerFactory = TransformerFactory.newInstance();
          Transformer transformer = transformerFactory.newTransformer();
          transformer.setOutputProperty(OutputKeys.INDENT, "yes");
          DOMSource source = new DOMSource(doc);
          StreamResult result = new StreamResult(exportedFile);

          // Output to console for testing
          // StreamResult result = new StreamResult(System.out);

          transformer.transform(source, result);

          LOG.info("Merge script exported to " + exportedFile.getAbsolutePath());
        }
      } catch (Exception e) {
        if (e instanceof IOException) 
          throw (IOException)e;
        else
          throw new IOException(e);
      }
    }
  }

}
