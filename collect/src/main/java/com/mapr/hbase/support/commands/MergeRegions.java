package com.mapr.hbase.support.commands;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MetaUtils;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.DataInputBuffer;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import com.mapr.hbase.support.Main;
import com.mapr.hbase.support.objects.MHRegionInfo;

@SuppressWarnings("deprecation")
public class MergeRegions extends CommandRunner {

  protected File scriptFile_ = null;

  protected MetaUtils metaUtils_;

  private boolean localMeta_ = false;

  public static final String META_REGION_NAME = "1028785192";
  public static final String META_REGION_NAME_EX = "2114704891";
  public static final String ROOT_REGION_NAME = "70236052";
  public static final String ROOT_TABLENAME = "-ROOT-";
  public static final String META_TABLENAME = ".META.";
  public static final String META_TABLENAME_REGEX = "\\.META\\.";
  public static final String META_TABLENAME_EX = "-META-";
  public static final byte[] META_TABLE_BYTES = Bytes.toBytes(META_TABLENAME_EX);
  public static final HTableDescriptor META_TABLEDESC;
  static {
    HTableDescriptor desc = new HTableDescriptor();
    desc.setName(META_TABLE_BYTES);
    desc.addFamily(
      new HColumnDescriptor(HConstants.CATALOG_FAMILY,
        10, // Ten is arbitrary number.  Keep versions to help debugging.
        Compression.Algorithm.NONE.getName(), true, true, 8 * 1024,
        HConstants.FOREVER, StoreFile.BloomType.NONE.toString(),
        HConstants.REPLICATION_SCOPE_LOCAL)
        );
    META_TABLEDESC = new HTableDescriptor(desc);
  }

  @Override
  public boolean parseArgs(String[] args) {
    if (args.length < 2) {
      logErr("Missing xml file.");
      Main.printUsageAndExit(System.err, 1);
    }
    for (int i = 1; i < args.length; i++) {
      String arg = args[i];
      if (arg.equals("--local")) {
        localMeta_ = true;
      } else {
        File file = new File(arg);
        if (file.exists()) {
          scriptFile_ = file;
        }
      }
    }
    if (scriptFile_ == null) {
      logErr("No input xml file was specified.");
      return false;
    }
    return true;
  }

  @Override
  public void init() throws IOException {
    // TODO Auto-generated method stub
    super.init();
    metaUtils_ = new MetaUtils(conf_);
  }

  @Override
  public int run() throws Exception {
    DocumentBuilderFactory docBuilderFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = docBuilderFactory.newDocumentBuilder();
    Document doc = null;
    Element root = null;
    InputStream in = new BufferedInputStream(new FileInputStream(scriptFile_));
    try {
      doc = builder.parse(in);
      root = doc.getDocumentElement();
    } catch (Throwable t) {
      logErr("Unable to parse script file %s. %s", scriptFile_.getAbsolutePath(), t.getMessage());
      return 1;
    } finally {
      in.close();
    }

    String versionStr = root.getElementsByTagName("version").item(0).getTextContent();
    byte version = Byte.parseByte(versionStr);

    String startKey = root.getElementsByTagName("startKey").item(0).getTextContent();
    byte[] startKeyInBytes = Bytes.toBytesBinary(startKey);
    String endKey = root.getElementsByTagName("endKey").item(0).getTextContent();
    byte[] endKeyInBytes = Bytes.toBytesBinary(endKey);
    String tableDescStr = root.getElementsByTagName("table").item(0).getTextContent();
    byte[] tableDescInBytes = Base64.decode(tableDescStr);
    DataInputBuffer inBuffer = new DataInputBuffer();
    inBuffer.reset(tableDescInBytes, tableDescInBytes.length);
    HTableDescriptor tableDesc = new HTableDescriptor();
    tableDesc.readFields(inBuffer);
    
    ArrayList<String> familyList = new ArrayList<String>();
    
    for (HColumnDescriptor family : tableDesc.getFamilies()) {
      familyList.add(family.getNameAsString());
    }
    
    NodeList regions = root.getElementsByTagName("region");
    ArrayList<String> regionList = new ArrayList<String>();
    for (int i = 0; i < regions.getLength(); i++) {
      regionList.add(regions.item(i).getTextContent());
    }

    MHRegionInfo hri = new MHRegionInfo(tableDesc.getName(), startKeyInBytes, endKeyInBytes);
    hri.setVersion(version);
    hri.setTableDesc(tableDesc);
    Path tablePath = new Path(hbase_root_, tableDesc.getNameAsString());
    Path regionFolderPath = new Path(tablePath,  hri.getEncodedName());
    fs_.mkdirs(regionFolderPath);
    makeFamilyFolders(regionFolderPath, familyList);
    moveHFiles(tablePath, regionList, familyList, regionFolderPath);

    MetaOperations metaop = localMeta_ ? new LocalMetaOperations() : new HTableMetaOperations();
    metaop.deleteRegionsFromMeta(regionList);
    metaop.addRegionToMeta(hri);
    metaop.flushAndCompactMeta();

    Path regionInfofile = new Path(regionFolderPath, HRegion.REGIONINFO_FILE);
    FSDataOutputStream out = fs_.create(regionInfofile);
    try {
    hri.write(out);
    out.write('\n');
    out.write('\n');
    out.write(Bytes.toBytes(hri.toString()));
    } finally {
      out.close();
    }

    return 0;
  }


  private void moveHFiles(Path tablePath, ArrayList<String> regionList,
      ArrayList<String> familyList, Path regionFolderPath) throws IOException {
    for(String region : regionList) {
      Path regionPath = new Path(tablePath, region);
      if (!fs_.exists(regionPath)) {
        logWarn("The region folder '%s' does not exist, skipping.", regionPath.toString());
        continue;
      }
      for (String family : familyList) {
        Path familyPath = new Path(regionPath, family);
        if (!fs_.exists(familyPath)) {
          logWarn("The cf folder '%s' does not exist, skipping.", familyPath.toString());
          continue;
        }
        Path dstFamilyPath = new Path(regionFolderPath, family);
        FileStatus[] hfiles = fs_.listStatus(familyPath);
        for (FileStatus hfile : hfiles) {
          Path hfilePath = hfile.getPath();
          if (!StoreFile.isReference(hfilePath)) {
            logMsg("Moving file '%s' to merged region", hfilePath.toString());
            Path dstPath = new Path(dstFamilyPath, hfilePath.getName());
            fs_.rename(hfilePath, dstPath);
          }
          else {
            logMsg("Not moving reference file '%s', can be ignored", hfilePath.toString());
          }
        }
      }
      Path backupRegionPath = new Path(tablePath, "."+regionPath.getName());
      fs_.rename(regionPath, backupRegionPath );
      logMsg("Renaming region folder '%s' to '%s'", regionPath.toString(), backupRegionPath.getName());
    }
  }

  private void makeFamilyFolders(Path regionFolderPath, ArrayList<String> familyList) throws IOException {
    for (String family : familyList) {
      Path familyPath = new Path(regionFolderPath, family);
      logMsg("Creating family folder '%s'", familyPath);
      fs_.mkdirs(familyPath);
    }
  }

  protected Path getRegionFolderPath(String tableName, String encodedName) {
    Path tablePath = new Path(hbase_root_, tableName);
    return new Path(tablePath, encodedName);
  }

  interface MetaOperations {
    public void deleteRegionsFromMeta(final ArrayList<String> regionList) throws IOException;
    public void addRegionToMeta(MHRegionInfo hri) throws IOException;
    public void flushAndCompactMeta() throws Exception;
  }

  class HTableMetaOperations implements MetaOperations {

    @Override
    public void deleteRegionsFromMeta(ArrayList<String> regionList) throws IOException {
      @SuppressWarnings("unchecked")
      ArrayList<String> newList = (ArrayList<String>) regionList.clone();
      HTable metaTable = new HTable(conf_, HConstants.META_TABLE_NAME);
      metaTable.setAutoFlush(true);
      Scan scan = new Scan();
      ResultScanner s = metaTable.getScanner(scan);

      Result results = null;
      logMsg("Scanning META.");
      while ((results = s.next()) != null && newList.size() > 0) {
        byte[] row = results.getRow();
        String encodeRegionName = HRegionInfo.encodeRegionName(row);
        if (newList.contains(encodeRegionName)) {
          newList.remove(encodeRegionName);
          logMsg("Deleting region '%s' in meta. Row = %s", encodeRegionName, Bytes.toStringBinary(row));
          Delete delop = new Delete(row);
          metaTable.delete(delop);
        }
      }
      if (newList.size() > 0) {
        logWarn("Following regions were not found in meta: %s", newList.toString());
      }
      logMsg("Finished scanning META.");
      metaTable.close();
    }

    @Override
    public void addRegionToMeta(MHRegionInfo hri) throws IOException {
      Put p = new Put(hri.getRegionName());
      p.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER, Writables.getBytes(hri));
      HTable metaTable = new HTable(conf_, HConstants.META_TABLE_NAME);
      metaTable.setAutoFlush(true);
      metaTable.put(p);
      metaTable.close();
    }

    @Override
    public void flushAndCompactMeta() throws Exception {
      getAdmin().flush(HConstants.META_TABLE_NAME);
      getAdmin().majorCompact(HConstants.META_TABLE_NAME);
    }
    
    private HBaseAdmin admin_ = null;
    private synchronized HBaseAdmin getAdmin() throws IOException {
      if (admin_ == null) {
        admin_ = new HBaseAdmin(conf_);
      }
      return admin_;
    }
  }

  class LocalMetaOperations implements MetaOperations {
    public void flushAndCompactMeta() throws IOException {
      HRegion metaRegion = getMetaRegion();
      metaRegion.flushcache();
      metaRegion.compactStores();
    }

    public void addRegionToMeta(MHRegionInfo hri) throws IOException {
      Put p = new Put(hri.getRegionName());
      p.add(HConstants.CATALOG_FAMILY, HConstants.REGIONINFO_QUALIFIER, Writables.getBytes(hri));
      HRegion metaRegion = getMetaRegion();
      metaRegion.put(p);
      logMsg("Added regino '%s' to META table.", hri.toString());
    }

    public void deleteRegionsFromMeta(final ArrayList<String> regionList) throws IOException {
      @SuppressWarnings("unchecked")
      ArrayList<String> newList = (ArrayList<String>) regionList.clone();
      HRegion metaRegion = getMetaRegion();
      InternalScanner s = null;
      List<KeyValue> results = new ArrayList<KeyValue>();
      Scan scan = new Scan();
      s = metaRegion.getScanner(scan);
      boolean hasNext = true;
      logMsg("Scanning META.");
      do {
        hasNext = s.next(results);
        byte[] row = results.get(0).getRow();
        String encodeRegionName = HRegionInfo.encodeRegionName(row);
        if (newList.contains(encodeRegionName)) {
          newList.remove(encodeRegionName);
          logMsg("Deleting region '%s' in meta. Row = %s", encodeRegionName, Bytes.toStringBinary(row));
          Delete delop = new Delete(row);
          metaRegion.delete(delop, null, false);
        }
        results.clear();
      } while (hasNext && newList.size() > 0);
      if (newList.size() > 0) {
        logWarn("Following regions were not found in meta: %s", newList.toString());
      }
      logMsg("Finished scanning META.");
    }

    protected HRegion metaRegion_ = null;
    private synchronized HRegion getMetaRegion() throws IOException {
      if (metaRegion_ == null) {
        metaRegion_ = getHRegion(META_TABLEDESC, META_REGION_NAME_EX);
      }
      return metaRegion_;
    }

    private HRegion getHRegion(HTableDescriptor tableDesc, String regionName)
        throws IOException {
      HRegionInfo info = new HRegionInfo();
      FSDataInputStream in = fs_.open(new Path(getRegionFolderPath(
        tableDesc.getNameAsString(), regionName), HRegion.REGIONINFO_FILE));
      info.readFields(in);
      in.close();

      try {
        try {
          //  0.92.x and newer
          //  HRegion.createHRegion(HRegionInfo, Path, Configuration, HTableDescriptor, HLog);
          Method createHRegion = HRegion.class.getDeclaredMethod("createHRegion",
            new Class[] {HRegionInfo.class, Path.class, Configuration.class, 
              HTableDescriptor.class, HLog.class});
          return (HRegion) createHRegion.invoke(null, info, hbase_root_, conf_, tableDesc, metaUtils_.getLog());
        }
        catch (NoSuchMethodException e) {
          //  0.90.x
          //  HRegion.createHRegion(HRegionInfo, Path, Configuration);
          Method openHRegion = HRegion.class.getDeclaredMethod("openHRegion",
              new Class[] {HRegionInfo.class, HLog.class, Configuration.class});
          return (HRegion) openHRegion.invoke(null, info, metaUtils_.getLog(), conf_);
        }
      } catch (Exception e) {
        if (e instanceof IOException)
          throw (IOException)e;
        else
          throw new IOException(e);
      }
    }
  }
  
  public static void main(String[] args) {
    if (!StoreFile.isReference(new Path("4115747475165261600.fkjfhgdsoi"))) {
      System.out.println();
    }
  }
}
