package com.mapr.hbase.support;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.wal.HLog;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MetaUtils;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.log4j.Logger;

import com.mapr.hbase.support.RegionInfo.RegionError;
import com.mapr.hbase.support.objects.MHRegionInfo;

@SuppressWarnings("deprecation")
public class HMetaInfo {
  static final Logger LOG = Logger.getLogger(HMetaInfo.class);

  public static final String META_REGION_NAME = "1028785192";
  public static final String META_REGION_NAME_EX = "2114704891";
  public static final String ROOT_REGION_NAME = "70236052";
  static final Pattern region_p = Pattern.compile("^[0-9a-fA-F]{32}$");

  private Map<String, RegionInfo> regionInfoMap_ = new ConcurrentHashMap<String, RegionInfo>();
  private Map<String, HTableDescriptor> tableHTDMap_ = new ConcurrentHashMap<String, HTableDescriptor>();
  private FileSystem fs_;
  private Configuration conf_;
  private MetaUtils metaUtils_;
  private Path hbaseRootPath_;
  private File hbaseRoot_;
  private byte hbaseVersion_ = -1;

  private HRegion metaRegion_ = null;

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

  public HMetaInfo(FileSystem fs, File hbaseRoot) throws IOException {
    fs_ = fs;
    hbaseRoot_ = hbaseRoot;
    hbaseRootPath_ = new Path(hbaseRoot_.toURI());
    conf_ = fs_.getConf();
    metaUtils_ = new MetaUtils(conf_);
    regionInfoMap_.clear();
    
    String[] paths = {ROOT_TABLENAME+"/"+ROOT_REGION_NAME, META_TABLENAME_EX+"/"+META_REGION_NAME_EX};
    for (String path : paths) {
      File riFile = new File(hbaseRoot, path+"/"+HRegion.REGIONINFO_FILE);
      if (riFile.exists()) {
        FSDataInputStream in = fs.open(new Path(riFile.toURI().toString()));
        hbaseVersion_ = in.readByte();
        in.close();
        break;
      }
    }
  }

  public void loadMeta() throws Exception {
    HRegion r = null;
    InternalScanner s = null;
    List<KeyValue> results = new ArrayList<KeyValue>();
    Scan scan = new Scan();
    try {
      r = getMetaRegion();
      r.compactStores(true);
      s = r.getScanner(scan);
      boolean hasNext = true;
      LOG.info("Scanning META.");
      do {
        hasNext = s.next(results);
        byte[] row = results.get(0).getRow();
        String encodeRegionName = MHRegionInfo.encodeRegionName(row);
        RegionInfo ri = new RegionInfo(encodeRegionName);
        ri.setError(RegionError.ERROR_MISSING_FROM_FS);
        regionInfoMap_.put(encodeRegionName, ri);
        for (KeyValue kv: results) {
          if (Bytes.equals(kv.getQualifier(), HConstants.REGIONINFO_QUALIFIER)) {
            byte[] rinfo = kv.getValue();
            if (rinfo != null && rinfo.length > 0) {
              readRegionInfo(ri , rinfo);
            }
            else {
              ri.setError(RegionError.ERROR_MISSING_REGIONINFO);
            }
          }
          else if (Bytes.equals(kv.getQualifier(), HConstants.SERVER_QUALIFIER)) {
            ri.setServer(Bytes.toString(kv.getValue()));
          }
          else if (Bytes.equals(kv.getQualifier(), HConstants.STARTCODE_QUALIFIER)) {
            ri.setStartCode(kv.getValue());
          }
          else if (Bytes.equals(kv.getQualifier(), HConstants.SPLITA_QUALIFIER)) {
            if (kv.getValue() != null && kv.getValue().length > 0) {
              MHRegionInfo info = new MHRegionInfo();
              Writables.getWritable(kv.getValue(), info);
              ri.setSplitA(info.getEncodedName());
            }
          }
          else if (Bytes.equals(kv.getQualifier(), HConstants.SPLITB_QUALIFIER)) {
            if (kv.getValue() != null && kv.getValue().length > 0) {
              MHRegionInfo info = new MHRegionInfo();
              Writables.getWritable(kv.getValue(), info);
              ri.setSplitB(info.getEncodedName());
            }
          }
        }
        results.clear();
      } while (hasNext);
      LOG.info("Finished scanning META.");
    } finally {
      if (s != null)
        s.close();
      if (r != null)
        r.close();
    }
  }

  public void loadRegionInfoFiles() throws Exception {
    ThreadPoolExecutor poolExecutor = Utils.createThreadPool();
    LOG.info("Collecting region infos");
    for (File tableFile : hbaseRoot_.listFiles()) {
      if (isTable(tableFile)) {
        loadRegionInfo(poolExecutor, tableFile);
      }
    }
    Utils.disposeThreadPool(poolExecutor);
    LOG.info("Collected region infos");
  }

  void loadRegionInfo(ThreadPoolExecutor poolExecutor, File tableFile) {
    File[] files = tableFile.listFiles();
    if (files == null)
      return;
    for (int i = 0; i < files.length; i++) {
      File regionFile = files[i];
      if (isRegion(regionFile)) {
        poolExecutor.execute(new RegionInfoReader(regionFile));
      }
    }
  }

  boolean isTable(File table) {
    if (table.isDirectory()
        && (table.getName().equals(META_TABLENAME_EX) || table.getName().equals("-ROOT-") || table.getName()
            .charAt(0) != '.'))
      return true;
    return false;
  }

  boolean isRegion(File region) {
    if (region.isDirectory()
        && (region_p.matcher(region.getName()).matches()
            || region.getName().equals(META_REGION_NAME_EX) || region.getName().equals("70236052"))
            && new File(region, HRegion.REGIONINFO_FILE).exists()) {
      return true;
    }
    return false;
  }

  boolean isCF(File cf) {
    return cf.isDirectory() && !cf.getName().startsWith(".")
        && !cf.getName().equals("recovered.edits");
  }

  boolean isHFile(File hfile) {
    return !hfile.getName().startsWith(".");
    //!hfile.isDirectory();
    //&& region_p.matcher(hfile.getName()).matches();
  }

  RegionInfo getRegionInfo(File regionInfoFile)
      throws IOException {
    RegionInfo ri = regionInfoMap_.get(regionInfoFile.getName());
    if (ri == null) {
      ri = readRegionInfo(null, fs_, regionInfoFile);
      ri.setError(RegionError.ERROR_MISSING_IN_META);
      regionInfoMap_.put(ri.encoded_, ri);
    }
    ri.clearError(RegionError.ERROR_MISSING_FROM_FS);
    return ri;
  }

  public Map<String, RegionInfo> getEncodedRegionInfoMap() {
    return regionInfoMap_;
  }

  public synchronized HRegion getMetaRegion() throws IOException {
    if (metaRegion_ == null) {
      metaRegion_ = getHRegion(META_TABLEDESC, META_REGION_NAME_EX);
    }
    return metaRegion_;
  }

  public HRegion getHRegion(HTableDescriptor tableDesc, String regionName)
      throws IOException {
    HRegionInfo info = new HRegionInfo();
    FSDataInputStream in = fs_.open(new Path(getRegionPath(hbaseRootPath_, 
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
        return (HRegion) createHRegion.invoke(null, info, hbaseRootPath_, conf_, tableDesc, metaUtils_.getLog());
      }
      catch (NoSuchMethodException e) {
        //  0.90.x
        //  HRegion.createHRegion(HRegionInfo, Path, Configuration);
        Method createHRegion = HRegion.class.getDeclaredMethod("createHRegion",
            new Class[] {HRegionInfo.class, Path.class, Configuration.class});
        return (HRegion) createHRegion.invoke(null, info, hbaseRootPath_, conf_);
      }
    } catch (Exception e) {
      if (e instanceof IOException)
        throw (IOException)e;
      else
        throw new IOException(e);
    }
  }

  public static Path getTablePath(Path root, String tableName) {
    return new Path(root, tableName);
  }

  public static Path getRegionPath(Path root,
      String tableName, String regionName) {
    return new Path(getTablePath(root, tableName), regionName);
  }

  public static HRegionInfo regionInfoFromFile(FileSystem fs, File riFile)
      throws IOException {
    HRegionInfo info = new HRegionInfo();
    FSDataInputStream in = fs.open(
      new Path(riFile.toURI().toString(), HRegion.REGIONINFO_FILE));
    info.readFields(in);
    in.close();
    return info;
  }

  class RegionInfoReader implements Runnable {
    private File hriFile_;

    public RegionInfoReader(File hriFile) {
      hriFile_ = hriFile;
    }

    @Override
    public void run() {
      try {
        getRegionInfo(hriFile_);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
  }

  public RegionInfo getRegionInfo(String text) {
    return regionInfoMap_.get(text);
  }

  public RegionInfo getMetaRegionInfo() {
    return regionInfoMap_.get(META_REGION_NAME_EX);
  }
  
  public RegionInfo getRootRegionInfo() {
    return regionInfoMap_.get(ROOT_REGION_NAME);
  }

  public byte getHbaseVersion() {
    return hbaseVersion_;
  }

  public RegionInfo readRegionInfo(RegionInfo ri, FileSystem fs, File riFile) throws IOException {
    FSDataInputStream in = null;
    try {
      in = fs.open(new Path(riFile.toURI().toString(), HRegion.REGIONINFO_FILE));
      return readRegionInfo(ri, in);
    } finally {
      if (in != null)
        in.close();
    }
  }
  
  public RegionInfo readRegionInfo(RegionInfo ri, byte[] data) throws IOException {
    DataInputBuffer in = new DataInputBuffer();
    try {
      in.reset(data, 0, data.length);
      return readRegionInfo(ri, in);
    } finally {
      in.close();
    }
  }

  public RegionInfo readRegionInfo(RegionInfo ri, DataInput in) throws IOException {
    if (ri == null)
      ri = new RegionInfo();

    byte version = in.readByte();
    if (version == 0) {
      // This is the old HRI that carried an HTD.  Migrate it.  The below
      // was copied from the old 0.90 HRI readFields.
      ri.endKey_ = Bytes.readByteArray(in);
      ri.offLine_ = in.readBoolean();
      ri.id_ = in.readLong();
      ri.regionName_ = Bytes.readByteArray(in);
      ri.split_ = in.readBoolean();
      ri.startKey_ = Bytes.readByteArray(in);
      try {
        HTableDescriptor htd = new HTableDescriptor();
        htd.readFields(in);
        ri.tableName_ = htd.getName();
        setHTD(htd.getNameAsString(), htd);
      } catch(EOFException eofe) {
         throw new IOException("HTD not found in input buffer", eofe);
      }
      ri.hashCode_ = in.readInt();
    } else {
      ri.endKey_ = Bytes.readByteArray(in);
      ri.offLine_ = in.readBoolean();
      ri.id_ = in.readLong();
      ri.regionName_ = Bytes.readByteArray(in);
      ri.split_ = in.readBoolean();
      ri.startKey_ = Bytes.readByteArray(in);
      ri.tableName_ = Bytes.readByteArray(in);
      ri.hashCode_ = in.readInt();
    }
    ri.encoded_ = HRegionInfo.encodeRegionName(ri.regionName_);
    return ri;
  }
  
  public void writeRegionInfo(RegionInfo ri, DataOutput out) throws IOException {
    out.writeByte(hbaseVersion_);
    Bytes.writeByteArray(out, ri.endKey_);
    out.writeBoolean(ri.offLine_);
    out.writeLong(System.currentTimeMillis());
    Bytes.writeByteArray(out, ri.regionName_);
    out.writeBoolean(ri.split_);
    Bytes.writeByteArray(out, ri.startKey_);
    Bytes.writeByteArray(out, ri.tableName_);
    out.writeInt(ri.hashCode_);

  }

  public HTableDescriptor getHTD(String name) {
    return tableHTDMap_.get(name);
  }

  public void setHTD(String name, HTableDescriptor htd) {
    tableHTDMap_.put(name, htd);
  }
}
