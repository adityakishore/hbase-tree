package com.mapr.hbase.support;

import java.util.ArrayList;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;

import com.mapr.hbase.support.objects.MHRegionInfo;

public class RegionInfo {
  enum RegionError {
    ERROR_NONE,
    ERROR_MISSING_REGIONINFO,
    ERROR_MISSING_IN_META,
    ERROR_MISSING_FROM_FS,
    ERROR_REGION_OVERLAP,
    ERROR_REGION_HOLE,
    ;

    long position_;

    public static final long ALL_ERRORS;
    static {
      int value = 0;
      for (RegionError error : values()) {
        value += error.position_;
      }
      ALL_ERRORS = value;
    }

    RegionError() {
      position_ = ordinal() > 0 ? 1 << ordinal() : 0;
    }
  }

  //boolean exists_ = true;
  boolean split_;
  boolean offLine_;
  long id_;
  String encoded_;
  String server_;
  long startCode_ = 0;
  String splitA_ = null;
  String splitB_ = null;
  byte[] startKey_ = HConstants.EMPTY_BYTE_ARRAY;;
  byte[] endKey_ = HConstants.EMPTY_BYTE_ARRAY;
  byte[] tableName_ = null;
  transient byte [] regionName_ = HConstants.EMPTY_BYTE_ARRAY;
  int hashCode_;
  long errors_ = 0;

  private String tableNameStr = null;

  public static final RegionInfo EMPTY_REGION_INFO = new RegionInfo();

  public void copyFromHRegionInfo(MHRegionInfo info) {
    split_ = info.isSplit();
    offLine_ = info.isOffline();
    id_ = info.getRegionId();
    startKey_ = info.getStartKey();
    endKey_ = info.getEndKey();
    tableName_ = info.getTableName();
    regionName_ = info.getRegionName();
    hashCode_ = info.hashCode();
  }

  public RegionInfo(byte[] startKey, byte[] endKey, String encoded, 
      long id, boolean split, boolean offLine) {
    split_ = split;
    offLine_ = offLine;
    id_ = id;
    startKey_ = startKey;
    endKey_ = endKey;
    encoded_ = encoded;
  }

  public RegionInfo(String encodeRegionName) {
    encoded_ = encodeRegionName;
  }

  public RegionInfo() {
    //exists_ = false;
  }

  public int compareTo(RegionInfo o) {
    if (o == null) {
      return 1;
    }

    // Compare start keys.
    int result = Bytes.compareTo(this.startKey_, o.startKey_);
    if (result != 0) {
      return result;
    }

    // Compare end keys.
    result = Bytes.compareTo(this.endKey_, o.endKey_);

    if (result != 0) {
      if (this.startKey_.length != 0
          && this.endKey_.length == 0) {
        return 1; // this is last region
      }
      if (o.startKey_.length != 0
          && o.endKey_.length == 0) {
        return -1; // o is the last region
      }
      return result;
    }

    // regionId is usually milli timestamp -- this defines older stamps
    // to be "smaller" than newer stamps in sort order.
    if (this.id_ > o.id_) {
      return 1;
    } else if (this.id_ < o.id_) {
      return -1;
    }

    if (this.offLine_ == o.offLine_)
      return 0;
    if (this.offLine_ == true) return -1;

    return 1;
  }

  public void setSplit(boolean split) {
    split_ = split;
  }

  public void setOffLine(boolean offLine) {
    offLine_ = offLine;
  }

  public void setId(long id) {
    id_ = id;
  }

  public void setEncoded(String encoded) {
    encoded_ = encoded;
  }

  public void setSplitA(String splitA) {
    splitA_ = splitA;
  }

  public void setSplitB(String splitB) {
    splitB_ = splitB;
  }

  public void setStartKey(byte[] startKey) {
    startKey_ = startKey;
  }

  public void setEndKey(byte[] endKey) {
    endKey_ = endKey;
  }

  public RegionError[] getErrors() {
    if (errors_ == 0)
      return null;
    ArrayList<RegionError> list = new ArrayList<RegionInfo.RegionError>();
    for (RegionError error : RegionError.values()) {
      if (hasError(error))
        list.add(error);
    }
    return list.toArray(new RegionError[list.size()]);
  }

  public boolean hasError(RegionError error) {
    return (errors_ & error.position_) != 0;
  }
  
  public void clearError(RegionError error) {
    errors_ &= ~error.position_;
  }

  public void setError(RegionError error) {
    errors_ |= error.position_;
  }

  public void setServer(String server) {
    server_ = server;
  }

  public void setStartCode(byte[] startCode) {
    startCode_ = Bytes.toLong(startCode);
  }

  public void setStartCode_(long startCode) {
    startCode_ = startCode;
  }

  @Override
  public String toString() {
    return encoded_;
  }

  public String getTableNameStr() {
    if (tableNameStr == null) {
      tableNameStr = Bytes.toString(tableName_);
    }
    return tableNameStr;
  }

//  public boolean isExists() {
//    return exists_;
//  }
//
//  public void setExists(boolean exists) {
//    this.exists_ = exists;
//  }
  
}
