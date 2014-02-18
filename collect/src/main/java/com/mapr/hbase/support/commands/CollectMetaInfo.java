package com.mapr.hbase.support.commands;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.mapr.hbase.support.Main;

public class CollectMetaInfo extends CommandRunner {

  private byte[] byte_buffer_ = new byte[512];
  private File zipFile_;

  @Override
  public boolean parseArgs(String[] args) {
    if (args.length < 2) {
      logErr("Missing filename.");
      Main.printUsageAndExit(System.err, 1);
    }
    zipFile_ = new File(args[1]);
    if (zipFile_.exists()) {
      logErr("The specified file %s exists.", zipFile_.getAbsolutePath());
      return false;
    }
    File parent = zipFile_.getParentFile();
    if (parent == null || !parent.exists()) {
      logErr("The parent folder of the specified file %s does not exists.",
        zipFile_.getAbsolutePath());
      return false;
    }
    return true;
  }

  @Override
  public int run() throws Exception {
    ZipOutputStream zip = new ZipOutputStream(new FileOutputStream(zipFile_));
    FileStatus[] filesOrFolders = fs_.listStatus(hbase_root_);
    for (FileStatus file : filesOrFolders) {
      String filename = file.getPath().getName();
      if (filename.equals(META_TABLE_NAME)
          || filename.equals(ROOT_TABLE_NAME)
          || filename.equals(HBASE_VERSION_FILE)) {
        addFileOrFolderToZip(fs_, zip, file);
      } else {
        addTreeInfoToZip(fs_, zip, file);
      }
    }
    zip.close();
    logMsg("HBase meta exported to '%s'", zipFile_.getAbsolutePath());
    return 0;
  }

  protected void addFileOrFolderToZip(FileSystem fs, ZipOutputStream zip, FileStatus file) throws IOException {
    if (file.isDir()) {
      ZipEntry entry = new ZipEntry(getRelativePath(file.getPath()) +"/");
      entry.setTime(file.getModificationTime());
      zip.putNextEntry(entry );
      zip.closeEntry();
      FileStatus[] children = fs.listStatus(file.getPath());
      for (FileStatus child : children) {
        addFileOrFolderToZip(fs,zip, child);
      }
    } else {
      writeFileEntry(fs, file, zip, true);
    }
  }

  protected void addTreeInfoToZip(FileSystem fs, ZipOutputStream zip, FileStatus file) throws IOException {
    if (file.isDir()) {
      ZipEntry entry = new ZipEntry(getRelativePath(file.getPath()) +"/");
      entry.setTime(file.getModificationTime());
      zip.putNextEntry(entry );
      zip.closeEntry();
      FileStatus[] children = fs.listStatus(file.getPath());
      for (FileStatus child : children) {
        addTreeInfoToZip(fs,zip, child);
      }
    } else {
      writeFileEntry(fs, file, zip,
        (file.getPath().getName().equals(".regioninfo")
        || file.getPath().getName().startsWith(".tableinfo")));
    }
  }

  protected String getRelativePath(Path path) {
    return path.toUri().toString().substring(hbase_root_uri_.length()+1);
  }

  private void writeFileEntry(FileSystem fs, FileStatus file, ZipOutputStream zip, boolean writeData) throws IOException {
    ZipEntry entry = new ZipEntry(getRelativePath(file.getPath()));
    entry.setTime(file.getModificationTime());
    zip.putNextEntry(entry );
    FSDataInputStream in = fs.open(file.getPath());
    if (writeData) {
      int read = 0;
      while((read = in.read(byte_buffer_)) > 0) {
        zip.write(byte_buffer_, 0, read);
      }
    } else {
      zip.write(String.format("%d", file.getLen()).getBytes());
    }
    in.close();
    zip.closeEntry();
  }
}
