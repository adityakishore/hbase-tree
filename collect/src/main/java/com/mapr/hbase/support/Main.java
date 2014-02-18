/* Copyright (c) 2012 & onwards. MapR Tech, Inc., All rights reserved */
package com.mapr.hbase.support;

import java.io.PrintStream;

import com.mapr.hbase.support.commands.CommandRunner;

public class Main
{
  enum SupportCommand
  {
    CollectMetaInfo("com.mapr.hbase.support.commands.CollectMetaInfo",
      "<path/to/zip/file>"),
    MergeRegions("com.mapr.hbase.support.commands.MergeRegions", "<path/to/xml/file>");

    String className_;
    String options_;
    SupportCommand(String className, String options) {
      className_ = className;
      options_ = options;
    }
    String getClassName() {
      return className_;
    }
    public String getCmdOptions() {
      return name() + " " + options_;
    }
  }

  public static void main(String[] args) {
    banner();
    if (args.length == 0) {
      printUsageAndExit(System.out, 0);
    }

    try {
      SupportCommand cmd = SupportCommand.valueOf(args[0]);
      CommandRunner cmdRunner = (CommandRunner) Class.forName(cmd.getClassName()).newInstance();
      if (!cmdRunner.parseArgs(args)) {
        System.err.println("Unable to parse agruments: ");
        printUsageAndExit(System.err, 1);
      }

      cmdRunner.init();
      System.exit(cmdRunner.run());
    } catch (IllegalArgumentException e) {
      System.err.println("Unknwon command: " + args[0]);
      printUsageAndExit(System.err, 1);
    } catch (Throwable e) {
      System.err.println(e.getMessage());
      e.printStackTrace();
    }
  }

  private static void banner() {
    String text = "|\\/| _  _ |~)  (~    _  _  _  __|_\n" +
                  "|  |(_||_)|~\\  _)|_||_)|_)(_)|  |\n" +
                  "       |   v 0.024   |  |\n";
    System.err.println(text);
  }

  public static void printUsageAndExit(PrintStream out, int code) {
    out.println("Usage: hbase " + Main.class.getName() + " <command> [options...]");
    out.println("Available commands are:");
    for (int i = 0; i < SupportCommand.values().length; i++) {
      SupportCommand cmd = SupportCommand.values()[i];
      out.println("  " + cmd.getCmdOptions());
    }
    System.exit(code);
  }
}