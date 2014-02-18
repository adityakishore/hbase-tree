package com.mapr.hbase.support;

import org.eclipse.swt.widgets.TreeItem;

public interface TreeItemVisitor {
  public boolean visit(TreeItem item);
}
