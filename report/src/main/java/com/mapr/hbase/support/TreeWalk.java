package com.mapr.hbase.support;

import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;

import com.mapr.hbase.support.HFileSystemLoader.TreeItemType;

public class TreeWalk {

  public static void dfsWalk(Tree tree, TreeItemVisitor visitor)
  {
    dfsVisit(tree.getItem(0), visitor);
  }

  private static boolean dfsVisit(TreeItem item, TreeItemVisitor visitor) {
    if (!visitor.visit(item))
      return false;
    if (item.getData(HFileSystemLoader.ITEM_TYPE) != TreeItemType.HRegion) {
      for(int i = 0; i < item.getItemCount(); i++) {
        TreeItem child = item.getItem(i);
        if (!dfsVisit(child, visitor))
          return false;
      }
    }
    return true;
  }

  public static void bfsWalk(Tree tree, TreeItemVisitor visitor)
  {
    visitor.visit(tree.getItem(0));
    bfsVisit(tree.getItem(0), visitor);
  }

  private static void bfsVisit(TreeItem item, TreeItemVisitor visitor) {
    for(int i = 0; i < item.getItemCount(); i++) {
      TreeItem child = item.getItem(i);
      visitor.visit(child);
    }
    for(int i = 0; i < item.getItemCount(); i++) {
      TreeItem child = item.getItem(i);
      bfsVisit(child, visitor);
    }
  }
}
