package com.mapr.hbase.support;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.eclipse.swt.SWT;
import org.eclipse.swt.SWTException;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.events.FocusAdapter;
import org.eclipse.swt.events.FocusEvent;
import org.eclipse.swt.events.MenuAdapter;
import org.eclipse.swt.events.MenuEvent;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.MenuItem;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.swt.widgets.TableColumn;
import org.eclipse.swt.widgets.Text;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.swt.widgets.TreeItem;
import org.eclipse.wb.swt.SWTResourceManager;

import com.mapr.hbase.support.HFileSystemLoader.TreeItemType;

public class HBaseClusterMap {
  static final Logger LOG = Logger.getLogger(HBaseClusterMap.class);

  public static final String APPLICATION_WND_TITLE = "HBase Tree";
  private static final String SEARCH_ENCODED_REGION_NAME = "Search encoded region name...";
  public static final int MNTM_OPENZIPFILE = 10001;
  public static final int MNTM_SHOWHIDDENFILES = 10002;
  public static final int MNTM_OPENFOLDER = 10003;
  public static final int MNTM_COLORMERED = 10004;
  public static final int MNTM_NEXT_ISSUE = 10005;
  public static final int MNTM_PREV_ISSUE = 10006;
  public static final int MNTM_SHOWREPORT = 10007;

  public static final int MNTM_RECENT_FILE = 10011;

  protected Shell shlHbaseTree;
  private Table tableDetails_;
  private StyledText txtDetails_;
  private Tree treeTables_;
  private Menu mnuCtxtTree;
  private MenuItem mntmExpandAll_;
  //private RecentList recentList_;
  private MenuItem mntmShowReport;
  private Text txtFind;
  private MenuItem mntmExportMergedRegion;
  private Text txtStatus;
  /**
   * Launch the application.
   * @param args
   */
  public static void main(String[] args) {
    try {
      HBaseClusterMap window = new HBaseClusterMap();
      window.open();
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
  }

  /**
   * Open the window.
   * @throws IOException
   */
  public void open() throws IOException {
    Display display = Display.getDefault();
    createContents();
    shlHbaseTree.open();
    shlHbaseTree.layout();
    while (!shlHbaseTree.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
  }

  public void canShowReport(boolean can) {
    mntmShowReport.setEnabled(can);
  }

  private void setMenuStates() {
    mntmExpandAll_.setEnabled(treeTables_.getSelectionCount() > 0);
    mntmExportMergedRegion.setEnabled(canMergeSelection());
  }

  private boolean canMergeSelection() {
    int lastIdx = -1;
    int selectionCount = 0;
    for (TreeItem item : treeTables_.getSelection()) {
      if (item.getData(HFileSystemLoader.ITEM_TYPE) != TreeItemType.HRegion)
        return false;
      Integer itemIndex = (Integer) item.getData(HFileSystemLoader.TREE_ITEM_INDEX);
      if (itemIndex == null)
        return false;
      else if (lastIdx == -1)
        lastIdx = itemIndex;
      else if (lastIdx+1 != itemIndex)
        return false;
      else {
        lastIdx = itemIndex;
        selectionCount++;
      }
    }
    return selectionCount > 0;
  }

  /**
   * Create contents of the window.
   * @throws IOException
   */
  protected void createContents() throws IOException {
    shlHbaseTree = new Shell();
    shlHbaseTree.setFont(SWTResourceManager.getFont("Tahoma", 9, SWT.NORMAL));
    shlHbaseTree.setSize(1140, 857);
    shlHbaseTree.setText(APPLICATION_WND_TITLE);
    FillLayout fl_shlHbaseTree = new FillLayout();
    fl_shlHbaseTree.marginWidth = 3;
    fl_shlHbaseTree.marginHeight = 3;
    shlHbaseTree.setLayout(fl_shlHbaseTree);

    SashForm sashForm_2 = new SashForm(shlHbaseTree, SWT.SMOOTH | SWT.VERTICAL);

    SashForm form = new SashForm(sashForm_2, SWT.SMOOTH);
    form.setLayout(new FillLayout());

    Composite child1 = new Composite(form, SWT.NONE);
    child1.setLayout(new FillLayout());

    Composite child2 = new Composite(form, SWT.NONE);
    child2.setLayout(new FillLayout());

    SashForm sashForm = new SashForm(child2, SWT.NONE);
    sashForm.setOrientation(SWT.VERTICAL);

    txtDetails_ = new StyledText(sashForm, SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
    txtDetails_.setFont(SWTResourceManager.getFont("Courier New", 10, SWT.NORMAL));

    tableDetails_ = new Table(sashForm, SWT.BORDER | SWT.FULL_SELECTION | SWT.H_SCROLL | SWT.V_SCROLL);
    tableDetails_.setFont(SWTResourceManager.getFont("Tahoma", 9, SWT.NORMAL));
    tableDetails_.setHeaderVisible(true);
    tableDetails_.setLinesVisible(true);

    Composite composite = new Composite(child1, SWT.NONE);
    composite.setLayout(new FormLayout());

    treeTables_ = new Tree(composite, SWT.BORDER | SWT.MULTI);
    FormData fd_treeTables_ = new FormData();
    fd_treeTables_.bottom = new FormAttachment(100);
    fd_treeTables_.left = new FormAttachment(0);
    fd_treeTables_.right = new FormAttachment(100);
    treeTables_.setLayoutData(fd_treeTables_);
    treeTables_.setFont(SWTResourceManager.getFont("Courier New", 9, SWT.NORMAL));

    mnuCtxtTree = new Menu(treeTables_);
    mnuCtxtTree.addMenuListener(new MenuAdapter() {
      @Override
      public void menuShown(MenuEvent e) {
        setMenuStates();
      }
    });
    treeTables_.setMenu(mnuCtxtTree);
    final HFileSystemLoader loader = new HFileSystemLoader(this, shlHbaseTree, treeTables_, tableDetails_, txtDetails_);

    mntmExpandAll_ = new MenuItem(mnuCtxtTree, SWT.NONE);
    mntmExpandAll_.addSelectionListener(new SelectionAdapter() {
      @Override
      public void widgetSelected(SelectionEvent e) {
        if (treeTables_.getSelectionCount() > 0) {
          expandAll(treeTables_.getSelection());
        }
      }

      private void expandAll(TreeItem[] selection) {
        if (selection == null || selection.length == 0) return;
        for (TreeItem item : selection) {
          item.setExpanded(true);
          expandAll(item.getItems());
        }
      }
    });
    mntmExpandAll_.setText("Expand All");

    mntmExportMergedRegion = new MenuItem(mnuCtxtTree, SWT.NONE);
    mntmExportMergedRegion.addSelectionListener(new SelectionAdapter() {
      @Override
      public void widgetSelected(SelectionEvent e) {
        try {
          loader.exportMergeScript();
        } catch (IOException e1) {
          LOG.error(e1.getMessage(), e1);
        }
      }
    });
    mntmExportMergedRegion.setText("Generate Merged Region Script...");

    TableColumn tblclmnProperty = new TableColumn(tableDetails_, SWT.NONE);
    tblclmnProperty.setWidth(126);
    tblclmnProperty.setText("Property");

    TableColumn tblclmnValue = new TableColumn(tableDetails_, SWT.NONE);
    tblclmnValue.setWidth(651);
    tblclmnValue.setText("Value");

    Button findButton = new Button(composite, SWT.NONE);
    findButton.addSelectionListener(new SelectionAdapter() {
      @Override
      public void widgetSelected(SelectionEvent e) {
        if (txtFind.getText().length() <= 0 ||
            treeTables_.getItemCount() <= 0 ){
          return;
        }
        TreeItem[] items = treeTables_.getSelection();
        TreeItem item = null;
        item = (items == null || items.length == 0) ? treeTables_.getItem(0) : items[0];
        Integer itemIndex = (Integer) item.getData(HFileSystemLoader.TREE_ITEM_INDEX);
        while (itemIndex == null) {
          item = item.getParentItem();
          itemIndex = (Integer) item.getData(HFileSystemLoader.TREE_ITEM_INDEX);
        }
        final Integer currentIndex = itemIndex;
        final String text = txtFind.getText();
        TreeWalk.dfsWalk(treeTables_, new TreeItemVisitor() {
          @Override
          public boolean visit(TreeItem item) {
            Integer index = (Integer) item.getData(HFileSystemLoader.TREE_ITEM_INDEX);
            if (index < currentIndex)
              return true;
            if (item.getText().contains(text)) {
              treeTables_.setSelection(item);
              try {
                loader.showSelectionInfo();
              } catch (IOException e) {
                LOG.error(e.getMessage(), e);
              }
              return false;
            }
            return true;
          }
        });
      }
    });
    findButton.setText("Find");
    FormData fd_findButton = new FormData();
    fd_findButton.right = new FormAttachment(100);
    fd_findButton.top = new FormAttachment(0, 1);
    findButton.setLayoutData(fd_findButton);

    txtFind = new Text(composite, SWT.BORDER);
    txtFind.addFocusListener(new FocusAdapter() {
      @Override
      public void focusGained(FocusEvent e) {
        super.focusGained(e);
        if (txtFind.getText().equals(SEARCH_ENCODED_REGION_NAME)) {
          txtFind.setText("");
          txtFind.setForeground(SWTResourceManager.getColor(0, 0, 0));
        }
      }

      @Override
      public void focusLost(FocusEvent e) {
        super.focusLost(e);
        if (txtFind.getText().equals("")) {
          txtFind.setText(SEARCH_ENCODED_REGION_NAME);
          txtFind.setForeground(SWTResourceManager.getColor(100, 100, 100));
        }
      }
    });
    txtFind.setForeground(SWTResourceManager.getColor(100, 100, 100));
    txtFind.setText(SEARCH_ENCODED_REGION_NAME);
    fd_findButton.left = new FormAttachment(txtFind);
    fd_treeTables_.top = new FormAttachment(txtFind);
    txtFind.setFont(SWTResourceManager.getFont("Tahoma", 9, SWT.NORMAL));
    FormData fd_txtFind = new FormData();
    fd_txtFind.bottom = new FormAttachment(0, 25);
    fd_txtFind.right = new FormAttachment(100, -47);
    fd_txtFind.left = new FormAttachment(0);
    fd_txtFind.top = new FormAttachment(0, 2);
    txtFind.setLayoutData(fd_txtFind);
    treeTables_.addSelectionListener(loader);
    sashForm.setWeights(new int[] {305, 206});
    form.setWeights(new int[] {343, 780});

    Composite composite_1 = new Composite(sashForm_2, SWT.NONE);
    composite_1.setBackground(SWTResourceManager.getColor(SWT.COLOR_WHITE));
    composite_1.setLayout(new FillLayout(SWT.HORIZONTAL));

    txtStatus = new Text(composite_1, SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL | SWT.CANCEL | SWT.MULTI);
    txtStatus.setFont(SWTResourceManager.getFont("Courier New", 10, SWT.NORMAL));
    txtStatus.setEditable(false);
    txtStatus.setBackground(SWTResourceManager.getColor(SWT.COLOR_WHITE));
    txtStatus.setForeground(SWTResourceManager.getColor(SWT.COLOR_RED));
    sashForm_2.setWeights(new int[] {597, 121});

    Menu menu = new Menu(shlHbaseTree, SWT.BAR);
    shlHbaseTree.setMenuBar(menu);

    MenuItem mntmFile = new MenuItem(menu, SWT.CASCADE);
    mntmFile.setText("&File");

    Menu menu_1 = new Menu(mntmFile);
    mntmFile.setMenu(menu_1);

    MenuItem menuOpenFolder = new MenuItem(menu_1, SWT.NONE);
    menuOpenFolder.setText("&Open HBase Folder...\tCtrl+O");
    menuOpenFolder.setAccelerator(SWT.CONTROL | 'O');
    menuOpenFolder.addSelectionListener(loader);
    menuOpenFolder.setData("ID", MNTM_OPENFOLDER);
    MenuItem mntmOpenZipFile = new MenuItem(menu_1, SWT.NONE);
    mntmOpenZipFile.setData("ID", MNTM_OPENZIPFILE);
    mntmOpenZipFile.setAccelerator(SWT.CONTROL | SWT.SHIFT | 'O');
    mntmOpenZipFile.addSelectionListener(loader);
    mntmOpenZipFile.setText("Open &Zip File...\tCtrl+Shift+O");

    MenuItem mntmShowHiddenFiles = new MenuItem(menu_1, SWT.CHECK);
    mntmShowHiddenFiles.setData("ID", MNTM_SHOWHIDDENFILES);
    mntmShowHiddenFiles.addSelectionListener(loader);
    mntmShowHiddenFiles.setText("Show &Hidden Files");

    new MenuItem(menu_1, SWT.SEPARATOR);

    MenuItem mntmRecent = new MenuItem(menu_1, SWT.CASCADE);
    mntmRecent.setText("&Recent");

    //recentList_ = new RecentList(mntmRecent, loader);
    new MenuItem(menu_1, SWT.SEPARATOR);

    MenuItem mntmExit = new MenuItem(menu_1, SWT.NONE);
    mntmExit.setAccelerator(SWT.CONTROL | 'Q');
    mntmExit.addSelectionListener(new SelectionAdapter() {
      @Override
      public void widgetSelected(SelectionEvent e) {
        System.exit(0);
      }
    });
    mntmExit.setText("E&xit\tCtrl+Q");

    MenuItem mntmsearch = new MenuItem(menu, SWT.CASCADE);
    mntmsearch.setText("&Search");

    Menu menu_3 = new Menu(mntmsearch);
    mntmsearch.setMenu(menu_3);

    MenuItem mntmFind = new MenuItem(menu_3, SWT.NONE);
    mntmFind.setText("Find...\tCtrl+F");

    MenuItem mntmPrevIssue = new MenuItem(menu_3, SWT.NONE);
    mntmPrevIssue.setText("&Prev Issue\tCtrl+P");
    mntmPrevIssue.setData("ID", MNTM_PREV_ISSUE);
    mntmPrevIssue.setAccelerator(SWT.CONTROL | 'P');
    mntmPrevIssue.addSelectionListener(loader);

    MenuItem mntmNextIssue = new MenuItem(menu_3, SWT.NONE);
    mntmNextIssue.setText("&Next Issue\tCtrl+N");
    mntmNextIssue.setData("ID", MNTM_NEXT_ISSUE);
    mntmNextIssue.setAccelerator(SWT.CONTROL | 'N');
    mntmNextIssue.addSelectionListener(loader);

    MenuItem mntmAnalyze = new MenuItem(menu, SWT.CASCADE);
    mntmAnalyze.setText("&Analyze");

    Menu menu_2 = new Menu(mntmAnalyze);
    mntmAnalyze.setMenu(menu_2);

    MenuItem mntmColorMeRed = new MenuItem(menu_2, SWT.NONE);
    mntmColorMeRed.setText("&Check Tables Structure");
    mntmColorMeRed.addSelectionListener(loader);
    mntmColorMeRed.setData("ID", MNTM_COLORMERED);

    mntmShowReport = new MenuItem(menu_2, SWT.NONE);
    mntmShowReport.setText("Show Report...");
    mntmShowReport.addSelectionListener(loader);
    mntmShowReport.setData("ID", MNTM_SHOWREPORT);

    final DateFormat timeFmt = new SimpleDateFormat("HH:mm:ss,SSS");
    final HBaseClusterMap window = this;
    Logger.getRootLogger().addAppender(new AppenderSkeleton() {

      @Override
      public void close() {
      }

      @Override
      public boolean requiresLayout() {
        return false;
      }

      @Override
      protected void append(LoggingEvent event) {
        final StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        pw.append(timeFmt.format(System.currentTimeMillis()));
        pw.append(" ");
        pw.append(event.getLevel().toString());
        pw.append(" ");
        pw.append(event.getRenderedMessage());
        pw.append("\n");
        if (event.getThrowableInformation() != null) {
          event.getThrowableInformation().getThrowable().printStackTrace(pw);
          pw.append("\n");
        }
        pw.flush();
        final String text = sw.toString();
        try {
          window.txtStatus.append(text);
        } catch (SWTException e) {
          Display.getDefault().asyncExec(new Runnable() {
            public void run() {
              window.txtStatus.append(text);
            } 
          });
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
  }

  public void setStatusText(String string) {
    txtStatus.setText(string);
  }
}
