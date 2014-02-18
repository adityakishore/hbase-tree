package com.mapr.hbase.support;

import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.StyledText;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.FormAttachment;
import org.eclipse.swt.layout.FormData;
import org.eclipse.swt.layout.FormLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Dialog;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.wb.swt.SWTResourceManager;

public class ReportDialog extends Dialog {

  protected Object result;
  protected Shell shlReport;
  private StyledText styledText_;

  /**
   * Create the dialog.
   * @param parent
   * @param style
   */
  public ReportDialog(Shell parent, int style) {
    super(parent, style);
    setText("SWT Dialog");
  }
  
  public void open(String text) {
    createContents();
    shlReport.open();
    shlReport.layout();
    styledText_.setText(text);
    Display display = getParent().getDisplay();
    while (!shlReport.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
  }

  /**
   * Open the dialog.
   * @param text 
   * @return the result
   */
  public Object open() {
    createContents();
    shlReport.open();
    shlReport.layout();
    Display display = getParent().getDisplay();
    while (!shlReport.isDisposed()) {
      if (!display.readAndDispatch()) {
        display.sleep();
      }
    }
    return result;
  }

  /**
   * Create contents of the dialog.
   */
  private void createContents() {
    shlReport = new Shell(getParent(), SWT.SHELL_TRIM | SWT.BORDER | SWT.PRIMARY_MODAL);
    shlReport.setSize(862, 624);
    shlReport.setText("Report");
    shlReport.setLayout(new FormLayout());
    
    styledText_ = new StyledText(shlReport, SWT.BORDER | SWT.H_SCROLL | SWT.V_SCROLL);
    styledText_.setFont(SWTResourceManager.getFont("Courier New", 10, SWT.NORMAL));
    FormData fd_styledText = new FormData();
    fd_styledText.left = new FormAttachment(0, 3);
    fd_styledText.top = new FormAttachment(0, 5);
    fd_styledText.right = new FormAttachment(100, -3);
    styledText_.setLayoutData(fd_styledText);
    
    Button btnClose = new Button(shlReport, SWT.NONE);
    btnClose.addSelectionListener(new SelectionAdapter() {
      @Override
      public void widgetSelected(SelectionEvent e) {
        shlReport.close();
      }
    });
    fd_styledText.bottom = new FormAttachment(btnClose, -6);
    FormData fd_btnClose = new FormData();
    fd_btnClose.left = new FormAttachment(100, -72);
    fd_btnClose.bottom = new FormAttachment(100, -5);
    fd_btnClose.right = new FormAttachment(100, -10);
    btnClose.setLayoutData(fd_btnClose);
    btnClose.setText("Close");

  }

}
