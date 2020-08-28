/*
 * #%L
 * org.eclipse.emf.eson.ui
 * %%
 * Copyright (C) 2015 - 2015 Michael Vorburger and others
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.ui.editor.tree;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.eclipse.jface.dialogs.IDialogConstants;
import org.eclipse.jface.viewers.ContentViewer;
import org.eclipse.jface.viewers.DoubleClickEvent;
import org.eclipse.jface.viewers.IDoubleClickListener;
import org.eclipse.jface.viewers.ILabelProvider;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.jface.viewers.StructuredSelection;
import org.eclipse.jface.viewers.TableViewer;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.widgets.Table;
import org.eclipse.xtext.resource.IEObjectDescription;
import org.eclipse.xtext.ui.search.IXtextEObjectSearch;
import org.eclipse.xtext.ui.search.XtextEObjectSearchDialog;

import com.google.common.collect.Lists;

/**
 * XtextEObjectSearchDialog extension with two columns, for multi selection.
 * 
 * @author Ramesh Sampenga
 */
public class XtextMultiEObjectSearchDialog extends XtextEObjectSearchDialog {

    private static final String SELECTED_ELEMENTS_LABEL = "Selected items: ";
    private static final String SELECTED_LABEL = " selected";

    private Label selectedElementsLabel;
    private TableViewer selectedTableViewer;

    private List<EObject> initialSelection;

    public XtextMultiEObjectSearchDialog(Shell parent, IXtextEObjectSearch searchEngine, ILabelProvider labelProvider) {
        super(parent, searchEngine, labelProvider);
    }

    public XtextMultiEObjectSearchDialog(Shell parent, IXtextEObjectSearch searchEngine, ILabelProvider labelProvider, boolean enableStyledLabels) {
        super(parent, searchEngine, labelProvider, enableStyledLabels);
    }

    @Override
    protected int getTableStyle() {
        return SWT.MULTI | SWT.H_SCROLL | SWT.V_SCROLL | SWT.BORDER | SWT.VIRTUAL;
    }

    @Override
    protected Control createDialogArea(Composite container) {
        Composite parent = new Composite(container, SWT.NONE);
        parent.setLayout(new GridLayout(3, false));
        parent.setLayoutData(new GridData(SWT.FILL, SWT.FILL, true, true));

        Composite leftComp = (Composite) super.createDialogArea(parent);

        createPickerButtons(parent);

        Composite rightComp = new Composite(parent, SWT.NONE);
        selectedElementsLabel = new Label(rightComp, SWT.NONE);
        selectedElementsLabel.setText(SELECTED_ELEMENTS_LABEL);
        selectedElementsLabel.setFont(rightComp.getFont());

        GridLayout rightCompLayout = new GridLayout();
        rightCompLayout.marginHeight = convertVerticalDLUsToPixels(IDialogConstants.VERTICAL_MARGIN);
        rightCompLayout.marginWidth = convertHorizontalDLUsToPixels(IDialogConstants.HORIZONTAL_MARGIN);
        rightCompLayout.verticalSpacing = convertVerticalDLUsToPixels(IDialogConstants.VERTICAL_SPACING);
        rightCompLayout.horizontalSpacing = convertHorizontalDLUsToPixels(IDialogConstants.HORIZONTAL_SPACING);
        rightComp.setLayout(rightCompLayout);
        rightComp.setLayoutData(new GridData(GridData.FILL_BOTH));
        applyDialogFont(rightComp);

        GridLayout leftCompLayout = (GridLayout) leftComp.getLayout();
        leftCompLayout.marginLeft = rightCompLayout.marginRight = convertHorizontalDLUsToPixels(IDialogConstants.HORIZONTAL_MARGIN) / 2;
        leftCompLayout.marginWidth = rightCompLayout.marginWidth = 0;

        selectedTableViewer = new TableViewer(rightComp, getTableStyle());
        selectedTableViewer.setContentProvider(getTableViewer().getContentProvider());
        selectedTableViewer.setLabelProvider(getTableViewer().getLabelProvider());

        GridData gd = new GridData(GridData.FILL_BOTH);
        gd.heightHint = convertHeightInCharsToPixels(getHeightInChars());
        gd.widthHint = convertWidthInCharsToPixels(getWidthInChars());
        Table table = selectedTableViewer.getTable();
        table.setLayoutData(gd);
        table.setFont(container.getFont());

        Label selectionMessageLabel = new Label(rightComp, SWT.NONE);
        setDefaultGridData(selectionMessageLabel);

        getTableViewer().addDoubleClickListener(new IDoubleClickListener() {
            @Override
            public void doubleClick(DoubleClickEvent event) {
                moveToSection();
            }
        });

        getSelectedTableViewer().addDoubleClickListener(new IDoubleClickListener() {
            @Override
            public void doubleClick(DoubleClickEvent event) {
                removeFromSelection();
            }
        });

        return leftComp;
    }

    public void setInitialSelection(List<EObject> initialSelection) {
        this.initialSelection = initialSelection;
    }

    private void setDefaultGridData(Control control) {
        control.setLayoutData(new GridData(GridData.HORIZONTAL_ALIGN_FILL | GridData.GRAB_HORIZONTAL));
    }

    @Override
    protected void okPressed() {
        // do nothing
    }

    protected void buttonPressed(int buttonId) {
        if (IDialogConstants.OK_ID == buttonId) {
            setResult(getInputList(getSelectedTableViewer()));
            setReturnCode(OK);
            close();
        } else if (IDialogConstants.CANCEL_ID == buttonId) {
            cancelPressed();
        }
    }

    private void createPickerButtons(Composite composite) {
        Composite buttonBar = new Composite(composite, SWT.NONE);
        buttonBar.setBackground(composite.getBackground());
        GridLayout layout = new GridLayout(1, true);
        layout.marginWidth = layout.marginHeight = 0;
        buttonBar.setLayout(layout);
        GridData data = new GridData(SWT.LEFT, SWT.CENTER, false, false);
        buttonBar.setLayoutData(data);

        Button addButton = new Button(buttonBar, SWT.NONE);
        GridData addButtonData = new GridData(GridData.FILL_HORIZONTAL);
        addButton.setLayoutData(addButtonData);
        addButton.setText(">");
        addButton.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
                moveToSection();
            }
        });

        Button remButton = new Button(buttonBar, SWT.NONE);
        GridData remButtonData = new GridData(GridData.FILL_HORIZONTAL);
        remButton.setLayoutData(remButtonData);
        remButton.setText("<");
        remButton.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
                removeFromSelection();
            }
        });

        Button addAllButton = new Button(buttonBar, SWT.NONE);
        GridData addAllButtonData = new GridData(GridData.FILL_HORIZONTAL);
        addAllButton.setLayoutData(addAllButtonData);
        addAllButton.setText(">>");
        addAllButton.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
                getTableViewer().getTable().selectAll();
                moveToSection();
            }
        });

        Button remAllButton = new Button(buttonBar, SWT.NONE);
        GridData remAllButtonData = new GridData(GridData.FILL_HORIZONTAL);
        remAllButton.setLayoutData(remAllButtonData);
        remAllButton.setText("<<");
        remAllButton.addSelectionListener(new SelectionAdapter() {
            @Override
            public void widgetSelected(SelectionEvent e) {
                getSelectedTableViewer().getTable().selectAll();
                removeFromSelection();
            }
        });

    }

    private void moveToSection() {
        List<IEObjectDescription> leftList = getInputList(getTableViewer());
        List<IEObjectDescription> selectedItems = getSelectedList(getTableViewer());
        leftList.removeAll(selectedItems);
        List<IEObjectDescription> rightList = getInputList(getSelectedTableViewer());
        rightList.addAll(selectedItems);
        updateMatches2(sortedCopy(rightList));
        super.updateMatches(sortedCopy(leftList), true);
        getTableViewer().setSelection(new StructuredSelection());
    }

    private void removeFromSelection() {
        List<IEObjectDescription> selectedItems = getSelectedList(getSelectedTableViewer());
        if (!selectedItems.isEmpty()) {
            List<IEObjectDescription> rightList = new ArrayList<IEObjectDescription>();
            rightList.addAll(getInputList(getSelectedTableViewer()));
            rightList.removeAll(selectedItems);
            List<IEObjectDescription> leftList = getInputList(getTableViewer());
            leftList.addAll(selectedItems);
            super.updateMatches(sortedCopy(leftList), true);
            updateMatches2(sortedCopy(rightList));
            getTableViewer().setSelection(new StructuredSelection(selectedItems), true);
        }
    }

    @Override
    public void updateMatches(final Collection<IEObjectDescription> matches, final boolean isFinished) {
        if (getSelectedTableViewer() != null) {
            if (initialSelection != null && !initialSelection.isEmpty()) {
                Collection<IEObjectDescription> selectionList = getInitialEObjectDesciptionList(matches, isFinished);
                updateMatches2(sortedCopy(selectionList));
                matches.removeAll(selectionList);
                super.updateMatches(matches, isFinished);
            } else {
                matches.removeAll(getInputList(getSelectedTableViewer()));
                super.updateMatches(matches, isFinished);
            }
        } else {
            Collection<IEObjectDescription> initialSelectionList = getInitialEObjectDesciptionList(matches, isFinished);
            updateMatches2(sortedCopy(initialSelectionList));
            matches.removeAll(initialSelectionList);
            XtextMultiEObjectSearchDialog.super.updateMatches(matches, isFinished);
        }
    }

    public void updateMatches2(final Collection<IEObjectDescription> matches) {
        Display.getDefault().asyncExec(new Runnable() {
            @Override
            public void run() {
                if (getShell() != null) {
                    if (getSelectedTableViewer() != null) {
                        getSelectedTableViewer().setItemCount(matches.size());
                        if (!getSelectedTableViewer().isBusy()) {
                            getSelectedTableViewer().setInput(matches);
                        }
                        if (getSelectedTableViewer().getSelection().isEmpty() && matches.size() > 0)
                            getSelectedTableViewer().getTable().select(0);
                        selectedElementsLabel.setText(SELECTED_ELEMENTS_LABEL + " (" + matches.size() + SELECTED_LABEL + ")");
                    }
                }
            }
        });
    }

    private Collection<IEObjectDescription> getInitialEObjectDesciptionList(
            final Collection<IEObjectDescription> matches, boolean clearList) {
        Collection<IEObjectDescription> eDescriptionList = new ArrayList<IEObjectDescription>();
        if ((initialSelection != null && !initialSelection.isEmpty())) {
            for (EObject eObject : initialSelection) {
                for (Iterator<IEObjectDescription> iterator = matches.iterator(); iterator.hasNext();) {
                    IEObjectDescription eObjectDesc = (IEObjectDescription) iterator.next();
                    EObject eProxy = eObjectDesc.getEObjectOrProxy();
                    EObject resolvedEObject = EcoreUtil.resolve(eProxy, eObject);
                    if (eObject.equals(resolvedEObject)) {
                        eDescriptionList.add(eObjectDesc);
                        break;
                    }
                }
            }
            if (clearList) {
                initialSelection = null;
            }
        }
        return eDescriptionList;
    }

    @SuppressWarnings("unchecked")
    private List<IEObjectDescription> getSelectedList(ContentViewer tableViewer) {
        IStructuredSelection selection = (IStructuredSelection) tableViewer.getSelection();
        List<IEObjectDescription> selectedItems = new ArrayList<IEObjectDescription>();
        if (selection != null) {
            selectedItems.addAll(selection.toList());
        }
        return selectedItems;
    }

    @SuppressWarnings("unchecked")
    private List<IEObjectDescription> getInputList(ContentViewer tableViewer) {
        Object input = tableViewer.getInput();
        List<IEObjectDescription> list = new ArrayList<IEObjectDescription>();
        if (input != null) {
            if (input instanceof IStructuredSelection) {
                list.addAll(((IStructuredSelection) input).toList());
            } else if (input instanceof Collection) {
                list.addAll((Collection<IEObjectDescription>) input);
            } else {
                // Undefined input given to TableViewer
            }
            input = new ArrayList<IEObjectDescription>();
        }
        return list;
    }

    private Collection<IEObjectDescription> sortedCopy(Iterable<IEObjectDescription> list) {
        List<IEObjectDescription> result = Lists.newArrayList(list);
        Collections.sort(result, new Comparator<IEObjectDescription>() {
            @Override
            public int compare(IEObjectDescription o1, IEObjectDescription o2) {
                int diff = o1.getQualifiedName().compareToIgnoreCase(o2.getQualifiedName());
                if (diff == 0) {
                    String className1 = o1.getEClass().getName();
                    String className2 = o2.getEClass().getName();
                    if (className1 == null)
                        diff = className2 == null ? 0 : -1;
                    else
                        diff = className2 == null ? 1 : className1.compareToIgnoreCase(className2);
                    if (diff == 0) {
                        diff = o1.getEObjectURI().toString().compareTo(o2.getEObjectURI().toString());
                    }
                }
                return diff;
            }
        });
        return result;
    }

    private TableViewer getSelectedTableViewer() {
        return selectedTableViewer;
    }

}
