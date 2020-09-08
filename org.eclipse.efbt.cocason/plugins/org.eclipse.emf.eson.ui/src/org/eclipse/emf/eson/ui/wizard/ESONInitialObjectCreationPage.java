/*
 * #%L
 * org.eclipse.emf.eson.ui
 * %%
 * Copyright (C) 2016 Michael Vorburger and others
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.ui.wizard;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.eclipse.emf.common.CommonPlugin;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EClassifier;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.jface.wizard.WizardPage;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.ModifyEvent;
import org.eclipse.swt.events.ModifyListener;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Combo;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Label;

/**
 * This wizard page displays all the possible model objects given a EPackage.
 * One can select the model to create the resource
 * @author Girish Kumar V
 */
public class ESONInitialObjectCreationPage extends WizardPage {

    protected Combo initialObjectField;

    protected List<String> initialObjectNames;

    protected EPackage ePackage;

    public ESONInitialObjectCreationPage(String pageId) {
        super(pageId);
        setTitle("Model Object");
        setDescription("Select a model object to create");
    }

    public void createControl(Composite parent) {
        Composite composite = new Composite(parent, SWT.NONE);
        {
            GridLayout layout = new GridLayout();
            layout.numColumns = 1;
            layout.verticalSpacing = 12;
            composite.setLayout(layout);

            GridData data = new GridData();
            data.verticalAlignment = GridData.FILL;
            data.grabExcessVerticalSpace = true;
            data.horizontalAlignment = GridData.FILL;
            composite.setLayoutData(data);
        }

        Label containerLabel = new Label(composite, SWT.LEFT);
        {
            containerLabel.setText("Model Object");

            GridData data = new GridData();
            data.horizontalAlignment = GridData.FILL;
            containerLabel.setLayoutData(data);
        }

        initialObjectField = new Combo(composite, SWT.BORDER | SWT.READ_ONLY);
        {
            GridData data = new GridData();
            data.horizontalAlignment = GridData.FILL;
            data.grabExcessHorizontalSpace = true;
            initialObjectField.setLayoutData(data);
        }

        initialObjectField.addModifyListener(validator);

        setPageComplete(validatePage());
        setControl(composite);
    }

    protected ModifyListener validator = new ModifyListener() {
        public void modifyText(ModifyEvent e) {
            setPageComplete(validatePage());
        }
    };

    protected boolean validatePage() {
        String initialObjectName = getInitialObjectName();
        if (initialObjectName == null || initialObjectName.trim().equals("")) {
            setErrorMessage("Please Select the Model Object!!!");
            return false;
        }
        setErrorMessage(null);
        return true;
    }

    @Override
    public void setVisible(boolean visible) {
        super.setVisible(visible);
        if (visible) {
            if (initialObjectField.getItemCount() == 1) {
                initialObjectField.clearSelection();
            } else {
                initialObjectField.setFocus();
            }
        }
    }

    public String getInitialObjectName() {
        return initialObjectField.getText();
    }

    protected Collection<String> getInitialObjectNames() {
        if (initialObjectNames == null) {
            initialObjectNames = new ArrayList<String>();
            for (EClassifier eClassifier : ePackage.getEClassifiers()) {
                if (eClassifier instanceof EClass) {
                    EClass eClass = (EClass) eClassifier;
                    if (!eClass.isAbstract()) {
                        initialObjectNames.add(eClass.getName());
                    }
                }
            }
            Collections.sort(initialObjectNames, CommonPlugin.INSTANCE.getComparator());
        }
        return initialObjectNames;
    }

    public void setEPackage(EPackage ePackage) {
        this.ePackage = ePackage;
        resetContents();
    }

    private void resetContents() {
        initialObjectNames = null;
        initialObjectField.removeAll();

        for (String objectName : getInitialObjectNames()) {
            initialObjectField.add(objectName);
        }

        if (initialObjectField.getItemCount() == 1) {
            initialObjectField.select(0);
        }
    }
}