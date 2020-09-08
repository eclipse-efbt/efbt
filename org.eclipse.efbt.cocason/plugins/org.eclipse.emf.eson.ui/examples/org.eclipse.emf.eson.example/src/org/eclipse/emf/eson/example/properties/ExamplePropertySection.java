/*
 * #%L
 * org.eclipse.emf.eson.tests.xtextintegration.ui
 * %%
 * Copyright (C) 2016 Michael Vorburger and others
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.example.properties;

import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.eclipse.emf.eson.example.library.Library;
import org.eclipse.jface.viewers.ISelection;
import org.eclipse.jface.viewers.ITreeSelection;
import org.eclipse.swt.SWT;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.layout.GridData;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Text;
import org.eclipse.ui.IWorkbenchPart;
import org.eclipse.ui.views.properties.tabbed.AbstractPropertySection;
import org.eclipse.ui.views.properties.tabbed.TabbedPropertySheetPage;
import org.eclipse.ui.views.properties.tabbed.TabbedPropertySheetWidgetFactory;
import org.eclipse.xtext.resource.XtextResource;
import org.eclipse.xtext.ui.editor.XtextEditor;
import org.eclipse.xtext.ui.editor.model.IXtextDocument;
import org.eclipse.xtext.util.concurrent.IUnitOfWork;

/**
 * Example Property Section.
 *
 * @author vgirishkumar
 */
public class ExamplePropertySection extends AbstractPropertySection {

    private Text createText;
    private Button createButton;

    private IXtextDocument document;
    private URI objectURI;   // NOTE: Do NOT keep an instance of the (derived) EMF EObject (e.g. Library) as an instance variable here!  

    @Override
    public void createControls(Composite parent, TabbedPropertySheetPage aTabbedPropertySheetPage) {
        super.createControls(parent, aTabbedPropertySheetPage);
        TabbedPropertySheetWidgetFactory widgetFactory = aTabbedPropertySheetPage.getWidgetFactory();
        parent.setLayout(new GridLayout(2, false));
        
        widgetFactory.createLabel(parent, "Name:");
        createText = widgetFactory.createText(parent, "", SWT.BORDER);
        GridData gridData = new GridData();
        gridData.horizontalAlignment = SWT.FILL;
        gridData.grabExcessHorizontalSpace = true;
        createText.setLayoutData(gridData);
        
        createButton = widgetFactory.createButton(parent, "SET NAME", SWT.PUSH);
        createButton.addMouseListener(new MouseAdapter() {
            @Override
            public void mouseUp(MouseEvent e) {
                final String text = createText.getText();
                document.modify(new IUnitOfWork.Void<XtextResource>() {
                    @Override
                    public void process(XtextResource state) throws Exception {
                        Library library = (Library) state.getEObject(objectURI.fragment());
                        library.setName(text);
                    }
                });
            }
        });
    }

    @Override
    public void setInput(IWorkbenchPart part, ISelection selection) {
        if (part instanceof XtextEditor) {
            document = ((XtextEditor) part).getDocument();
            if (selection instanceof ITreeSelection) {
                Object firstElement = ((ITreeSelection) selection).getFirstElement();
                if (firstElement instanceof Library) {
                    Library library = (Library) firstElement;
                    this.objectURI = EcoreUtil.getURI(library);
                    createText.setText(library.getName());
                }
            }
        }
    }

}
