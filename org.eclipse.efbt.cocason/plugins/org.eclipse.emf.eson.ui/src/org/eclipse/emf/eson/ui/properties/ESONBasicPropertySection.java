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
package org.eclipse.emf.eson.ui.properties;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.eclipse.emf.common.ui.celleditor.ExtendedDialogCellEditor;
import org.eclipse.emf.ecore.EDataType;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.provider.EcoreItemProviderAdapterFactory;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.eclipse.emf.edit.provider.ComposedAdapterFactory;
import org.eclipse.emf.edit.provider.IItemPropertyDescriptor;
import org.eclipse.emf.edit.provider.IItemPropertySource;
import org.eclipse.emf.edit.provider.ReflectiveItemProviderAdapterFactory;
import org.eclipse.emf.edit.provider.resource.ResourceItemProviderAdapterFactory;
import org.eclipse.emf.edit.ui.provider.AdapterFactoryContentProvider;
import org.eclipse.emf.edit.ui.provider.PropertyDescriptor;
import org.eclipse.emf.eson.ui.editor.tree.EFactoryWithTreeEditor;
import org.eclipse.emf.eson.ui.editor.tree.URIBasedPropertySource;
import org.eclipse.emf.eson.ui.editor.tree.XtextMultiEObjectSearchDialog;
import org.eclipse.jface.viewers.CellEditor;
import org.eclipse.jface.viewers.ISelection;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.jface.viewers.ITreeSelection;
import org.eclipse.jface.viewers.StructuredSelection;
import org.eclipse.jface.window.Window;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.ui.IWorkbenchPart;
import org.eclipse.ui.views.properties.IPropertyDescriptor;
import org.eclipse.ui.views.properties.tabbed.AdvancedPropertySection;
import org.eclipse.ui.views.properties.tabbed.TabbedPropertySheetPage;
import org.eclipse.xtext.resource.IEObjectDescription;
import org.eclipse.xtext.ui.editor.model.IXtextDocument;
import org.eclipse.xtext.ui.label.GlobalDescriptionLabelProvider;
import org.eclipse.xtext.ui.search.IXtextEObjectSearch;
import org.eclipse.xtext.ui.search.XtextEObjectSearchDialog;

/**
 * The ESON default "Basic" Property Section.
 * 
 * This default Tab just shows all properties in flat list, similarly to the
 * default EMF generated Properties. As illustrated in the eson.example project,
 * ESON users may wish to suppress this default section by registering a custom
 * IFilter to hide this section via the org.eclipse.emf.eson.ui.properties
 * filterBasicPropertySection extension point, and instead register their own
 * AbstractPropertySection via the extension point
 * org.eclipse.ui.views.properties.tabbed.propertySections.
 *
 * @author vgirishkumar Factored out from EFactoryWithTreeEditor where this
 *         originally used to be (written by others)
 */
public class ESONBasicPropertySection extends AdvancedPropertySection {

    private @Inject IXtextEObjectSearch eObjectSearch;
    private @Inject GlobalDescriptionLabelProvider globalDescriptionLabelProvider;

    private ComposedAdapterFactory adapterFactory;
    private IXtextDocument document;

    public ESONBasicPropertySection() {
        adapterFactory = new ComposedAdapterFactory(ComposedAdapterFactory.Descriptor.Registry.INSTANCE);
        adapterFactory.addAdapterFactory(new ResourceItemProviderAdapterFactory());
        adapterFactory.addAdapterFactory(new EcoreItemProviderAdapterFactory());
        adapterFactory.addAdapterFactory(new ReflectiveItemProviderAdapterFactory());
    }
    
    @Override
    public void createControls(Composite parent, TabbedPropertySheetPage atabbedPropertySheetPage) {
        super.createControls(parent, atabbedPropertySheetPage);
        // In case of normal PropertySheetPage, this is set by EMF
        page.setPropertySourceProvider(new AdapterFactoryContentProvider(adapterFactory));
    }

    @Override
    public void setInput(IWorkbenchPart part, ISelection selection) {
        if (part instanceof EFactoryWithTreeEditor) {
            document = ((EFactoryWithTreeEditor) part).getDocument();
            if (selection instanceof ITreeSelection) {
                super.setInput(part, mapToStructuredSelection((ITreeSelection) selection));
            }
        } else {
            super.setInput(part, selection);
        }
    }

    private IStructuredSelection mapToStructuredSelection(ITreeSelection treeSelection) {
        Object object = treeSelection.getFirstElement();
        if (object instanceof EObject) {
            return buildStructuredSelection((EObject) object);
        }
        return new StructuredSelection();
    }

    private IStructuredSelection buildStructuredSelection(final EObject object) {
        if (object == null) {
            return new StructuredSelection();
        }
        final IItemPropertySource source = (IItemPropertySource) adapterFactory.adapt(object, IItemPropertySource.class);
        return new StructuredSelection() {
            public Object[] toArray() {
                return new Object[] { new URIBasedPropertySource(object, source, document) {

                    protected IPropertyDescriptor createPropertyDescriptor(final EObject eObject,
                            IItemPropertyDescriptor itemPropertyDescriptor) {
                        return new EFactoryPropertyDescriptor(eObject, itemPropertyDescriptor) {
                            @Override
                            protected CellEditor createEDataTypeCellEditor(final EDataType eDataType,
                                    Composite composite) {
                                return new EFactoryEDataTypeCellEditor(eDataType, composite);
                            }

                            @Override
                            public CellEditor createPropertyEditor(final Composite composite) {
                                CellEditor editor = null;
                                final Object feature = itemPropertyDescriptor.getFeature(this.object);
                                if (feature instanceof EReference) {
                                    final EReference eReference = (EReference) feature;
                                    editor = new ExtendedDialogCellEditor(composite, getEditLabelProvider()) {
                                        @Override
                                        @SuppressWarnings("unchecked")
                                        protected Object openDialogBox(Control cellEditorWindow) {
                                            Object currentEObjects = eObject.eGet(eReference);
                                            XtextEObjectSearchDialog dlg;
                                            if (eReference.isMany()) {
                                                dlg = new XtextMultiEObjectSearchDialog(composite.getShell(),
                                                        eObjectSearch, globalDescriptionLabelProvider);
                                                ((XtextMultiEObjectSearchDialog) dlg)
                                                        .setInitialSelection((List<EObject>) currentEObjects);
                                            } else {
                                                dlg = new XtextEObjectSearchDialog(composite.getShell(), eObjectSearch,
                                                        globalDescriptionLabelProvider);
                                            }
                                            dlg.setInitialTypePattern(eReference.getEReferenceType().getName(), false);
                                            int status = dlg.open();
                                            if (status == Window.OK) {
                                                Object[] dlgResult = dlg.getResult();
                                                ArrayList<EObject> result = new ArrayList<EObject>();
                                                if (dlgResult != null) {
                                                    for (Object eObjectDesc : dlgResult) {
                                                        EObject eObject = ((IEObjectDescription) eObjectDesc)
                                                                .getEObjectOrProxy();
                                                        // This is probably more
                                                        // of a work-around than
                                                        // a fix of the actual
                                                        // root cause :( but it
                                                        // works
                                                        if (eObject != null && eObject.eIsProxy()) {
                                                            eObject = EcoreUtil
                                                                    .resolve(eObject, eReference.eResource());
                                                        }
                                                        result.add(eObject);
                                                    }
                                                }
                                                if (eReference.isMany()) {
                                                    return result;
                                                } else {
                                                    return (result.size() > 0) ? result.get(0) : null;
                                                }
                                            } else {
                                                return currentEObjects;
                                            }
                                        }
                                    };
                                } else {
                                    // return the default cell editor
                                    editor = super.createPropertyEditor(composite);
                                }
                                return editor;
                            }
                        };
                    }
                } };
            }
        };
    }

    public class EFactoryPropertyDescriptor extends PropertyDescriptor {

        public class EFactoryEDataTypeCellEditor extends PropertyDescriptor.EDataTypeCellEditor {
            public EFactoryEDataTypeCellEditor(EDataType eDataType, Composite parent) {
                super(eDataType, parent);
                valueHandler = new EFactoryEDataTypeValueHandler(eDataType);
                setValidator(valueHandler);
            }
        }

        protected class EFactoryEDataTypeValueHandler extends PropertyDescriptor.EDataTypeValueHandler {
            public EFactoryEDataTypeValueHandler(EDataType eDataType) {
                super(eDataType);
            }

            @Override
            public String toString(Object value) {
                if (value instanceof IItemPropertySource) {
                    IItemPropertySource propertySource = (IItemPropertySource) value;
                    return super.toString(propertySource.getEditableValue(value));
                }
                return super.toString(value);
            }
        }

        @Override
        public CellEditor createPropertyEditor(Composite composite) {
            CellEditor editor = super.createPropertyEditor(composite);
            return editor;
        }

        public EFactoryPropertyDescriptor(Object object, IItemPropertyDescriptor itemPropertyDescriptor) {
            super(object, itemPropertyDescriptor);
        }
    }

    @Override
    public void dispose() {
        super.dispose();
        if (adapterFactory != null)
            adapterFactory.dispose();
    }

}
