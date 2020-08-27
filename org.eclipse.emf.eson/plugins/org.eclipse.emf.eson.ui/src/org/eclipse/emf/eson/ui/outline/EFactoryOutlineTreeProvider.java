/*
 * #%L
 * org.eclipse.emf.eson.ui
 * %%
 * Copyright (C) 2013 - 2014 Michael Vorburger and others
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.ui.outline;

import static com.google.common.collect.Lists.newArrayList;

import java.util.List;

import org.apache.log4j.Logger;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.eson.eFactory.EFactoryPackage;
import org.eclipse.emf.eson.eFactory.impl.ContainmentImpl;
import org.eclipse.emf.eson.eFactory.impl.FactoryImpl;
import org.eclipse.emf.eson.eFactory.impl.FeatureImpl;
import org.eclipse.xtext.ui.editor.outline.IOutlineNode;
import org.eclipse.xtext.ui.editor.outline.IOutlineTreeProvider;
import org.eclipse.xtext.ui.editor.outline.impl.DefaultOutlineTreeProvider;
import org.eclipse.xtext.ui.editor.outline.impl.OutlineMode;

/**
 * Outline.
 * 
 * This is used (only) for the "inline" Quick Outline (the Ctrl-O one). The
 * read-only Outline view is, intentionally, disabled, to avoid end-user
 * confusion with the right-hand side read/write Outline in the Editor.
 * 
 * @author Michael Vorburger
 */
public class EFactoryOutlineTreeProvider extends DefaultOutlineTreeProvider implements IOutlineTreeProvider.ModeAware {

    private static final Logger logger = Logger.getLogger(EFactoryOutlineTreeProvider.class);
	
    @Override
    protected void createNode(IOutlineNode parent, EObject modelElement) {
        EClass eClass = null;
        if (modelElement != null && (eClass = modelElement.eClass()) != null) {
            switch (eClass.getClassifierID()) {
            case EFactoryPackage.FACTORY:
                // Get the rootObject from the Factory
                createNode(parent, ((FactoryImpl) modelElement).getRoot());
                break;
            case EFactoryPackage.PACKAGE_IMPORT:
            case EFactoryPackage.NAMESPACE_IMPORT:
            case EFactoryPackage.ANNOTATION:
            case EFactoryPackage.CUSTOM_NAME_MAPPING:
                // Skip these model elements from creation of outline node
                break;
            case EFactoryPackage.FEATURE:
                // Create Node for the value of the Feature
                createNode(parent, ((FeatureImpl) modelElement).getValue());
                break;
            // case EFactoryPackage.VALUE:
            case EFactoryPackage.MULTI_VALUE:
                // Create Children for the MultiValued element
                createChildren(parent, modelElement);
                break;
            case EFactoryPackage.REFERENCE:
            case EFactoryPackage.ATTRIBUTE:
            case EFactoryPackage.ENUM_ATTRIBUTE:
            case EFactoryPackage.STRING_ATTRIBUTE:
            case EFactoryPackage.INTEGER_ATTRIBUTE:
            case EFactoryPackage.DOUBLE_ATTRIBUTE:
            case EFactoryPackage.DATE_ATTRIBUTE:
            case EFactoryPackage.NULL_ATTRIBUTE:
            case EFactoryPackage.BOOLEAN_ATTRIBUTE:
                // Skip these model elements from creation of outline node
                break;
            case EFactoryPackage.CONTAINMENT:
                // Create Node for the value of the Containment
                createNode(parent, ((ContainmentImpl) modelElement).getValue());
                break;
            default:
                // Create Node for NewObjectImpl by calling super method
                // case EFactoryPackage.NEW_OBJECT:
                super.createNode(parent, modelElement);
            }
        } else {
            logger.trace("CreateNode Method cannot recognize the eClass for the given modelElement:" + modelElement);
        }
    }

    public static final OutlineMode SHOW_FIRST_LEVEL = new OutlineMode("level2", "show first level");

    public static final OutlineMode SHOW_ALL_LEVEL = new OutlineMode("levelall", "show all levels");

    private static final List<OutlineMode> MODES = newArrayList(SHOW_FIRST_LEVEL, SHOW_ALL_LEVEL);

    private int currentModeIndex = 0;

    @Override
    public List<OutlineMode> getOutlineModes() {
        return MODES;
    }

    @Override
    public OutlineMode getCurrentMode() {
        return getOutlineModes().get(currentModeIndex);
    }

    @Override
    public OutlineMode getNextMode() {
        return getOutlineModes().get((currentModeIndex + 1) % getOutlineModes().size());
    }

    @Override
    public void setCurrentMode(OutlineMode outlineMode) {
        int newIndex = getOutlineModes().indexOf(outlineMode);
        if (newIndex != -1){
            currentModeIndex = newIndex;
        }
    }

}
