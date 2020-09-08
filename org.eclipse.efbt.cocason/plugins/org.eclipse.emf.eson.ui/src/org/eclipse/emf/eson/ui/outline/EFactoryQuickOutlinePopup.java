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
package org.eclipse.emf.eson.ui.outline;

import org.eclipse.swt.widgets.Shell;
import org.eclipse.xtext.ui.editor.outline.IOutlineTreeProvider;
import org.eclipse.xtext.ui.editor.outline.impl.OutlineMode;
import org.eclipse.xtext.ui.editor.outline.quickoutline.Messages;
import org.eclipse.xtext.ui.editor.outline.quickoutline.QuickOutlinePopup;

import com.google.inject.Inject;

/**
 * TODO: Document me!
 *
 * @author Ramesh Sampenga
 */
public class EFactoryQuickOutlinePopup extends QuickOutlinePopup {

    @Inject
    private IOutlineTreeProvider treeProvider;

    public EFactoryQuickOutlinePopup(Shell parent) {
        super(parent);
    }

    @Override
    protected void changeOutlineMode() {
        if (treeProvider instanceof IOutlineTreeProvider.ModeAware) {
            IOutlineTreeProvider.ModeAware modeTreeProvider = (IOutlineTreeProvider.ModeAware) treeProvider;
            OutlineMode nextMode = modeTreeProvider.getNextMode();
            modeTreeProvider.setCurrentMode(nextMode);
            if (nextMode.getId().equals(EFactoryOutlineTreeProvider.SHOW_FIRST_LEVEL.getId())) {
                getTreeViewer().setAutoExpandLevel(2);
            } else {
                getTreeViewer().setAutoExpandLevel(-1);
            }
            setInfoText();
            super.scheduleRefresh();
        }
    }

    @Override
    protected void setInfoText() {
        if (treeProvider instanceof IOutlineTreeProvider.ModeAware) {
            setInfoText("Press 'CTRL+O' to "
                    + ((IOutlineTreeProvider.ModeAware) treeProvider).getNextMode().getDescription());
        } else {
            setInfoText(Messages.QuickOutlinePopup_pressESC);
        }
    }
}
