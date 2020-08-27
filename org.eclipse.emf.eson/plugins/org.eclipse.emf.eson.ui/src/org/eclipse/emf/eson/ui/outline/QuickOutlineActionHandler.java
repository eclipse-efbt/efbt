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
import org.eclipse.xtext.ui.editor.outline.quickoutline.QuickOutlinePopup;
import org.eclipse.xtext.ui.editor.outline.quickoutline.ShowQuickOutlineActionHandler;

import com.google.inject.Inject;
import com.google.inject.Injector;

/**
 * TODO: Document me!
 *
 * @author Ramesh Sampenga
 */
public class QuickOutlineActionHandler extends ShowQuickOutlineActionHandler {

    @Inject
    private Injector injector;

    @Override
    protected QuickOutlinePopup createPopup(Shell parent) {
        EFactoryQuickOutlinePopup result = new EFactoryQuickOutlinePopup(parent);
        injector.injectMembers(result);
        return result;
    }

}
