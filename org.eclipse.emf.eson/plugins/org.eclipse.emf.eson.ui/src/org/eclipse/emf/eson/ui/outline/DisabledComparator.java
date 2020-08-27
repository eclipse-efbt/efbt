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

import org.eclipse.xtext.ui.editor.outline.IOutlineNode;
import org.eclipse.xtext.ui.editor.outline.impl.OutlineFilterAndSorter.IComparator;

public class DisabledComparator implements IComparator {

    @Override
    public int compare(IOutlineNode o1, IOutlineNode o2) {
        return 0;
    }

    @Override
    public boolean isEnabled() {
        return false;
    }

}
