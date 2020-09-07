/*
 * #%L
 * org.eclipse.emf.eson.ui
 * %%
 * Copyright (C) 2015 Michael Vorburger and others
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.ui.editor.tree;

import org.eclipse.jface.viewers.TreeViewer;
import org.eclipse.swt.widgets.Item;
import org.eclipse.swt.widgets.Tree;

/**
 * TreeViewer which ignores programatic requests to collapse (but end-user
 * initiated collapsing by clicking on triangles in the UI still work).
 *
 * This is an ugly hack to solve the annoying problem of the auto-re-collapsing
 * when Add-ing new sub-elements in the Tree, for which we are too dumb to
 * figure out a better solution (it has something to do with the complex
 * interaction between Xtext/ESON magic/EMF Edit Adapters/JFace).
 * 
 * @author Michael Vorburger
 */
public class NonCollapsingTreeViewer extends TreeViewer {

    public NonCollapsingTreeViewer(Tree tree) {
        super(tree);
    }
    
    @Override
    protected void setExpanded(Item node, boolean expand) {
        if (expand)
            super.setExpanded(node, expand);
         // else Do nothing, ignore. (so effectively never collapse)
    }

// This doesn't actually seem to be needed after all, so better not:
//    @Override
//    public void setExpandedElements(Object[] elements) {
//        // Do nothing, ignore.
//    }

}
