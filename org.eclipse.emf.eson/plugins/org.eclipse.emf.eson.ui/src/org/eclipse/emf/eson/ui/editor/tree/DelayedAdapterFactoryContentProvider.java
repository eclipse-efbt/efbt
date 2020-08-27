/*
 * #%L
 * org.eclipse.emf.eson.ui
 * %%
 * Copyright (C) 2016 - 2016 Michael Vorburger and David Aguirre
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.ui.editor.tree;

import java.util.concurrent.atomic.AtomicBoolean;

import org.eclipse.emf.common.notify.AdapterFactory;
import org.eclipse.emf.common.notify.Notification;
import org.eclipse.emf.edit.ui.provider.AdapterFactoryContentProvider;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.swt.widgets.Display;
 
/**
 * Use this {@link AdapterFactoryContentProvider} if the viewer's underlying
 * domain model frequently changes and would produce many notification events.
 * In this case it's much more efficient to trigger a full refresh after a short
 * delay.
 *
 * See https://javahacks.wordpress.com/2014/08/15/119/
 */
public class DelayedAdapterFactoryContentProvider extends AdapterFactoryContentProvider {
 
    private static final int DELAY_IN_MS = 1000;
    private final AtomicBoolean refresh = new AtomicBoolean(false);
    private Viewer viewer;
 
    public DelayedAdapterFactoryContentProvider(AdapterFactory adapterFactory) {
        super(adapterFactory);
    }
 
    @Override
    public void notifyChanged(final Notification notification) {
        if (!notification.isTouch() && !refresh.getAndSet(true)) {
            Display.getDefault().asyncExec(new Runnable() {
                @Override
                public void run() {
 
                    Display.getDefault().timerExec(DELAY_IN_MS, new Runnable() {
                        @Override
                        public void run() {
                            refresh.set(false);
                            if (viewer.getControl() != null
                                    && !viewer.getControl().isDisposed()) {
                                viewer.refresh();
                            }
                        }
                    });
                }
            });
 
        }
    }
 
    @Override
    public void inputChanged(Viewer viewer, Object oldInput, Object newInput) {
        this.viewer = viewer;
        super.inputChanged(viewer, oldInput, newInput);
    }
}
