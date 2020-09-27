/*
 * #%L
 * org.eclipse.emf.eson.ui
 * %%
 * Copyright (C) 2014 by no one (no copyright), as per discussion in https://dev.eclipse.org/ipzilla/show_bug.cgi?id=8690
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

import org.eclipse.emf.ecore.presentation.EcoreEditorPlugin;
import org.eclipse.emf.edit.domain.EditingDomain;
import org.eclipse.emf.edit.domain.IEditingDomainProvider;
import org.eclipse.emf.edit.ui.action.CreateChildAction;
import org.eclipse.emf.edit.ui.action.CreateSiblingAction;
import org.eclipse.emf.edit.ui.action.EditingDomainActionBarContributor;
import org.eclipse.jface.action.Action;
import org.eclipse.jface.action.ActionContributionItem;
import org.eclipse.jface.action.IAction;
import org.eclipse.jface.action.IContributionItem;
import org.eclipse.jface.action.IContributionManager;
import org.eclipse.jface.action.IMenuManager;
import org.eclipse.jface.action.MenuManager;
import org.eclipse.jface.action.SubContributionItem;
import org.eclipse.jface.viewers.ISelection;
import org.eclipse.jface.viewers.ISelectionChangedListener;
import org.eclipse.jface.viewers.ISelectionProvider;
import org.eclipse.jface.viewers.IStructuredSelection;
import org.eclipse.jface.viewers.SelectionChangedEvent;
import org.eclipse.ui.IEditorPart;
import org.eclipse.ui.PartInitException;
import org.eclipse.ui.actions.ActionFactory;


/**
 * This is the action bar contributor for the Efactory model editor.
 * <!-- begin-user-doc -->
 * <!-- end-user-doc -->
 * @generated
 */
public class ActionBarContributor
        extends EditingDomainActionBarContributor
        implements ISelectionChangedListener {
        /**
         * This keeps track of the active editor.
         * <!-- begin-user-doc -->
         * <!-- end-user-doc -->
         * @generated
         */
        protected IEditorPart activeEditorPart;

        /**
         * This keeps track of the current selection provider.
         * <!-- begin-user-doc -->
         * <!-- end-user-doc -->
         * @generated
         */
        protected ISelectionProvider selectionProvider;

        /**
         * This action opens the Properties view.
         * <!-- begin-user-doc -->
         * <!-- end-user-doc -->
         * @generated
         */
        protected IAction showPropertiesViewAction =
                new Action(EcoreEditorPlugin.INSTANCE.getString("_UI_ShowPropertiesView_menu_item")) {
                        @Override
                        public void run() {
                                try {
                                        getPage().showView("org.eclipse.ui.views.PropertySheet");
                                }
                                catch (PartInitException exception) {
//                                        Activator.INSTANCE.log(exception);
                                }
                        }
                };

        /**
         * This will contain one {@link org.eclipse.emf.edit.ui.action.CreateChildAction} corresponding to each descriptor
         * generated for the current selection by the item provider.
         * <!-- begin-user-doc -->
         * <!-- end-user-doc -->
         * @generated
         */
        protected Collection<IAction> createChildActions;

        /**
         * This is the menu manager into which menu contribution items should be added for CreateChild actions.
         * <!-- begin-user-doc -->
         * <!-- end-user-doc -->
         * @generated
         */
        protected IMenuManager createChildMenuManager;

        /**
         * This will contain one {@link org.eclipse.emf.edit.ui.action.CreateSiblingAction} corresponding to each descriptor
         * generated for the current selection by the item provider.
         * <!-- begin-user-doc -->
         * <!-- end-user-doc -->
         * @generated
         */
        protected Collection<IAction> createSiblingActions;

        /**
         * This is the menu manager into which menu contribution items should be added for CreateSibling actions.
         * <!-- begin-user-doc -->
         * <!-- end-user-doc -->
         * @generated
         */
        protected IMenuManager createSiblingMenuManager;

        /**
         * This creates an instance of the contributor.
         * <!-- begin-user-doc -->
         * <!-- end-user-doc -->
         * @generated
         */
        public ActionBarContributor() {
                super(ADDITIONS_LAST_STYLE);
        }

        /**
         * When the active editor changes, this remembers the change and registers with it as a selection provider.
         * <!-- begin-user-doc -->
         * <!-- end-user-doc -->
         * @generated
         */
        @Override
        public void setActiveEditor(IEditorPart part) {
                super.setActiveEditor(part);
                activeEditorPart = part;

                // Switch to the new selection provider.
                //
                if (selectionProvider != null) {
                        selectionProvider.removeSelectionChangedListener(this);
                }
                if (part == null) {
                        selectionProvider = null;
                } else {
                        selectionProvider = part.getSite().getSelectionProvider();
                        if (selectionProvider == null)
                        	return;
                        
                        selectionProvider.addSelectionChangedListener(this);

                        // Fake a selection changed event to update the menus.
                        //
                        if (selectionProvider.getSelection() != null) {
                                selectionChanged(new SelectionChangedEvent(selectionProvider, selectionProvider.getSelection()));
                        }
                }
        }

        /**
         * This implements {@link org.eclipse.jface.viewers.ISelectionChangedListener},
         * handling {@link org.eclipse.jface.viewers.SelectionChangedEvent}s by querying for the children and siblings
         * that can be added to the selected object and updating the menus accordingly.
         * <!-- begin-user-doc -->
         * <!-- end-user-doc -->
         * @generated
         */
        public void selectionChanged(SelectionChangedEvent event) {
                // Remove any menu items for old selection.
                //
                if (createChildMenuManager != null) {
                        depopulateManager(createChildMenuManager, createChildActions);
                }
                if (createSiblingMenuManager != null) {
                        depopulateManager(createSiblingMenuManager, createSiblingActions);
                }

                // Query the new selection for appropriate new child/sibling descriptors
                //
                Collection<?> newChildDescriptors = null;
                Collection<?> newSiblingDescriptors = null;

                ISelection selection = event.getSelection();
                if (selection instanceof IStructuredSelection && ((IStructuredSelection)selection).size() == 1) {
                        Object object = ((IStructuredSelection)selection).getFirstElement();

                        EditingDomain domain = ((IEditingDomainProvider)activeEditorPart).getEditingDomain();

                        newChildDescriptors = domain.getNewChildDescriptors(object, null);
                        newSiblingDescriptors = domain.getNewChildDescriptors(null, object);
                }

                // Generate actions for selection; populate and redraw the menus.
                //
                createChildActions = generateCreateChildActions(newChildDescriptors, selection);
                createSiblingActions = generateCreateSiblingActions(newSiblingDescriptors, selection);

                if (createChildMenuManager != null) {
                        populateManager(createChildMenuManager, createChildActions, null);
                        createChildMenuManager.update(true);
                }
                if (createSiblingMenuManager != null) {
                        populateManager(createSiblingMenuManager, createSiblingActions, null);
                        createSiblingMenuManager.update(true);
                }
        }

        /**
         * This generates a {@link org.eclipse.emf.edit.ui.action.CreateChildAction} for each object in <code>descriptors</code>,
         * and returns the collection of these actions.
         * <!-- begin-user-doc -->
         * <!-- end-user-doc -->
         * @generated
         */
        protected Collection<IAction> generateCreateChildActions(Collection<?> descriptors, ISelection selection) {
                Collection<IAction> actions = new ArrayList<IAction>();
                if (descriptors != null) {
                        for (Object descriptor : descriptors) {
                                actions.add(new CreateChildAction(activeEditorPart, selection, descriptor));
                        }
                }
                return actions;
        }

        /**
         * This generates a {@link org.eclipse.emf.edit.ui.action.CreateSiblingAction} for each object in <code>descriptors</code>,
         * and returns the collection of these actions.
         * <!-- begin-user-doc -->
         * <!-- end-user-doc -->
         * @generated
         */
        protected Collection<IAction> generateCreateSiblingActions(Collection<?> descriptors, ISelection selection) {
                Collection<IAction> actions = new ArrayList<IAction>();
                if (descriptors != null) {
                        for (Object descriptor : descriptors) {
                                actions.add(new CreateSiblingAction(activeEditorPart, selection, descriptor));
                        }
                }
                return actions;
        }

        /**
         * This populates the specified <code>manager</code> with {@link org.eclipse.jface.action.ActionContributionItem}s
         * based on the {@link org.eclipse.jface.action.IAction}s contained in the <code>actions</code> collection,
         * by inserting them before the specified contribution item <code>contributionID</code>.
         * If <code>contributionID</code> is <code>null</code>, they are simply added.
         * <!-- begin-user-doc -->
         * <!-- end-user-doc -->
         * @generated
         */
        protected void populateManager(IContributionManager manager, Collection<? extends IAction> actions, String contributionID) {
                if (actions != null) {
                        for (IAction action : actions) {
                                if (contributionID != null) {
                                        manager.insertBefore(contributionID, action);
                                }
                                else {
                                        manager.add(action);
                                }
                        }
                }
        }
                
        /**
         * This removes from the specified <code>manager</code> all {@link org.eclipse.jface.action.ActionContributionItem}s
         * based on the {@link org.eclipse.jface.action.IAction}s contained in the <code>actions</code> collection.
         * <!-- begin-user-doc -->
         * <!-- end-user-doc -->
         * @generated
         */
        protected void depopulateManager(IContributionManager manager, Collection<? extends IAction> actions) {
                if (actions != null) {
                        IContributionItem[] items = manager.getItems();
                        for (int i = 0; i < items.length; i++) {
                                // Look into SubContributionItems
                                //
                                IContributionItem contributionItem = items[i];
                                while (contributionItem instanceof SubContributionItem) {
                                        contributionItem = ((SubContributionItem)contributionItem).getInnerItem();
                                }

                                // Delete the ActionContributionItems with matching action.
                                //
                                if (contributionItem instanceof ActionContributionItem) {
                                        IAction action = ((ActionContributionItem)contributionItem).getAction();
                                        if (actions.contains(action)) {
                                                manager.remove(contributionItem);
                                        }
                                }
                        }
                }
        }

        /**
         * This populates the pop-up menu before it appears.
         * <!-- begin-user-doc -->
         * <!-- end-user-doc -->
         * @generated
         */
        @Override
        public void menuAboutToShow(IMenuManager menuManager) {
                super.menuAboutToShow(menuManager);
                MenuManager submenuManager = null;

                submenuManager = new MenuManager(EcoreEditorPlugin.getPlugin().getString("_UI_CreateChild_menu_item"));
                populateManager(submenuManager, createChildActions, null);
                menuManager.insertBefore("edit", submenuManager);

                submenuManager = new MenuManager(EcoreEditorPlugin.getPlugin().getString("_UI_CreateSibling_menu_item"));
                populateManager(submenuManager, createSiblingActions, null);
                menuManager.insertBefore("edit", submenuManager);
        }

       /**
         * This ensures that a delete action will clean up all references to deleted objects.
         * <!-- begin-user-doc -->
         * <!-- end-user-doc -->
         * @generated
         */
        @Override
        protected boolean removeAllReferencesOnDelete() {
                return true;
        }
        
        /**
         * CCP = Cut/Copy/Paste
         */
        public void deactivateCCPActions() {
                getActionBars().setGlobalActionHandler(ActionFactory.CUT.getId(), null);
                getActionBars().setGlobalActionHandler(ActionFactory.COPY.getId(), null);
                getActionBars().setGlobalActionHandler(ActionFactory.PASTE.getId(), null);
                getActionBars().updateActionBars();
        }

        /**
         * CCP = Cut/Copy/Paste
         */
        public void activateCCPActions() {
                getActionBars().setGlobalActionHandler(ActionFactory.CUT.getId(), cutAction);
                getActionBars().setGlobalActionHandler(ActionFactory.COPY.getId(), copyAction);
                getActionBars().setGlobalActionHandler(ActionFactory.PASTE.getId(), pasteAction);
                getActionBars().updateActionBars();
        }
}
