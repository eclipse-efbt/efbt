/*
 * #%L
 * org.eclipse.emf.eson.ui
 * %%
 * Copyright (C) 2013 - 2016 Michael Vorburger and others
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.ui.editor.tree;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.getFirst;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import javax.inject.Inject;

import org.apache.log4j.Logger;
import org.eclipse.emf.common.command.BasicCommandStack;
import org.eclipse.emf.common.command.Command;
import org.eclipse.emf.common.notify.AdapterFactory;
import org.eclipse.emf.common.notify.impl.AdapterImpl;
import org.eclipse.emf.common.ui.viewer.ColumnViewerInformationControlToolTipSupport;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.ecore.plugin.EcorePlugin;
import org.eclipse.emf.ecore.presentation.EcoreEditorPlugin;
import org.eclipse.emf.ecore.provider.EcoreItemProviderAdapterFactory;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.eclipse.emf.edit.command.CommandParameter;
import org.eclipse.emf.edit.command.DeleteCommand;
import org.eclipse.emf.edit.domain.AdapterFactoryEditingDomain;
import org.eclipse.emf.edit.domain.EditingDomain;
import org.eclipse.emf.edit.domain.IEditingDomainProvider;
import org.eclipse.emf.edit.provider.ComposedAdapterFactory;
import org.eclipse.emf.edit.provider.ReflectiveItemProviderAdapterFactory;
import org.eclipse.emf.edit.provider.resource.ResourceItemProviderAdapterFactory;
import org.eclipse.emf.edit.ui.action.EditingDomainActionBarContributor;
import org.eclipse.emf.edit.ui.celleditor.AdapterFactoryTreeEditor;
import org.eclipse.emf.edit.ui.dnd.EditingDomainViewerDropAdapter;
import org.eclipse.emf.edit.ui.dnd.LocalTransfer;
import org.eclipse.emf.edit.ui.dnd.ViewerDragAdapter;
import org.eclipse.emf.edit.ui.provider.AdapterFactoryLabelProvider;
import org.eclipse.emf.edit.ui.provider.DecoratingColumLabelProvider;
import org.eclipse.emf.edit.ui.provider.DiagnosticDecorator;
import org.eclipse.emf.edit.ui.provider.UnwrappingSelectionProvider;
import org.eclipse.emf.eson.eFactory.NewObject;
import org.eclipse.emf.eson.eFactory.impl.FactoryImpl;
import org.eclipse.emf.eson.resource.EFactoryResource;
import org.eclipse.jface.action.IMenuListener;
import org.eclipse.jface.action.IMenuManager;
import org.eclipse.jface.action.MenuManager;
import org.eclipse.jface.action.Separator;
import org.eclipse.jface.text.ITextSelection;
import org.eclipse.jface.text.ITextViewerExtension;
import org.eclipse.jface.text.source.ISourceViewer;
import org.eclipse.jface.util.LocalSelectionTransfer;
import org.eclipse.jface.viewers.IPostSelectionProvider;
import org.eclipse.jface.viewers.ISelection;
import org.eclipse.jface.viewers.ISelectionChangedListener;
import org.eclipse.jface.viewers.ITreeSelection;
import org.eclipse.jface.viewers.SelectionChangedEvent;
import org.eclipse.jface.viewers.StructuredSelection;
import org.eclipse.jface.viewers.TreeViewer;
import org.eclipse.jface.viewers.Viewer;
import org.eclipse.jface.viewers.ViewerFilter;
import org.eclipse.swt.SWT;
import org.eclipse.swt.custom.SashForm;
import org.eclipse.swt.dnd.DND;
import org.eclipse.swt.dnd.FileTransfer;
import org.eclipse.swt.dnd.Transfer;
import org.eclipse.swt.events.MouseAdapter;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Menu;
import org.eclipse.swt.widgets.Tree;
import org.eclipse.ui.IActionBars;
import org.eclipse.ui.IEditorInput;
import org.eclipse.ui.IEditorSite;
import org.eclipse.ui.PartInitException;
import org.eclipse.ui.contexts.IContextService;
import org.eclipse.ui.views.properties.IPropertySheetPage;
import org.eclipse.ui.views.properties.tabbed.ITabbedPropertySheetPageContributor;
import org.eclipse.ui.views.properties.tabbed.TabbedPropertySheetPage;
import org.eclipse.xtext.resource.EObjectAtOffsetHelper;
import org.eclipse.xtext.resource.ILocationInFileProvider;
import org.eclipse.xtext.resource.XtextResource;
import org.eclipse.xtext.ui.editor.XtextEditor;
import org.eclipse.xtext.ui.editor.model.IXtextDocument;
import org.eclipse.xtext.util.ITextRegion;
import org.eclipse.xtext.util.concurrent.IUnitOfWork;

import com.google.common.base.Throwables;

public class EFactoryWithTreeEditor extends XtextEditor implements IEditingDomainProvider, IMenuListener, ITabbedPropertySheetPageContributor {
	private final static Logger LOGGER = Logger.getLogger(EFactoryWithTreeEditor.class);

	private TreeViewer treeViewer;
	private AdapterFactoryEditingDomain editingDomain;
	private ComposedAdapterFactory adapterFactory;
	private IXtextDocument document;
	private ResourceSet resourceSet;
  	private ISelectionChangedListener editorSelectionChangedListener;
	private SashForm sashForm;

	private @Inject EObjectAtOffsetHelper eObjectAtOffsetHelper;
	private @Inject ILocationInFileProvider locationInFileProvider;
	
	private boolean ignoreTreeViewSelectionChange = false;
	private boolean ignoreSourceViewSelectionChange = false;

	public AdapterFactory getAdapterFactory() {
		return adapterFactory;
	}
	
	protected class EFactoryCommandStack extends BasicCommandStack {
		@Override public void execute(final Command command) {
			setSourceRedraw(false);
			try {
				document.modify(new IUnitOfWork.Void<XtextResource>() {
					@Override public void process(XtextResource state) throws Exception {
						EFactoryCommandStack.super.execute(command);
					}
				});
				getContainer().getDisplay().asyncExec(new Runnable() {
					public void run() {
						try {
							final EObject affectedObject = getAffectedObject(command);
							if (affectedObject == null) {
								setSourceRedraw(true);
							} else {
								getContainer().getDisplay().asyncExec(new Runnable() {
									public void run() {
										try {
											updateTreeView(EcoreUtil.getURI(affectedObject), false);
										} finally {
											setSourceRedraw(true);
										}
									}
								});
							}
						} catch (Throwable throwable) {
							setSourceRedraw(true);
							Throwables.propagate(throwable);
						}
					}
				});
			} catch (Throwable throwable) {
				setSourceRedraw(true);
				Throwables.propagate(throwable);
			}
		}

		private EObject getAffectedObject(final Command command) {
			return getFirst(filter(command.getAffectedObjects(), EObject.class), null);
		}

	}
    
	private void initializeEditingDomain() {
		EFactoryCommandStack eFactoryCommandStack = new EFactoryCommandStack();
		editingDomain = new AdapterFactoryEditingDomain(adapterFactory, eFactoryCommandStack, resourceSet) {

			@Override
			public boolean isReadOnly(Resource resource) {
				return super.isReadOnly(resource) || getResourceSet().getResources().indexOf(resource) != 0;
			}

			@Override
			public Command createCommand(Class<? extends Command> commandClass, CommandParameter commandParameter) {
			    if (commandClass == DeleteCommand.class) {
			        //  we override Deletion because the default DeleteCommand.execute(), because that attempts to
			        // find the references to the object being deleted in the entire ResourceSet, potentially loading
			        // a huge amount of models, and thus "hanging" the UI in real-world  application. Since we are
			        // using Xtext here, we do not actually need to do that, so findReferences() just returns an
			        // emptyMap instead, meaning no references.
			        return new DeleteCommand(this, commandParameter.getCollection()) {
			            protected Map<EObject, Collection<EStructuralFeature.Setting>> findReferences(Collection<EObject> eObjects) {
			                return Collections.emptyMap();
			            };
			        };
			    }
			    return super.createCommand(commandClass, commandParameter);
			}
		};
		resourceSet.eAdapters().add(new EditingDomainProvider());
		resourceSet.getURIConverter().getURIMap().putAll(EcorePlugin.computePlatformURIMap(true));
	}

	protected void updateTreeView(final URI uri, final boolean ignoreSelection) {
		if (treeViewer == null)
			return;
		
		EObject obj = resolve(uri);
		if (obj != null) {
			treeViewer.refresh(obj.eResource());
			
			ignoreTreeViewSelectionChange = ignoreSelection;
			treeViewer.setSelection(new StructuredSelection(obj), true);
		}
		
	}

	protected Composite getContainer() {
		return sashForm;
	}
	
  	protected Object getInputForSelectionViewer(EditingDomain editingDomain) {
  		return editingDomain.getResourceSet().getResources().get(0);
	}  
      
	@Override
    public void createPartControl(Composite parent) {
		sashForm = new SashForm(parent, SWT.HORIZONTAL);
		// composite to have control on the xtextEditor control created by super class.
		final Composite leftComposite = new Composite(sashForm, SWT.NONE);
		leftComposite.setLayout(new FillLayout());
			super.createPartControl(leftComposite);
			getSourceViewer().getTextWidget().addMouseListener(new MouseAdapter() {
				@Override
				public void mouseDoubleClick(MouseEvent e) {
					maximizeControl(leftComposite);
				}
			}); 
             document = getDocument();
             resourceSet = document.readOnly(new IUnitOfWork<ResourceSet, XtextResource>() {
				public ResourceSet exec(final XtextResource xtextResource) throws Exception {
					return xtextResource.getResourceSet();
					}
				});
            adapterFactory = new ComposedAdapterFactory(ComposedAdapterFactory.Descriptor.Registry.INSTANCE);
            adapterFactory.addAdapterFactory(new ResourceItemProviderAdapterFactory());
            adapterFactory.addAdapterFactory(new EcoreItemProviderAdapterFactory());
            adapterFactory.addAdapterFactory(new ReflectiveItemProviderAdapterFactory());
            
            initializeEditingDomain();
            
            editorSelectionChangedListener = createEditorSelectionChangedListener();
     		if (getSelectionProvider() instanceof IPostSelectionProvider) {
    			((IPostSelectionProvider) getSelectionProvider()).addPostSelectionChangedListener(editorSelectionChangedListener);
    		}
    		
             final Tree tree = new Tree(sashForm, SWT.MULTI);
             treeViewer = new NonCollapsingTreeViewer(tree);

             treeViewer.setContentProvider(new DelayedAdapterFactoryContentProvider(adapterFactory));
             treeViewer.setLabelProvider(new DecoratingColumLabelProvider(new AdapterFactoryLabelProvider(adapterFactory), new DiagnosticDecorator(editingDomain, treeViewer, EcoreEditorPlugin.getPlugin().getDialogSettings())));
             treeViewer.setInput(getInputForSelectionViewer(editingDomain));
             treeViewer.setSelection(new StructuredSelection(editingDomain.getResourceSet().getResources().get(0)), true);
             treeViewer.expandAll();
             tree.addMouseListener(new MouseAdapter() {
     			@Override
     			public void mouseDoubleClick(MouseEvent e) {
     				maximizeControl(tree);
     			}
     		 });
             
             new AdapterFactoryTreeEditor(treeViewer.getTree(), adapterFactory);
             new ColumnViewerInformationControlToolTipSupport(treeViewer, new DiagnosticDecorator.EditingDomainLocationListener(editingDomain, treeViewer));
             getSite().setSelectionProvider(treeViewer);
             createContextMenuFor(treeViewer);

             ViewerFilter filter = new ViewerFilter() {
 				@Override
 				public boolean select(Viewer viewer, Object parentElement, Object element) {
 					return (!(element instanceof FactoryImpl));
 				}
 		      };
 		      treeViewer.addFilter(filter);
      }

	  private void createContextMenuFor(TreeViewer viewer) {
          MenuManager contextMenu = new MenuManager("#PopUp");
          contextMenu.add(new Separator("additions"));
          contextMenu.setRemoveAllWhenShown(true);
          contextMenu.addMenuListener(this);
          Menu menu = contextMenu.createContextMenu(viewer.getControl());
          viewer.getControl().setMenu(menu);
          getSite().registerContextMenu(contextMenu,
                          new UnwrappingSelectionProvider(viewer));
  
          int dndOperations = DND.DROP_COPY | DND.DROP_MOVE | DND.DROP_LINK;
          Transfer[] transfers = new Transfer[] { LocalTransfer.getInstance(), LocalSelectionTransfer.getTransfer(), FileTransfer.getInstance() };
          viewer.addDragSupport(dndOperations, transfers, new ViewerDragAdapter(viewer));
          viewer.addDropSupport(dndOperations, transfers, new EditingDomainViewerDropAdapter(editingDomain, viewer));
	}


	private class EditingDomainProvider extends AdapterImpl implements IEditingDomainProvider {
	    	public EditingDomain getEditingDomain() {
				return editingDomain;
			}
			@Override
			public boolean isAdapterForType(Object type) {
				return IEditingDomainProvider.class.equals(type);
			}
	}


	public EditingDomain getEditingDomain() {
		return editingDomain;
	}
	
	 /**
     * This implements {@link org.eclipse.jface.action.IMenuListener} to help
     * fill the context menus with contributions from the Edit menu. 
     */
    public void menuAboutToShow(IMenuManager menuManager) {
    	((IMenuListener) getEditorSite()
    			.getActionBarContributor())
                .menuAboutToShow(menuManager);
    }

    public EditingDomainActionBarContributor getActionBarContributor() {
    	return (EditingDomainActionBarContributor) getEditorSite()
    			.getActionBarContributor();
    }

    public IActionBars getActionBars() {
    	return getActionBarContributor().getActionBars();
    }
	

	
	protected ISelectionChangedListener createEditorSelectionChangedListener() {
		return new ISelectionChangedListener() {
			public void selectionChanged(SelectionChangedEvent event) {
				try {
					dispatchSelectionChangedEvent(event);
				} catch (Exception e) {
					LOGGER.error("selectionChanged() failed", e);
				}
			}
			
		};
	}
	
	protected void dispatchSelectionChangedEvent(SelectionChangedEvent event) {
		if (isIgnored(event)) {
			resetIgnore(event);
			return;
		}
		
		ISelection selection = event.getSelection();
		if (selection.isEmpty())
			return;
		
		if (event.getSource() == treeViewer) {
			if (selection instanceof ITreeSelection) {
				onTreeViewSelectionChange((ITreeSelection) selection);
			}
		}
		
		if (event.getSource() == getSourceViewer()) {
			if (selection instanceof ITextSelection) {
				onSourceViewSelectionChange((ITextSelection) selection);
			}
		}
	}
	
	protected boolean isIgnored(SelectionChangedEvent event) {
		if (event.getSource() == treeViewer) {
			return ignoreTreeViewSelectionChange;
		}
		if (event.getSource() == getSourceViewer()) {
			return ignoreSourceViewSelectionChange;
		}
		return false;
	}
	
	protected void resetIgnore(SelectionChangedEvent event) {
		if (event.getSource() == treeViewer) {
			ignoreTreeViewSelectionChange = false;
		}
		if (event.getSource() == getSourceViewer()) {
			ignoreSourceViewSelectionChange = false;
		}
	}

	protected void onSourceViewSelectionChange(ITextSelection textSelection) {
		final EObject selectedObject = getSelectedObject(textSelection.getOffset());
		if (selectedObject == null)
			return;
		
		getContainer().getDisplay().asyncExec(new Runnable() {
		
			public void run() {
				updateTreeView(EcoreUtil.getURI(selectedObject), true);
			}
		
		});
	}
	
	protected void onTreeViewSelectionChange(ITreeSelection selection) {
		
		Object firstElement = selection.getFirstElement();
		if (firstElement instanceof EObject)
			updateSourceView((EObject) firstElement);
	}

	protected void updateSourceView(final EObject obj) {
		ITextRegion textRegion = getTextRegion(obj);
		if (textRegion == null)
			return;
		
		setSourceRedraw(false);
		try {
			getSourceViewer().setSelectedRange(textRegion.getOffset(), 0);
			getSourceViewer().revealRange(textRegion.getOffset(), textRegion.getLength());
		} finally {
			setSourceRedraw(true);
		}
	}

	protected ITextRegion getTextRegion(final EObject obj) {
		return readOnly(new IUnitOfWork<ITextRegion, EFactoryResource>() {
			public ITextRegion exec(EFactoryResource resource) throws Exception {
				NewObject newObject = resource.getEFactoryNewObject(obj);
				return locationInFileProvider.getSignificantTextRegion(newObject);
			}
		});
	}
	
	protected void setSourceRedraw(boolean sourceRedraw) {
		if (sourceRedraw) {
			ignoreSourceViewSelectionChange = true;
		}
		ISourceViewer sourceViewer = getSourceViewer();
		if (sourceViewer instanceof ITextViewerExtension) {
			ITextViewerExtension textViewer = (ITextViewerExtension) sourceViewer;
			textViewer.setRedraw(sourceRedraw);
		}
	}

	protected EObject getSelectedObject(final int offset) {
		return readOnly(new IUnitOfWork<EObject, EFactoryResource>() {
			public EObject exec(EFactoryResource resource) throws Exception {
				EObject semanticObject = eObjectAtOffsetHelper.resolveContainedElementAt(resource, offset);
				while (semanticObject != null) {
					if (semanticObject instanceof NewObject) {
						NewObject newObject = (NewObject) semanticObject;
						return resource.getEFactoryEObject(newObject).orNull();
					}
					semanticObject = semanticObject.eContainer();
				}
				return null;
			}
		});
	}
		
		@Override
		public void dispose() {
			if (adapterFactory != null) {
				adapterFactory.dispose();
			}
			editingDomain = null;
			editorSelectionChangedListener = null;
			treeViewer = null;
			super.dispose();
		}
		
		@Override
		public void init(IEditorSite site, IEditorInput input) throws PartInitException {
			super.init(site, input);
			IContextService contextService = (IContextService) getSite().getService(IContextService.class);
			if (contextService != null) {
				// This String constant is referenced in the plugin.xml
				contextService.activateContext("org.eclipse.emf.eson.ui.context");
			}
		}
		
	/**
	 * Maximize the child control of sashform passed.
	 * 
	 * @param control
	 *            - the child control of sashform to maximize or restore
	 */
	private void maximizeControl(Control control) {
		if (sashForm.getMaximizedControl() == control) {
			sashForm.setMaximizedControl(null);
		} else {
			sashForm.setMaximizedControl(control);
		}
	}
	
	protected EObject resolve(final URI uri) {
		return getDocument().readOnly(new IUnitOfWork<EObject, XtextResource>() {
			@Override  public EObject exec(XtextResource state) throws Exception {
				return state.getResourceSet().getEObject(uri, true);
			}
			
		});
	}
	
	protected <T> T readOnly(final IUnitOfWork<T, EFactoryResource> unitOfWork) {
		return getDocument().readOnly(new IUnitOfWork<T, XtextResource>() {
			@Override public T exec(XtextResource state) throws Exception {
				return unitOfWork.exec((EFactoryResource) state);
			}
			
		});
	}

    @Override
    public String getContributorId() {
        return getSite().getId();
    }
    
    @Override
    public Object getAdapter(Class adapter) {
        if (adapter == IPropertySheetPage.class)
            return new TabbedPropertySheetPage(this);
        return super.getAdapter(adapter);
    }

}
