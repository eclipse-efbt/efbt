/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2015 Anton Kosyakov, Michael Vorburger & others.
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
import java.util.List;

import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.eclipse.emf.edit.provider.IItemPropertyDescriptor;
import org.eclipse.emf.edit.provider.IItemPropertySource;
import org.eclipse.emf.edit.ui.provider.PropertyDescriptor;
import org.eclipse.ui.views.properties.IPropertyDescriptor;
import org.eclipse.ui.views.properties.IPropertySource;
import org.eclipse.ui.views.properties.IPropertySource2;
import org.eclipse.xtext.resource.XtextResource;
import org.eclipse.xtext.ui.editor.model.IXtextDocument;
import org.eclipse.xtext.util.concurrent.IUnitOfWork;

import com.google.common.collect.Lists;

/**
 * IPropertySource which uses IXtextDocument IReadAccess readOnly() and
 * IWriteAccess modify() instead of directly reading/changing the EObject.
 * 
 * @author Anton Kosyakov
 */
public class URIBasedPropertySource implements IPropertySource, IPropertySource2 {

	protected final URI objectURI;
	protected final IXtextDocument document;
	protected final IItemPropertySource itemPropertySource;
	
	public URIBasedPropertySource(EObject object, IItemPropertySource itemPropertySource, IXtextDocument document) {
		this.document = document;
		this.objectURI = EcoreUtil.getURI(object);
		this.itemPropertySource = itemPropertySource;
	}

	public Object getEditableValue() {
		return document.readOnly(new IUnitOfWork<Object, XtextResource>() {

			public Object exec(XtextResource state) throws Exception {
				if (state == null)
					return null;
				EObject eObject = state.getEObject(objectURI.fragment());
				if (eObject == null) {
					return null;
				}
				return itemPropertySource.getEditableValue(eObject);
			}
			
		});
	}

	public IPropertyDescriptor[] getPropertyDescriptors() {
		return document.readOnly(new IUnitOfWork<IPropertyDescriptor[], XtextResource>() {

			public IPropertyDescriptor[] exec(XtextResource state) throws Exception {
				Collection<IPropertyDescriptor> result = new ArrayList<IPropertyDescriptor>();
				// state "should" never be null, but it sometimes is.. Xtext bug? Better guard:
				EObject eObject = state != null ? state.getEObject(objectURI.fragment()) : null;
				if (eObject != null) {
				    List<IItemPropertyDescriptor> itemPropertyDescriptors = itemPropertySource.getPropertyDescriptors(eObject);
				    if (itemPropertyDescriptors != null)
				    {
				      for (IItemPropertyDescriptor itemPropertyDescriptor : itemPropertyDescriptors)
				      {
				        result.add(createPropertyDescriptor(eObject, itemPropertyDescriptor));
				      }
				    }
				}
			    return result.toArray(new IPropertyDescriptor [result.size()]);
			}
			
		});
	}
	
	protected IPropertyDescriptor createPropertyDescriptor(EObject eObject, IItemPropertyDescriptor itemPropertyDescriptor) {
	    return new PropertyDescriptor(eObject, itemPropertyDescriptor);
	}

	public Object getPropertyValue(final Object propertyId) {
		return document.readOnly(new IUnitOfWork<Object, XtextResource>() {

			public Object exec(XtextResource state) throws Exception {
				EObject eObject = state.getEObject(objectURI.fragment());
				if (eObject == null) {
					return null;
				}
				return itemPropertySource.getPropertyDescriptor(eObject, propertyId).getPropertyValue(eObject);
			}
			
		});
	}

	public boolean isPropertySet(final Object propertyId) {
		return document.readOnly(new IUnitOfWork<Boolean, XtextResource>() {

			public Boolean exec(XtextResource state) throws Exception {
				EObject eObject = state.getEObject(objectURI.fragment());
				if (eObject == null) {
					return false;
				}
				return itemPropertySource.getPropertyDescriptor(eObject, propertyId).isPropertySet(eObject);
			}
			
		});
	};

	public boolean isPropertyResettable(final Object propertyId) {
		return document.readOnly(new IUnitOfWork<Boolean, XtextResource>() {

			public Boolean exec(XtextResource state) throws Exception {
				EObject eObject = state.getEObject(objectURI.fragment());
				if (eObject == null) {
					return false;
				}
			    IItemPropertyDescriptor propertyDescriptor = itemPropertySource.getPropertyDescriptor(eObject, propertyId);
			    return propertyDescriptor.canSetProperty(eObject) && propertyDescriptor.isPropertySet(eObject);
			}
			
		});
	}

	/** is most likely only accessed within the UI thread, so I skipped 'synchronized' blocks. */
	private List<Object> ongoingChanges = Lists.newLinkedList();
	
	private boolean aboutToChange(final Object propertyId) {
		if (ongoingChanges.contains(propertyId)) {
			return true;
		} else {
			ongoingChanges.add(propertyId);
			return false;
		}
	}

	private void completedChange(final Object propertyId) {
		Lists.reverse(ongoingChanges).remove(propertyId);
	}

	public void setPropertyValue(final Object propertyId, final Object value) {
		if (aboutToChange(propertyId)) {
			// we're running into a potentially infinite loop! stop here!	
			return;
		}

		document.modify(new IUnitOfWork.Void<XtextResource>() {

			@Override
			public void process(XtextResource state) throws Exception {
				EObject eObject = state.getEObject(objectURI.fragment());
				if (eObject != null) {
					itemPropertySource.getPropertyDescriptor(eObject, propertyId).setPropertyValue(eObject, value);
				}
			}
		});
		
		completedChange(propertyId);
	};

	public void resetPropertyValue(final Object propertyId) {
		if (aboutToChange(propertyId)) {
			// we're running into a potentially infinite loop! stop here!			
			return;
		}

		document.modify(new IUnitOfWork.Void<XtextResource>() {

			@Override
			public void process(XtextResource state) throws Exception {
				EObject eObject = state.getEObject(objectURI.fragment());
				if (eObject != null) {
					itemPropertySource.getPropertyDescriptor(eObject, propertyId).resetPropertyValue(eObject);
				}
			}			
		});

		completedChange(propertyId);
	}

}
