/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2009 - 2014 Michael Vorburger
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.util;

import java.util.List;

import javax.inject.Inject;

import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.InternalEObject;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.eson.eFactory.Attribute;
import org.eclipse.emf.eson.eFactory.Containment;
import org.eclipse.emf.eson.eFactory.Feature;
import org.eclipse.emf.eson.eFactory.MultiValue;
import org.eclipse.emf.eson.eFactory.NewObject;
import org.eclipse.emf.eson.eFactory.Reference;
import org.eclipse.emf.eson.eFactory.Value;
import org.eclipse.emf.eson.resource.EFactoryResource;
import org.eclipse.xtext.linking.impl.LinkingHelper;
import org.eclipse.xtext.linking.lazy.LazyURIEncoder;
import org.eclipse.xtext.nodemodel.ICompositeNode;
import org.eclipse.xtext.nodemodel.INode;
import org.eclipse.xtext.nodemodel.util.NodeModelUtils;
import org.eclipse.xtext.util.Strings;

import com.google.common.base.Preconditions;

/**
 * Utility for Xtext related to EMF Proxies.
 * 
 * @see com.googlecode.efactory.tests.xtextintegration.tests.XtextProxyUtilXtextTest
 * @see com.googlecode.efactory.util.tests.XtextProxyUtilESONTest

 * @author Michael Vorburger
 */
public class XtextProxyUtil {
	
	// Originally published as https://github.com/vorburger/com.temenos.ds.op/search?q=XtextProxyUtil&type=Code
	// TODO remove it in ds.op, as that now depends on ESON (but KEEP OSS https://github.com/vorburger/com.temenos.ds.op/search?q=XtextProxyUtilTest&ref=cmdform in ds.op?) 
	
	// TODO Confirm if following slightly hack-ish (?) code, while working (see
	// tests ref. above), is the right/best way to go about this, or if an alternative
	// implementation approach idea I had, along the lines of somehow using an
	// ILocationInFileProvider (or ILocationInFileProviderExtension), with our special EFactoryLocationInFileProvider, and then
	// org.eclipse.xtext.resource.EObjectAtOffsetHelper.getCrossReferenceNode(XtextResource, ITextRegion)
	// would be possible, and more elegant?
	
	@Inject private LazyURIEncoder encoder;
	@Inject private LinkingHelper linkingHelper;

	/**
	 * Obtain the "name" of an EMF EReference cross reference instance as String.
	 * 
	 * Normally this is not needed, as you just getXYZ() to obtain the actual
	 * (non-Proxy) cross referenced instance itself. But in certain corner cases
	 * it can be useful to use this helper; e.g. when gen. code, on "broken" (red) 
	 * models with some problem in cross refs, if you'd like to still get the
	 * Cross Ref Link text (String) to... do something with it
	 * (i.e. imagine better error messages?).
	 * 
	 * @param context is the EObject that you got (via getXYZ()) the proxy from
	 * @param proxy the EObject, must be eIsProxy() - check before calling
	 * @return String textual representation of cross reference
	 * 
	 * @throws IllegalArgumentException if !eIsProxy(), or not part of a Xtext resource, etc.
	 */
	public String getProxyCrossRefAsString(EObject context, EObject proxy) {
		Preconditions.checkArgument(proxy.eIsProxy(), "EObject !eIsProxy() : %s", proxy.toString());
		URI proxyURI = ((InternalEObject) proxy).eProxyURI();
		Preconditions.checkNotNull(proxyURI, "EObject eIsProxy() BUT URI is null : %s", proxy.toString());
		String fragment = proxyURI.fragment();
		Preconditions.checkArgument(encoder.isCrossLinkFragment(context.eResource(), fragment),
				"URI is not Xtext Cross Link Fragment: %s", proxyURI);
		
		// TODO After Xtext upgrade, change to:
		// INode node = encoder.getNode(context, fragment);
		// instead of current:
		INode node = this.getNode(context, fragment, proxy);
		
		Preconditions.checkNotNull(node, "EObject context is not 'near' enough (it has no Node model)");
		String linkText = linkingHelper.getCrossRefNodeAsString(node, true);
		return linkText;
	}
	
	/**
	 * Copy/paste of a method from Xtext LazyURIEncoder.
	 * Couldn't avoid the copy/paste, because this needs to call NodeModelUtils.getNode() using realEObject, from proxy, not object; see 2nd getNode() below.
	 * TODO Avoid copy/paste, by making LazyURIEncoder extensible, as needed here.
	 */
	protected INode getNode(EObject object, String fragment, EObject proxy) {
		if (encoder.isUseIndexFragment(object.eResource())) {
			return encoder.decode(object.eResource(), fragment).getThird();
		}
		INode compositeNode = this.getNode(object, proxy); // INSTEAD of NodeModelUtils.getNode(object);
		if (compositeNode == null)
			throw new IllegalStateException("Couldn't resolve lazy link, because no node model is attached.");
		List<String> split = Strings.split(fragment, LazyURIEncoder.SEP);
		INode node = encoder.getNode(compositeNode, split.get(3));
		return node;
	}

	protected ICompositeNode getNode(EObject eObject, EObject proxy) {
		EObject esonRealObject = getParentEObjectWithNodelModel(eObject);
		EObject realEObject = getCorrectSubEObject(esonRealObject, proxy);
		return NodeModelUtils.getNode(realEObject);
	}

	protected EObject getCorrectSubEObject(EObject esonRealObject, EObject proxy) {
		if (!(esonRealObject instanceof NewObject))
			return esonRealObject;
		NewObject newObject = (NewObject) esonRealObject;
		return getCorrectSubEObject(newObject, (InternalEObject) proxy);
	}

	protected EObject getCorrectSubEObject(NewObject newObject, InternalEObject proxy) {
		for (Feature feature : newObject.getFeatures()) {
			Value value = feature.getValue();
			EObject eObject = getCorrectSubEObject(value, proxy);
			if (eObject != null) {
				return eObject;
			}
		}
		return null;
	}
	
	protected EObject getCorrectSubEObject(Value value, InternalEObject proxy) {
		if (value instanceof Reference) {
			Reference reference = (Reference) value;
			EObject referencedValue = reference.getValue();
			if (referencedValue.eIsProxy()) {
				URI referencedProxyURI = ((InternalEObject) referencedValue).eProxyURI();
				URI proxyURI = proxy.eProxyURI();
				if (proxyURI.equals(referencedProxyURI)) {
					return reference;
				}
			}
			return null;
		} else if (value instanceof Containment) {
			Containment containment = (Containment) value;
			NewObject containedNewObject = containment.getValue();
			return getCorrectSubEObject(containedNewObject, proxy);
		} else if (value instanceof MultiValue) {
			MultiValue mv = (MultiValue) value;
			EList<Value> values = mv.getValues();
			for (Value mvValue : values) {
				EObject eObject = getCorrectSubEObject(mvValue, proxy);
				if (eObject != null) {
					return eObject;
				}
			}
			return null;
		} else if (value instanceof Attribute) {
			return null;
		} else {
			throw new IllegalArgumentException();
		}
	}
	
	/**
	 * @param semanticObject an EObject from either an ESON Resource or a standard Xtext language resource 
	 * @return NewObject from ESON, or original semanticObject
	 */
	// TODO don't copy/paste from com.googlecode.efactory.resource.EFactoryLocationInFileProvider.getRealObject(EObject) but refactor into EFactoryResource
	protected EObject getParentEObjectWithNodelModel(EObject semanticObject) {
		Resource resource = semanticObject.eResource();
		if (!(resource instanceof EFactoryResource))
			return semanticObject;
		EFactoryResource efResource = (EFactoryResource) resource;
		NewObject newObject = efResource.getEFactoryNewObject(semanticObject);
		if (newObject != null)
			return newObject;
		else
			return semanticObject;
	}

}
