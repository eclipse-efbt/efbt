/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2016 Michael Vorburger and others
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */package org.eclipse.emf.eson.scoping;

import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EcorePackage;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.eclipse.emf.eson.eFactory.EFactoryPackage;
import org.eclipse.emf.eson.eFactory.PackageImport;
import org.eclipse.emf.eson.util.EPackageRegistry;
import org.eclipse.xtext.linking.impl.DefaultLinkingService;
import org.eclipse.xtext.linking.impl.IllegalNodeException;
import org.eclipse.xtext.naming.QualifiedName;
import org.eclipse.xtext.nodemodel.INode;
import org.eclipse.xtext.resource.IEObjectDescription;
import org.eclipse.xtext.resource.IResourceDescriptions;
import org.eclipse.xtext.resource.impl.ResourceDescriptionsProvider;
import org.eclipse.xtext.scoping.IGlobalScopeProvider;
import org.eclipse.xtext.scoping.IScope;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.inject.Inject;

/**
  * ILinkingService which deals with EPackage references in a particular manner.
  * 
  * @see org.eclipse.xtext.xtext.XtextLinkingService which was the inspiration for this class
  * 
  * @author Michael Vorburger
  */
@SuppressWarnings("restriction")
public class ESONLinkingService extends DefaultLinkingService {

	private static final Logger log = Logger.getLogger(ESONLinkingService.class);

	private @Inject IGlobalScopeProvider scopeProvider;
	private @Inject ResourceDescriptionsProvider descriptionsProvider;
	private @Inject EPackageRegistry ePackageRegistry;
	
	@Override
	public List<EObject> getLinkedObjects(EObject context, EReference ref, INode node) throws IllegalNodeException {
		if (ref == EFactoryPackage.eINSTANCE.getPackageImport_EPackage() && context instanceof PackageImport)
			return getPackage((PackageImport)context, getLinkingHelper().getCrossRefNodeAsString(node, true));
		return super.getLinkedObjects(context, ref, node);
	}

	// Following code is (way too much..) copy/pasted (almost) verbatim/as-is from XtextLinkingService
	// Proposed to Xtext to factor out re-usable code not related to xtext.xtext,
	// but of interest to any langs dealing with EPackage, via bug/issue
	// https://bugs.eclipse.org/bugs/show_bug.cgi?id=489748.
	
	private List<EObject> getPackage(PackageImport context, String nsUri) {
		if (nsUri == null)
			return Collections.emptyList();
		QualifiedName packageNsURI = QualifiedName.create(nsUri);
		EPackage pack = findPackageInScope(context, packageNsURI);
		if (pack == null) {
			pack = findPackageInAllDescriptions(context, packageNsURI);
			if (pack == null) {
				pack = loadEPackage(nsUri, context.eResource().getResourceSet());
			}
		}
		if (pack != null)
			return Collections.<EObject>singletonList(pack);
		return Collections.emptyList();
	}

	private EPackage findPackageInScope(EObject context, QualifiedName packageNsURI) {
		IScope scopedPackages = scopeProvider.getScope(context.eResource(), EFactoryPackage.Literals.PACKAGE_IMPORT__EPACKAGE, new Predicate<IEObjectDescription>() {
			@Override
			public boolean apply(IEObjectDescription input) {
				return isNsUriIndexEntry(input);
			}
		});
		IEObjectDescription description = scopedPackages.getSingleElement(packageNsURI);
		if (description != null) {
			return getResolvedEPackage(description, context);
		}
		return null;
	}
	
	private EPackage findPackageInAllDescriptions(EObject context, QualifiedName packageNsURI) {
		IResourceDescriptions descriptions = descriptionsProvider.getResourceDescriptions(context.eResource());
		if (descriptions != null) {
			Iterable<IEObjectDescription> exported = descriptions.getExportedObjects(EcorePackage.Literals.EPACKAGE, packageNsURI, false);
			for(IEObjectDescription candidate: exported) {
				if (isNsUriIndexEntry(candidate)) {
					EPackage result = getResolvedEPackage(candidate, context);
					if (result != null)
						return result;
				}
			}
		}
		return null;
	}

	private EPackage loadEPackage(String resourceOrNsURI, ResourceSet resourceSet) {
		Optional<EPackage> optionalResult = ePackageRegistry.getEPackage(resourceSet, resourceOrNsURI);
		if (optionalResult.isPresent())
			return optionalResult.get();

		URI uri = URI.createURI(resourceOrNsURI);
		try {
			if ("http".equalsIgnoreCase(uri.scheme()))
				return null;
			if (uri.fragment() == null) {
				Resource resource = resourceSet.getResource(uri, true);
				if (resource.getContents().isEmpty())
					return null;
				EPackage result = (EPackage) resource.getContents().get(0);
				return result;
			}
			EPackage result = (EPackage) resourceSet.getEObject(uri, true);
			return result;
		} catch(RuntimeException ex) {
			if (uri.isPlatformResource()) {
				String platformString = uri.toPlatformString(true);
				URI platformPluginURI = URI.createPlatformPluginURI(platformString, true);
				return loadEPackage(platformPluginURI.toString(), resourceSet);
			}
			log.trace("Cannot load package with URI '" + resourceOrNsURI + "'", ex);
			return null;
		}
	}

	private EPackage getResolvedEPackage(IEObjectDescription description, EObject context) {
		EObject resolved = EcoreUtil.resolve(description.getEObjectOrProxy(), context);
		if (resolved != null && !resolved.eIsProxy() && resolved instanceof EPackage)
			return (EPackage) resolved;
		return null;
	}
	
	private boolean isNsUriIndexEntry(IEObjectDescription candidate) {
		return Boolean.TRUE.toString().equalsIgnoreCase(candidate.getUserData("nsURI"));
	}

}
