/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2013 - 2014 Michael Vorburger
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.resource;

import java.util.List;

import org.apache.log4j.Logger;
import org.eclipse.emf.common.notify.Adapter;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.eson.building.ModelBuilder;
import org.eclipse.emf.eson.building.ModelBuilderException;
import org.eclipse.emf.eson.eFactory.Factory;
import org.eclipse.emf.eson.eFactory.NewObject;
import org.eclipse.emf.eson.serialization.EFactoryAdapter;
import org.eclipse.xtext.EcoreUtil2;
import org.eclipse.xtext.parser.antlr.IReferableElementsUnloader;
import org.eclipse.xtext.resource.DerivedStateAwareResource;
import org.eclipse.xtext.resource.IDerivedStateComputer;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Provider;

/**
 * Adds the actual EObject. Uses the FactoryBuilder.
 * Xtext Index Builder calls this at appropriate times.
 * 
 * Credit where credit is due: the idea of using (something like) this was
 * originally raised by Sebastian Benz in private email exchange - I only dug
 * into it and coded it out.
 * 
 * @author Michael Vorburger
 */
public class EFactoryDerivedStateComputer implements IDerivedStateComputer {
	private static final Logger logger = Logger.getLogger(EFactoryDerivedStateComputer.class);

	@Inject
	private IReferableElementsUnloader unloader;
	
	@Inject 
	private Provider<EFactoryAdapter> eFactoryAdapterProvider;
	
	/**
	 * Creates the "real" EObjects, represented by ESON.
	 * 
	 * Care must be taken here to not resolve cross references in preLinkingPhase.
	 * This implementation ensures this is respected for the value EReference in Reference,
	 * but must make an exception to the rule, because it HAS to resolve the references
	 * to the metamodel, namely NewObject eClass (i.e. getEClass(), NOT its eClass())
	 * and Feature's eFeature EStructuralFeature, etc.  In practice this is not a
	 * problem, because the Ecore model for an ESON in a workspace is either in
	 * some plug-in behind, or in a different project in the same workspace;
	 * this constraint is verified by a Validator check. 
	 * 
	 * @param preLinkingPhase see also org.eclipse.xtext.xbase.jvmmodel.IJvmModelInferrer.infer(EObject, IJvmDeclaredTypeAcceptor, boolean) for documentation.
	 * 		true -> create EObjects which should be exported, don't resolve any cross references
	 * 		false -> create all EObjects, also cross references can be resolved
	 */
	@Override
	public void installDerivedState(DerivedStateAwareResource resource, boolean preLinkingPhase) {
		// @see https://bugs.eclipse.org/bugs/show_bug.cgi?id=437848 (AKA DS-7543)
		try {
			safeInstallDerivedState(resource, preLinkingPhase);
		} catch (RuntimeException e) {
			handleRuntimeException("installDerivedState", resource, e);
		} finally {
			resource.eAdapters().add(eFactoryAdapterProvider.get());
		}
	}

	protected void safeInstallDerivedState(DerivedStateAwareResource resource, boolean preLinkingPhase) {
		// implementation inspired by XcoreModelAssociator (more than JvmModelAssociator) 
		Factory model = getFactory(resource);
		if (model == null)
			return;
    	NewObject newObjectRoot = model.getRoot();
		if (newObjectRoot != null && newObjectRoot.getEClass() == null) {
    		// Special handling for common use case of completely empty *.efactory.
    		// It would work without this as well, but this avoids the Exception & log below.
    		return;
    	}
    	if (newObjectRoot != null && newObjectRoot.getEClass() != null && newObjectRoot.getEClass().eIsProxy()) {
    		// If linking to the used eClass is not available yet,
    		// then just give up (as it will come back and retry)
    		return;
    	}
    	
    	EFactoryResource efResource = (EFactoryResource) resource;
		ModelBuilder builder = efResource.getBuilder();
		try {
			Optional<EObject> eModel = builder.buildWithoutLinking(model, preLinkingPhase);
			
			if (eModel.isPresent()) {
				if (!preLinkingPhase) {
					builder.link();
				}
				// Do add() only AFTER the buildWithoutLinking() + link(),
				// because we don't want/need to get the notifications from our
				// ModelBuilder - only from external clients (e.g. Generic Ecore
				// editor UI, etc.)
				EObject object = eModel.get();
				resource.getContents().add(object);
			} else {
				builder.clear();
			}
		} catch (ModelBuilderException e) {
			builder.clear();
			logger.error(resource.getURI() + " could not be transformed by ModelBuilder (this may be normal if incomplete while editing; but please raise a bug attaching your *.eson so that the code can be improved to log this yet continue to build the rest of the internal model)", e);
			// No need for something like this:
			// resource.getErrors().add(new ExceptionDiagnostic(e));
			// that would only lead to duplicate errors - the resource
			// will (most likely, should) already have error markers if
			// not, it's better to write new specific validation rules.
		}
		resource.getCache().clear(resource);
	}

	protected Factory getFactory(DerivedStateAwareResource resource) {
		if (resource.getContents().isEmpty()) {
			return null;
		}
		return (Factory) resource.getContents().get(0);
	}
	
	@Override
	public void discardDerivedState(DerivedStateAwareResource resource) {
		EList<Adapter> eAdapters = resource.eAdapters();
		Adapter adapter = EcoreUtil2.getAdapter(eAdapters, EFactoryAdapter.class);
		eAdapters.remove(adapter);
		
		// @see https://bugs.eclipse.org/bugs/show_bug.cgi?id=437848 (AKA DS-7543)
		try {
			safeDiscardDerivedState(resource);
		} catch (RuntimeException e) {
			handleRuntimeException("discardDerivedState()", resource, e);
		}
	}
	
	protected void safeDiscardDerivedState(DerivedStateAwareResource resource) {
		// implementation again inspired by XcoreModelAssociator and JvmModelAssociator 
		EFactoryResource efResource = (EFactoryResource) resource;
		ModelBuilder builder = efResource.getBuilder();
		builder.clear();
		
	    EList<EObject> contents = resource.getContents();
	    if (contents.size() > 1) // first content (size 1) is original Xtext (non-derived) model
	    {
	    	List<EObject> derived = Lists.newArrayList();
	    	// note start index 1 instead of 0 - we need to skip first content, see above
	    	for (int i = 1; i< contents.size(); i++) {
				EObject eObject = contents.get(i);
	    		unloader.unloadRoot(eObject);
	    		derived.add(eObject);
			}
	    	try {
		    	contents.removeAll(derived);	    		
	    	} catch (ArrayIndexOutOfBoundsException e) {
	    		// Bof.
	    	}
	    }
	}

	protected  void handleRuntimeException(String method, Resource resource, RuntimeException e) {
		String uri = "?";
		if (resource != null && resource.getURI() != null) {
			uri = resource.getURI().toString();
		}
		logger.error("RuntimeException occured during " + method + "() of " + uri + " :" + e.getMessage(), e);
	}

}
