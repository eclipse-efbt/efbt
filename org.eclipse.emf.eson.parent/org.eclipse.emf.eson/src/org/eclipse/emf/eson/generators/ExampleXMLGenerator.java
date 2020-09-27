/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2015 Michael Vorburger
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.generators;

import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.eclipse.emf.ecore.xmi.impl.GenericXMLResourceFactoryImpl;
import org.eclipse.xtext.generator.IFileSystemAccess;
import org.eclipse.xtext.generator.IFileSystemAccessExtension2;

import com.google.common.base.Optional;

/**
 * Example of an ESON to XML generator.
 * 
 * This is an example - you may wish to sub-class this to refine it for your particular needs.
 * To activate (a subclass of) this, list it in your META-INF/services/org.eclipse.xtext.generator.IGenerator
 *  
 * @author Michael Vorburger
 */
public class ExampleXMLGenerator extends AbstractGeneratorWithException {

	protected GeneratorHelper helper = new GeneratorHelper(); // NOTE: @Inject is NOT supported here

	@Override
	public void doGenerateWithException(Resource input, IFileSystemAccess fsa) throws Exception {
		Optional<EObject> optionalRoot = getESONEObjectFromResource(input);
		if (!optionalRoot.isPresent())
			return;

		EObject newRoot = EcoreUtil.copy(optionalRoot.get());
		transform(newRoot, input);
		
		URI xmlURI = getXmlURI(input.getURI(), input, fsa);
		Resource xmlResource = getXMLResource(xmlURI, input, fsa);
		xmlResource.getContents().add(newRoot);
		try {
			xmlResource.save(null);
		} finally {
			xmlResource.unload();
		}
	}

	protected URI getXmlURI(URI esonURI, Resource context, IFileSystemAccess fsa) {
		String esonPath = helper.getProjectRelativeFileName(esonURI, fsa);
		String xmlPath = helper.replaceExtension(esonPath, "xml");

		IFileSystemAccessExtension2 fsa2 = (IFileSystemAccessExtension2) fsa;
		return fsa2.getURI(xmlPath);
	}

	protected Resource.Factory getXMLResourceFactory(Resource context, IFileSystemAccess fsa) {
		return new GenericXMLResourceFactoryImpl();
	}
	
	protected Resource getXMLResource(URI xmlURI, Resource context, IFileSystemAccess fsa) {
		return getXMLResourceFactory(context, fsa).createResource(xmlURI);
	}

	protected Optional<EObject> getESONEObjectFromResource(Resource input) {
		EList<EObject> contents = input.getContents();
		if (contents.size() > 1)
			return Optional.of(contents.get(1));
		else 
			return Optional.absent();
	}

	/**
	 * @see http://blog2.vorburger.ch/2012/07/xtext-xml.html
	 */
	protected void transform(EObject xmlRoot, Resource context) {
	}

}