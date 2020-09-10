/*
 * #%L
 * org.eclipse.emf.eson.tests
 * %%
 * Copyright (C) 2013 - 2014 Michael Vorburger
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.serialization.tests;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import javax.inject.Inject;

import org.eclipse.emf.common.util.DiagnosticException;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.compare.Comparison;
import org.eclipse.emf.compare.Diff;
import org.eclipse.emf.compare.EMFCompare;
import org.eclipse.emf.compare.scope.DefaultComparisonScope;
import org.eclipse.emf.compare.scope.IComparisonScope;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.eson.EFactoryServiceProvider;
import org.eclipse.emf.eson.eFactory.Factory;
import org.eclipse.emf.eson.resource.EFactoryResource;
import org.eclipse.emf.eson.serialization.FactoryBuilder;
import org.eclipse.emf.eson.tests.util.ResourceProvider;
import org.eclipse.xtext.resource.IEObjectDescription;
import org.eclipse.xtext.resource.IResourceDescription;
import org.eclipse.xtext.resource.XtextResourceSet;
import org.junit.Assert;

public abstract class AbstractSerializationTest {

	private File temp;
	static final String ROOT_FOLDER = "res/SerializationTests/";

	@Inject protected ResourceProvider resourceProvider; // = new ResourceProvider(TestConstants.PLUGIN_ID);
	
//	@Inject
//	private ISerializer serializer;

	@Inject IResourceDescription.Manager resourceDescriptionManager;
	@Inject EFactoryServiceProvider serviceProvider;
	
	protected void printExportedObjects(Resource r) {
		System.out.println(r.getURI());
		Iterable<IEObjectDescription> eods = resourceDescriptionManager.getResourceDescription(r).getExportedObjects();
		for (IEObjectDescription eod : eods) {
			System.out.println("\t" + eod.getQualifiedName() + " - " + eod.getEClass().getName());
		}
	}
	
	protected URI createTempUri() throws IOException {
		temp = File.createTempFile("testModel", ".efactory");
		temp.deleteOnExit();
		return URI.createFileURI(temp.getAbsolutePath());
	}

	public void compare(Factory expected, Factory actual)
			throws InterruptedException, IOException {
		
		// Following is needed to remove the TestModel(Impl) object which we
		// added in the EFactoryDerivedStateComputer. It's a bit of a hack, but
		// how to do this better with EMF Compare? Why is it even looking at
		// another element in content the resource - if you're asking it to
		// match from the Factory only?!
		final EList<EObject> contents = expected.eResource().getContents();
		if (contents.size() == 2) {
			contents.remove(1);
		} 
		if (contents.size() != 1) {
			throw new IllegalStateException("why is there MORE contents in the resource? EMF Compare diff test will not work..");
		}
		
		// @see http://wiki.eclipse.org/EMF_Compare/Developer_Guide
		IComparisonScope scope = new DefaultComparisonScope(expected, actual, null);
		EMFCompare comparator = EMFCompare.builder().build();
		Comparison comparison = comparator.compare(scope);

		// Matching model elements
		EList<Diff> differences = comparison.getDifferences();
		// TODO not sure if differences.toString() is useful?
		Assert.assertTrue("Models differ: " + differences.toString(), differences.isEmpty());
	}

	protected void performSerializationTest(String name) throws Exception {
		EObject testModel = loadTestModel(name);
		Factory expected = ((EFactoryResource) testModel.eResource()).getEFactoryFactory();
		
		FactoryBuilder builder = new FactoryBuilder(serviceProvider);
		Factory actual = builder.buildFactory(testModel);

		assertModelsEquals(expected, actual);
	}

	private void assertModelsEquals(Factory expected, Factory actual) throws InterruptedException, IOException {
		printExportedObjects(expected.eResource());
		// System.out.println(toString(expected));
		
		XtextResourceSet rs = new XtextResourceSet();
		Resource r = rs.createResource(createTempUri());
		r.getContents().add(actual);		
		// !!!!!!!!!!
/*		
		DerivedStateAwareResource dR = (DerivedStateAwareResource) r;
		dR.discardDerivedState();
		dR.installDerivedState(true);
		dR.installDerivedState(false);
*/		
		r.getContents().get(1);
		printExportedObjects(r);
		// TODO careful.. just using serializer.serialize without going through Resource will not trigger derived state calculation and IResourceDescription.Manager invocation!
		System.out.println(SerializationUtils.toString(actual.eResource()));
		compare(expected, actual);
	}

	protected EObject loadTestModel(String name) throws IOException, DiagnosticException {
		String path = ROOT_FOLDER + name;
		EObject model = resourceProvider.loadModel(path);
		removeAdapters(model);
		return model;
	}

	private void removeAdapters(EObject model) {
		model.eAdapters().clear();
		Iterator<EObject> iterator = model.eAllContents();
		while (iterator.hasNext()) {
			EObject object = iterator.next();
			object.eAdapters().clear();
		}
	}

}
