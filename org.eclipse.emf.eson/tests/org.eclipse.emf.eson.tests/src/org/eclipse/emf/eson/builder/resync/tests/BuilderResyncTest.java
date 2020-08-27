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
package org.eclipse.emf.eson.builder.resync.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.inject.Inject;

import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.eson.eFactory.Containment;
import org.eclipse.emf.eson.eFactory.EnumAttribute;
import org.eclipse.emf.eson.eFactory.Factory;
import org.eclipse.emf.eson.eFactory.Feature;
import org.eclipse.emf.eson.eFactory.IntegerAttribute;
import org.eclipse.emf.eson.eFactory.MultiValue;
import org.eclipse.emf.eson.eFactory.NewObject;
import org.eclipse.emf.eson.eFactory.StringAttribute;
import org.eclipse.emf.eson.eFactory.Value;
import org.eclipse.emf.eson.resource.EFactoryResource;
import org.eclipse.emf.eson.tests.util.ESONWithTestmodelInjectorProvider;
import org.eclipse.emf.eson.tests.util.LineEndingUtil;
import org.eclipse.emf.eson.tests.util.ResourceProvider;
import org.eclipse.xtext.junit4.InjectWith;
import org.eclipse.xtext.junit4.XtextRunner;
import org.eclipse.xtext.nodemodel.INode;
import org.eclipse.xtext.nodemodel.util.NodeModelUtils;
import org.eclipse.xtext.resource.SaveOptions;
import org.eclipse.xtext.resource.XtextResource;
import org.eclipse.xtext.serializer.ISerializer;
import org.eclipse.xtext.util.ReplaceRegion;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.inject.Provider;

import testmodel.AttributeSample;
import testmodel.AttributeTestContainer;
import testmodel.NameAttributeContainer;
import testmodel.SampleEnum;
import testmodel.SingleRequired;
import testmodel.TestModel;
import testmodel.TestmodelFactory;
import testmodel.TestmodelPackage;

/**
 * Tests EFactoryAdapter's "re-sychronization" of changes to the derived "real"
 * EObjects to the source EFactory model.
 * 
 * @author Michael Vorburger
 */
@RunWith(XtextRunner.class)
@InjectWith(ESONWithTestmodelInjectorProvider.class)
public class BuilderResyncTest {

	// using a Provider because we want each test to get a fresh ResourceProvider 
	@Inject Provider<ResourceProvider> rp;
	// @Inject DumpParsedStructureUtil dumper;

	@Test
	public void testChangeNameStringValueFeature() throws Exception {
		EList<EObject> resourceContents = rp.get().load("res/BuilderResyncTests/1TestModelWithNameProperty.efactory", true);
		
		// Check the EFactory model
		Factory eFactory = (Factory) resourceContents.get(0);
		checkNodes(eFactory);
		assertNotNull(eFactory);
		Value efValue = eFactory.getRoot().getFeatures().get(0).getValue();
		StringAttribute efStringValue = (StringAttribute) efValue;
		assertEquals("test", efStringValue.getValue());
		
		// Check the TestModel
		TestModel testModel = (TestModel) resourceContents.get(1);
		assertEquals("test", testModel.getName());
		
		// Change the EFactory model and re-check the test model (ModelBuilder recreated it)
		efStringValue.setValue("testit");
		((EFactoryResource)eFactory.eResource()).discardDerivedState();
		((EFactoryResource)eFactory.eResource()).installDerivedState(true); // intentionally true instead of false, because it simulates indexing, and makes sure attributes named "name" attributes are indexed (not just NewObject name) - unless we decide otherwise, see discussion point a) in ModelBuilder
		testModel = (TestModel) resourceContents.get(1);
		assertEquals("testit", testModel.getName());
		assertEquals(2, eFactory.getRoot().getFeatures().size());
		
		// Change the TestModel to a ValidID and re-check the EFactory model (EFactoryAdapter did it's thing)
		testModel.setName("tested");
		assertEquals(1, eFactory.getRoot().getFeatures().size());
		assertEquals("tested", eFactory.getRoot().getName()); // as the name is a ValidID, it SHOULD become "TestModel tested { ..."

		// Change the TestModel to a NON-ValidID and re-check the EFactory model (EFactoryAdapter did it's thing)
		testModel.setName("test#Model Name");
		assertEquals(2, eFactory.getRoot().getFeatures().size());
		assertEquals("test#Model Name", getRootObjectSecondFeatureAsString(eFactory));
		assertNull(eFactory.getRoot().getName());

		testModel.setName(null);
		assertEquals(1, eFactory.getRoot().getFeatures().size());
		checkNodes(eFactory);
	}

	protected String getRootObjectSecondFeatureAsString(Factory eFactory) {
		final Value efValue = eFactory.getRoot().getFeatures().get(1).getValue();
		final StringAttribute efStringValue = (StringAttribute) efValue;
		return efStringValue.getValue();
	}

	@Test
	public void testChangeNewObjectNameAfterClass() throws Exception {
		EList<EObject> resourceContents = rp.get().load("res/BuilderResyncTests/2TestModelWithNameAfterEClass.efactory", true);
		Factory eFactory = (Factory) resourceContents.get(0);

		// Check the TestModel
		TestModel testModel = (TestModel) resourceContents.get(1);
		assertEquals("test", testModel.getName());
		
		// Change the TestModel and re-check the EFactory model
		testModel.setName("tested");
		assertEquals("tested", eFactory.getRoot().getName());
		
		// Check setting null (unsetting)
		testModel.setName(null);
		assertNull(eFactory.getRoot().getName());
		checkNodes(eFactory);
	}

	@Test
	public void testSetNewFeature() throws Exception {
		EList<EObject> resourceContents = rp.get().load("res/BuilderResyncTests/1TestModelWithNameProperty.efactory", true);

		// Change the TestModel
		TestModel testModel = (TestModel) resourceContents.get(1);		
		AttributeSample attributeSample = TestmodelFactory.eINSTANCE.createAttributeSample();
		attributeSample.setSingleIntOptional(123);
		testModel.setAttributeSample(attributeSample);

		// Check the EFactory model
		Factory eFactory = (Factory) resourceContents.get(0);
		Value efValue = eFactory.getRoot().getFeatures().get(2).getValue();
		Containment efContainmentValue = (Containment) efValue;
		final NewObject newObject = efContainmentValue.getValue();
		// Since Containment's EClass became optional, expect null instead of TestmodelPackage.Literals.ATTRIBUTE_SAMPLE
		assertEquals(null, newObject.getEClass());
		IntegerAttribute singleIntOptional = (IntegerAttribute)newObject.getFeatures().get(0).getValue();
		assertEquals(123, singleIntOptional.getValue());
		checkNodes(eFactory);
	}

	/**
	 * Tests the NodeFixer stuff.
	 * This is required for the new synchronizing new split DSL/Tree Editor.
	 */
	@Test
	public void testAddNewContainerFeature() throws Exception {
		testLoadCheckAndSerialize("res/BuilderResyncTests/1TestModelWithNameProperty.efactory",
				"res/BuilderResyncTests/1TestModelWithNamePropertyEXPECTED.efactory");		
	}

	/**
	 * Test package scope when NS URIs are java like packages with dots. Example xxx.yyy.zzz instead of http://xxx.yyy.zzz
	 * <p>Test is done on sub-package to ensure that it does not only work with main package
	 */
	@Test
	public void testMultiPackageScoping() throws Exception {
		testLoadCheckAndSerialize("res/BuilderResyncTests/4TestModelWithSubPackage.efactory",
				"res/BuilderResyncTests/4TestModelWithSubPackageEXPECTED.efactory");
	}

	private void testLoadCheckAndSerialize(String eFactoryInput, String expectedDSLFile) throws Exception {
		ResourceProvider resourceProvider = rp.get();
		EList<EObject> resourceContents = resourceProvider.load(eFactoryInput, true);

		// Change the TestModel
		TestModel testModel = (TestModel) resourceContents.get(1);

		NameAttributeContainer nameAttributeContainer = TestmodelFactory.eINSTANCE.createNameAttributeContainer();
		// nameAttributeContainer.set...
		testModel.getNameAttributeTest().add(nameAttributeContainer);

		// Check the EFactory model
		Factory eFactory = (Factory) resourceContents.get(0);
		int posLastFeature = eFactory.getRoot().getFeatures().size() - 1;
		Value efValue = eFactory.getRoot().getFeatures().get(posLastFeature).getValue();
		MultiValue multiValue = (MultiValue) efValue;
		Value firstValue = multiValue.getValues().get(0);
		
		Containment efContainmentValue = (Containment) firstValue;
		final NewObject newObject = efContainmentValue.getValue();
		// Since Containment's EClass became optional, expect null instead of TestmodelPackage.Literals.NAME_ATTRIBUTE_CONTAINER
		assertEquals(null, newObject.getEClass());

		// Check if we can serialize the complete thing and it looks as expected
		XtextResource resource = (XtextResource) eFactory.eResource();
		ISerializer serializer = resource.getSerializer();
		String dsl = LineEndingUtil.fixLineEndings(serializer.serialize(eFactory));
		// NOTE The result is not as perfectly indented as we'd like it to
		// ideally be, this is because of https://bugs.eclipse.org/bugs/show_bug.cgi?id=396283;
		// see also EFactoryTextEditComposer.  However we do NOT want to force
		// format it, not in tests and not in real editor UI etc. because one of
		// the main points of ESON is that it doesn't have to reformat
		// everything.
		String expectedDSL = LineEndingUtil.fixLineEndings(resourceProvider.loadAsString(expectedDSLFile));
		assertEquals(expectedDSL, dsl);
		checkNodes(eFactory);
	}

	
	/**
	 * Tests that the NodeFixer correctly spaces out (indents) new features
	 * added programmatically; incl. e.g. via Properties sheet. This test didn't
	 * actually repro. the problem it was written to provide non-regression for,
	 * but is still useful; @see UIResyncTest for another similar test which
	 * does.
	 */
	@Test
	public void testNewFeaturesAreNotGluedToEachOther() throws Exception {
		ResourceProvider resourceProvider = rp.get();
		EList<EObject> resourceContents = resourceProvider.load("res/BuilderResyncTests/3TestModelWithNoSpace.eson", true);
		Factory eFactory = (Factory) resourceContents.get(0);
		TestModel testModel = (TestModel) resourceContents.get(1);
		XtextResource resource = (XtextResource) eFactory.eResource();
		ISerializer serializer = resource.getSerializer();

		Containment singleReqContainment = (Containment) eFactory.getRoot().getFeatures().get(0).getValue();
		ReplaceRegion replaceRegionOriginal = serializer.serializeReplacement(singleReqContainment, SaveOptions.defaultOptions());
		assertEquals(" SingleRequired {}", replaceRegionOriginal.getText());
		
		SingleRequired singleReq = testModel.getSingleRequired();
		singleReq.setParentAttribute(true);
		singleReq.setParentReference(testModel);
		
		// Check if we can serialize the complete thing and it looks as expected
		String expectedDSL = LineEndingUtil.fixLineEndings(resourceProvider.loadAsString("res/BuilderResyncTests/3TestModelWithNoSpaceEXPECTED.eson"));
		String dsl = LineEndingUtil.fixLineEndings(serializer.serialize(eFactory));
		assertEquals(expectedDSL, dsl);
		checkNodes(eFactory);
	}
	
	/**
	 * New objects, with an empty resource. Note: For technical implementation
	 * reasons, this MUST be done by starting with loading an empty resource,
	 * not with creating a new resource and adding objects to its content and
	 * then saving it. This is because if it were done like this, then the
	 * content(0) EFactory Factory NewObject root object wouldn't be created
	 * first as it needs to be. This programmatic "trick" (loading empty
	 * resource) actually corresponds to how e.g. the Sample Ecore Model Editor
	 * (EcoreEditor), or other such UIs, would operate in practice anyways (the
	 * would open an empty new file), so... it actually isn't really a "hack".
	 */
	@Test
	public void testCreateCompletelyNew() throws Exception {
		// Java 7: Path newEmptyFilePath = Files.createTempFile(); File newEmptyFile = newEmptyFilePath.toFile();
		File newEmptyFile = File.createTempFile(getClass().getSimpleName() + "Model", ".efactory");
		com.google.common.io.Files.touch(newEmptyFile); 
		
		EList<EObject> resContents = rp.get().load(newEmptyFile, false);
		assertEquals(1, resContents.size()); // sic! Note: An empty resource - but it already has the Factory NewObject root object

		TestModel testModel = TestmodelFactory.eINSTANCE.createTestModel();
		SingleRequired singleRequired = TestmodelFactory.eINSTANCE.createSingleRequired();
		testModel.setSingleRequired(singleRequired);
		testModel.setName("testCreateCompletelyNew");
		resContents.add(testModel);
		assertEquals(2, resContents.size());
		
		Factory factory = (Factory) resContents.get(0);
		String nameAgain = factory.getRoot().getName();
		assertEquals("testCreateCompletelyNew", nameAgain);
		EClass eClass = factory.getRoot().getEClass();
		assertEquals(TestmodelPackage.Literals.TEST_MODEL, eClass);

		assertTrue(factory.getEPackages().isEmpty());
		assertTrue(factory.getAnnotations().isEmpty());
		checkNodes(factory);
	}
	
	@Test
	public void testAddRemoveMoveListElements() throws Exception {
		// add (nested) stuff.. add element to list before (no change tracker attached yet) and after attaching it to resource
		EList<EObject> resourceContents = rp.get().load("res/BuilderResyncTests/1TestModelWithNameProperty.efactory", true);

		// Change the TestModel
		TestModel testModel = (TestModel) resourceContents.get(1);
		AttributeTestContainer firstAttributeTestContainer = TestmodelFactory.eINSTANCE.createAttributeTestContainer();
		firstAttributeTestContainer.setOneInt(9876);
		testModel.getAttributeTest().add(firstAttributeTestContainer);

		// Check the EFactory model
		Factory eFactory = (Factory) resourceContents.get(0);
		checkListFeature(eFactory, 2, 0, 9876);
		assertEquals(1, getMultiValueValues(eFactory, 2).size());

		// add another element to existing list
		AttributeTestContainer secondAttributeTestContainer = TestmodelFactory.eINSTANCE.createAttributeTestContainer();
		secondAttributeTestContainer.setOneInt(5432);
		testModel.getAttributeTest().add(secondAttributeTestContainer);
		// check again just to make sure first (above) is still there
		checkListFeature(eFactory, 2, 0, 9876);
		// check second element just added
		checkListFeature(eFactory, 2, 1, 5432);
		assertEquals(2, getMultiValueValues(eFactory, 2).size());

		// move elements
		testModel.getAttributeTest().move(1, 0);
		checkListFeature(eFactory, 2, 0, 5432);
		checkListFeature(eFactory, 2, 1, 9876);
		assertEquals(2, getMultiValueValues(eFactory, 2).size());

		// remove the first element from list
		testModel.getAttributeTest().remove(0);
		checkListFeature(eFactory, 2, 0, 9876);
		assertEquals(1, getMultiValueValues(eFactory, 2).size());

		// addAll two new elements element to list
		Collection<AttributeTestContainer> moreAttributeTestContainers = new ArrayList<AttributeTestContainer>(2);
		AttributeTestContainer thirdAttributeTestContainer = TestmodelFactory.eINSTANCE.createAttributeTestContainer();
		thirdAttributeTestContainer.setOneInt(3487);
		moreAttributeTestContainers.add(thirdAttributeTestContainer);
		AttributeTestContainer forthAttributeTestContainer = TestmodelFactory.eINSTANCE.createAttributeTestContainer();
		forthAttributeTestContainer.setOneInt(1273);
		moreAttributeTestContainers.add(forthAttributeTestContainer);
		testModel.getAttributeTest().addAll(moreAttributeTestContainers);
		checkListFeature(eFactory, 2, 0, 9876);
		checkListFeature(eFactory, 2, 1, 3487);
		checkListFeature(eFactory, 2, 2, 1273);
		assertEquals(3, getMultiValueValues(eFactory, 2).size());
		
		// removeAll an elements from middle of list
		moreAttributeTestContainers.clear();
		// Note: need to add() two - because if it's only one, there is actually
		// an internal optimization somewhere in EMF which turns it into a
		// Notification.REMOVE (sufficient), but won't test REMOVE_MANY. 
		moreAttributeTestContainers.add(firstAttributeTestContainer);
		moreAttributeTestContainers.add(forthAttributeTestContainer);
		testModel.getAttributeTest().removeAll(moreAttributeTestContainers);
		assertEquals(1, getMultiValueValues(eFactory, 2).size());
		checkListFeature(eFactory, 2, 0, 3487);
		checkNodes(eFactory);
	}
	
	@Test // see https://bugs.eclipse.org/bugs/show_bug.cgi?id=492080
	public void testSetManyAttribute() throws Exception {
	    // Load the Test Model
	    EList<EObject> resourceContents = rp.get().load("res/BuilderResyncTests/TestModelAddManyAttribute.eson", true);
	    // Get the Test Model for edit
	    TestModel testModel = (TestModel) resourceContents.get(1);
	    AttributeTestContainer attributeTestContainer = testModel.getAttributeTest().get(0);
	    
        // create a List of EAttribute/s, in this case manyInts
	    List<Integer> ints = new ArrayList<>();
        ints.add(1);
        ints.add(2);
        // Now set the same here using EMF EAttribute Reference..
        // NOTE: Property Sheet always does the eSET, instead of distinguishing add, remove or any
	    attributeTestContainer.eSet(TestmodelPackage.eINSTANCE.getAttributeTestContainer_ManyInt(), ints);
	    
	    EList<Integer> manyInt = attributeTestContainer.getManyInt();
	    assertEquals(2, manyInt.size());
	    assertTrue(manyInt.contains(1));
	    assertTrue(manyInt.contains(2));
	}

	protected void checkListFeature(Factory eFactory, int featureIndex, int multiValueIndex, int expectedInt) {
		NewObject newObject = checkNewObjectAttributeTestContainer(eFactory, featureIndex, multiValueIndex);
		IntegerAttribute oneInt = (IntegerAttribute)newObject.getFeatures().get(0).getValue();
		assertEquals(expectedInt, oneInt.getValue());
	}

	protected NewObject checkNewObjectAttributeTestContainer(Factory eFactory, int featureIndex, int multiValueIndex) {
		final EList<Value> values = getMultiValueValues(eFactory, featureIndex);
		Value listItemValue = values.get(multiValueIndex);
		assertTrue(listItemValue.eClass().toString(), listItemValue instanceof Containment);
		Containment efContainmentValue = (Containment) listItemValue;
		NewObject newObject = efContainmentValue.getValue();
		// Since Containment's EClass became optional, expect null instead of TestmodelPackage.Literals.ATTRIBUTE_TEST_CONTAINER
		assertEquals(null, newObject.getEClass());
		return newObject;
	}

	protected EList<Value> getMultiValueValues(Factory eFactory, int featureIndex) {
		Feature newFeature = eFactory.getRoot().getFeatures().get(featureIndex);
		return getMultiValueValues(newFeature);
	}

	protected EList<Value> getMultiValueValues(Feature newFeature) {
		final Value value = newFeature.getValue();
		assertTrue(value.eClass().toString(), value instanceof MultiValue);
		MultiValue multiValue = (MultiValue) value;
		final EList<Value> values = multiValue.getValues();
		return values;
	}

	@Test
	public void testEnum() throws Exception {
		// add (nested) stuff.. add element to list before (no change tracker attached yet) and after attaching it to resource
		EList<EObject> resourceContents = rp.get().load("res/BuilderResyncTests/1TestModelWithNameProperty.efactory", true);

		// Change the TestModel
		TestModel testModel = (TestModel) resourceContents.get(1);
		AttributeTestContainer attributeTestContainer = TestmodelFactory.eINSTANCE.createAttributeTestContainer();
		attributeTestContainer.setOneEnum(SampleEnum.SAMPLE2);
		attributeTestContainer.getManyEnums().add(SampleEnum.SAMPLE);
		testModel.getAttributeTest().add(attributeTestContainer);

		// Check the EFactory model
		Factory eFactory = (Factory) resourceContents.get(0);
		NewObject newObject = checkNewObjectAttributeTestContainer(eFactory, 2, 0);
		EnumAttribute oneEnum = (EnumAttribute)newObject.getFeatures().get(3).getValue();
		assertEquals(SampleEnum.SAMPLE2.getName(), oneEnum.getValue().getName());
		
		EList<Value> manyEnums = getMultiValueValues(newObject.getFeatures().get(5));
		assertEquals(1, manyEnums.size());
		EnumAttribute firstOfManyEnums = (EnumAttribute)manyEnums.get(0);
		assertEquals(SampleEnum.SAMPLE.getName(), firstOfManyEnums.getValue().getName());
		
		// change TestModel by adding one more enum to list
		attributeTestContainer.getManyEnums().add(SampleEnum.SAMPLE2);
		// check it again
		manyEnums = getMultiValueValues(newObject.getFeatures().get(5));
		assertEquals(2, manyEnums.size());
		EnumAttribute secondOfManyEnums = (EnumAttribute)manyEnums.get(1);
		assertEquals(SampleEnum.SAMPLE2.getName(), secondOfManyEnums.getValue().getName());
		firstOfManyEnums = (EnumAttribute)manyEnums.get(0);
		assertEquals(SampleEnum.SAMPLE.getName(), firstOfManyEnums.getValue().getName());
		checkNodes(eFactory);
	}
	
	private void checkNodes(Factory eFactory) {
		INode node = NodeModelUtils.findActualNodeFor(eFactory);
		@SuppressWarnings("unused")
		String text = NodeModelUtils.compactDump(node, true);
		// Even if we don't dump this text to stdout, compactDump is very useful
		// because it detects inconsistencies in the Node model which the
		// NodeFixer may have caused by throwing exceptions.
		// System.out.println(text);
	}
	
	@Test
	public void testAssociateFileExtension() throws Exception {
	    EList<EObject> resourceContents = rp.get().load("res/BuilderResyncTests/1TestModelWithNameProperty.testmodel", true);
	    assertTrue(resourceContents != null);
	    assertTrue(!resourceContents.isEmpty());
	    
	    //with different extension than above, this is a proof that extensions with comma seperated values work
        resourceContents = rp.get().load("res/BuilderResyncTests/1TestModelWithNameProperty.tm", true);
        assertTrue(resourceContents != null);
        assertTrue(!resourceContents.isEmpty());	    
	}

}
