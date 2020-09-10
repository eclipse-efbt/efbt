/*
 * #%L
 * org.eclipse.emf.eson.tests
 * %%
 * Copyright (C) 2013 - 2015 Michael Vorburger
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.builder.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import javax.inject.Inject;

import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EEnumLiteral;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.eson.tests.util.DumpIndexUtil;
import org.eclipse.emf.eson.tests.util.ESONWithTestmodelAndDynamicECoreInjectorProvider;
import org.eclipse.emf.eson.tests.util.ResourceProvider;
import org.eclipse.xtext.junit4.InjectWith;
import org.eclipse.xtext.junit4.XtextRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(XtextRunner.class)
@InjectWith(ESONWithTestmodelAndDynamicECoreInjectorProvider.class) // NOT just ESONWithTestmodelInjectorProvider
public class SimplestWeiredNameTest {
	// see also SimplestDynamicECoreTest - these tests here are written in the same style

	@Inject ResourceProvider provider;

	@Test public void testNormallyNamedEClassWithWeirdoAttributeName() throws Exception {
		EObject em = check("model/TestModelWithDotInNames.ecore",
				"res/BuilderTests/SimplestWithWeiredNamesWithDots1.eson",
				"NormallyNamed");
		String strangelyNamedAttributeValue = (String) em.eGet(em.eClass().getEAllAttributes().get(0));
		assertEquals("hello", strangelyNamedAttributeValue);
	}

	@Test public void testWeirdoNamedEClassAndAttributeName() throws Exception {
		EObject em = check("model/TestModelWithDotInNames.ecore",
				"res/BuilderTests/SimplestWithWeiredNamesWithDots2.eson",
				"WEIRDO.NAMED");
		String strangelyNamedAttributeValue = (String) em.eGet(em.eClass().getEAllAttributes().get(0));
		assertEquals("hello", strangelyNamedAttributeValue);
	}

    @Test public void testWeirdoNamedEClassAndAttributeNameWithNameAnnotation() throws Exception {
        EObject em = check("model/TestModelWithDotInNames.ecore",
                "res/BuilderTests/SimplestWithWeiredNamesWithDots3_namedID.eson",
                "WEIRDO.NAMED");
        EAttribute eAttribute = em.eClass().getEAllAttributes().get(0);
        assertEquals("STRANGE.NAME", eAttribute.getName());
        String strangelyNamedAttributeValue = (String) em.eGet(eAttribute);
        assertEquals("1234", strangelyNamedAttributeValue);
    }

	@Test public void testNormallyNamedEClassWithWeirdoEnumNameAndNormalLiteral() throws Exception {
		EObject em = check("model/TestModelWithDotInNames.ecore",
				"res/BuilderTests/SimplestWithWeiredNamesWithDots3enumA.eson",
				"NormallyNamed");
		EEnumLiteral strangelyNamedEnum = (EEnumLiteral) em.eGet(em.eClass().getEAllAttributes().get(1));
		assertEquals("normal", strangelyNamedEnum.getName());
	}

	@Test public void testNormallyNamedEClassWithWeirdoEnumNameAndWeirdoLiteral() throws Exception {
		EObject em = check("model/TestModelWithDotInNames.ecore",
				"res/BuilderTests/SimplestWithWeiredNamesWithDots3enumB.eson",
				"NormallyNamed");
		EEnumLiteral strangelyNamedEnum = (EEnumLiteral) em.eGet(em.eClass().getEAllAttributes().get(1));
		assertEquals("weird.literal", strangelyNamedEnum.getName());
	}

	@Test public void testNormallyNamedEClassWithWeirdoContained() throws Exception {
		EObject em = check("model/TestModelWithDotInNames.ecore",
				"res/BuilderTests/SimplestWithWeiredNamesWithDots4contained.eson",
				"NormallyNamed");
		EReference weirdoContainerERef = em.eClass().getEAllContainments().get(0);
		assertNotNull(weirdoContainerERef);
		EObject strangelyNamedContained = (EObject) em.eGet(weirdoContainerERef);
		assertNotNull(strangelyNamedContained);
		EAttribute strangeNameEAttribute = strangelyNamedContained.eClass().getEAllAttributes().get(0);
		String strangelyNamedAttributeValue = (String) strangelyNamedContained.eGet(strangeNameEAttribute);
		assertEquals("saluton", strangelyNamedAttributeValue);
	}

	@Test public void testWeirdoNamedEClassAndAttributeWithoutPackageImportOMG() throws Exception {
		EObject em = check("model/TestModelWithDotInNames.ecore",
				"res/BuilderTests/SimplestWithWeiredNamesWithDots5omg.eson",
				"WEIRDO.NAMED");
		String strangelyNamedAttributeValue = (String) em.eGet(em.eClass().getEAllAttributes().get(0));
		assertEquals("hello", strangelyNamedAttributeValue);
	}

	protected EObject check(String ecorePath, String esonPath, String expectedEClassName) throws Exception {
		provider.load(ecorePath, false /* do NOT validate, as the weird names with dot violate ECore validation */);
		// Do NOT DumpIndexUtil.dumpXtextIndex(ePackage.eResource()); as that does not work yet for *.ecore as this stage (later below on an *.eson it works - and dumps the *.ecore as well)

		// DEBUG:
		EList<EObject> content = provider.load(esonPath, false);
		DumpIndexUtil.dumpXtextIndex(content.get(0).eResource());

		EObject em = provider.loadModel(esonPath, EObject.class);
		assertEquals(expectedEClassName, em.eClass().getName());
		return em;
	}
}
