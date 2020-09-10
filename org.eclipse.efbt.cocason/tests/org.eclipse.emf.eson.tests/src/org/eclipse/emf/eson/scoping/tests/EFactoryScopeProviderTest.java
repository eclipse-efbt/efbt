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
 */
package org.eclipse.emf.eson.scoping.tests;

import static org.junit.Assert.assertEquals;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.eson.scoping.EFactoryScopeProvider;
import org.junit.Test;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import testmodel.TestmodelPackage;
import testmodel.testsubmodel.TestsubmodelPackage;

public class EFactoryScopeProviderTest {

	@Test
	public void testGetAllEClassesOfTestModel() {
		EFactoryScopeProvider p = new EFactoryScopeProvider();
		Iterable<EClass> eClassesInTestmodelPackage = p.getAllEClasses(Lists.newArrayList(TestmodelPackage.eINSTANCE));
		assertEquals(17, Iterables.size(eClassesInTestmodelPackage));
	}
	
	@Test
	public void testGetAllEClassesOfTestSubModel() {
		EFactoryScopeProvider p = new EFactoryScopeProvider();
		Iterable<EClass> eClassesInTestmodelPackage = p.getAllEClasses(Lists.newArrayList(TestsubmodelPackage.eINSTANCE));
		assertEquals(1, Iterables.size(eClassesInTestmodelPackage));
	}
	
	@Test
	public void testGetAllEClassesOfBothTestAndSubModel() {
		EFactoryScopeProvider p = new EFactoryScopeProvider();
		Iterable<EClass> eClassesInTestmodelPackage = p.getAllEClasses(Lists.newArrayList(TestmodelPackage.eINSTANCE, TestsubmodelPackage.eINSTANCE));
		assertEquals(18, Iterables.size(eClassesInTestmodelPackage));
	}
}
