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
package org.eclipse.emf.eson.validation.tests;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.inject.Inject;

import org.eclipse.emf.common.util.BasicDiagnostic;
import org.eclipse.emf.common.util.Diagnostic;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.resource.ResourceSet;
import org.eclipse.emf.eson.tests.util.ESONWithTestmodelInjectorProvider;
import org.eclipse.emf.eson.validation.EFactoryJavaValidator;
import org.eclipse.xtext.diagnostics.Severity;
import org.eclipse.xtext.junit4.InjectWith;
import org.eclipse.xtext.junit4.XtextRunner;
import org.eclipse.xtext.junit4.util.ParseHelper;
import org.eclipse.xtext.junit4.validation.AssertableDiagnostics;
import org.eclipse.xtext.junit4.validation.ValidatorTester;
import org.eclipse.xtext.util.CancelIndicator;
import org.eclipse.xtext.util.IAcceptor;
import org.eclipse.xtext.validation.CheckMode;
import org.eclipse.xtext.validation.IDiagnosticConverter;
import org.eclipse.xtext.validation.IResourceValidator;
import org.eclipse.xtext.validation.Issue;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * Tests for EFactoryJavaValidator.
 * 
 * @author Michael Vorburger
 */
@RunWith(XtextRunner.class)
@InjectWith(ESONWithTestmodelInjectorProvider.class)
public class EFactoryJavaValidatorTest {

	@Inject ResourceSet rs;
	@Inject ParseHelper<EObject> parseHelper;
	@Inject ValidatorTester<EFactoryJavaValidator> tester;
	@Inject IResourceValidator resourceValidator;
	@Inject IDiagnosticConverter diagnosticConverter;
	
	@Test
	public void testNoValidationError() throws Exception {
		EObject testModel = parseHelper.parse("use testmodel.* TestModel { singleRequired: SingleRequired { } }");
		dump(tester.validate(testModel)).assertDiagnosticsCount(0);
	}

	/**
	 * Test validation of invalid attempt to name an object (which isn't
	 * possible if it doesn't have a name mapping; i.e. no setName()).
	 */
	@Test
	public void testNoNameFeature() throws Exception {
		InputStream is = getClass().getResourceAsStream("/BuilderTests/NameAttribute.efactory");
		assertNotNull(is);
		EObject testModel = parseHelper.parse(is, URI.createURI("BuilderTests/NameAttribute.efactory"), null, rs);
		tester.validate(testModel).assertDiagnosticsCount(1).assertError(EFactoryJavaValidator.ERR_CANNOT_NAME);
	}

	/**
	 * Test validation of duplicate attribute
	 */
	@Test
	public void testDuplicateAttribute() throws Exception {
		EObject testModel = parseHelper.parse("use testmodel.* TestModel { singleRequired: SingleRequired { }"
				+ "\nsingleRequired: SingleRequired { } }");
		AssertableDiagnostics assertDiag = tester.validate(testModel).assertDiagnosticsCount(1);
		assertDiag.assertErrorContains("Duplicate feature");
		final AtomicBoolean acceptPassed = new AtomicBoolean();
		new MyDiagnosticConverter(diagnosticConverter).convertValidatorDiagnostic(assertDiag.getDiagnostic(),
				new IAcceptor<Issue>() {
					@Override
					public void accept(Issue t) {
						assertEquals("lineNumber", Integer.valueOf(2), t.getLineNumber());
						assertEquals("length", Integer.valueOf("singleRequired: SingleRequired { }".length()),
								t.getLength());
						acceptPassed.set(true);
					}
				});
		assertTrue("accept run", acceptPassed.get());
	}

	/**
	 * There used to be two identical error markers (one at the correct position
	 * and one on row 0) for each missing required property; this test ensures
	 * that there is only one (non-regression).
	 * 
	 * The solution to fix the problem above was to bind the
	 * DerivedStateAwareResourceValidator as IResourceValidator.
	 */
	@Test
	public void testOnlyOneErrorForMissingRequiredProperty() throws Exception {
		EObject testModel = parseHelper.parse("use testmodel.* TestModel {}");
		AssertableDiagnostics assertDiag = tester.validate(testModel).assertDiagnosticsCount(1);
		assertDiag.assertErrorContains("The required feature");
		final AtomicBoolean acceptPassed = new AtomicBoolean();
		new MyDiagnosticConverter(diagnosticConverter).convertValidatorDiagnostic(assertDiag.getDiagnostic(),
				new IAcceptor<Issue>() {
					@Override
					public void accept(Issue t) {
						assertEquals("lineNumber", Integer.valueOf(1), t.getLineNumber());
						assertEquals("length", Integer.valueOf("TestModel {".length()), t.getLength());
						acceptPassed.set(true);
					}
				});
		assertTrue("accept run", acceptPassed.get());
	}

	/**
	 * There used to be 3 or 4 highly technical error markers in case of broken
	 * references, which made non sense to end-users. This non-regression test
	 * ensure that there is only one, and that it is clear (it must include the
	 * text of the broken reference).
	 */
	@Test
	public void testOnlyOneErrorForBrokenReference() throws Exception {
		EObject testModel = parseHelper.parse("use testmodel.* TestModel test { singleRequired: SingleRequired { parentReference: ItsNotLinkedYet } }");
		AssertableDiagnostics diag = tester.validate(testModel);
		// dump(diag);
		diag.assertError(org.eclipse.xtext.diagnostics.Diagnostic.LINKING_DIAGNOSTIC, "ItsNotLinkedYet");
		// Message must include correct type of broken link. This is less
		// obvious than it may seem at first.. because a Reference value is
		// just an [EObject] in the grammar, LinkingDiagnosticMessageProvider
		// cannot get this right - but we do in our custom
		// EFactoryJavaValidator.checkIsBrokenReference().
		diag.assertErrorContains("TestModel");
		
		// It's important to additionally test using an IResourceValidator that
		// we really do only have 1 error, because the ValidatorTester doesn't
		// catch errors added from LazyLinkingResource's getEObject() &
		// createAndAddDiagnostic(). Implementation wise this tests that the
		// suppression in ESONLinkingDiagnosticMessageProvider works.
		List<Issue> issues = resourceValidator.validate(testModel.eResource(), CheckMode.ALL, CancelIndicator.NullImpl);
		assertEquals(1, issues.size());
	}
	
	@Test
	public void testTypeBoolean() throws Exception {
		EObject testModel = parseHelper.parse("use testmodel.* TestModel { attributeTest: [ AttributeTestContainer { oneBool: 123 } ] singleRequired: SingleRequired { } }");
		dump(tester.validate(testModel)).assertDiagnosticsCount(1).assertError(EFactoryJavaValidator.ERR_BAD_TYPE 
				// TODO , "EF Attribute 'oneBool' must be one of type(s) EBoolean but was EInt"
			);
	}
	
	@Test
	public void testCannotHaveNameAttributeWithinNamedNewObject() throws Exception {
		EObject testModel = parseHelper.parse("use testmodel.* TestModel abc { name: \"def\" singleRequired: SingleRequired { } }");
		tester.validate(testModel).assertDiagnosticsCount(1).assertError(EFactoryJavaValidator.ERR_CANNOT_HAVE_ANOTHER_NAME);
	}
	
	protected AssertableDiagnostics dump(AssertableDiagnostics diag) {
		Iterable<Diagnostic> all = diag.getAllDiagnostics();
		for (Diagnostic diagnostic : all) {
			System.out.println(diagnostic.getMessage());
			if (diagnostic.getException() != null)
				diagnostic.getException().printStackTrace();
		}
		return diag;
	}

	private static class MyDiagnosticConverter implements IDiagnosticConverter {
		private IDiagnosticConverter coreDiagConverter;

		public MyDiagnosticConverter(IDiagnosticConverter coreDiagConverter) {
			super();
			this.coreDiagConverter = coreDiagConverter;
		}

		@Override
		public void convertResourceDiagnostic(org.eclipse.emf.ecore.resource.Resource.Diagnostic diagnostic,
				Severity severity, IAcceptor<Issue> acceptor) {
		}

		@Override
		public void convertValidatorDiagnostic(Diagnostic diagnostic, IAcceptor<Issue> acceptor) {
			// Browse all children diagnostics!
			if (diagnostic instanceof BasicDiagnostic) {
				BasicDiagnostic basicDiag = (BasicDiagnostic) diagnostic;
				for (Diagnostic childDiagnostic : basicDiag.getChildren()) {
					this.convertValidatorDiagnostic(childDiagnostic, acceptor);
				}
			} else {
				coreDiagConverter.convertValidatorDiagnostic(diagnostic, acceptor);
			}
		}

	}
}
