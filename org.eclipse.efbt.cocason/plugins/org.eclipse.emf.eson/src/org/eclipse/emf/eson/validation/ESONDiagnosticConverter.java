/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2016 Michael Vorburger and others.
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.validation;

import org.eclipse.emf.eson.eFactory.NewObject;
import org.eclipse.xtext.validation.DiagnosticConverterImpl;
import org.eclipse.xtext.validation.FeatureBasedDiagnostic;
import org.eclipse.xtext.validation.ValidationMessageAcceptor;

/**
 * Custom converter of diagnostic to avoid underlying too much info in file
 * 
 * @author David Aguirre
 */
public class ESONDiagnosticConverter extends DiagnosticConverterImpl {

	@Override
	protected IssueLocation getLocationData(
			org.eclipse.emf.common.util.Diagnostic diagnostic) {
		IssueLocation issueLocation = super.getLocationData(diagnostic);
		if (null != diagnostic) {
			if (diagnostic instanceof FeatureBasedDiagnostic) {
				fixLocationLength((FeatureBasedDiagnostic) diagnostic,
						issueLocation);
			}
		}

		return issueLocation;
	}

	private void fixLocationLength(FeatureBasedDiagnostic featDiag,
			IssueLocation issueLocation) {
		if (null != featDiag) {
			if (featDiag.getSourceEObject() instanceof NewObject && featDiag
					.getIndex() == ValidationMessageAcceptor.INSIGNIFICANT_INDEX) {
				NewObject newObject = (NewObject) featDiag.getSourceEObject();
				if (newObject.eClass() != null
						&& newObject.eClass().getName() != null) {
					// ~first line if we have an error without an index
					// hence at new object level
					issueLocation.length = newObject.eClass().getName().length()
							+ 2;
				}
			}
		}
	}

}
