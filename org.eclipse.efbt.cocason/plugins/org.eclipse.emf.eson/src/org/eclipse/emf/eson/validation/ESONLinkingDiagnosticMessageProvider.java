/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2014 Michael Vorburger
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.validation;

import org.eclipse.emf.eson.eFactory.Reference;
import org.eclipse.xtext.diagnostics.DiagnosticMessage;
import org.eclipse.xtext.linking.impl.LinkingDiagnosticMessageProvider;

/**
 * ILinkingDiagnosticMessageProvider which suppresses the
 * "Couldn't resolve reference to ..." error. We do this so that we can handle
 * that in our EFactoryJavaValidator instead; there with a custom message, and
 * better referenced type name for ESON (because in the grammar it's always
 * "EObject", but we want a clearer message), and so that checkIsAssignable
 * validation can be conditional on it.
 * 
 * @author Michael Vorburger - initial implementation
 */
public class ESONLinkingDiagnosticMessageProvider extends LinkingDiagnosticMessageProvider {
	
	@Override
	public DiagnosticMessage getUnresolvedProxyMessage(ILinkingDiagnosticContext context) {
		if (context.getContext() instanceof Reference)
			return null;
		return super.getUnresolvedProxyMessage(context);
	}

}
