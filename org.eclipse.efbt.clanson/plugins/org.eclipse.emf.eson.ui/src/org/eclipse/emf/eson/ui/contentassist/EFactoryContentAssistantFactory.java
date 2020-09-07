/*
 * #%L
 * org.eclipse.emf.eson.ui
 * %%
 * Copyright (C) 2009 - 2014 Sebastian Benz, Michael Vorburger and others
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.ui.contentassist;

import org.eclipse.jface.text.contentassist.ContentAssistant;
import org.eclipse.xtext.ui.editor.contentassist.DefaultContentAssistantFactory;

public class EFactoryContentAssistantFactory extends DefaultContentAssistantFactory {

	@Override
	protected void setAutoInsert(ContentAssistant assistant) {
		super.setAutoInsert(assistant);
//		assistant.setAutoActivationDelay(0);
		assistant.enableAutoInsert(true);
//		assistant.enablePrefixCompletion(true);
		assistant.enableAutoActivation(true);
	}
}
