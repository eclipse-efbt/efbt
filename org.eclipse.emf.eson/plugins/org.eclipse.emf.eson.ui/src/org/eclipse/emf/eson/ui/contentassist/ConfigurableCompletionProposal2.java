/*
 * #%L
 * org.eclipse.emf.eson.ui
 * %%
 * Copyright (C) 2013 - 2014 Michael Vorburger
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.ui.contentassist;

import org.eclipse.jface.text.contentassist.IContextInformation;
import org.eclipse.jface.viewers.StyledString;
import org.eclipse.swt.graphics.Image;
import org.eclipse.xtext.ui.editor.contentassist.ConfigurableCompletionProposal;

/**
 * ConfigurableCompletionProposal which exposes its
 * additionalProposalInfo as Object instead of String.
 * 
 * @see https://bugs.eclipse.org/bugs/show_bug.cgi?id=359548, but even though that's closed and pointing to getAdditionalData() that's not sufficient, because existing Xtext code still sets setAdditionalProposalInfo.
 * 
 * @author Michael Vorburger
 */
public class ConfigurableCompletionProposal2 extends ConfigurableCompletionProposal {

	private Object additionalProposalInfoAsObject;

	public ConfigurableCompletionProposal2(String replacementString,
			int replacementOffset, int replacementLength, int cursorPosition,
			Image image, StyledString displayString,
			IContextInformation contextInformation,
			String additionalProposalInfo) {
		super(replacementString, replacementOffset, replacementLength, cursorPosition,
				image, displayString, contextInformation, additionalProposalInfo);
	}

	public ConfigurableCompletionProposal2(String replacementString,
			int replacementOffset, int replacementLength, int cursorPosition) {
		super(replacementString, replacementOffset, replacementLength, cursorPosition);
	}

	@Override
	public void setAdditionalProposalInfo(Object additionalTypedProposalInfo) {
		super.setAdditionalProposalInfo(additionalTypedProposalInfo);
		this.additionalProposalInfoAsObject = additionalTypedProposalInfo;
	}	
	
	public Object getAdditionalTypedProposalObject() {
		return additionalProposalInfoAsObject;
	}
	
}
