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
package org.eclipse.emf.eson.ui.contentassist.future;

import org.eclipse.jface.text.contentassist.ICompletionProposal;
import org.eclipse.xtext.ui.editor.contentassist.ICompletionProposalAcceptor;

import com.google.common.base.Predicate;

/**
 * ICompletionProposalAcceptor which filters proposals using a Predicate.
 * 
 * @author Michael Vorburger
 */
public class FilteringCompletionProposalAcceptor extends ICompletionProposalAcceptor.Delegate {

	private final Predicate<ICompletionProposal> proposalFilter;

	public FilteringCompletionProposalAcceptor(ICompletionProposalAcceptor delegate, Predicate<ICompletionProposal> proposalFilter) {
		super(delegate);
		if (proposalFilter == null)
			throw new IllegalArgumentException();
		this.proposalFilter = proposalFilter;
	}

	@Override
	public void accept(ICompletionProposal proposal) {
		if (proposal != null && proposalFilter.apply(proposal)) {
			super.accept(proposal);
		}
	}
	
}
