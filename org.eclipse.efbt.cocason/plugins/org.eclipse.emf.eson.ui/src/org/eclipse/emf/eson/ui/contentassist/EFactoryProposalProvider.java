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

import javax.inject.Inject;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.eson.eFactory.Containment;
import org.eclipse.emf.eson.eFactory.Feature;
import org.eclipse.emf.eson.eFactory.MultiValue;
import org.eclipse.emf.eson.eFactory.NewObject;
import org.eclipse.emf.eson.services.EFactoryGrammarAccess;
import org.eclipse.emf.eson.ui.contentassist.future.FilteringCompletionProposalAcceptor;
import org.eclipse.emf.eson.util.EcoreUtil3;
import org.eclipse.jface.text.contentassist.ICompletionProposal;
import org.eclipse.jface.viewers.StyledString;
import org.eclipse.swt.graphics.Image;
import org.eclipse.xtext.Assignment;
import org.eclipse.xtext.EcoreUtil2;
import org.eclipse.xtext.Keyword;
import org.eclipse.xtext.RuleCall;
import org.eclipse.xtext.nodemodel.ILeafNode;
import org.eclipse.xtext.nodemodel.INode;
import org.eclipse.xtext.ui.editor.contentassist.ConfigurableCompletionProposal;
import org.eclipse.xtext.ui.editor.contentassist.ContentAssistContext;
import org.eclipse.xtext.ui.editor.contentassist.ICompletionProposalAcceptor;

import com.google.common.base.Predicate;

/**
 * Restricted ProposalProvider,
 * which takes the highly dynamic nature of EFactory into account,
 * and constrains the proposal based on what actually makes sense in the model.
 */
public class EFactoryProposalProvider extends AbstractEFactoryProposalProvider {

	protected @Inject EFactoryGrammarAccess grammar;
	protected @Inject IEAttributeStringProposalProvider attributeProposalProvider;

	/**
	 * Blacklist certain Grammar keywords such as "NULL" or ":" (Enum), 
	 * because otherwise they appear too often. They are each
	 * handled separately in other methods below.
	 */
	@Override
	public void completeKeyword(Keyword keyword, ContentAssistContext context, ICompletionProposalAcceptor acceptor) {
		// http://kthoms.wordpress.com/2012/05/22/xtext-content-assist-filtering-keyword-proposals/
		if (!grammar.getNullAttributeAccess().getValueNULLKeyword_0().equals(keyword)
		 && !grammar.getEnumAttributeAccess().getColonKeyword_0().equals(keyword)
		 && !grammar.getMultiValueAccess().getLeftSquareBracketKeyword_1().equals(keyword)) 
		{
			if (!isNextSiblingAlreadyTheSame(keyword.getValue(), context)
			 && !isInEAttributeOrNonContainmentEReferenceAndMustSuppressUselessLeftCurlyBrace(keyword, context)) {
				super.completeKeyword(keyword, context, acceptor);
			}
		}
	}
	
	private boolean isInEAttributeOrNonContainmentEReferenceAndMustSuppressUselessLeftCurlyBrace(Keyword keyword, ContentAssistContext context) {
		if (!"{".equals(keyword.getValue()))
			return false;
		if (!(context.getCurrentModel() instanceof Feature))
			return false;

		Feature feature = (Feature) context.getCurrentModel();
		EStructuralFeature eFeature = feature.getEFeature();
		if (eFeature instanceof EAttribute)
			return true;
		
		EReference eReference = (EReference) eFeature;
		return !eReference.isContainment();
	}

	/**
	 * Why doesn't Xtext do this OOB?
	 */
	protected boolean isNextSiblingAlreadyTheSame(String proposal, ContentAssistContext context) {
		INode nextSibling = context.getCurrentNode().getNextSibling();
		nextSibling = skipHiddenLeafNodes(nextSibling);
		String wazNext = nextSibling != null ? nextSibling.getText() : null;
		return proposal.equals(wazNext);
	}

	/**
	 * This helps if the next relevant sibling is after a comment.
	 */
	private INode skipHiddenLeafNodes(INode node) {
		boolean keepTrying = true;
		do {
			if (node instanceof ILeafNode) {
				ILeafNode iLeafNode = (ILeafNode) node;
				if (iLeafNode.isHidden()) {
					node = node.getNextSibling();
				} else {
					keepTrying = false;
				}
			} else {
				keepTrying = false;
			}
		} while (keepTrying);
		return node;
	}

	@Override
	public void completeNewObject_EClass(EObject model, Assignment assignment, ContentAssistContext context, ICompletionProposalAcceptor acceptor) {
		// NOOP override, to prevent default implementation from kicking in
		// for first model arg for anything else than NewObject. 
		// We want ours, below, only.
	}
	public void completeNewObject_EClass(MultiValue multiValue, Assignment assignment, ContentAssistContext context, ICompletionProposalAcceptor acceptor) {
		super.completeNewObject_EClass(multiValue, assignment, context, acceptor);
	}
	public void completeNewObject_EClass(Feature feature, Assignment assignment, ContentAssistContext context, ICompletionProposalAcceptor acceptor) {
		// TODO needed? If yes, create missing non-reg. test!! if (EcoreUtil3.isEContainment(feature.getEFeature())) {
		if (!feature.getEFeature().isMany())
			// This is needed to avoid EClass proposal BEFORE the [ in a MultiValue
			// The one INSIDE the [] is offered below by completeMultiValue_Values
			super.completeNewObject_EClass(feature, assignment, context, acceptor);
	}
	
	@Override
	public void completeFeature_EFeature(EObject model, Assignment assignment, ContentAssistContext context, ICompletionProposalAcceptor acceptor) {
		// NOOP override, to prevent default implementation from kicking in
		// for first model arg for anything else than NewObject. 
		// We want ours, below, only.
	}
	
	public void completeFeature_EFeature(Containment containment, Assignment assignment, ContentAssistContext context, ICompletionProposalAcceptor acceptor) {
		NewObject parentNewObject = EcoreUtil2.getContainerOfType(containment, NewObject.class);
		completeFeature_EFeature(parentNewObject, assignment, context, acceptor);
	}

	public void completeFeature_EFeature(Feature feature, Assignment assignment, ContentAssistContext context, ICompletionProposalAcceptor acceptor) {
		NewObject parentNewObject = EcoreUtil2.getContainerOfType(feature, NewObject.class);
		completeFeature_EFeature(parentNewObject, assignment, context, acceptor);
	}
	
	public void completeFeature_EFeature(final NewObject newObject, Assignment assignment, ContentAssistContext context, ICompletionProposalAcceptor acceptor) {
		if (hasFeaturesWithNoValue(newObject)) // this is needed because Feature value is (has to be) optional..
			return;
		super.completeFeature_EFeature(newObject, assignment, context, new FilteringCompletionProposalAcceptor(acceptor, new Predicate<ICompletionProposal>() {
			public boolean apply(ICompletionProposal proposal) {
				ConfigurableCompletionProposal2 _proposal = (ConfigurableCompletionProposal2) proposal;
				EStructuralFeature eFeature = (EStructuralFeature) _proposal.getAdditionalTypedProposalObject();
				return isValidEFeature(newObject, eFeature);
			}
		}));
	}
	
	protected boolean hasFeaturesWithNoValue(NewObject newObject) {
		for (Feature feature : newObject.getFeatures()) {
			if (feature.getValue() == null)
				return true;
		}
		return false;
	}
	
	protected boolean isValidEFeature(final NewObject model, EStructuralFeature eFeature) {
		return isNoDuplicateEFeature(model, eFeature)
			&& eFeature.isChangeable();
	}

	protected boolean isNoDuplicateEFeature(NewObject model, EStructuralFeature eFeature) {
		return !EcoreUtil3.isDuplicate(model.getFeatures(), eFeature);
	}

	public void complete_MultiValue(Feature feature, RuleCall ruleCall, ContentAssistContext context, ICompletionProposalAcceptor acceptor) {
		if (feature.getEFeature().isMany()) {
			acceptor.accept(createCompletionProposal(grammar.getMultiValueAccess().getLeftSquareBracketKeyword_1().getValue(), context)); // '['
		}
	}
	
	public void complete_Attribute(Feature feature, RuleCall ruleCall, ContentAssistContext context, ICompletionProposalAcceptor acceptor) {
		final EStructuralFeature eFeature = feature.getEFeature();
		if (!EcoreUtil3.isEAttribute(eFeature))
			return;
		final EAttribute eAttribute = (EAttribute) eFeature;
		String[] proposals = attributeProposalProvider.getProposals(eAttribute);
		for (String proposal : proposals) {
			acceptor.accept(createCompletionProposal(proposal, context));
		}
	}
	
/*  TODO write tests for completion with references! is (something like) this needed:
	 
	@Override
	public void completeReference_Value(EObject model, Assignment assignment,
			ContentAssistContext context, ICompletionProposalAcceptor acceptor) {
		if (model instanceof Feature) {
			Feature feature = (Feature) model;
			if (EcoreUtil3.isEReference(feature.getEFeature())) {
				super.completeReference_Value(model, assignment, context, acceptor);
			}
		}
	}
*/
	
	/**
	 * This was an attempt to get proposals inside MultiValue working.
	 * Unfortunately I'm either too tired right now, too dumb, or.. this isn't possible (with Xtext 2.3.1) ?
	 * I cannot seem to get to the expected type, because a half-baked Feature inside a MultiValue never parses correctly.
	 */
/*	
	@Override
	public void completeFeature_Value(EObject model, Assignment assignment,
			ContentAssistContext context, ICompletionProposalAcceptor acceptor) {
		super.completeFeature_Value(model, assignment, context, acceptor);
		if (model instanceof MultiValue) {
			final MultiValue multiValue = (MultiValue) model;
			EObject container = multiValue.eContainer();
			while (!(container instanceof Feature)) {
				// According to our Grammar, if a MultiValue isn't in a Feature, it must be in another MultiValue
				// NO NEED: final MultiValue parentMultiValue = (MultiValue) container;
				container = container.eContainer();
				if (container == null)
					return;
			}
			final Feature feature = (Feature) container;
			final EStructuralFeature eFeature = feature.getEFeature();
			if (eFeature instanceof EAttribute) {
				// TODO test and handle enum..
				EAttribute eAttribute = (EAttribute) eFeature;
				EDataType eAttributeType = eAttribute.getEAttributeType();
				Object defaultValue = eAttributeType.getDefaultValue();
				String defaultString = null;
				if (eAttributeType instanceof EDataType.Internal) {
					EDataType.Internal internalEAttributeType = (EDataType.Internal) eAttributeType;
					ConversionDelegate converter = internalEAttributeType.getConversionDelegate();
					if (converter != null)
						defaultString = converter.convertToString(defaultValue);
				}
				if (defaultString == null) {
					defaultString = defaultValue.toString();
				}
				acceptor.accept(createCompletionProposal(defaultString, context));
			} else if (eFeature instanceof EReference) {
				// final EReference eReference = (EReference) eFeature;
				// EClass referenceType = eReference.getEReferenceType();
				// ...
			}
		}
	}
*/
	@Override
	protected ConfigurableCompletionProposal doCreateProposal(String proposal, StyledString displayString, Image image, int replacementOffset, int replacementLength) {
		return new ConfigurableCompletionProposal2(proposal, replacementOffset, replacementLength, proposal.length(), image, displayString, null, null);
	}

	/**
	 * This is needed because we inherit a dumb STRING proposal via org.eclipse.xtext.common.Terminals.
	 * (This problem doesn't occur for LONG/DOUBLE/DATE/boolean, because we defined them ourselves in grammar.)
	 */
	@Override protected boolean doCreateStringProposals() {
		return false;
	}

}
