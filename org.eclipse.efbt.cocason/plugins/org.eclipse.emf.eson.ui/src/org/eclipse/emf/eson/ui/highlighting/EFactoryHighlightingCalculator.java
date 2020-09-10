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
package org.eclipse.emf.eson.ui.highlighting;

import java.util.Iterator;
import java.util.List;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.eclipse.emf.eson.resource.EFactoryResource;
import org.eclipse.xtext.nodemodel.ICompositeNode;
import org.eclipse.xtext.nodemodel.ILeafNode;
import org.eclipse.xtext.nodemodel.INode;
import org.eclipse.xtext.nodemodel.util.NodeModelUtils;
import org.eclipse.xtext.resource.XtextResource;
import org.eclipse.xtext.ui.editor.syntaxcoloring.IHighlightedPositionAcceptor;
import org.eclipse.xtext.ui.editor.syntaxcoloring.ISemanticHighlightingCalculator;

import org.eclipse.emf.eson.eFactory.Annotation;
import org.eclipse.emf.eson.eFactory.EFactoryPackage;
import org.eclipse.emf.eson.eFactory.Factory;
import org.eclipse.emf.eson.eFactory.Feature;
import org.eclipse.emf.eson.eFactory.NewObject;
import org.eclipse.emf.eson.eFactory.Reference;

/**
 * Highlight Annotation with SemanticHighlightingConfiguration.ANNOTATION_ID,
 * and NewObject eClass EClass + Feature eFeature(EStructuralFeature) as
 * DefaultHighlightingConfiguration.KEYWORD_ID.
 * 
 * @author Sebastian Benz - initial API and implementation
 * @author Michael Vorburger - attempted fixing OutOfMemoryError :-( + more colors :)
 * @author Anton Kosyakov - actually fixed the OutOfMemoryError ;-) 
 */
public class EFactoryHighlightingCalculator implements ISemanticHighlightingCalculator {

	public void provideHighlightingFor(XtextResource resource, final IHighlightedPositionAcceptor acceptor) {
		if (resource == null)
			return;
		
		Iterator<EObject> iter = getContentIterator(resource);
		while (iter.hasNext()) {
			EObject semanticObject = iter.next();
			if (semanticObject instanceof Annotation) {
				// highlight Annotation
				ICompositeNode node = NodeModelUtils.findActualNodeFor(semanticObject);
				highlightNode(node, EFactorySemanticHighlightingConfiguration.ANNOTATION_ID, acceptor);
			} else if (semanticObject instanceof NewObject) {
				// highlight NewObject's eClass
				EStructuralFeature expectedFeature = EFactoryPackage.eINSTANCE.getNewObject_EClass();
				highlightLeafNode(semanticObject, expectedFeature, EFactorySemanticHighlightingConfiguration.ECLASS_ID, acceptor);
				// highlight NewObject's name
				expectedFeature = EFactoryPackage.eINSTANCE.getNewObject_Name();
				highlightLeafNode(semanticObject, expectedFeature, EFactorySemanticHighlightingConfiguration.NAME_ID, acceptor);
			} else if (semanticObject instanceof Feature) {
				// highlight Feature's eFeature
				EReference expectedFeature = EFactoryPackage.eINSTANCE.getFeature_EFeature();
				highlightLeafNode(semanticObject, expectedFeature, EFactorySemanticHighlightingConfiguration.EFEATURE_ID, acceptor);
			} else if (semanticObject instanceof Reference) {
				// highlight Reference's value
				EReference expectedFeature = EFactoryPackage.eINSTANCE.getReference_Value();
				highlightLeafNode(semanticObject, expectedFeature, EFactorySemanticHighlightingConfiguration.CROSSREF_ID, acceptor);
			}
		}
	}

	protected Iterator<EObject> getContentIterator(XtextResource resource) {
		EFactoryResource efResource = (EFactoryResource) resource;
		Factory factory = efResource.getEFactoryFactory();
		// It's probably safe (and faster) to use false to not resolve proxies here
		Iterator<EObject> iter = EcoreUtil.getAllContents(factory, false);
		// We do NOT need the complete resource, this would unnecessarily iterator over all the derrived "real" EObjects:
		// Iterator<EObject> iter = EcoreUtil.getAllContents(resource, false);
		return iter;
	}

	protected void highlightLeafNode(final EObject semanticElement, EStructuralFeature expectedFeature, String id, final IHighlightedPositionAcceptor acceptor) {
		List<INode> nodes = NodeModelUtils.findNodesForFeature(semanticElement, expectedFeature);
		for (INode node : nodes) {
			highlightNode(node, id, acceptor);
		}
	};

	/**
	 * Highlights the non-hidden parts of {@code node} with the style that is associated with {@code id}.
	 * 
	 * This method is shamelessly copy/pasted from
	 * org.eclipse.xtext.ui.codetemplates.ui.highlighting.SemanticHighlighter.highlightNode(INode, String, IHighlightedPositionAcceptor).
	 */
	protected void highlightNode(INode node, String id, IHighlightedPositionAcceptor acceptor) {
		if (node == null)
			return;
		if (node instanceof ILeafNode) {
			acceptor.addPosition(node.getOffset(), node.getLength(), id);
		} else {
			for (ILeafNode leaf : node.getLeafNodes()) {
				if (!leaf.isHidden()) {
					acceptor.addPosition(leaf.getOffset(), leaf.getLength(), id);
				}
			}
		}
	}
}
