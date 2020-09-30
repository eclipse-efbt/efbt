/*
 * #%L
 * org.eclipse.emf.eson.tests
 * %%
 * Copyright (C) 2015 Michael Vorburger
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.scoping;

import java.util.Collections;

import org.eclipse.xtext.naming.QualifiedName;
import org.eclipse.xtext.resource.IEObjectDescription;
import org.eclipse.xtext.scoping.IScope;
import org.eclipse.xtext.scoping.impl.AbstractScope;

/**
 * IScope wrapper which "uses different combinations of QNs' segments to find a single element or elements".
 *
 * @see SimplestWeiredNameTest
 *
 * @author Anton Kosyakov
 */
public class DottedQualifiedNameAwareScope extends AbstractScope {

	public DottedQualifiedNameAwareScope(IScope parent, boolean ignoreCase) {
		super(parent, ignoreCase);
	}

	@Override
	public IEObjectDescription getSingleElement(QualifiedName name) {
		IEObjectDescription singleElement = super.getSingleElement(name);
		if (singleElement != null) {
			return singleElement;
		}
		if (name.getSegmentCount() > 2) {
			// Required for XcoreTest, because when Xcore indexes packages it does not care about segments
			String mergedSegments = name.skipLast(1).toString(".");
			QualifiedName mergedName = QualifiedName.create(mergedSegments, name.getLastSegment());
			singleElement = super.getSingleElement(mergedName);
			if (singleElement != null) {
				return singleElement;
			}
		}
		QualifiedName mergedName = mergeTwoLastSegments(name);
		if (mergedName == null) {
			return null;
		}
		return getSingleElement(mergedName);
	}

	@Override
	public Iterable<IEObjectDescription> getElements(QualifiedName name) {
		Iterable<IEObjectDescription> elements = super.getElements(name);
		if (elements.iterator().hasNext()) {
			return elements;
		}
		QualifiedName mergedName = mergeTwoLastSegments(name);
		if (mergedName == null) {
			return elements;
		}
		return getElements(mergedName);
	}

	protected QualifiedName mergeTwoLastSegments(QualifiedName name) {
		if (name.getSegmentCount() <= 1) {
			return null;
		}
		String mergedSegments = name.skipFirst(name.getSegmentCount() - 2).toString(".");
		return name.skipLast(2).append(mergedSegments);
	}

	@Override
	protected Iterable<IEObjectDescription> getAllLocalElements() {
		return Collections.emptyList();
	}

}
