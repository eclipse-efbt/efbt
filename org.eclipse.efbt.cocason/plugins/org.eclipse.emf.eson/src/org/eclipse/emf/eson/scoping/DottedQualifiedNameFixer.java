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

import java.util.ArrayList;
import java.util.List;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.xtext.naming.QualifiedName;
import org.eclipse.xtext.util.SimpleAttributeResolver;

import com.google.common.base.Function;
import com.google.common.base.Splitter;

/**
 * Utility which split QNs that have a dot in one their segments into a
 * qualified name that has these segments split as well.
 *
 * See SimplestWeiredNameTest for why this is needed.
 *
 * @author Michael Vorburger, based on discussion with Jan Köhnlein
 */
public class DottedQualifiedNameFixer {

	private static final Function<EObject, QualifiedName> originalFunction = QualifiedName.wrapper(SimpleAttributeResolver.NAME_RESOLVER);

	public static final Function<EObject, QualifiedName> FUNCTION = new Function<EObject, QualifiedName>() {
		@Override
		public QualifiedName apply(EObject input) {
			QualifiedName qn = originalFunction.apply(input);
			QualifiedName splitQN = split(qn);
			return splitQN;
		}
	};

	private static Splitter dotSplitter = Splitter.on('.');

	public static QualifiedName split(QualifiedName qn) {
		List<String> newSegments = new ArrayList<String>(qn.getSegments().size() + 1);
		List<String> segments = qn.getSegments();
		for (String segment : segments) {
			if (segment.contains(".")) {
				Iterable<String> split = dotSplitter.split(segment);
				for (String subSegment : split) {
					newSegments.add(subSegment);
				}
			} else {
				newSegments.add(segment);
			}
		}
		if (newSegments.size() == segments.size())
			return qn;
		else
			return QualifiedName.create(newSegments);
	}

}
