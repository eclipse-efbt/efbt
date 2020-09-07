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
package org.eclipse.emf.eson.tests.util;

import javax.inject.Inject;

import org.eclipse.emf.ecore.EEnumLiteral;
import org.eclipse.emf.ecore.ENamedElement;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.eson.eFactory.BooleanAttribute;
import org.eclipse.emf.eson.eFactory.Containment;
import org.eclipse.emf.eson.eFactory.DateAttribute;
import org.eclipse.emf.eson.eFactory.DoubleAttribute;
import org.eclipse.emf.eson.eFactory.EnumAttribute;
import org.eclipse.emf.eson.eFactory.Factory;
import org.eclipse.emf.eson.eFactory.Feature;
import org.eclipse.emf.eson.eFactory.IntegerAttribute;
import org.eclipse.emf.eson.eFactory.MultiValue;
import org.eclipse.emf.eson.eFactory.NewObject;
import org.eclipse.emf.eson.eFactory.NullAttribute;
import org.eclipse.emf.eson.eFactory.Reference;
import org.eclipse.emf.eson.eFactory.StringAttribute;
import org.eclipse.emf.eson.eFactory.Value;
import org.eclipse.emf.eson.resource.EFactoryResource;
import org.eclipse.emf.eson.util.XtextProxyUtil;

public class DumpParsedStructureUtil {
	
	protected @Inject XtextProxyUtil xtextProxyUtil;  

	public void dump(EObject eo) {
		Resource eResource = eo.eResource();
		final EFactoryResource eFactoryResource = (EFactoryResource) eResource;
		Factory factory = eFactoryResource.getEFactoryFactory();
		if (factory == null)
			return;
		dump("", factory.getRoot());
	}
	
	protected void dump(String indent, NewObject no) {
		dumpPossiblyNamedOrProxy(no, no.getEClass());
		if (no.getName() != null) {
			System.out.print(" '");
			System.out.print(no.getName());
			System.out.print("'");
		}
		System.out.print(" ");
		System.out.print("{");
		if (!no.getFeatures().isEmpty())
			System.out.println();
		
		for (Feature feature : no.getFeatures()) {
			System.out.print(indent + "    ");
			dumpPossiblyNamedOrProxy(feature, feature.getEFeature());
			System.out.print(": ");
			dumpValue(indent + "    ", feature.getValue(), no);
		}
		
		if (!no.getFeatures().isEmpty())
			System.out.print(indent);
		System.out.println("}");
	}

	protected void dumpPossiblyNamedOrProxy(EObject ctx, EObject eObject) {
		if (eObject == null) {
			System.out.print("NULL");
		} else if (!eObject.eIsProxy()) {
			if (eObject instanceof ENamedElement) {
				ENamedElement namedElement = (ENamedElement) eObject;
				System.out.print(namedElement.getName());
			}
			else
				System.out.print(eObject.toString());
		} else {
			System.out.print("PROXY-");
			System.out.print(xtextProxyUtil.getProxyCrossRefAsString(ctx, eObject));
		}
	}
	
	protected void dumpValue(String indent, Value value, EObject ctx) {
		if (value == null) {
			System.out.println("!Feature-Value-isNull");
		} else if (value instanceof Containment) {
			Containment containment = (Containment) value;
			dump(indent, containment.getValue());
		} else if (value instanceof MultiValue) {
			System.out.println("[");
			MultiValue mv = (MultiValue) value;
			for (Value avalue : mv.getValues()) {
				System.out.print(indent + "    ");
				dumpValue(indent + "    ", avalue, mv);
			}
			System.out.println(indent + "]");
		} else if (value instanceof Reference) {
			Reference ref = (Reference) value;
			System.out.print("#");
			dumpPossiblyNamedOrProxy(ctx, ref.getValue());
			System.out.println();
		} else if (value instanceof EnumAttribute) {
			EnumAttribute enumAttribute = (EnumAttribute) value;
			final EEnumLiteral enumLiteral = enumAttribute.getValue();
			final String enumLiternalText = (enumLiteral != null) ? enumLiteral.toString() : "nullValueEEnumLiteral";
			System.out.println(indent + "ENUM " + enumLiternalText);
		} else if (value instanceof StringAttribute) {
			StringAttribute stringAttribute = (StringAttribute) value;
			System.out.println(indent + stringAttribute.getValue());
		} else if (value instanceof IntegerAttribute) {
			IntegerAttribute integerAttribute = (IntegerAttribute) value;
			System.out.println(indent + integerAttribute.getValue());
		} else if (value instanceof BooleanAttribute) {
			BooleanAttribute booleanAttribute = (BooleanAttribute) value;
			System.out.println(indent + booleanAttribute.isValue());
		} else if (value instanceof DateAttribute) {
			DateAttribute dateAttribute = (DateAttribute) value;
			System.out.println(indent + dateAttribute.getValue());
		} else if (value instanceof DoubleAttribute) {
			DoubleAttribute doubleAttribute = (DoubleAttribute) value;
			System.out.println(indent + doubleAttribute.getValue());
		} else if (value instanceof NullAttribute) {
			NullAttribute nullAttribute = (NullAttribute) value;
			System.out.println(indent + nullAttribute.getValue());

		} else {
			throw new IllegalStateException("Implement instanceof for: " + value.getClass());
		}
	}
}
