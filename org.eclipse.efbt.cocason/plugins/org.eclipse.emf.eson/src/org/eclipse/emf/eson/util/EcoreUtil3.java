/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2009 - 2014 Sebastian Benz & Michael Vorburger
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.Enumerator;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EClassifier;
import org.eclipse.emf.ecore.EDataType;
import org.eclipse.emf.ecore.EEnum;
import org.eclipse.emf.ecore.EEnumLiteral;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.eclipse.emf.eson.eFactory.Feature;
import org.eclipse.xtext.xbase.lib.Functions;
import org.eclipse.xtext.xbase.lib.IterableExtensions;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

public final class EcoreUtil3 {

	private EcoreUtil3() {
		// should not be instantiated
	}

	public static boolean isInstantiatable(EClass eClass) {
		return !eClass.isAbstract() && !eClass.isInterface();
	}

	public static boolean isSubClass(EClass eClass, EClass potentialSubclass) {
		if (potentialSubclass == null) {
			return false;
		}

		if (eClass == potentialSubclass) {
			return true;
		}
		//we are saying that everything is a sublclass of EObject 
		//if (eClass.equals(EcoreFactory.eINSTANCE.createEObject().eClass())) {
		//	return true;
		//}
		
		for (EClass superType : potentialSubclass.getEAllSuperTypes()) {
			if (eClass == superType) {
				return true;
			}
		}

		return false;
	}

	public static boolean isEReference(EStructuralFeature eFeature) {
		return eFeature instanceof EReference
				&& !((EReference) eFeature).isContainment();
	}

	public static boolean isEContainment(EStructuralFeature eStructuralFeature) {
		if (eStructuralFeature instanceof EReference) {
			EReference eReference = (EReference) eStructuralFeature;
			return eReference.isContainment();
		}
		return false;
	}

	public static boolean isEAttribute(EStructuralFeature eFeature) {
		if (eFeature == null) {
			return false;
		}
		return eFeature instanceof EAttribute;
	}

	public static boolean isStringAttribute(EStructuralFeature eFeature) {
		if (eFeature.getEType() instanceof EDataType) {
			EDataType dataType = (EDataType) eFeature.getEType();
			return dataType.getInstanceClass() == String.class;
		}
		return false;
	}

	public static boolean isIntegerAttribute(EDataType dataType) {
		Class<?> instanceClass = dataType.getInstanceClass();
		return instanceClass == int.class || instanceClass == short.class
				|| instanceClass == long.class
				|| instanceClass == Integer.class
				|| instanceClass == BigInteger.class;
	}

	public static boolean isBooleanAttribute(EDataType dataType) {
		return dataType.getInstanceClass() == boolean.class;
	}

//	public static boolean hasFeature(EClass eClass, EStructuralFeature eFeature) {
//		return eClass.getEAllStructuralFeatures().contains(eFeature);
//	}

	@SuppressWarnings("unchecked")
	public static void setOrAddValue(EObject eObject, EStructuralFeature eFeature, Object newValue) {
		if (eFeature.isMany()) {
			if (newValue != null) {
				EList<Object> eList = (EList<Object>) eObject.eGet(eFeature);
				eList.add(newValue);
			}
		} else {
			if (newValue instanceof EObject) {
				EObject newEValue = (EObject) newValue;
				if (newEValue.eIsProxy()) {
					EcoreUtil.resolve(newEValue, eObject);
				}
			}
			if (newValue == null && eFeature.getEType().getInstanceClass().isPrimitive())
				throw new IllegalArgumentException("EFeature of type primitive instance class cannot be set to null: " + eFeature);
			eObject.eSet(eFeature, newValue);
		}

	}

	public static boolean isDoubleAttribute(EDataType dataType) {
		Class<?> instanceClass = dataType.getInstanceClass();
		return instanceClass == double.class
				|| instanceClass == BigDecimal.class;
	}

	public static Iterable<EAttribute> getAllAttributes(EClass eClass,
			final Class<String> type) {
		return Iterables.filter(eClass.getEAllAttributes(),
				new Predicate<EAttribute>() {

					public boolean apply(EAttribute input) {
						if (input.getEType() instanceof EDataType) {
							EDataType dataType = (EDataType) input.getEType();
							return dataType.getInstanceClass() == type;
						}
						return false;
					}
				});
	}

	public static boolean isDateAttribute(EDataType dataTyp) {
		return dataTyp.getInstanceClass() == Date.class;
	}

	public static boolean isEnum(EClassifier eType) {
		return eType instanceof EEnum;
	}

	public static Iterable<? extends EStructuralFeature> getAssignableFeatures(EClass eClass) {
		return Iterables.filter(eClass.getEAllStructuralFeatures(),
				new Predicate<EStructuralFeature>() {
					public boolean apply(EStructuralFeature input) {
						return input.isChangeable();
					}
				});
	}

	public static EEnumLiteral getEnumLiteral(EEnum enumType,
			final Enumerator enumerator) {
		return Iterables.find(enumType.getELiterals(),
				new Predicate<EEnumLiteral>() {

					public boolean apply(EEnumLiteral input) {
						return enumerator.toString().equals(input.getLiteral());
					}
				});
	}

	public static boolean isDuplicate(EList<Feature> existingFeatures, EStructuralFeature featureToCheck) {
		for (Feature feature : existingFeatures) {
			if (feature.getEFeature() == featureToCheck) {
				return true;
			}
		}
		return false;
	}
	
	 // TODO a helper like this probably already exists somewhere in EMF? Found nothing obvious in EcoreUtil.. :(
    public static String getFullyQualifiedName(EClass eClass) {
    	List<EPackage> allParentEPackages = new ArrayList<EPackage>(5);
    	{
	    	EPackage ePackage = eClass.getEPackage();
	    	do {
	        	allParentEPackages.add(0, ePackage);
	        	ePackage = ePackage.getESuperPackage();
	    	} while (ePackage != null);
    	}
    	
        return IterableExtensions.join(allParentEPackages, null, ".", "." + eClass.getName(), new Functions.Function1<EPackage, String>() {

			@Override
			public String apply(EPackage p) {
				return p.getName();
			}
		});
    }
}
