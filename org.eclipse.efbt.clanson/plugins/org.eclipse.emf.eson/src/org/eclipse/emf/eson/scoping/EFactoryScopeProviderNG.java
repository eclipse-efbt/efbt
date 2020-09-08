/*
 * #%L
 * org.eclipse.emf.eson
 * %%
 * Copyright (C) 2009 - 2014 Sebastian Benz, Michael Vorburger and others
 * %%
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 * #L%
 */
package org.eclipse.emf.eson.scoping;

import java.util.List;

import org.eclipse.efbt.clan.model.clan.Program;
import org.eclipse.efbt.testing.common.model.test_definition.Clause;
import org.eclipse.efbt.testing.common.model.test_definition.Given;
import org.eclipse.efbt.testing.common.model.test_definition.Param;
import org.eclipse.efbt.testing.common.model.test_definition.TestContraints;
import org.eclipse.efbt.testing.common.model.test_definition.TestDefinition;
import org.eclipse.efbt.testing.common.model.test_definition.TestDefinitionModule;
import org.eclipse.efbt.testing.common.model.test_definition.TestTemplate;
import org.eclipse.efbt.testing.common.model.test_definition.Test_definitionPackage;
import org.eclipse.efbt.testing.common.model.test_definition.Then;
import org.eclipse.efbt.testing.common.model.test_definition.When;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EEnum;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EReference;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.ecore.EcoreFactory;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.eson.building.NewObjectExtensions;
import org.eclipse.emf.eson.eFactory.Attribute;
import org.eclipse.emf.eson.eFactory.Containment;
import org.eclipse.emf.eson.eFactory.CustomNameMapping;
import org.eclipse.emf.eson.eFactory.EnumAttribute;
import org.eclipse.emf.eson.eFactory.Feature;
import org.eclipse.emf.eson.eFactory.MultiValue;
import org.eclipse.emf.eson.eFactory.NewObject;
import org.eclipse.emf.eson.eFactory.StringAttribute;
import org.eclipse.emf.eson.util.EcoreUtil3;
import org.eclipse.xtext.EcoreUtil2;
import org.eclipse.xtext.resource.IEObjectDescription;
import org.eclipse.xtext.scoping.ICaseInsensitivityHelper;
import org.eclipse.xtext.scoping.IScope;
import org.eclipse.xtext.scoping.Scopes;
import org.eclipse.xtext.scoping.impl.AbstractDeclarativeScopeProvider;
import org.eclipse.xtext.scoping.impl.FilteringScope;
import org.eclipse.xtext.scoping.impl.SimpleScope;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.inject.Inject;

/**
 * Scope Provider which finds everything only from within the workspace using the Xtext index, as usual.
 * 
 * @see EFactoryScopeProvider
 */
public class EFactoryScopeProviderNG extends AbstractDeclarativeScopeProvider {

	protected @Inject ICaseInsensitivityHelper caseInsensitivityHelper;
	protected @Inject NewObjectExtensions newObjectExtensions;

	public IScope scope_EnumAttribute_value(EnumAttribute attribute, EReference reference) {
		Feature feature = getFeature(attribute);
		if (feature.getEFeature().getEType() instanceof EEnum) {
			EEnum enumType = (EEnum) feature.getEFeature().getEType();
			Iterable<IEObjectDescription> elements = Scopes.scopedElementsFor(enumType.getELiterals(), DottedQualifiedNameFixer.FUNCTION);
			return new SimpleScope(elements);
		}
		return IScope.NULLSCOPE;
	}
	
	// This may look a bit strange, but is required for 
	// org.eclipse.emf.eson.ui.contentassist.EFactoryProposalProvider.completeFeature_EFeature()
	public IScope scope_Feature_eFeature(NewObject newObject, EReference reference) {
		Optional<EClass> optionalEClass = newObjectExtensions.getDeclaredOrInferredEClass(newObject);
		if (optionalEClass.isPresent()) {
			EClass eClass = optionalEClass.get();
			Iterable<? extends EObject> assignableFeature = EcoreUtil3.getAssignableFeatures(eClass );
			Iterable<IEObjectDescription> descs = Scopes.scopedElementsFor(assignableFeature, DottedQualifiedNameFixer.FUNCTION);
			return new SimpleScope(descs);
		} else {
			return IScope.NULLSCOPE;
		}
	}

	public IScope scope_Feature_eFeature(Feature feature, EReference reference) {
		NewObject newObject = (NewObject) feature.eContainer();
		return scope_Feature_eFeature(newObject, reference);
	}

	public IScope scope_Feature_reference(NewObject newObject, EReference reference) {
		Optional<EClass> optionalEClass = newObjectExtensions.getDeclaredOrInferredEClass(newObject);
		if (optionalEClass.isPresent()) {
			return new SimpleScope(Scopes.scopedElementsFor(optionalEClass.get().getEAllStructuralFeatures(), DottedQualifiedNameFixer.FUNCTION));
		} else {
			return IScope.NULLSCOPE;
		}
	}

	public IScope scope_EClass(EObject context, EReference reference) {
		IScope scope = delegateGetScope(context, reference);
		scope = new FilteringScope(scope, new Predicate<IEObjectDescription>() {
			public boolean apply(IEObjectDescription desc) {
				// @see org.eclipse.xtext.ecore.EcoreResourceDescriptionStrategy
				return !"true".equals(desc.getUserData("nsURI"));
			}
		});
		return new DottedQualifiedNameAwareScope(scope, isIgnoreCase(reference));
	}

	public IScope scope_Reference_value(Feature feature, EReference eReference) {
		EStructuralFeature sourceFeature = feature.getEFeature();
		EClass classContainingFeature = sourceFeature.getEContainingClass();
		
		//check if the cFeature was in a param which was part of a test template.
		//then , somehow, find the actaul instance of the Param, lookup using the test template name
		// we can serach for that specific test defintion if we have access to the resource, then
		// check the index of our param feature  and somehow find if we are in given or then or when.
		
		boolean isParam = isTestDefinitionParam(feature);
		if(isParam)
		{
		Param theParam = getParameterFromTestDefinition(feature);
		
		if(theParam != null)
		if ((theParam.eContainer()) instanceof Clause) {
			if (theParam instanceof Param) {
				Clause bddclauseElement = (Clause) (theParam.eContainer());
				TestDefinition bddElement = (TestDefinition) (bddclauseElement.eContainer());
				TestContraints coverageTestSet = bddElement.getTestContraints();
				TestTemplate coverageTestType = coverageTestSet.getTemplate();

				// find out index of containing bddclauseElement
				int index = bddclauseElement.getParams().indexOf(theParam);

				if (bddclauseElement instanceof Given) {

					int size = coverageTestSet.getGivenParams().size();
					if (index < size) {
						EObject templateObject = coverageTestSet.getGivenParams().get(index).getParam();
						EObject templateType = coverageTestType.getGivenParams().get(index).getParam();

						List<? extends EObject> candidates = EcoreUtil2.getAllContentsOfType(templateObject,
								templateType.getClass());

						return Scopes.scopeFor(candidates);
					}

				}

				if (bddclauseElement instanceof When) {

					int size = coverageTestSet.getWhenParams().size();
					if (index < size) {
						EObject templateObject = coverageTestSet.getWhenParams().get(index).getParam();
						EObject templateType = coverageTestType.getWhenParams().get(index).getParam();

						List<? extends EObject> candidates = EcoreUtil2.getAllContentsOfType(templateObject,
								templateType.getClass());

						return Scopes.scopeFor(candidates);
					}

				}

				if (bddclauseElement instanceof Then) {

					int size = coverageTestSet.getThenParams().size();
					if (index < size) {

						EObject templateObject = coverageTestSet.getThenParams().get(index).getParam();
						EObject templateType = coverageTestType.getThenParams().get(index).getParam();

						List<? extends EObject> candidates = EcoreUtil2.getAllContentsOfType(templateObject,
								templateType.getClass());

						return Scopes.scopeFor(candidates);

					}
				}

			}

		}
		}
		//return super.getScope(context, reference);
		
		
		
		
		if (EcoreUtil3.isEReference(sourceFeature)) {
			EReference realEReference = (EReference) sourceFeature;
			IScope parentScope = delegateGetScope(feature, realEReference);
			final EClass referenceType = realEReference.getEReferenceType();
			//realEReference.get
			return new FilteringScope(parentScope, new Predicate<IEObjectDescription>() {
				public boolean apply(IEObjectDescription desc) {
					boolean returnValue = false;
					returnValue = referenceType.isSuperTypeOf(desc.getEClass());
					if(referenceType.equals(EcoreFactory.eINSTANCE.createEObject().eClass()))
							returnValue = true;
					return returnValue;
				}
			});
		}
		return IScope.NULLSCOPE;
	}

	private boolean isTestDefinitionParam(Feature feature) {

		boolean returnValue = false;
		EStructuralFeature theEFeature = feature.getEFeature();

		if (theEFeature.getName().equalsIgnoreCase("param")) {

			if (theEFeature
					.getFeatureID() == Test_definitionPackage.PARAM__PARAM) {
				EObject temp = feature.eContainer().eContainer().eContainer()
						.eContainer().eContainer().eContainer().eContainer()
						.eContainer().eContainer().eContainer();
				if (temp != null) {
					EObject otherFeature = temp.eContainer();
					if (otherFeature instanceof Feature) {

						if (otherFeature != null) {
							if (((Feature) otherFeature)
									.getEFeature() != null) {
								if (((Feature) otherFeature).getEFeature()
										.getName()
										.equalsIgnoreCase("testDefinitions")) {
									returnValue = true;
								}
							}
						}
					}
				}
			}
		}
		return returnValue;

	}

	private Param getParameterFromTestDefinition(
			Feature feature) {
		//get the NewObject
		NewObject testDefintionNewObject = (NewObject)feature.eContainer().eContainer().
				eContainer().eContainer().eContainer().eContainer().eContainer().eContainer();
		//get the name  Attribute For the object.
		
		
		String testDefintionName  = "";
		EList<Feature> theFeatures = testDefintionNewObject.getFeatures();
		for (Feature feature2 : theFeatures) {
			if(feature2.getEFeature().getFeatureID() == Test_definitionPackage.TEST_DEFINITION__NAME)
			{
				testDefintionName = ((StringAttribute) feature2.getValue()).getValue(); 
			}
		}

		TestDefinition definition = getTestDefinitionWithName(testDefintionName,feature.eResource());
	
			int paramIndex = getParamIndex(feature);
			
		int clauseType = getClauseType(feature);
		
		Param returnParam =  getParameterAtIndex(definition, paramIndex, clauseType);
		
		return returnParam;
		
	}

	private int getClauseType(Feature feature) {
		// TODO Auto-generated method stub
		Feature clauseFeature = (Feature) feature.eContainer().eContainer().eContainer().eContainer().eContainer().eContainer().eContainer();
		int featureId = clauseFeature.getEFeature().getFeatureID();
		return featureId;
	}

	private Param getParameterAtIndex(TestDefinition definition,
			int paramIndex, int clauseType) {
	
		
		
		Clause theClause = null;
		
		if(clauseType == Test_definitionPackage.TEST_DEFINITION__GIVEN )
			theClause = definition.getGiven();
		
		if(clauseType == Test_definitionPackage.TEST_DEFINITION__WHEN )
			theClause = definition.getWhen();
		
		if(clauseType == Test_definitionPackage.TEST_DEFINITION__THEN )
			theClause = definition.getThen();
		
		Param param = null;
		if (theClause != null)
		{
		EList<Param> params = theClause.getParams();
		param = params.get(paramIndex);
		}
		
		return param;
		
		
		
	}

	private int getParamIndex(Feature feature) {
		// TODO Auto-generated method stub
		Containment c = (Containment) feature.eContainer().eContainer();
		MultiValue m = (MultiValue) feature.eContainer().eContainer().eContainer();
		EList<EObject> contents = m.eContents();
		int counter=0;
		for (EObject eObject : contents) {
		  if	(c.equals(eObject))
			  break;
		  else
			  counter++;
		}
		return counter;
		
	}

	private TestDefinition getTestDefinitionWithName(String testDefintionName,
			Resource resource) {
		
		Program program = (Program) resource.getContents().get(1);
		
		TestDefinitionModule tdm = program.getTestDefinitions();
		EList<TestDefinition> testDefinitions = tdm.getTestDefinitions();
		
		TestDefinition theDefinition = null;
		for (TestDefinition testDefinition : testDefinitions) {
			if(testDefinition.getName().equals(testDefintionName))
				theDefinition = testDefinition;
		}
		return theDefinition;
	}

	public IScope scope_CustomNameMapping_nameFeature(CustomNameMapping mapping, EReference reference) {
		Iterable<EAttribute> attributes = EcoreUtil3.getAllAttributes(mapping.getEClass(), String.class);
		Iterable<IEObjectDescription> elements = Scopes.scopedElementsFor(attributes, DottedQualifiedNameFixer.FUNCTION);
		return new SimpleScope(elements);
	}

	public IScope scope_Containment_value(Feature feature, EReference eReference) {
		if (EcoreUtil3.isEContainment(feature.getEFeature())) {
			return super.getDelegate().getScope(feature, eReference);
		}
		return IScope.NULLSCOPE;
	}

	protected Feature getFeature(Attribute attribute) {
		 EObject container = attribute.eContainer();
		 if (container instanceof MultiValue) {
			 // MultiValue mv = (MultiValue) container;
			 container = container.eContainer();
		 }
		 return (Feature) container; 
	}

	protected boolean isIgnoreCase(EReference reference) {
		return caseInsensitivityHelper.isIgnoreCase(reference);
	}

}
