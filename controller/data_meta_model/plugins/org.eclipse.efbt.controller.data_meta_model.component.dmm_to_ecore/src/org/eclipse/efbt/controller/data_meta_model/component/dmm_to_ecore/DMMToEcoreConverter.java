package org.eclipse.efbt.controller.data_meta_model.component.dmm_to_ecore;

import java.io.IOException;
import java.util.Collections;

import org.eclipse.efbt.cocalimo.smcubes.model.core.DOMAIN;
import org.eclipse.efbt.cocalimo.smcubes.model.core.FACET_VALUE_TYPE;
import org.eclipse.efbt.cocalimo.smcubes.model.core.MEMBER;
import org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN;
import org.eclipse.efbt.cocalimo.smcubes.model.core.SUBDOMAIN_ENUMERATION;
import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Attribute;
import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.BasicEntity;
import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.DerivedEntity;
import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Entity;
import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.EntityModule;
import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.GeneratedEntity;
import org.eclipse.emf.common.util.EList;
import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EClassifier;
import org.eclipse.emf.ecore.EDataType;
import org.eclipse.emf.ecore.EEnum;
import org.eclipse.emf.ecore.EEnumLiteral;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EcoreFactory;
import org.eclipse.emf.ecore.impl.EPackageImpl;
import org.eclipse.emf.ecore.impl.EcorePackageImpl;
import org.eclipse.emf.ecore.resource.Resource;
import org.eclipse.emf.ecore.resource.impl.ResourceFactoryImpl;
import org.eclipse.emf.ecore.xmi.impl.XMIResourceFactoryImpl;

public class DMMToEcoreConverter {

	public static void convert(String fileDirectory, EntityModule entityModule) {
		//create the Ecore package
		//set the attributes from the modules attributes
		//loop through the entities and create an Eclass for each
		//loop though h attributes and set an attribute for each with defualt choices.
		
		//EPackage thePackage = EcoreFactory.eINSTANCE.getEPackage();
		// EList<EClassifier> ecoreClassifiers = thePackage.getEClassifiers();
		// System.out.println(ecoreClassifiers);
		EPackage epackage = EcoreFactory.eINSTANCE.createEPackage();
		epackage.setNsPrefix(entityModule.getNsPrefix());
		epackage.setNsURI(entityModule.getNsURI());
		epackage.setName(entityModule.getName());
		EList<Entity> enitities = entityModule.getEntities();
		for (Entity entity : enitities) {
			EClass eclass =  EcoreFactory.eINSTANCE.createEClass();
			eclass.setName(entity.getName());
			if(entity instanceof BasicEntity)
			{
				BasicEntity basicEntity = (BasicEntity) entity;
				EList<Attribute> attributes = basicEntity.getAttributes();
				for (Attribute attribute : attributes) {
					 EAttribute eAttribute = EcoreFactory.eINSTANCE.createEAttribute();
					 eAttribute.setName(attribute.getName());
					 boolean isDomainEnumerated = attribute.getVariable().getDomain_id().isIs_enumerated();
					 if(isDomainEnumerated)
					 {
						 EEnum eenum = getEnumClassifier(epackage, (SUBDOMAIN) attribute.getClassifier());
						 eAttribute.setEType(eenum);
					 }
					 else
					 {
						 FACET_VALUE_TYPE facetValueType = attribute.getVariable().getDomain_id().getData_type();
						 EDataType dtype = getEcoreDataTypeFromFacetValueType(facetValueType,epackage);
						 eAttribute.setEType(dtype);
					 }
					
					 eclass.getEAttributes().add(eAttribute);
				}	
				
				epackage.getEClassifiers().add(eclass);
			}
			if(entity instanceof DerivedEntity)
			{
				DerivedEntity derivedEntity = (DerivedEntity) entity;
				EList<Attribute> attributes = derivedEntity.getAttributes();
				for (Attribute attribute : attributes) {
					 EAttribute eAttribute = EcoreFactory.eINSTANCE.createEAttribute();
					 eAttribute.setName(attribute.getName());
					 boolean isDomainEnumerated = attribute.getVariable().getDomain_id().isIs_enumerated();
					 if(isDomainEnumerated)
					 {
						 EEnum eenum = getEnumClassifier(epackage, (SUBDOMAIN) attribute.getClassifier());
						 eAttribute.setEType(eenum);
					 }
					 else
					 {
						 FACET_VALUE_TYPE facetValueType = attribute.getVariable().getDomain_id().getData_type();
						 EDataType dtype = getEcoreDataTypeFromFacetValueType(facetValueType,epackage);
						 eAttribute.setEType(dtype);
					 }
					
					 eclass.getEAttributes().add(eAttribute);
				}	
				
				epackage.getEClassifiers().add(eclass);
			}
			if(entity instanceof GeneratedEntity)
			{
				GeneratedEntity generatedEntity = (GeneratedEntity) entity;
				EList<Attribute> attributes = generatedEntity.getAttributes();
				for (Attribute attribute : attributes) {
					 EAttribute eAttribute = EcoreFactory.eINSTANCE.createEAttribute();
					 eAttribute.setName(attribute.getName());
					 boolean isDomainEnumerated = attribute.getVariable().getDomain_id().isIs_enumerated();
					 if(isDomainEnumerated)
					 {
						 EEnum eenum = getEnumClassifier(epackage, (SUBDOMAIN) attribute.getClassifier());
						 eAttribute.setEType(eenum);
					 }
					 else
					 {
						 FACET_VALUE_TYPE facetValueType = attribute.getVariable().getDomain_id().getData_type();
						 EDataType dtype = getEcoreDataTypeFromFacetValueType(facetValueType,epackage);
						 eAttribute.setEType(dtype);
					 }
					
					 eclass.getEAttributes().add(eAttribute);
				}	
				
				epackage.getEClassifiers().add(eclass);
			}
		}
		
		//add superclass
		for (Entity entity : enitities) {
			if(entity instanceof BasicEntity)
			{
				BasicEntity basicEntity = (BasicEntity) entity;
				BasicEntity superClass = basicEntity.getSuperClass();
				if(superClass != null)
				{
					EClass eclass = getEClassForEntity(epackage,entity);
					EClass eSuperclass = getEClassForEntity(epackage,superClass);
					eclass.getESuperTypes().add(eSuperclass);
				}
			}
		}
		//add referenced enums
		
		ResourceFactoryImpl resourceFactory = new XMIResourceFactoryImpl();
		URI modelURI = URI.createFileURI(fileDirectory + "\\dmm.ecore");

		Resource dmmResource = resourceFactory.createResource(modelURI);
		dmmResource.getContents().add(epackage);
		try {
			dmmResource.save(Collections.EMPTY_MAP);
		} catch (IOException e) {
			e.printStackTrace();
		}
		// persist the resource
		
		
	}
	
	private static EClass getEClassForEntity(EPackage epackage, Entity entity) {
		EList<EClassifier> classifiers = epackage.getEClassifiers();
		EClass returnValue= null;
		for (EClassifier eClassifier : classifiers) {
			if(eClassifier instanceof EClass)
			{
				if(eClassifier.getName().equals(entity.getName()))
					returnValue = (EClass) eClassifier;
			}
					
		}
		return returnValue;
	}

	private static EEnum getEnumClassifier(EPackage epackage, SUBDOMAIN subdomain) {
		// return Enum or create it and return it
		EList<EClassifier> classifiers = epackage.getEClassifiers();
		EEnum returnValue= null;
		for (EClassifier eClassifier : classifiers) {
			if(eClassifier instanceof EEnum)
			{
				if(eClassifier.getName().equals(subdomain.getName()))
					returnValue = (EEnum) eClassifier;
			}
					
		}
		if (returnValue == null)
		{
			EEnum eenum = EcoreFactory.eINSTANCE.createEEnum();
			eenum.setName(subdomain.getCode());
			EList<SUBDOMAIN_ENUMERATION> subDomainItems = subdomain.getItems();
			// EList<MEMBER> members = subdomain_ENUMERATION.getMember_ids();
			int counter=0;
			for (SUBDOMAIN_ENUMERATION subdomain_ENUMERATION : subDomainItems) {
				
				MEMBER member = subdomain_ENUMERATION.getMember_id();
				EEnumLiteral eliteral= EcoreFactory.eINSTANCE.createEEnumLiteral();
				counter++;
				eliteral.setLiteral(member.getCode());
				eliteral.setName(member.getCode());
				eliteral.setValue(counter);
				eenum.getELiterals().add(eliteral);

			}
			epackage.getEClassifiers().add(eenum);
			returnValue = eenum;
		}
		return returnValue;
	}

	private static EDataType getEcoreDataTypeFromFacetValueType(FACET_VALUE_TYPE facetValueType, EPackage epackage) {
		EDataType returnType = null;
		if (facetValueType == FACET_VALUE_TYPE.BIG_INTEGER )
			returnType = ((EcorePackageImpl)(((EPackageImpl) epackage).eClass().getEPackage())).getEBigInteger();
		if (facetValueType == FACET_VALUE_TYPE.STRING )
			returnType =  ((EcorePackageImpl)(((EPackageImpl) epackage).eClass().getEPackage())).getEString();
		if (facetValueType == FACET_VALUE_TYPE.DOUBLE )
			returnType =  ((EcorePackageImpl)(((EPackageImpl) epackage).eClass().getEPackage())).getEDouble();
		
		return returnType;
		
		
	}

	public static void main(String[] args)
	{
		convert(null,null);
	}
}
