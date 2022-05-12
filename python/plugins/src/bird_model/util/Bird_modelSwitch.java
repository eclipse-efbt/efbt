/**
 */
package bird_model.util;

import bird_model.Attribute;
import bird_model.BIRDModel;
import bird_model.BasicEntity;
import bird_model.Bird_modelPackage;
import bird_model.Classifier;
import bird_model.Concept;
import bird_model.DOMAIN;
import bird_model.DerivedEntity;
import bird_model.DomainModule;
import bird_model.Element;
import bird_model.Entity;
import bird_model.EntityModule;
import bird_model.EnumMember;
import bird_model.FACET_COLLECTION;
import bird_model.FACET_ENUMERATION;
import bird_model.GeneratedEntity;
import bird_model.MAINTENANCE_AGENCY;
import bird_model.MEMBER;
import bird_model.MEMBER_HIERARCHY;
import bird_model.MEMBER_HIERARCHY_NODE;
import bird_model.ManyToManyRelationshipAttribute;
import bird_model.ManyToOneRelationshipAttribute;
import bird_model.MemberHierarchyModule;
import bird_model.MemberModule;
import bird_model.ModuleDependencies;
import bird_model.ModuleDependency;
import bird_model.ModuleLongName;
import bird_model.NamedElement;
import bird_model.OneToManyRelationshipAttribute;
import bird_model.OneToOneRelationshipAttribute;
import bird_model.RelationshipAttribute;
import bird_model.SMCubesCoreModel;
import bird_model.SUBDOMAIN;
import bird_model.SUBDOMAIN_ENUMERATION;
import bird_model.StructuralFeature;
import bird_model.SubDomainModule;
import bird_model.TypedElement;
import bird_model.VARIABLE;
import bird_model.VARIABLE_SET;
import bird_model.VARIABLE_SET_ENUMERATION;
import bird_model.VariableModule;
import bird_model.facet_type;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.util.Switch;

/**
 * <!-- begin-user-doc -->
 * The <b>Switch</b> for the model's inheritance hierarchy.
 * It supports the call {@link #doSwitch(EObject) doSwitch(object)}
 * to invoke the <code>caseXXX</code> method for each class of the model,
 * starting with the actual class of the object
 * and proceeding up the inheritance hierarchy
 * until a non-null result is returned,
 * which is the result of the switch.
 * <!-- end-user-doc -->
 * @see bird_model.Bird_modelPackage
 * @generated
 */
public class Bird_modelSwitch<T> extends Switch<T> {
	/**
	 * The cached model package
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static Bird_modelPackage modelPackage;

	/**
	 * Creates an instance of the switch.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Bird_modelSwitch() {
		if (modelPackage == null) {
			modelPackage = Bird_modelPackage.eINSTANCE;
		}
	}

	/**
	 * Checks whether this is a switch for the given package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param ePackage the package in question.
	 * @return whether this is a switch for the given package.
	 * @generated
	 */
	@Override
	protected boolean isSwitchFor(EPackage ePackage) {
		return ePackage == modelPackage;
	}

	/**
	 * Calls <code>caseXXX</code> for each class of the model until one returns a non null result; it yields that result.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the first non-null result returned by a <code>caseXXX</code> call.
	 * @generated
	 */
	@Override
	protected T doSwitch(int classifierID, EObject theEObject) {
		switch (classifierID) {
			case Bird_modelPackage.DOMAIN_MODULE: {
				DomainModule domainModule = (DomainModule)theEObject;
				T result = caseDomainModule(domainModule);
				if (result == null) result = caseModule(domainModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.MEMBER_HIERARCHY_MODULE: {
				MemberHierarchyModule memberHierarchyModule = (MemberHierarchyModule)theEObject;
				T result = caseMemberHierarchyModule(memberHierarchyModule);
				if (result == null) result = caseModule(memberHierarchyModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.MEMBER_MODULE: {
				MemberModule memberModule = (MemberModule)theEObject;
				T result = caseMemberModule(memberModule);
				if (result == null) result = caseModule(memberModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.VARIABLE_MODULE: {
				VariableModule variableModule = (VariableModule)theEObject;
				T result = caseVariableModule(variableModule);
				if (result == null) result = caseModule(variableModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.SUB_DOMAIN_MODULE: {
				SubDomainModule subDomainModule = (SubDomainModule)theEObject;
				T result = caseSubDomainModule(subDomainModule);
				if (result == null) result = caseModule(subDomainModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.SM_CUBES_CORE_MODEL: {
				SMCubesCoreModel smCubesCoreModel = (SMCubesCoreModel)theEObject;
				T result = caseSMCubesCoreModel(smCubesCoreModel);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.DOMAIN: {
				DOMAIN domain = (DOMAIN)theEObject;
				T result = caseDOMAIN(domain);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.FACET_COLLECTION: {
				FACET_COLLECTION faceT_COLLECTION = (FACET_COLLECTION)theEObject;
				T result = caseFACET_COLLECTION(faceT_COLLECTION);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.FACET_ENUMERATION: {
				FACET_ENUMERATION faceT_ENUMERATION = (FACET_ENUMERATION)theEObject;
				T result = caseFACET_ENUMERATION(faceT_ENUMERATION);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.FACET_TYPE: {
				facet_type facet_type = (facet_type)theEObject;
				T result = casefacet_type(facet_type);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.MAINTENANCE_AGENCY: {
				MAINTENANCE_AGENCY maintenancE_AGENCY = (MAINTENANCE_AGENCY)theEObject;
				T result = caseMAINTENANCE_AGENCY(maintenancE_AGENCY);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.MEMBER: {
				MEMBER member = (MEMBER)theEObject;
				T result = caseMEMBER(member);
				if (result == null) result = caseEnumMember(member);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.MEMBER_HIERARCHY: {
				MEMBER_HIERARCHY membeR_HIERARCHY = (MEMBER_HIERARCHY)theEObject;
				T result = caseMEMBER_HIERARCHY(membeR_HIERARCHY);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.MEMBER_HIERARCHY_NODE: {
				MEMBER_HIERARCHY_NODE membeR_HIERARCHY_NODE = (MEMBER_HIERARCHY_NODE)theEObject;
				T result = caseMEMBER_HIERARCHY_NODE(membeR_HIERARCHY_NODE);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.SUBDOMAIN: {
				SUBDOMAIN subdomain = (SUBDOMAIN)theEObject;
				T result = caseSUBDOMAIN(subdomain);
				if (result == null) result = caseClassifier(subdomain);
				if (result == null) result = caseNamedElement(subdomain);
				if (result == null) result = caseElement(subdomain);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.SUBDOMAIN_ENUMERATION: {
				SUBDOMAIN_ENUMERATION subdomaiN_ENUMERATION = (SUBDOMAIN_ENUMERATION)theEObject;
				T result = caseSUBDOMAIN_ENUMERATION(subdomaiN_ENUMERATION);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.VARIABLE: {
				VARIABLE variable = (VARIABLE)theEObject;
				T result = caseVARIABLE(variable);
				if (result == null) result = caseConcept(variable);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.VARIABLE_SET: {
				VARIABLE_SET variablE_SET = (VARIABLE_SET)theEObject;
				T result = caseVARIABLE_SET(variablE_SET);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.VARIABLE_SET_ENUMERATION: {
				VARIABLE_SET_ENUMERATION variablE_SET_ENUMERATION = (VARIABLE_SET_ENUMERATION)theEObject;
				T result = caseVARIABLE_SET_ENUMERATION(variablE_SET_ENUMERATION);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.ATTRIBUTE: {
				Attribute attribute = (Attribute)theEObject;
				T result = caseAttribute(attribute);
				if (result == null) result = caseStructuralFeature(attribute);
				if (result == null) result = caseTypedElement(attribute);
				if (result == null) result = caseNamedElement(attribute);
				if (result == null) result = caseElement(attribute);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.RELATIONSHIP_ATTRIBUTE: {
				RelationshipAttribute relationshipAttribute = (RelationshipAttribute)theEObject;
				T result = caseRelationshipAttribute(relationshipAttribute);
				if (result == null) result = caseAttribute(relationshipAttribute);
				if (result == null) result = caseStructuralFeature(relationshipAttribute);
				if (result == null) result = caseTypedElement(relationshipAttribute);
				if (result == null) result = caseNamedElement(relationshipAttribute);
				if (result == null) result = caseElement(relationshipAttribute);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.ONE_TO_ONE_RELATIONSHIP_ATTRIBUTE: {
				OneToOneRelationshipAttribute oneToOneRelationshipAttribute = (OneToOneRelationshipAttribute)theEObject;
				T result = caseOneToOneRelationshipAttribute(oneToOneRelationshipAttribute);
				if (result == null) result = caseRelationshipAttribute(oneToOneRelationshipAttribute);
				if (result == null) result = caseAttribute(oneToOneRelationshipAttribute);
				if (result == null) result = caseStructuralFeature(oneToOneRelationshipAttribute);
				if (result == null) result = caseTypedElement(oneToOneRelationshipAttribute);
				if (result == null) result = caseNamedElement(oneToOneRelationshipAttribute);
				if (result == null) result = caseElement(oneToOneRelationshipAttribute);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.MANY_TO_ONE_RELATIONSHIP_ATTRIBUTE: {
				ManyToOneRelationshipAttribute manyToOneRelationshipAttribute = (ManyToOneRelationshipAttribute)theEObject;
				T result = caseManyToOneRelationshipAttribute(manyToOneRelationshipAttribute);
				if (result == null) result = caseRelationshipAttribute(manyToOneRelationshipAttribute);
				if (result == null) result = caseAttribute(manyToOneRelationshipAttribute);
				if (result == null) result = caseStructuralFeature(manyToOneRelationshipAttribute);
				if (result == null) result = caseTypedElement(manyToOneRelationshipAttribute);
				if (result == null) result = caseNamedElement(manyToOneRelationshipAttribute);
				if (result == null) result = caseElement(manyToOneRelationshipAttribute);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.ONE_TO_MANY_RELATIONSHIP_ATTRIBUTE: {
				OneToManyRelationshipAttribute oneToManyRelationshipAttribute = (OneToManyRelationshipAttribute)theEObject;
				T result = caseOneToManyRelationshipAttribute(oneToManyRelationshipAttribute);
				if (result == null) result = caseRelationshipAttribute(oneToManyRelationshipAttribute);
				if (result == null) result = caseAttribute(oneToManyRelationshipAttribute);
				if (result == null) result = caseStructuralFeature(oneToManyRelationshipAttribute);
				if (result == null) result = caseTypedElement(oneToManyRelationshipAttribute);
				if (result == null) result = caseNamedElement(oneToManyRelationshipAttribute);
				if (result == null) result = caseElement(oneToManyRelationshipAttribute);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.MANY_TO_MANY_RELATIONSHIP_ATTRIBUTE: {
				ManyToManyRelationshipAttribute manyToManyRelationshipAttribute = (ManyToManyRelationshipAttribute)theEObject;
				T result = caseManyToManyRelationshipAttribute(manyToManyRelationshipAttribute);
				if (result == null) result = caseRelationshipAttribute(manyToManyRelationshipAttribute);
				if (result == null) result = caseAttribute(manyToManyRelationshipAttribute);
				if (result == null) result = caseStructuralFeature(manyToManyRelationshipAttribute);
				if (result == null) result = caseTypedElement(manyToManyRelationshipAttribute);
				if (result == null) result = caseNamedElement(manyToManyRelationshipAttribute);
				if (result == null) result = caseElement(manyToManyRelationshipAttribute);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.CLASSIFIER: {
				Classifier classifier = (Classifier)theEObject;
				T result = caseClassifier(classifier);
				if (result == null) result = caseNamedElement(classifier);
				if (result == null) result = caseElement(classifier);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.ELEMENT: {
				Element element = (Element)theEObject;
				T result = caseElement(element);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.NAMED_ELEMENT: {
				NamedElement namedElement = (NamedElement)theEObject;
				T result = caseNamedElement(namedElement);
				if (result == null) result = caseElement(namedElement);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.ENTITY_MODULE: {
				EntityModule entityModule = (EntityModule)theEObject;
				T result = caseEntityModule(entityModule);
				if (result == null) result = caseModule(entityModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.STRUCTURAL_FEATURE: {
				StructuralFeature structuralFeature = (StructuralFeature)theEObject;
				T result = caseStructuralFeature(structuralFeature);
				if (result == null) result = caseTypedElement(structuralFeature);
				if (result == null) result = caseNamedElement(structuralFeature);
				if (result == null) result = caseElement(structuralFeature);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.TYPED_ELEMENT: {
				TypedElement typedElement = (TypedElement)theEObject;
				T result = caseTypedElement(typedElement);
				if (result == null) result = caseNamedElement(typedElement);
				if (result == null) result = caseElement(typedElement);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.GENERATED_ENTITY: {
				GeneratedEntity generatedEntity = (GeneratedEntity)theEObject;
				T result = caseGeneratedEntity(generatedEntity);
				if (result == null) result = caseEntity(generatedEntity);
				if (result == null) result = caseNamedElement(generatedEntity);
				if (result == null) result = caseElement(generatedEntity);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.ENTITY: {
				Entity entity = (Entity)theEObject;
				T result = caseEntity(entity);
				if (result == null) result = caseNamedElement(entity);
				if (result == null) result = caseElement(entity);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.DERIVED_ENTITY: {
				DerivedEntity derivedEntity = (DerivedEntity)theEObject;
				T result = caseDerivedEntity(derivedEntity);
				if (result == null) result = caseEntity(derivedEntity);
				if (result == null) result = caseNamedElement(derivedEntity);
				if (result == null) result = caseElement(derivedEntity);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.BASIC_ENTITY: {
				BasicEntity basicEntity = (BasicEntity)theEObject;
				T result = caseBasicEntity(basicEntity);
				if (result == null) result = caseEntity(basicEntity);
				if (result == null) result = caseNamedElement(basicEntity);
				if (result == null) result = caseElement(basicEntity);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.MODULE: {
				bird_model.Module module = (bird_model.Module)theEObject;
				T result = caseModule(module);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.MODULE_LONG_NAME: {
				ModuleLongName moduleLongName = (ModuleLongName)theEObject;
				T result = caseModuleLongName(moduleLongName);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.MODULE_DEPENDENCIES: {
				ModuleDependencies moduleDependencies = (ModuleDependencies)theEObject;
				T result = caseModuleDependencies(moduleDependencies);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.MODULE_DEPENDENCY: {
				ModuleDependency moduleDependency = (ModuleDependency)theEObject;
				T result = caseModuleDependency(moduleDependency);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.BIRD_MODEL: {
				BIRDModel birdModel = (BIRDModel)theEObject;
				T result = caseBIRDModel(birdModel);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.CONCEPT: {
				Concept concept = (Concept)theEObject;
				T result = caseConcept(concept);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Bird_modelPackage.ENUM_MEMBER: {
				EnumMember enumMember = (EnumMember)theEObject;
				T result = caseEnumMember(enumMember);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			default: return defaultCase(theEObject);
		}
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Domain Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Domain Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseDomainModule(DomainModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Member Hierarchy Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Member Hierarchy Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseMemberHierarchyModule(MemberHierarchyModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Member Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Member Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseMemberModule(MemberModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Variable Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Variable Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseVariableModule(VariableModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Sub Domain Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Sub Domain Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseSubDomainModule(SubDomainModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>SM Cubes Core Model</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>SM Cubes Core Model</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseSMCubesCoreModel(SMCubesCoreModel object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>DOMAIN</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>DOMAIN</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseDOMAIN(DOMAIN object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>FACET COLLECTION</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>FACET COLLECTION</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseFACET_COLLECTION(FACET_COLLECTION object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>FACET ENUMERATION</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>FACET ENUMERATION</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseFACET_ENUMERATION(FACET_ENUMERATION object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>facet type</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>facet type</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T casefacet_type(facet_type object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>MAINTENANCE AGENCY</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>MAINTENANCE AGENCY</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseMAINTENANCE_AGENCY(MAINTENANCE_AGENCY object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>MEMBER</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>MEMBER</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseMEMBER(MEMBER object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>MEMBER HIERARCHY</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>MEMBER HIERARCHY</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseMEMBER_HIERARCHY(MEMBER_HIERARCHY object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>MEMBER HIERARCHY NODE</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>MEMBER HIERARCHY NODE</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseMEMBER_HIERARCHY_NODE(MEMBER_HIERARCHY_NODE object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>SUBDOMAIN</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>SUBDOMAIN</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseSUBDOMAIN(SUBDOMAIN object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>SUBDOMAIN ENUMERATION</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>SUBDOMAIN ENUMERATION</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseSUBDOMAIN_ENUMERATION(SUBDOMAIN_ENUMERATION object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>VARIABLE</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>VARIABLE</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseVARIABLE(VARIABLE object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>VARIABLE SET</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>VARIABLE SET</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseVARIABLE_SET(VARIABLE_SET object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>VARIABLE SET ENUMERATION</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>VARIABLE SET ENUMERATION</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseVARIABLE_SET_ENUMERATION(VARIABLE_SET_ENUMERATION object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Attribute</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Attribute</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseAttribute(Attribute object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Relationship Attribute</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Relationship Attribute</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseRelationshipAttribute(RelationshipAttribute object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>One To One Relationship Attribute</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>One To One Relationship Attribute</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseOneToOneRelationshipAttribute(OneToOneRelationshipAttribute object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Many To One Relationship Attribute</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Many To One Relationship Attribute</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseManyToOneRelationshipAttribute(ManyToOneRelationshipAttribute object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>One To Many Relationship Attribute</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>One To Many Relationship Attribute</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseOneToManyRelationshipAttribute(OneToManyRelationshipAttribute object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Many To Many Relationship Attribute</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Many To Many Relationship Attribute</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseManyToManyRelationshipAttribute(ManyToManyRelationshipAttribute object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Classifier</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Classifier</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseClassifier(Classifier object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Element</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Element</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseElement(Element object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Named Element</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Named Element</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseNamedElement(NamedElement object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Entity Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Entity Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseEntityModule(EntityModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Structural Feature</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Structural Feature</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseStructuralFeature(StructuralFeature object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Typed Element</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Typed Element</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseTypedElement(TypedElement object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Generated Entity</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Generated Entity</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseGeneratedEntity(GeneratedEntity object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Entity</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Entity</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseEntity(Entity object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Derived Entity</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Derived Entity</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseDerivedEntity(DerivedEntity object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Basic Entity</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Basic Entity</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseBasicEntity(BasicEntity object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseModule(bird_model.Module object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Module Long Name</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Module Long Name</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseModuleLongName(ModuleLongName object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Module Dependencies</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Module Dependencies</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseModuleDependencies(ModuleDependencies object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Module Dependency</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Module Dependency</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseModuleDependency(ModuleDependency object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>BIRD Model</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>BIRD Model</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseBIRDModel(BIRDModel object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Concept</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Concept</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseConcept(Concept object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Enum Member</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Enum Member</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseEnumMember(EnumMember object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EObject</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch, but this is the last case anyway.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EObject</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject)
	 * @generated
	 */
	@Override
	public T defaultCase(EObject object) {
		return null;
	}

} //Bird_modelSwitch
