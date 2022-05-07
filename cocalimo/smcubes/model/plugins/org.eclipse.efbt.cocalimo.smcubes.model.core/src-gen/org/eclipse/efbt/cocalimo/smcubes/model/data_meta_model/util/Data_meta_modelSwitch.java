/**
 */
package org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.util;

import org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.*;

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
 * @see org.eclipse.efbt.cocalimo.smcubes.model.data_meta_model.Data_meta_modelPackage
 * @generated
 */
public class Data_meta_modelSwitch<T> extends Switch<T> {
	/**
	 * The cached model package
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static Data_meta_modelPackage modelPackage;

	/**
	 * Creates an instance of the switch.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Data_meta_modelSwitch() {
		if (modelPackage == null) {
			modelPackage = Data_meta_modelPackage.eINSTANCE;
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
			case Data_meta_modelPackage.ATTRIBUTE: {
				Attribute attribute = (Attribute)theEObject;
				T result = caseAttribute(attribute);
				if (result == null) result = caseStructuralFeature(attribute);
				if (result == null) result = caseTypedElement(attribute);
				if (result == null) result = caseNamedElement(attribute);
				if (result == null) result = caseElement(attribute);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Data_meta_modelPackage.RELATIONSHIP_ATTRIBUTE: {
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
			case Data_meta_modelPackage.ONE_TO_ONE_RELATIONSHIP_ATTRIBUTE: {
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
			case Data_meta_modelPackage.MANY_TO_ONE_RELATIONSHIP_ATTRIBUTE: {
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
			case Data_meta_modelPackage.ONE_TO_MANY_RELATIONSHIP_ATTRIBUTE: {
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
			case Data_meta_modelPackage.MANY_TO_MANY_RELATIONSHIP_ATTRIBUTE: {
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
			case Data_meta_modelPackage.CLASSIFIER: {
				Classifier classifier = (Classifier)theEObject;
				T result = caseClassifier(classifier);
				if (result == null) result = caseNamedElement(classifier);
				if (result == null) result = caseElement(classifier);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Data_meta_modelPackage.ELEMENT: {
				Element element = (Element)theEObject;
				T result = caseElement(element);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Data_meta_modelPackage.NAMED_ELEMENT: {
				NamedElement namedElement = (NamedElement)theEObject;
				T result = caseNamedElement(namedElement);
				if (result == null) result = caseElement(namedElement);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Data_meta_modelPackage.ENTITY_MODULE: {
				EntityModule entityModule = (EntityModule)theEObject;
				T result = caseEntityModule(entityModule);
				if (result == null) result = caseModule(entityModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Data_meta_modelPackage.STRUCTURAL_FEATURE: {
				StructuralFeature structuralFeature = (StructuralFeature)theEObject;
				T result = caseStructuralFeature(structuralFeature);
				if (result == null) result = caseTypedElement(structuralFeature);
				if (result == null) result = caseNamedElement(structuralFeature);
				if (result == null) result = caseElement(structuralFeature);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Data_meta_modelPackage.TYPED_ELEMENT: {
				TypedElement typedElement = (TypedElement)theEObject;
				T result = caseTypedElement(typedElement);
				if (result == null) result = caseNamedElement(typedElement);
				if (result == null) result = caseElement(typedElement);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Data_meta_modelPackage.GENERATED_ENTITY: {
				GeneratedEntity generatedEntity = (GeneratedEntity)theEObject;
				T result = caseGeneratedEntity(generatedEntity);
				if (result == null) result = caseEntity(generatedEntity);
				if (result == null) result = caseNamedElement(generatedEntity);
				if (result == null) result = caseElement(generatedEntity);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Data_meta_modelPackage.ENTITY: {
				Entity entity = (Entity)theEObject;
				T result = caseEntity(entity);
				if (result == null) result = caseNamedElement(entity);
				if (result == null) result = caseElement(entity);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Data_meta_modelPackage.DERIVED_ENTITY: {
				DerivedEntity derivedEntity = (DerivedEntity)theEObject;
				T result = caseDerivedEntity(derivedEntity);
				if (result == null) result = caseEntity(derivedEntity);
				if (result == null) result = caseNamedElement(derivedEntity);
				if (result == null) result = caseElement(derivedEntity);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Data_meta_modelPackage.BASIC_ENTITY: {
				BasicEntity basicEntity = (BasicEntity)theEObject;
				T result = caseBasicEntity(basicEntity);
				if (result == null) result = caseEntity(basicEntity);
				if (result == null) result = caseNamedElement(basicEntity);
				if (result == null) result = caseElement(basicEntity);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			default: return defaultCase(theEObject);
		}
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
	public T caseModule(org.eclipse.efbt.cocalimo.core.model.module_management.Module object) {
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

} //Data_meta_modelSwitch
