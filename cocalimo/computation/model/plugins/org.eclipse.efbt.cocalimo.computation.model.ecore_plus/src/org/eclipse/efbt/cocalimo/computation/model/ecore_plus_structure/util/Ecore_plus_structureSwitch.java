/**
 */
package org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.util;

import org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.*;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EClassifier;
import org.eclipse.emf.ecore.EDataType;
import org.eclipse.emf.ecore.EEnum;
import org.eclipse.emf.ecore.EEnumLiteral;
import org.eclipse.emf.ecore.EModelElement;
import org.eclipse.emf.ecore.ENamedElement;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.ecore.ETypedElement;

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
 * @see org.eclipse.efbt.cocalimo.computation.model.ecore_plus_structure.Ecore_plus_structurePackage
 * @generated
 */
public class Ecore_plus_structureSwitch<T> extends Switch<T> {
	/**
	 * The cached model package
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static Ecore_plus_structurePackage modelPackage;

	/**
	 * Creates an instance of the switch.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Ecore_plus_structureSwitch() {
		if (modelPackage == null) {
			modelPackage = Ecore_plus_structurePackage.eINSTANCE;
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
			case Ecore_plus_structurePackage.BASE_ENTITY: {
				BaseEntity baseEntity = (BaseEntity)theEObject;
				T result = caseBaseEntity(baseEntity);
				if (result == null) result = caseEntity(baseEntity);
				if (result == null) result = caseEClass(baseEntity);
				if (result == null) result = caseEClassifier(baseEntity);
				if (result == null) result = caseENamedElement(baseEntity);
				if (result == null) result = caseEModelElement(baseEntity);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_structurePackage.CELL: {
				Cell cell = (Cell)theEObject;
				T result = caseCell(cell);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_structurePackage.HEIRARCHICAL_ENUM: {
				HeirarchicalEnum heirarchicalEnum = (HeirarchicalEnum)theEObject;
				T result = caseHeirarchicalEnum(heirarchicalEnum);
				if (result == null) result = caseEEnum(heirarchicalEnum);
				if (result == null) result = caseEDataType(heirarchicalEnum);
				if (result == null) result = caseEClassifier(heirarchicalEnum);
				if (result == null) result = caseENamedElement(heirarchicalEnum);
				if (result == null) result = caseEModelElement(heirarchicalEnum);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_structurePackage.HEIRARCHICAL_ENUM_LITERAL: {
				HeirarchicalEnumLiteral heirarchicalEnumLiteral = (HeirarchicalEnumLiteral)theEObject;
				T result = caseHeirarchicalEnumLiteral(heirarchicalEnumLiteral);
				if (result == null) result = caseEEnumLiteral(heirarchicalEnumLiteral);
				if (result == null) result = caseENamedElement(heirarchicalEnumLiteral);
				if (result == null) result = caseEModelElement(heirarchicalEnumLiteral);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_structurePackage.ENUMERATED_DOMAIN: {
				EnumeratedDomain enumeratedDomain = (EnumeratedDomain)theEObject;
				T result = caseEnumeratedDomain(enumeratedDomain);
				if (result == null) result = caseHeirarchicalEnum(enumeratedDomain);
				if (result == null) result = caseEEnum(enumeratedDomain);
				if (result == null) result = caseEDataType(enumeratedDomain);
				if (result == null) result = caseEClassifier(enumeratedDomain);
				if (result == null) result = caseENamedElement(enumeratedDomain);
				if (result == null) result = caseEModelElement(enumeratedDomain);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_structurePackage.SUB_DOMAIN_ENUM: {
				SubDomainEnum subDomainEnum = (SubDomainEnum)theEObject;
				T result = caseSubDomainEnum(subDomainEnum);
				if (result == null) result = caseHeirarchicalEnum(subDomainEnum);
				if (result == null) result = caseEEnum(subDomainEnum);
				if (result == null) result = caseEDataType(subDomainEnum);
				if (result == null) result = caseEClassifier(subDomainEnum);
				if (result == null) result = caseENamedElement(subDomainEnum);
				if (result == null) result = caseEModelElement(subDomainEnum);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_structurePackage.CELLS_PARAMETER: {
				CellsParameter cellsParameter = (CellsParameter)theEObject;
				T result = caseCellsParameter(cellsParameter);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_structurePackage.ENTITY: {
				Entity entity = (Entity)theEObject;
				T result = caseEntity(entity);
				if (result == null) result = caseEClass(entity);
				if (result == null) result = caseEClassifier(entity);
				if (result == null) result = caseENamedElement(entity);
				if (result == null) result = caseEModelElement(entity);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_structurePackage.BASE_ENTITY_FEATURE: {
				BaseEntityFeature baseEntityFeature = (BaseEntityFeature)theEObject;
				T result = caseBaseEntityFeature(baseEntityFeature);
				if (result == null) result = caseEAttribute(baseEntityFeature);
				if (result == null) result = caseEStructuralFeature(baseEntityFeature);
				if (result == null) result = caseETypedElement(baseEntityFeature);
				if (result == null) result = caseENamedElement(baseEntityFeature);
				if (result == null) result = caseEModelElement(baseEntityFeature);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case Ecore_plus_structurePackage.BASE_ENTITY_MODULE: {
				BaseEntityModule baseEntityModule = (BaseEntityModule)theEObject;
				T result = caseBaseEntityModule(baseEntityModule);
				if (result == null) result = caseModule(baseEntityModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			default: return defaultCase(theEObject);
		}
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Base Entity</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Base Entity</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseBaseEntity(BaseEntity object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Cell</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Cell</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseCell(Cell object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Heirarchical Enum</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Heirarchical Enum</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseHeirarchicalEnum(HeirarchicalEnum object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Heirarchical Enum Literal</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Heirarchical Enum Literal</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseHeirarchicalEnumLiteral(HeirarchicalEnumLiteral object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Enumerated Domain</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Enumerated Domain</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseEnumeratedDomain(EnumeratedDomain object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Sub Domain Enum</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Sub Domain Enum</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseSubDomainEnum(SubDomainEnum object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Cells Parameter</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Cells Parameter</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseCellsParameter(CellsParameter object) {
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
	 * Returns the result of interpreting the object as an instance of '<em>Base Entity Feature</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Base Entity Feature</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseBaseEntityFeature(BaseEntityFeature object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Base Entity Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Base Entity Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseBaseEntityModule(BaseEntityModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EModel Element</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EModel Element</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseEModelElement(EModelElement object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>ENamed Element</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>ENamed Element</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseENamedElement(ENamedElement object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EClassifier</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EClassifier</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseEClassifier(EClassifier object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EClass</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EClass</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseEClass(EClass object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EData Type</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EData Type</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseEDataType(EDataType object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EEnum</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EEnum</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseEEnum(EEnum object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EEnum Literal</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EEnum Literal</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseEEnumLiteral(EEnumLiteral object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>ETyped Element</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>ETyped Element</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseETypedElement(ETypedElement object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EStructural Feature</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EStructural Feature</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseEStructuralFeature(EStructuralFeature object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EAttribute</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EAttribute</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseEAttribute(EAttribute object) {
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

} //Ecore_plus_structureSwitch
