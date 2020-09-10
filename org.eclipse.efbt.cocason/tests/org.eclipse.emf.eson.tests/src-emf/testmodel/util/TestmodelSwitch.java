/**
 */
package testmodel.util;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.util.Switch;

import testmodel.*;

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
 * @see testmodel.TestmodelPackage
 * @generated
 */
public class TestmodelSwitch<T> extends Switch<T> {
	/**
	 * The cached model package
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static TestmodelPackage modelPackage;

	/**
	 * Creates an instance of the switch.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TestmodelSwitch() {
		if (modelPackage == null) {
			modelPackage = TestmodelPackage.eINSTANCE;
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
			case TestmodelPackage.TEST_MODEL: {
				TestModel testModel = (TestModel)theEObject;
				T result = caseTestModel(testModel);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case TestmodelPackage.CHILD: {
				Child child = (Child)theEObject;
				T result = caseChild(child);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case TestmodelPackage.SINGLE_OPTIONAL: {
				SingleOptional singleOptional = (SingleOptional)theEObject;
				T result = caseSingleOptional(singleOptional);
				if (result == null) result = caseInterface(singleOptional);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case TestmodelPackage.SINGLE_REQUIRED: {
				SingleRequired singleRequired = (SingleRequired)theEObject;
				T result = caseSingleRequired(singleRequired);
				if (result == null) result = caseAbstract(singleRequired);
				if (result == null) result = caseSingleOptional(singleRequired);
				if (result == null) result = caseInterface(singleRequired);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case TestmodelPackage.INTERFACE: {
				Interface interface_ = (Interface)theEObject;
				T result = caseInterface(interface_);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case TestmodelPackage.ABSTRACT: {
				Abstract abstract_ = (Abstract)theEObject;
				T result = caseAbstract(abstract_);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case TestmodelPackage.NESTED_ELEMENTS: {
				NestedElements nestedElements = (NestedElements)theEObject;
				T result = caseNestedElements(nestedElements);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case TestmodelPackage.NESTED_ELEMENT: {
				NestedElement nestedElement = (NestedElement)theEObject;
				T result = caseNestedElement(nestedElement);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case TestmodelPackage.ATTRIBUTE_SAMPLE: {
				AttributeSample attributeSample = (AttributeSample)theEObject;
				T result = caseAttributeSample(attributeSample);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case TestmodelPackage.REFERENCE_TEST_CONTAINER: {
				ReferenceTestContainer referenceTestContainer = (ReferenceTestContainer)theEObject;
				T result = caseReferenceTestContainer(referenceTestContainer);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case TestmodelPackage.REFERENCE_TARGET: {
				ReferenceTarget referenceTarget = (ReferenceTarget)theEObject;
				T result = caseReferenceTarget(referenceTarget);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case TestmodelPackage.REFERENCE_TARGET_SUBCLASS: {
				ReferenceTargetSubclass referenceTargetSubclass = (ReferenceTargetSubclass)theEObject;
				T result = caseReferenceTargetSubclass(referenceTargetSubclass);
				if (result == null) result = caseReferenceTarget(referenceTargetSubclass);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case TestmodelPackage.ATTRIBUTE_TEST_CONTAINER: {
				AttributeTestContainer attributeTestContainer = (AttributeTestContainer)theEObject;
				T result = caseAttributeTestContainer(attributeTestContainer);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case TestmodelPackage.NAME_ATTRIBUTE_CONTAINER: {
				NameAttributeContainer nameAttributeContainer = (NameAttributeContainer)theEObject;
				T result = caseNameAttributeContainer(nameAttributeContainer);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case TestmodelPackage.DEFAULT_NAME: {
				DefaultName defaultName = (DefaultName)theEObject;
				T result = caseDefaultName(defaultName);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case TestmodelPackage.CUSTOM_NAME: {
				CustomName customName = (CustomName)theEObject;
				T result = caseCustomName(customName);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case TestmodelPackage.NO_NAME: {
				NoName noName = (NoName)theEObject;
				T result = caseNoName(noName);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			default: return defaultCase(theEObject);
		}
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Test Model</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Test Model</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseTestModel(TestModel object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Child</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Child</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseChild(Child object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Single Optional</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Single Optional</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseSingleOptional(SingleOptional object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Single Required</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Single Required</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseSingleRequired(SingleRequired object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Interface</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Interface</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseInterface(Interface object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Abstract</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Abstract</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseAbstract(Abstract object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Nested Elements</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Nested Elements</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseNestedElements(NestedElements object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Nested Element</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Nested Element</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseNestedElement(NestedElement object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Attribute Sample</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Attribute Sample</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseAttributeSample(AttributeSample object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Reference Test Container</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Reference Test Container</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseReferenceTestContainer(ReferenceTestContainer object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Reference Target</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Reference Target</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseReferenceTarget(ReferenceTarget object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Reference Target Subclass</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Reference Target Subclass</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseReferenceTargetSubclass(ReferenceTargetSubclass object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Attribute Test Container</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Attribute Test Container</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseAttributeTestContainer(AttributeTestContainer object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Name Attribute Container</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Name Attribute Container</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseNameAttributeContainer(NameAttributeContainer object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Default Name</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Default Name</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseDefaultName(DefaultName object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Custom Name</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Custom Name</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseCustomName(CustomName object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>No Name</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>No Name</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseNoName(NoName object) {
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

} //TestmodelSwitch
