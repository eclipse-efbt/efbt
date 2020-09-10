/**
 */
package testmodel.util;

import org.eclipse.emf.common.notify.Adapter;
import org.eclipse.emf.common.notify.Notifier;

import org.eclipse.emf.common.notify.impl.AdapterFactoryImpl;

import org.eclipse.emf.ecore.EObject;

import testmodel.*;

/**
 * <!-- begin-user-doc -->
 * The <b>Adapter Factory</b> for the model.
 * It provides an adapter <code>createXXX</code> method for each class of the model.
 * <!-- end-user-doc -->
 * @see testmodel.TestmodelPackage
 * @generated
 */
public class TestmodelAdapterFactory extends AdapterFactoryImpl {
	/**
	 * The cached model package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static TestmodelPackage modelPackage;

	/**
	 * Creates an instance of the adapter factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TestmodelAdapterFactory() {
		if (modelPackage == null) {
			modelPackage = TestmodelPackage.eINSTANCE;
		}
	}

	/**
	 * Returns whether this factory is applicable for the type of the object.
	 * <!-- begin-user-doc -->
	 * This implementation returns <code>true</code> if the object is either the model's package or is an instance object of the model.
	 * <!-- end-user-doc -->
	 * @return whether this factory is applicable for the type of the object.
	 * @generated
	 */
	@Override
	public boolean isFactoryForType(Object object) {
		if (object == modelPackage) {
			return true;
		}
		if (object instanceof EObject) {
			return ((EObject)object).eClass().getEPackage() == modelPackage;
		}
		return false;
	}

	/**
	 * The switch that delegates to the <code>createXXX</code> methods.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected TestmodelSwitch<Adapter> modelSwitch =
		new TestmodelSwitch<Adapter>() {
			@Override
			public Adapter caseTestModel(TestModel object) {
				return createTestModelAdapter();
			}
			@Override
			public Adapter caseChild(Child object) {
				return createChildAdapter();
			}
			@Override
			public Adapter caseSingleOptional(SingleOptional object) {
				return createSingleOptionalAdapter();
			}
			@Override
			public Adapter caseSingleRequired(SingleRequired object) {
				return createSingleRequiredAdapter();
			}
			@Override
			public Adapter caseInterface(Interface object) {
				return createInterfaceAdapter();
			}
			@Override
			public Adapter caseAbstract(Abstract object) {
				return createAbstractAdapter();
			}
			@Override
			public Adapter caseNestedElements(NestedElements object) {
				return createNestedElementsAdapter();
			}
			@Override
			public Adapter caseNestedElement(NestedElement object) {
				return createNestedElementAdapter();
			}
			@Override
			public Adapter caseAttributeSample(AttributeSample object) {
				return createAttributeSampleAdapter();
			}
			@Override
			public Adapter caseReferenceTestContainer(ReferenceTestContainer object) {
				return createReferenceTestContainerAdapter();
			}
			@Override
			public Adapter caseReferenceTarget(ReferenceTarget object) {
				return createReferenceTargetAdapter();
			}
			@Override
			public Adapter caseReferenceTargetSubclass(ReferenceTargetSubclass object) {
				return createReferenceTargetSubclassAdapter();
			}
			@Override
			public Adapter caseAttributeTestContainer(AttributeTestContainer object) {
				return createAttributeTestContainerAdapter();
			}
			@Override
			public Adapter caseNameAttributeContainer(NameAttributeContainer object) {
				return createNameAttributeContainerAdapter();
			}
			@Override
			public Adapter caseDefaultName(DefaultName object) {
				return createDefaultNameAdapter();
			}
			@Override
			public Adapter caseCustomName(CustomName object) {
				return createCustomNameAdapter();
			}
			@Override
			public Adapter caseNoName(NoName object) {
				return createNoNameAdapter();
			}
			@Override
			public Adapter defaultCase(EObject object) {
				return createEObjectAdapter();
			}
		};

	/**
	 * Creates an adapter for the <code>target</code>.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param target the object to adapt.
	 * @return the adapter for the <code>target</code>.
	 * @generated
	 */
	@Override
	public Adapter createAdapter(Notifier target) {
		return modelSwitch.doSwitch((EObject)target);
	}


	/**
	 * Creates a new adapter for an object of class '{@link testmodel.TestModel <em>Test Model</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see testmodel.TestModel
	 * @generated
	 */
	public Adapter createTestModelAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link testmodel.Child <em>Child</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see testmodel.Child
	 * @generated
	 */
	public Adapter createChildAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link testmodel.SingleOptional <em>Single Optional</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see testmodel.SingleOptional
	 * @generated
	 */
	public Adapter createSingleOptionalAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link testmodel.SingleRequired <em>Single Required</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see testmodel.SingleRequired
	 * @generated
	 */
	public Adapter createSingleRequiredAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link testmodel.Interface <em>Interface</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see testmodel.Interface
	 * @generated
	 */
	public Adapter createInterfaceAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link testmodel.Abstract <em>Abstract</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see testmodel.Abstract
	 * @generated
	 */
	public Adapter createAbstractAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link testmodel.NestedElements <em>Nested Elements</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see testmodel.NestedElements
	 * @generated
	 */
	public Adapter createNestedElementsAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link testmodel.NestedElement <em>Nested Element</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see testmodel.NestedElement
	 * @generated
	 */
	public Adapter createNestedElementAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link testmodel.AttributeSample <em>Attribute Sample</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see testmodel.AttributeSample
	 * @generated
	 */
	public Adapter createAttributeSampleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link testmodel.ReferenceTestContainer <em>Reference Test Container</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see testmodel.ReferenceTestContainer
	 * @generated
	 */
	public Adapter createReferenceTestContainerAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link testmodel.ReferenceTarget <em>Reference Target</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see testmodel.ReferenceTarget
	 * @generated
	 */
	public Adapter createReferenceTargetAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link testmodel.ReferenceTargetSubclass <em>Reference Target Subclass</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see testmodel.ReferenceTargetSubclass
	 * @generated
	 */
	public Adapter createReferenceTargetSubclassAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link testmodel.AttributeTestContainer <em>Attribute Test Container</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see testmodel.AttributeTestContainer
	 * @generated
	 */
	public Adapter createAttributeTestContainerAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link testmodel.NameAttributeContainer <em>Name Attribute Container</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see testmodel.NameAttributeContainer
	 * @generated
	 */
	public Adapter createNameAttributeContainerAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link testmodel.DefaultName <em>Default Name</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see testmodel.DefaultName
	 * @generated
	 */
	public Adapter createDefaultNameAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link testmodel.CustomName <em>Custom Name</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see testmodel.CustomName
	 * @generated
	 */
	public Adapter createCustomNameAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link testmodel.NoName <em>No Name</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see testmodel.NoName
	 * @generated
	 */
	public Adapter createNoNameAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for the default case.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @generated
	 */
	public Adapter createEObjectAdapter() {
		return null;
	}

} //TestmodelAdapterFactory
