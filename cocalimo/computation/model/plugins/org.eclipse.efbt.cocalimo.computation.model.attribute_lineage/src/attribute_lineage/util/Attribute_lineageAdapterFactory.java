/**
 */
package attribute_lineage.util;

import attribute_lineage.*;

import org.eclipse.emf.common.notify.Adapter;
import org.eclipse.emf.common.notify.Notifier;

import org.eclipse.emf.common.notify.impl.AdapterFactoryImpl;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * The <b>Adapter Factory</b> for the model.
 * It provides an adapter <code>createXXX</code> method for each class of the model.
 * <!-- end-user-doc -->
 * @see attribute_lineage.Attribute_lineagePackage
 * @generated
 */
public class Attribute_lineageAdapterFactory extends AdapterFactoryImpl {
	/**
	 * The cached model package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static Attribute_lineagePackage modelPackage;

	/**
	 * Creates an instance of the adapter factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Attribute_lineageAdapterFactory() {
		if (modelPackage == null) {
			modelPackage = Attribute_lineagePackage.eINSTANCE;
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
	protected Attribute_lineageSwitch<Adapter> modelSwitch =
		new Attribute_lineageSwitch<Adapter>() {
			@Override
			public Adapter caseAttributeLineageModel(AttributeLineageModel object) {
				return createAttributeLineageModelAdapter();
			}
			@Override
			public Adapter caseAttributeLineageModelModule(AttributeLineageModelModule object) {
				return createAttributeLineageModelModuleAdapter();
			}
			@Override
			public Adapter caseIsNotSubset(IsNotSubset object) {
				return createIsNotSubsetAdapter();
			}
			@Override
			public Adapter caseIsSubset(IsSubset object) {
				return createIsSubsetAdapter();
			}
			@Override
			public Adapter caseSubsetBoolean(SubsetBoolean object) {
				return createSubsetBooleanAdapter();
			}
			@Override
			public Adapter caseModule(org.eclipse.efbt.cocalimo.core.model.module_management.Module object) {
				return createModuleAdapter();
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
	 * Creates a new adapter for an object of class '{@link attribute_lineage.AttributeLineageModel <em>Attribute Lineage Model</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see attribute_lineage.AttributeLineageModel
	 * @generated
	 */
	public Adapter createAttributeLineageModelAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link attribute_lineage.AttributeLineageModelModule <em>Attribute Lineage Model Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see attribute_lineage.AttributeLineageModelModule
	 * @generated
	 */
	public Adapter createAttributeLineageModelModuleAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link attribute_lineage.IsNotSubset <em>Is Not Subset</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see attribute_lineage.IsNotSubset
	 * @generated
	 */
	public Adapter createIsNotSubsetAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link attribute_lineage.IsSubset <em>Is Subset</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see attribute_lineage.IsSubset
	 * @generated
	 */
	public Adapter createIsSubsetAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link attribute_lineage.SubsetBoolean <em>Subset Boolean</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see attribute_lineage.SubsetBoolean
	 * @generated
	 */
	public Adapter createSubsetBooleanAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.cocalimo.core.model.module_management.Module <em>Module</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.cocalimo.core.model.module_management.Module
	 * @generated
	 */
	public Adapter createModuleAdapter() {
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

} //Attribute_lineageAdapterFactory
