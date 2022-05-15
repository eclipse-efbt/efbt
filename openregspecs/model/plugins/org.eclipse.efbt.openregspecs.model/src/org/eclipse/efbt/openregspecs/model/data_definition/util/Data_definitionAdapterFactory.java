/**
 */
package org.eclipse.efbt.openregspecs.model.data_definition.util;

import org.eclipse.efbt.openregspecs.model.data_definition.*;

import org.eclipse.emf.common.notify.Adapter;
import org.eclipse.emf.common.notify.Notifier;

import org.eclipse.emf.common.notify.impl.AdapterFactoryImpl;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * The <b>Adapter Factory</b> for the model.
 * It provides an adapter <code>createXXX</code> method for each class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.openregspecs.model.data_definition.Data_definitionPackage
 * @generated
 */
public class Data_definitionAdapterFactory extends AdapterFactoryImpl {
	/**
	 * The cached model package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static Data_definitionPackage modelPackage;

	/**
	 * Creates an instance of the adapter factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Data_definitionAdapterFactory() {
		if (modelPackage == null) {
			modelPackage = Data_definitionPackage.eINSTANCE;
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
	protected Data_definitionSwitch<Adapter> modelSwitch =
		new Data_definitionSwitch<Adapter>() {
			@Override
			public Adapter caseCOMBINATION(COMBINATION object) {
				return createCOMBINATIONAdapter();
			}
			@Override
			public Adapter caseCOMBINATION_ITEM(COMBINATION_ITEM object) {
				return createCOMBINATION_ITEMAdapter();
			}
			@Override
			public Adapter caseCUBE(CUBE object) {
				return createCUBEAdapter();
			}
			@Override
			public Adapter caseCUBE_GROUP(CUBE_GROUP object) {
				return createCUBE_GROUPAdapter();
			}
			@Override
			public Adapter caseCUBE_GROUP_ENUMERATION(CUBE_GROUP_ENUMERATION object) {
				return createCUBE_GROUP_ENUMERATIONAdapter();
			}
			@Override
			public Adapter caseCUBE_HIERARCHY(CUBE_HIERARCHY object) {
				return createCUBE_HIERARCHYAdapter();
			}
			@Override
			public Adapter caseCUBE_HIERARCHY_NODE(CUBE_HIERARCHY_NODE object) {
				return createCUBE_HIERARCHY_NODEAdapter();
			}
			@Override
			public Adapter caseCUBE_RELATIONSHIP(CUBE_RELATIONSHIP object) {
				return createCUBE_RELATIONSHIPAdapter();
			}
			@Override
			public Adapter caseCUBE_STRUCTURE(CUBE_STRUCTURE object) {
				return createCUBE_STRUCTUREAdapter();
			}
			@Override
			public Adapter caseCUBE_STRUCTURE_ITEM(CUBE_STRUCTURE_ITEM object) {
				return createCUBE_STRUCTURE_ITEMAdapter();
			}
			@Override
			public Adapter caseCUBE_TO_COMBINATION(CUBE_TO_COMBINATION object) {
				return createCUBE_TO_COMBINATIONAdapter();
			}
			@Override
			public Adapter caseFRAMEWORK(FRAMEWORK object) {
				return createFRAMEWORKAdapter();
			}
			@Override
			public Adapter caseFRAMEWORK_SUBDOMAIN(FRAMEWORK_SUBDOMAIN object) {
				return createFRAMEWORK_SUBDOMAINAdapter();
			}
			@Override
			public Adapter caseFRAMEWORK_VARIABLE_SET(FRAMEWORK_VARIABLE_SET object) {
				return createFRAMEWORK_VARIABLE_SETAdapter();
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
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.data_definition.COMBINATION <em>COMBINATION</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.data_definition.COMBINATION
	 * @generated
	 */
	public Adapter createCOMBINATIONAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.data_definition.COMBINATION_ITEM <em>COMBINATION ITEM</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.data_definition.COMBINATION_ITEM
	 * @generated
	 */
	public Adapter createCOMBINATION_ITEMAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.data_definition.CUBE <em>CUBE</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.data_definition.CUBE
	 * @generated
	 */
	public Adapter createCUBEAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.data_definition.CUBE_GROUP <em>CUBE GROUP</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.data_definition.CUBE_GROUP
	 * @generated
	 */
	public Adapter createCUBE_GROUPAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.data_definition.CUBE_GROUP_ENUMERATION <em>CUBE GROUP ENUMERATION</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.data_definition.CUBE_GROUP_ENUMERATION
	 * @generated
	 */
	public Adapter createCUBE_GROUP_ENUMERATIONAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.data_definition.CUBE_HIERARCHY <em>CUBE HIERARCHY</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.data_definition.CUBE_HIERARCHY
	 * @generated
	 */
	public Adapter createCUBE_HIERARCHYAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.data_definition.CUBE_HIERARCHY_NODE <em>CUBE HIERARCHY NODE</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.data_definition.CUBE_HIERARCHY_NODE
	 * @generated
	 */
	public Adapter createCUBE_HIERARCHY_NODEAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.data_definition.CUBE_RELATIONSHIP <em>CUBE RELATIONSHIP</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.data_definition.CUBE_RELATIONSHIP
	 * @generated
	 */
	public Adapter createCUBE_RELATIONSHIPAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.data_definition.CUBE_STRUCTURE <em>CUBE STRUCTURE</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.data_definition.CUBE_STRUCTURE
	 * @generated
	 */
	public Adapter createCUBE_STRUCTUREAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.data_definition.CUBE_STRUCTURE_ITEM <em>CUBE STRUCTURE ITEM</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.data_definition.CUBE_STRUCTURE_ITEM
	 * @generated
	 */
	public Adapter createCUBE_STRUCTURE_ITEMAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.data_definition.CUBE_TO_COMBINATION <em>CUBE TO COMBINATION</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.data_definition.CUBE_TO_COMBINATION
	 * @generated
	 */
	public Adapter createCUBE_TO_COMBINATIONAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.data_definition.FRAMEWORK <em>FRAMEWORK</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.data_definition.FRAMEWORK
	 * @generated
	 */
	public Adapter createFRAMEWORKAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.data_definition.FRAMEWORK_SUBDOMAIN <em>FRAMEWORK SUBDOMAIN</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.data_definition.FRAMEWORK_SUBDOMAIN
	 * @generated
	 */
	public Adapter createFRAMEWORK_SUBDOMAINAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.data_definition.FRAMEWORK_VARIABLE_SET <em>FRAMEWORK VARIABLE SET</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.data_definition.FRAMEWORK_VARIABLE_SET
	 * @generated
	 */
	public Adapter createFRAMEWORK_VARIABLE_SETAdapter() {
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

} //Data_definitionAdapterFactory
