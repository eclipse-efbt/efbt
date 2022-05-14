/**
 */
package org.eclipse.efbt.openregspecs.model.rendering.util;

import org.eclipse.efbt.openregspecs.model.rendering.*;

import org.eclipse.emf.common.notify.Adapter;
import org.eclipse.emf.common.notify.Notifier;

import org.eclipse.emf.common.notify.impl.AdapterFactoryImpl;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * The <b>Adapter Factory</b> for the model.
 * It provides an adapter <code>createXXX</code> method for each class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.openregspecs.model.rendering.RenderingPackage
 * @generated
 */
public class RenderingAdapterFactory extends AdapterFactoryImpl {
	/**
	 * The cached model package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static RenderingPackage modelPackage;

	/**
	 * Creates an instance of the adapter factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public RenderingAdapterFactory() {
		if (modelPackage == null) {
			modelPackage = RenderingPackage.eINSTANCE;
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
	protected RenderingSwitch<Adapter> modelSwitch =
		new RenderingSwitch<Adapter>() {
			@Override
			public Adapter caseAXIS(AXIS object) {
				return createAXISAdapter();
			}
			@Override
			public Adapter caseAXIS_ORDINATE(AXIS_ORDINATE object) {
				return createAXIS_ORDINATEAdapter();
			}
			@Override
			public Adapter caseCELL_POSITION(CELL_POSITION object) {
				return createCELL_POSITIONAdapter();
			}
			@Override
			public Adapter caseORDINATE_ITEM(ORDINATE_ITEM object) {
				return createORDINATE_ITEMAdapter();
			}
			@Override
			public Adapter caseTABLE(TABLE object) {
				return createTABLEAdapter();
			}
			@Override
			public Adapter caseTABLE_CELL(TABLE_CELL object) {
				return createTABLE_CELLAdapter();
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
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.rendering.AXIS <em>AXIS</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.rendering.AXIS
	 * @generated
	 */
	public Adapter createAXISAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.rendering.AXIS_ORDINATE <em>AXIS ORDINATE</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.rendering.AXIS_ORDINATE
	 * @generated
	 */
	public Adapter createAXIS_ORDINATEAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.rendering.CELL_POSITION <em>CELL POSITION</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.rendering.CELL_POSITION
	 * @generated
	 */
	public Adapter createCELL_POSITIONAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.rendering.ORDINATE_ITEM <em>ORDINATE ITEM</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.rendering.ORDINATE_ITEM
	 * @generated
	 */
	public Adapter createORDINATE_ITEMAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.rendering.TABLE <em>TABLE</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.rendering.TABLE
	 * @generated
	 */
	public Adapter createTABLEAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.rendering.TABLE_CELL <em>TABLE CELL</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.rendering.TABLE_CELL
	 * @generated
	 */
	public Adapter createTABLE_CELLAdapter() {
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

} //RenderingAdapterFactory
