/**
 */
package org.eclipse.efbt.openregspecs.model.rendering.impl;

import org.eclipse.efbt.openregspecs.model.rendering.*;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.impl.EFactoryImpl;

import org.eclipse.emf.ecore.plugin.EcorePlugin;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Factory</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class RenderingFactoryImpl extends EFactoryImpl implements RenderingFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static RenderingFactory init() {
		try {
			RenderingFactory theRenderingFactory = (RenderingFactory)EPackage.Registry.INSTANCE.getEFactory(RenderingPackage.eNS_URI);
			if (theRenderingFactory != null) {
				return theRenderingFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new RenderingFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public RenderingFactoryImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EObject create(EClass eClass) {
		switch (eClass.getClassifierID()) {
			case RenderingPackage.AXIS: return createAXIS();
			case RenderingPackage.AXIS_ORDINATE: return createAXIS_ORDINATE();
			case RenderingPackage.CELL_POSITION: return createCELL_POSITION();
			case RenderingPackage.ORDINATE_ITEM: return createORDINATE_ITEM();
			case RenderingPackage.TABLE: return createTABLE();
			case RenderingPackage.TABLE_CELL: return createTABLE_CELL();
			default:
				throw new IllegalArgumentException("The class '" + eClass.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AXIS createAXIS() {
		AXISImpl axis = new AXISImpl();
		return axis;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public AXIS_ORDINATE createAXIS_ORDINATE() {
		AXIS_ORDINATEImpl axiS_ORDINATE = new AXIS_ORDINATEImpl();
		return axiS_ORDINATE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CELL_POSITION createCELL_POSITION() {
		CELL_POSITIONImpl celL_POSITION = new CELL_POSITIONImpl();
		return celL_POSITION;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ORDINATE_ITEM createORDINATE_ITEM() {
		ORDINATE_ITEMImpl ordinatE_ITEM = new ORDINATE_ITEMImpl();
		return ordinatE_ITEM;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TABLE createTABLE() {
		TABLEImpl table = new TABLEImpl();
		return table;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public TABLE_CELL createTABLE_CELL() {
		TABLE_CELLImpl tablE_CELL = new TABLE_CELLImpl();
		return tablE_CELL;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public RenderingPackage getRenderingPackage() {
		return (RenderingPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static RenderingPackage getPackage() {
		return RenderingPackage.eINSTANCE;
	}

} //RenderingFactoryImpl
