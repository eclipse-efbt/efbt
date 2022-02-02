/**
 */
package entity_functions.impl;

import entity_functions.*;

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
public class Entity_functionsFactoryImpl extends EFactoryImpl implements Entity_functionsFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static Entity_functionsFactory init() {
		try {
			Entity_functionsFactory theEntity_functionsFactory = (Entity_functionsFactory)EPackage.Registry.INSTANCE.getEFactory(Entity_functionsPackage.eNS_URI);
			if (theEntity_functionsFactory != null) {
				return theEntity_functionsFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new Entity_functionsFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Entity_functionsFactoryImpl() {
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
			case Entity_functionsPackage.MEMBER_PARAMETER: return createMemberParameter();
			case Entity_functionsPackage.VALUE_PARAMETER: return createValueParameter();
			default:
				throw new IllegalArgumentException("The class '" + eClass.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public MemberParameter createMemberParameter() {
		MemberParameterImpl memberParameter = new MemberParameterImpl();
		return memberParameter;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public ValueParameter createValueParameter() {
		ValueParameterImpl valueParameter = new ValueParameterImpl();
		return valueParameter;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Entity_functionsPackage getEntity_functionsPackage() {
		return (Entity_functionsPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static Entity_functionsPackage getPackage() {
		return Entity_functionsPackage.eINSTANCE;
	}

} //Entity_functionsFactoryImpl
