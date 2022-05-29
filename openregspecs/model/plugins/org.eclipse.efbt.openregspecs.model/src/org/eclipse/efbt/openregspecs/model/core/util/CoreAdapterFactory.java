/**
 */
package org.eclipse.efbt.openregspecs.model.core.util;

import org.eclipse.efbt.openregspecs.model.core.*;
import org.eclipse.efbt.openregspecs.model.data_meta_model.Concept;
import org.eclipse.efbt.openregspecs.model.data_meta_model.Element;
import org.eclipse.efbt.openregspecs.model.data_meta_model.EnumMember;
import org.eclipse.efbt.openregspecs.model.data_meta_model.NamedElement;

import org.eclipse.efbt.openregspecs.model.data_meta_model.Type;
import org.eclipse.emf.common.notify.Adapter;
import org.eclipse.emf.common.notify.Notifier;

import org.eclipse.emf.common.notify.impl.AdapterFactoryImpl;

import org.eclipse.emf.ecore.EObject;

/**
 * <!-- begin-user-doc -->
 * The <b>Adapter Factory</b> for the model.
 * It provides an adapter <code>createXXX</code> method for each class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.openregspecs.model.core.CorePackage
 * @generated
 */
public class CoreAdapterFactory extends AdapterFactoryImpl {
	/**
	 * The cached model package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static CorePackage modelPackage;

	/**
	 * Creates an instance of the adapter factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public CoreAdapterFactory() {
		if (modelPackage == null) {
			modelPackage = CorePackage.eINSTANCE;
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
	protected CoreSwitch<Adapter> modelSwitch =
		new CoreSwitch<Adapter>() {
			@Override
			public Adapter caseDOMAIN(DOMAIN object) {
				return createDOMAINAdapter();
			}
			@Override
			public Adapter caseFACET_COLLECTION(FACET_COLLECTION object) {
				return createFACET_COLLECTIONAdapter();
			}
			@Override
			public Adapter caseFACET_ENUMERATION(FACET_ENUMERATION object) {
				return createFACET_ENUMERATIONAdapter();
			}
			@Override
			public Adapter casefacet_type(facet_type object) {
				return createfacet_typeAdapter();
			}
			@Override
			public Adapter caseMAINTENANCE_AGENCY(MAINTENANCE_AGENCY object) {
				return createMAINTENANCE_AGENCYAdapter();
			}
			@Override
			public Adapter caseMEMBER(MEMBER object) {
				return createMEMBERAdapter();
			}
			@Override
			public Adapter caseMEMBER_HIERARCHY(MEMBER_HIERARCHY object) {
				return createMEMBER_HIERARCHYAdapter();
			}
			@Override
			public Adapter caseMEMBER_HIERARCHY_NODE(MEMBER_HIERARCHY_NODE object) {
				return createMEMBER_HIERARCHY_NODEAdapter();
			}
			@Override
			public Adapter caseSUBDOMAIN(SUBDOMAIN object) {
				return createSUBDOMAINAdapter();
			}
			@Override
			public Adapter caseSUBDOMAIN_ENUMERATION(SUBDOMAIN_ENUMERATION object) {
				return createSUBDOMAIN_ENUMERATIONAdapter();
			}
			@Override
			public Adapter caseVARIABLE(VARIABLE object) {
				return createVARIABLEAdapter();
			}
			@Override
			public Adapter caseVARIABLE_SET(VARIABLE_SET object) {
				return createVARIABLE_SETAdapter();
			}
			@Override
			public Adapter caseVARIABLE_SET_ENUMERATION(VARIABLE_SET_ENUMERATION object) {
				return createVARIABLE_SET_ENUMERATIONAdapter();
			}
			@Override
			public Adapter caseEnumMember(EnumMember object) {
				return createEnumMemberAdapter();
			}
			@Override
			public Adapter caseElement(Element object) {
				return createElementAdapter();
			}
			@Override
			public Adapter caseNamedElement(NamedElement object) {
				return createNamedElementAdapter();
			}
			@Override
			public Adapter caseType(Type object) {
				return createTypeAdapter();
			}
			@Override
			public Adapter caseConcept(Concept object) {
				return createConceptAdapter();
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
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.core.DOMAIN <em>DOMAIN</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.core.DOMAIN
	 * @generated
	 */
	public Adapter createDOMAINAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.core.FACET_COLLECTION <em>FACET COLLECTION</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.core.FACET_COLLECTION
	 * @generated
	 */
	public Adapter createFACET_COLLECTIONAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.core.FACET_ENUMERATION <em>FACET ENUMERATION</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.core.FACET_ENUMERATION
	 * @generated
	 */
	public Adapter createFACET_ENUMERATIONAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.core.facet_type <em>facet type</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.core.facet_type
	 * @generated
	 */
	public Adapter createfacet_typeAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.core.MAINTENANCE_AGENCY <em>MAINTENANCE AGENCY</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.core.MAINTENANCE_AGENCY
	 * @generated
	 */
	public Adapter createMAINTENANCE_AGENCYAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.core.MEMBER <em>MEMBER</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.core.MEMBER
	 * @generated
	 */
	public Adapter createMEMBERAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.core.MEMBER_HIERARCHY <em>MEMBER HIERARCHY</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.core.MEMBER_HIERARCHY
	 * @generated
	 */
	public Adapter createMEMBER_HIERARCHYAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.core.MEMBER_HIERARCHY_NODE <em>MEMBER HIERARCHY NODE</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.core.MEMBER_HIERARCHY_NODE
	 * @generated
	 */
	public Adapter createMEMBER_HIERARCHY_NODEAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.core.SUBDOMAIN <em>SUBDOMAIN</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.core.SUBDOMAIN
	 * @generated
	 */
	public Adapter createSUBDOMAINAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.core.SUBDOMAIN_ENUMERATION <em>SUBDOMAIN ENUMERATION</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.core.SUBDOMAIN_ENUMERATION
	 * @generated
	 */
	public Adapter createSUBDOMAIN_ENUMERATIONAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.core.VARIABLE <em>VARIABLE</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.core.VARIABLE
	 * @generated
	 */
	public Adapter createVARIABLEAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.core.VARIABLE_SET <em>VARIABLE SET</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.core.VARIABLE_SET
	 * @generated
	 */
	public Adapter createVARIABLE_SETAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.core.VARIABLE_SET_ENUMERATION <em>VARIABLE SET ENUMERATION</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.core.VARIABLE_SET_ENUMERATION
	 * @generated
	 */
	public Adapter createVARIABLE_SET_ENUMERATIONAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.EnumMember <em>Enum Member</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.EnumMember
	 * @generated
	 */
	public Adapter createEnumMemberAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.Element <em>Element</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.Element
	 * @generated
	 */
	public Adapter createElementAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.NamedElement <em>Named Element</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.NamedElement
	 * @generated
	 */
	public Adapter createNamedElementAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.Type <em>Type</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.Type
	 * @generated
	 */
	public Adapter createTypeAdapter() {
		return null;
	}

	/**
	 * Creates a new adapter for an object of class '{@link org.eclipse.efbt.openregspecs.model.data_meta_model.Concept <em>Concept</em>}'.
	 * <!-- begin-user-doc -->
	 * This default implementation returns null so that we can easily ignore cases;
	 * it's useful to ignore a case when inheritance will catch all the cases anyway.
	 * <!-- end-user-doc -->
	 * @return the new adapter.
	 * @see org.eclipse.efbt.openregspecs.model.data_meta_model.Concept
	 * @generated
	 */
	public Adapter createConceptAdapter() {
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

} //CoreAdapterFactory
