/**
 */
package testmodel.impl;

import java.util.Collection;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;

import org.eclipse.emf.ecore.impl.EObjectImpl;

import org.eclipse.emf.ecore.util.EObjectResolvingEList;

import testmodel.Child;
import testmodel.SingleRequired;
import testmodel.TestmodelPackage;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Child</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link testmodel.impl.ChildImpl#getReferenceList <em>Reference List</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ChildImpl extends EObjectImpl implements Child {
	/**
	 * The cached value of the '{@link #getReferenceList() <em>Reference List</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getReferenceList()
	 * @generated
	 * @ordered
	 */
	protected EList<SingleRequired> referenceList;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ChildImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return TestmodelPackage.Literals.CHILD;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EList<SingleRequired> getReferenceList() {
		if (referenceList == null) {
			referenceList = new EObjectResolvingEList<SingleRequired>(SingleRequired.class, this, TestmodelPackage.CHILD__REFERENCE_LIST);
		}
		return referenceList;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case TestmodelPackage.CHILD__REFERENCE_LIST:
				return getReferenceList();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void eSet(int featureID, Object newValue) {
		switch (featureID) {
			case TestmodelPackage.CHILD__REFERENCE_LIST:
				getReferenceList().clear();
				getReferenceList().addAll((Collection<? extends SingleRequired>)newValue);
				return;
		}
		super.eSet(featureID, newValue);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eUnset(int featureID) {
		switch (featureID) {
			case TestmodelPackage.CHILD__REFERENCE_LIST:
				getReferenceList().clear();
				return;
		}
		super.eUnset(featureID);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public boolean eIsSet(int featureID) {
		switch (featureID) {
			case TestmodelPackage.CHILD__REFERENCE_LIST:
				return referenceList != null && !referenceList.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //ChildImpl
