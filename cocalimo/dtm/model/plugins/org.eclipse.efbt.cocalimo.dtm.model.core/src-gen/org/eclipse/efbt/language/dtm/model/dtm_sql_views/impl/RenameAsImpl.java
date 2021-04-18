/**
 */
package org.eclipse.efbt.language.dtm.model.dtm_sql_views.impl;

import org.eclipse.efbt.cocalimo.smcubes.model.core.VARIABLE;

import org.eclipse.efbt.cocalimo.smcubes_with_lineage.model.cubes.FreeBirdToolsCube;

import org.eclipse.efbt.language.dtm.model.dtm_sql_views.RenameAs;
import org.eclipse.efbt.language.dtm.model.dtm_sql_views.dtm_sql_viewsPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Rename As</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.impl.RenameAsImpl#getFromCube <em>From Cube</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.impl.RenameAsImpl#getFromVariable <em>From Variable</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.dtm.model.dtm_sql_views.impl.RenameAsImpl#getToVariable <em>To Variable</em>}</li>
 * </ul>
 *
 * @generated
 */
public class RenameAsImpl extends MinimalEObjectImpl.Container implements RenameAs
{
	/**
	 * The cached value of the '{@link #getFromCube() <em>From Cube</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getFromCube()
	 * @generated
	 * @ordered
	 */
	protected FreeBirdToolsCube fromCube;

	/**
	 * The cached value of the '{@link #getFromVariable() <em>From Variable</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getFromVariable()
	 * @generated
	 * @ordered
	 */
	protected VARIABLE fromVariable;

	/**
	 * The cached value of the '{@link #getToVariable() <em>To Variable</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getToVariable()
	 * @generated
	 * @ordered
	 */
	protected VARIABLE toVariable;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected RenameAsImpl()
	{
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass()
	{
		return dtm_sql_viewsPackage.Literals.RENAME_AS;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public FreeBirdToolsCube getFromCube()
	{
		if (fromCube != null && fromCube.eIsProxy()) {
			InternalEObject oldFromCube = (InternalEObject)fromCube;
			fromCube = (FreeBirdToolsCube)eResolveProxy(oldFromCube);
			if (fromCube != oldFromCube) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, dtm_sql_viewsPackage.RENAME_AS__FROM_CUBE, oldFromCube, fromCube));
			}
		}
		return fromCube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public FreeBirdToolsCube basicGetFromCube()
	{
		return fromCube;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setFromCube(FreeBirdToolsCube newFromCube)
	{
		FreeBirdToolsCube oldFromCube = fromCube;
		fromCube = newFromCube;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, dtm_sql_viewsPackage.RENAME_AS__FROM_CUBE, oldFromCube, fromCube));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public VARIABLE getFromVariable()
	{
		if (fromVariable != null && fromVariable.eIsProxy()) {
			InternalEObject oldFromVariable = (InternalEObject)fromVariable;
			fromVariable = (VARIABLE)eResolveProxy(oldFromVariable);
			if (fromVariable != oldFromVariable) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, dtm_sql_viewsPackage.RENAME_AS__FROM_VARIABLE, oldFromVariable, fromVariable));
			}
		}
		return fromVariable;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VARIABLE basicGetFromVariable()
	{
		return fromVariable;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setFromVariable(VARIABLE newFromVariable)
	{
		VARIABLE oldFromVariable = fromVariable;
		fromVariable = newFromVariable;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, dtm_sql_viewsPackage.RENAME_AS__FROM_VARIABLE, oldFromVariable, fromVariable));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public VARIABLE getToVariable()
	{
		if (toVariable != null && toVariable.eIsProxy()) {
			InternalEObject oldToVariable = (InternalEObject)toVariable;
			toVariable = (VARIABLE)eResolveProxy(oldToVariable);
			if (toVariable != oldToVariable) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, dtm_sql_viewsPackage.RENAME_AS__TO_VARIABLE, oldToVariable, toVariable));
			}
		}
		return toVariable;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public VARIABLE basicGetToVariable()
	{
		return toVariable;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setToVariable(VARIABLE newToVariable)
	{
		VARIABLE oldToVariable = toVariable;
		toVariable = newToVariable;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, dtm_sql_viewsPackage.RENAME_AS__TO_VARIABLE, oldToVariable, toVariable));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType)
	{
		switch (featureID) {
			case dtm_sql_viewsPackage.RENAME_AS__FROM_CUBE:
				if (resolve) return getFromCube();
				return basicGetFromCube();
			case dtm_sql_viewsPackage.RENAME_AS__FROM_VARIABLE:
				if (resolve) return getFromVariable();
				return basicGetFromVariable();
			case dtm_sql_viewsPackage.RENAME_AS__TO_VARIABLE:
				if (resolve) return getToVariable();
				return basicGetToVariable();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eSet(int featureID, Object newValue)
	{
		switch (featureID) {
			case dtm_sql_viewsPackage.RENAME_AS__FROM_CUBE:
				setFromCube((FreeBirdToolsCube)newValue);
				return;
			case dtm_sql_viewsPackage.RENAME_AS__FROM_VARIABLE:
				setFromVariable((VARIABLE)newValue);
				return;
			case dtm_sql_viewsPackage.RENAME_AS__TO_VARIABLE:
				setToVariable((VARIABLE)newValue);
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
	public void eUnset(int featureID)
	{
		switch (featureID) {
			case dtm_sql_viewsPackage.RENAME_AS__FROM_CUBE:
				setFromCube((FreeBirdToolsCube)null);
				return;
			case dtm_sql_viewsPackage.RENAME_AS__FROM_VARIABLE:
				setFromVariable((VARIABLE)null);
				return;
			case dtm_sql_viewsPackage.RENAME_AS__TO_VARIABLE:
				setToVariable((VARIABLE)null);
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
	public boolean eIsSet(int featureID)
	{
		switch (featureID) {
			case dtm_sql_viewsPackage.RENAME_AS__FROM_CUBE:
				return fromCube != null;
			case dtm_sql_viewsPackage.RENAME_AS__FROM_VARIABLE:
				return fromVariable != null;
			case dtm_sql_viewsPackage.RENAME_AS__TO_VARIABLE:
				return toVariable != null;
		}
		return super.eIsSet(featureID);
	}

} //RenameAsImpl
