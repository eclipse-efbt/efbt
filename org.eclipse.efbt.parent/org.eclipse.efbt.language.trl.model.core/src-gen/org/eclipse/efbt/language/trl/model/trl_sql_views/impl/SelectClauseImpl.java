/**
 */
package org.eclipse.efbt.language.trl.model.trl_sql_views.impl;

import java.util.Collection;

import org.eclipse.efbt.common.model.functions.AggregateFunction;
import org.eclipse.efbt.common.model.functions.BasicFunction;

import org.eclipse.efbt.data_structures.smcubes.model.core.VARIABLE;

import org.eclipse.efbt.language.trl.model.trl_sql_views.SelectClause;
import org.eclipse.efbt.language.trl.model.trl_sql_views.Trl_sql_viewsPackage;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.common.util.EList;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.MinimalEObjectImpl;

import org.eclipse.emf.ecore.util.EObjectResolvingEList;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Select Clause</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.SelectClauseImpl#getAggregateFunctions <em>Aggregate Functions</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.SelectClauseImpl#getBasicFunctions <em>Basic Functions</em>}</li>
 *   <li>{@link org.eclipse.efbt.language.trl.model.trl_sql_views.impl.SelectClauseImpl#getVaraibles <em>Varaibles</em>}</li>
 * </ul>
 *
 * @generated
 */
public class SelectClauseImpl extends MinimalEObjectImpl.Container implements SelectClause
{
	/**
	 * The cached value of the '{@link #getAggregateFunctions() <em>Aggregate Functions</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getAggregateFunctions()
	 * @generated
	 * @ordered
	 */
	protected EList<AggregateFunction> aggregateFunctions;

	/**
	 * The cached value of the '{@link #getBasicFunctions() <em>Basic Functions</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getBasicFunctions()
	 * @generated
	 * @ordered
	 */
	protected BasicFunction basicFunctions;

	/**
	 * The cached value of the '{@link #getVaraibles() <em>Varaibles</em>}' reference list.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getVaraibles()
	 * @generated
	 * @ordered
	 */
	protected EList<VARIABLE> varaibles;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected SelectClauseImpl()
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
		return Trl_sql_viewsPackage.Literals.SELECT_CLAUSE;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<AggregateFunction> getAggregateFunctions()
	{
		if (aggregateFunctions == null)
		{
			aggregateFunctions = new EObjectResolvingEList<AggregateFunction>(AggregateFunction.class, this, Trl_sql_viewsPackage.SELECT_CLAUSE__AGGREGATE_FUNCTIONS);
		}
		return aggregateFunctions;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public BasicFunction getBasicFunctions()
	{
		if (basicFunctions != null && basicFunctions.eIsProxy())
		{
			InternalEObject oldBasicFunctions = (InternalEObject)basicFunctions;
			basicFunctions = (BasicFunction)eResolveProxy(oldBasicFunctions);
			if (basicFunctions != oldBasicFunctions)
			{
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Trl_sql_viewsPackage.SELECT_CLAUSE__BASIC_FUNCTIONS, oldBasicFunctions, basicFunctions));
			}
		}
		return basicFunctions;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public BasicFunction basicGetBasicFunctions()
	{
		return basicFunctions;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void setBasicFunctions(BasicFunction newBasicFunctions)
	{
		BasicFunction oldBasicFunctions = basicFunctions;
		basicFunctions = newBasicFunctions;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Trl_sql_viewsPackage.SELECT_CLAUSE__BASIC_FUNCTIONS, oldBasicFunctions, basicFunctions));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EList<VARIABLE> getVaraibles()
	{
		if (varaibles == null)
		{
			varaibles = new EObjectResolvingEList<VARIABLE>(VARIABLE.class, this, Trl_sql_viewsPackage.SELECT_CLAUSE__VARAIBLES);
		}
		return varaibles;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType)
	{
		switch (featureID)
		{
			case Trl_sql_viewsPackage.SELECT_CLAUSE__AGGREGATE_FUNCTIONS:
				return getAggregateFunctions();
			case Trl_sql_viewsPackage.SELECT_CLAUSE__BASIC_FUNCTIONS:
				if (resolve) return getBasicFunctions();
				return basicGetBasicFunctions();
			case Trl_sql_viewsPackage.SELECT_CLAUSE__VARAIBLES:
				return getVaraibles();
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
	public void eSet(int featureID, Object newValue)
	{
		switch (featureID)
		{
			case Trl_sql_viewsPackage.SELECT_CLAUSE__AGGREGATE_FUNCTIONS:
				getAggregateFunctions().clear();
				getAggregateFunctions().addAll((Collection<? extends AggregateFunction>)newValue);
				return;
			case Trl_sql_viewsPackage.SELECT_CLAUSE__BASIC_FUNCTIONS:
				setBasicFunctions((BasicFunction)newValue);
				return;
			case Trl_sql_viewsPackage.SELECT_CLAUSE__VARAIBLES:
				getVaraibles().clear();
				getVaraibles().addAll((Collection<? extends VARIABLE>)newValue);
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
		switch (featureID)
		{
			case Trl_sql_viewsPackage.SELECT_CLAUSE__AGGREGATE_FUNCTIONS:
				getAggregateFunctions().clear();
				return;
			case Trl_sql_viewsPackage.SELECT_CLAUSE__BASIC_FUNCTIONS:
				setBasicFunctions((BasicFunction)null);
				return;
			case Trl_sql_viewsPackage.SELECT_CLAUSE__VARAIBLES:
				getVaraibles().clear();
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
		switch (featureID)
		{
			case Trl_sql_viewsPackage.SELECT_CLAUSE__AGGREGATE_FUNCTIONS:
				return aggregateFunctions != null && !aggregateFunctions.isEmpty();
			case Trl_sql_viewsPackage.SELECT_CLAUSE__BASIC_FUNCTIONS:
				return basicFunctions != null;
			case Trl_sql_viewsPackage.SELECT_CLAUSE__VARAIBLES:
				return varaibles != null && !varaibles.isEmpty();
		}
		return super.eIsSet(featureID);
	}

} //SelectClauseImpl
