/**
 */
package advanced_variable_lineagefunctions.impl;

import advanced_variable_lineagefunctions.Advanced_variable_lineagefunctionsPackage;
import advanced_variable_lineagefunctions.ResolvedStructColumnParameter;

import org.eclipse.emf.common.notify.Notification;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EStructuralFeature;
import org.eclipse.emf.ecore.InternalEObject;

import org.eclipse.emf.ecore.impl.ENotificationImpl;
import org.eclipse.emf.ecore.impl.EParameterImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model object '<em><b>Resolved Struct Column Parameter</b></em>'.
 * <!-- end-user-doc -->
 * <p>
 * The following features are implemented:
 * </p>
 * <ul>
 *   <li>{@link advanced_variable_lineagefunctions.impl.ResolvedStructColumnParameterImpl#getColumnInsideStruct <em>Column Inside Struct</em>}</li>
 *   <li>{@link advanced_variable_lineagefunctions.impl.ResolvedStructColumnParameterImpl#getCubeColumn <em>Cube Column</em>}</li>
 * </ul>
 *
 * @generated
 */
public class ResolvedStructColumnParameterImpl extends EParameterImpl implements ResolvedStructColumnParameter {
	/**
	 * The cached value of the '{@link #getColumnInsideStruct() <em>Column Inside Struct</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getColumnInsideStruct()
	 * @generated
	 * @ordered
	 */
	protected EAttribute columnInsideStruct;

	/**
	 * The cached value of the '{@link #getCubeColumn() <em>Cube Column</em>}' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #getCubeColumn()
	 * @generated
	 * @ordered
	 */
	protected EStructuralFeature cubeColumn;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected ResolvedStructColumnParameterImpl() {
		super();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected EClass eStaticClass() {
		return Advanced_variable_lineagefunctionsPackage.Literals.RESOLVED_STRUCT_COLUMN_PARAMETER;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getColumnInsideStruct() {
		if (columnInsideStruct != null && columnInsideStruct.eIsProxy()) {
			InternalEObject oldColumnInsideStruct = (InternalEObject)columnInsideStruct;
			columnInsideStruct = (EAttribute)eResolveProxy(oldColumnInsideStruct);
			if (columnInsideStruct != oldColumnInsideStruct) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Advanced_variable_lineagefunctionsPackage.RESOLVED_STRUCT_COLUMN_PARAMETER__COLUMN_INSIDE_STRUCT, oldColumnInsideStruct, columnInsideStruct));
			}
		}
		return columnInsideStruct;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute basicGetColumnInsideStruct() {
		return columnInsideStruct;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setColumnInsideStruct(EAttribute newColumnInsideStruct) {
		EAttribute oldColumnInsideStruct = columnInsideStruct;
		columnInsideStruct = newColumnInsideStruct;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Advanced_variable_lineagefunctionsPackage.RESOLVED_STRUCT_COLUMN_PARAMETER__COLUMN_INSIDE_STRUCT, oldColumnInsideStruct, columnInsideStruct));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EStructuralFeature getCubeColumn() {
		if (cubeColumn != null && cubeColumn.eIsProxy()) {
			InternalEObject oldCubeColumn = (InternalEObject)cubeColumn;
			cubeColumn = (EStructuralFeature)eResolveProxy(oldCubeColumn);
			if (cubeColumn != oldCubeColumn) {
				if (eNotificationRequired())
					eNotify(new ENotificationImpl(this, Notification.RESOLVE, Advanced_variable_lineagefunctionsPackage.RESOLVED_STRUCT_COLUMN_PARAMETER__CUBE_COLUMN, oldCubeColumn, cubeColumn));
			}
		}
		return cubeColumn;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EStructuralFeature basicGetCubeColumn() {
		return cubeColumn;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void setCubeColumn(EStructuralFeature newCubeColumn) {
		EStructuralFeature oldCubeColumn = cubeColumn;
		cubeColumn = newCubeColumn;
		if (eNotificationRequired())
			eNotify(new ENotificationImpl(this, Notification.SET, Advanced_variable_lineagefunctionsPackage.RESOLVED_STRUCT_COLUMN_PARAMETER__CUBE_COLUMN, oldCubeColumn, cubeColumn));
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object eGet(int featureID, boolean resolve, boolean coreType) {
		switch (featureID) {
			case Advanced_variable_lineagefunctionsPackage.RESOLVED_STRUCT_COLUMN_PARAMETER__COLUMN_INSIDE_STRUCT:
				if (resolve) return getColumnInsideStruct();
				return basicGetColumnInsideStruct();
			case Advanced_variable_lineagefunctionsPackage.RESOLVED_STRUCT_COLUMN_PARAMETER__CUBE_COLUMN:
				if (resolve) return getCubeColumn();
				return basicGetCubeColumn();
		}
		return super.eGet(featureID, resolve, coreType);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public void eSet(int featureID, Object newValue) {
		switch (featureID) {
			case Advanced_variable_lineagefunctionsPackage.RESOLVED_STRUCT_COLUMN_PARAMETER__COLUMN_INSIDE_STRUCT:
				setColumnInsideStruct((EAttribute)newValue);
				return;
			case Advanced_variable_lineagefunctionsPackage.RESOLVED_STRUCT_COLUMN_PARAMETER__CUBE_COLUMN:
				setCubeColumn((EStructuralFeature)newValue);
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
			case Advanced_variable_lineagefunctionsPackage.RESOLVED_STRUCT_COLUMN_PARAMETER__COLUMN_INSIDE_STRUCT:
				setColumnInsideStruct((EAttribute)null);
				return;
			case Advanced_variable_lineagefunctionsPackage.RESOLVED_STRUCT_COLUMN_PARAMETER__CUBE_COLUMN:
				setCubeColumn((EStructuralFeature)null);
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
			case Advanced_variable_lineagefunctionsPackage.RESOLVED_STRUCT_COLUMN_PARAMETER__COLUMN_INSIDE_STRUCT:
				return columnInsideStruct != null;
			case Advanced_variable_lineagefunctionsPackage.RESOLVED_STRUCT_COLUMN_PARAMETER__CUBE_COLUMN:
				return cubeColumn != null;
		}
		return super.eIsSet(featureID);
	}

} //ResolvedStructColumnParameterImpl
