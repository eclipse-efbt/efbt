/**
 */
package org.eclipse.efbt.lineage.common.model.lineagefunctions;

import org.eclipse.efbt.common.model.functions.FunctionsPackage;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;

/**
 * <!-- begin-user-doc -->
 * The <b>Package</b> for the model.
 * It contains accessors for the meta objects to represent
 * <ul>
 *   <li>each class,</li>
 *   <li>each feature of each class,</li>
 *   <li>each operation of each class,</li>
 *   <li>each enum,</li>
 *   <li>and each data type</li>
 * </ul>
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.LineagefunctionsFactory
 * @model kind="package"
 * @generated
 */
public interface LineagefunctionsPackage extends EPackage
{
	/**
	 * The package name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNAME = "lineagefunctions";

	/**
	 * The package namespace URI.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_URI = "http://www.eclipse.org/lineagefunctions";

	/**
	 * The package namespace name.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	String eNS_PREFIX = "lineagefunctions";

	/**
	 * The singleton instance of the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	LineagefunctionsPackage eINSTANCE = org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.LineagefunctionsPackageImpl.init();

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.MemberParameterImpl <em>Member Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.MemberParameterImpl
	 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.LineagefunctionsPackageImpl#getMemberParameter()
	 * @generated
	 */
	int MEMBER_PARAMETER = 0;

	/**
	 * The feature id for the '<em><b>Param</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_PARAMETER__PARAM = FunctionsPackage.PARAMETER_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Member Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_PARAMETER_FEATURE_COUNT = FunctionsPackage.PARAMETER_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Member Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int MEMBER_PARAMETER_OPERATION_COUNT = FunctionsPackage.PARAMETER_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.ResolvedCubeColumnParameterImpl <em>Resolved Cube Column Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.ResolvedCubeColumnParameterImpl
	 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.LineagefunctionsPackageImpl#getResolvedCubeColumnParameter()
	 * @generated
	 */
	int RESOLVED_CUBE_COLUMN_PARAMETER = 1;

	/**
	 * The feature id for the '<em><b>Cube Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RESOLVED_CUBE_COLUMN_PARAMETER__CUBE_COLUMN = FunctionsPackage.PARAMETER_FEATURE_COUNT + 0;

	/**
	 * The number of structural features of the '<em>Resolved Cube Column Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RESOLVED_CUBE_COLUMN_PARAMETER_FEATURE_COUNT = FunctionsPackage.PARAMETER_FEATURE_COUNT + 1;

	/**
	 * The number of operations of the '<em>Resolved Cube Column Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int RESOLVED_CUBE_COLUMN_PARAMETER_OPERATION_COUNT = FunctionsPackage.PARAMETER_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.CubeColumnImpl <em>Cube Column</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.CubeColumnImpl
	 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.LineagefunctionsPackageImpl#getCubeColumn()
	 * @generated
	 */
	int CUBE_COLUMN = 2;

	/**
	 * The feature id for the '<em><b>Column ID</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_COLUMN__COLUMN_ID = 0;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_COLUMN__CUBE = 1;

	/**
	 * The feature id for the '<em><b>Used In Subsets</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_COLUMN__USED_IN_SUBSETS = 2;

	/**
	 * The feature id for the '<em><b>Variable</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_COLUMN__VARIABLE = 3;

	/**
	 * The number of structural features of the '<em>Cube Column</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_COLUMN_FEATURE_COUNT = 4;

	/**
	 * The number of operations of the '<em>Cube Column</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int CUBE_COLUMN_OPERATION_COUNT = 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.SpeculativeCubeColumnParameterImpl <em>Speculative Cube Column Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.SpeculativeCubeColumnParameterImpl
	 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.LineagefunctionsPackageImpl#getSpeculativeCubeColumnParameter()
	 * @generated
	 */
	int SPECULATIVE_CUBE_COLUMN_PARAMETER = 3;

	/**
	 * The feature id for the '<em><b>Column</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SPECULATIVE_CUBE_COLUMN_PARAMETER__COLUMN = FunctionsPackage.PARAMETER_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Cube</b></em>' reference.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SPECULATIVE_CUBE_COLUMN_PARAMETER__CUBE = FunctionsPackage.PARAMETER_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Speculative Cube Column Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SPECULATIVE_CUBE_COLUMN_PARAMETER_FEATURE_COUNT = FunctionsPackage.PARAMETER_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Speculative Cube Column Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int SPECULATIVE_CUBE_COLUMN_PARAMETER_OPERATION_COUNT = FunctionsPackage.PARAMETER_OPERATION_COUNT + 0;

	/**
	 * The meta object id for the '{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.ValueParameterImpl <em>Value Parameter</em>}' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.ValueParameterImpl
	 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.LineagefunctionsPackageImpl#getValueParameter()
	 * @generated
	 */
	int VALUE_PARAMETER = 4;

	/**
	 * The feature id for the '<em><b>Data Type</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VALUE_PARAMETER__DATA_TYPE = FunctionsPackage.PARAMETER_FEATURE_COUNT + 0;

	/**
	 * The feature id for the '<em><b>Value</b></em>' attribute.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VALUE_PARAMETER__VALUE = FunctionsPackage.PARAMETER_FEATURE_COUNT + 1;

	/**
	 * The number of structural features of the '<em>Value Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VALUE_PARAMETER_FEATURE_COUNT = FunctionsPackage.PARAMETER_FEATURE_COUNT + 2;

	/**
	 * The number of operations of the '<em>Value Parameter</em>' class.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 * @ordered
	 */
	int VALUE_PARAMETER_OPERATION_COUNT = FunctionsPackage.PARAMETER_OPERATION_COUNT + 0;


	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.MemberParameter <em>Member Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Member Parameter</em>'.
	 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.MemberParameter
	 * @generated
	 */
	EClass getMemberParameter();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.MemberParameter#getParam <em>Param</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Param</em>'.
	 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.MemberParameter#getParam()
	 * @see #getMemberParameter()
	 * @generated
	 */
	EReference getMemberParameter_Param();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.ResolvedCubeColumnParameter <em>Resolved Cube Column Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Resolved Cube Column Parameter</em>'.
	 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.ResolvedCubeColumnParameter
	 * @generated
	 */
	EClass getResolvedCubeColumnParameter();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.ResolvedCubeColumnParameter#getCubeColumn <em>Cube Column</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube Column</em>'.
	 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.ResolvedCubeColumnParameter#getCubeColumn()
	 * @see #getResolvedCubeColumnParameter()
	 * @generated
	 */
	EReference getResolvedCubeColumnParameter_CubeColumn();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.CubeColumn <em>Cube Column</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Cube Column</em>'.
	 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.CubeColumn
	 * @generated
	 */
	EClass getCubeColumn();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.CubeColumn#getColumnID <em>Column ID</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Column ID</em>'.
	 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.CubeColumn#getColumnID()
	 * @see #getCubeColumn()
	 * @generated
	 */
	EAttribute getCubeColumn_ColumnID();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.CubeColumn#getCube <em>Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube</em>'.
	 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.CubeColumn#getCube()
	 * @see #getCubeColumn()
	 * @generated
	 */
	EReference getCubeColumn_Cube();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.CubeColumn#isUsedInSubsets <em>Used In Subsets</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Used In Subsets</em>'.
	 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.CubeColumn#isUsedInSubsets()
	 * @see #getCubeColumn()
	 * @generated
	 */
	EAttribute getCubeColumn_UsedInSubsets();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.CubeColumn#getVariable <em>Variable</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Variable</em>'.
	 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.CubeColumn#getVariable()
	 * @see #getCubeColumn()
	 * @generated
	 */
	EReference getCubeColumn_Variable();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.SpeculativeCubeColumnParameter <em>Speculative Cube Column Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Speculative Cube Column Parameter</em>'.
	 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.SpeculativeCubeColumnParameter
	 * @generated
	 */
	EClass getSpeculativeCubeColumnParameter();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.SpeculativeCubeColumnParameter#getColumn <em>Column</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Column</em>'.
	 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.SpeculativeCubeColumnParameter#getColumn()
	 * @see #getSpeculativeCubeColumnParameter()
	 * @generated
	 */
	EReference getSpeculativeCubeColumnParameter_Column();

	/**
	 * Returns the meta object for the reference '{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.SpeculativeCubeColumnParameter#getCube <em>Cube</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the reference '<em>Cube</em>'.
	 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.SpeculativeCubeColumnParameter#getCube()
	 * @see #getSpeculativeCubeColumnParameter()
	 * @generated
	 */
	EReference getSpeculativeCubeColumnParameter_Cube();

	/**
	 * Returns the meta object for class '{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.ValueParameter <em>Value Parameter</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for class '<em>Value Parameter</em>'.
	 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.ValueParameter
	 * @generated
	 */
	EClass getValueParameter();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.ValueParameter#getDataType <em>Data Type</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Data Type</em>'.
	 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.ValueParameter#getDataType()
	 * @see #getValueParameter()
	 * @generated
	 */
	EAttribute getValueParameter_DataType();

	/**
	 * Returns the meta object for the attribute '{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.ValueParameter#getValue <em>Value</em>}'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the meta object for the attribute '<em>Value</em>'.
	 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.ValueParameter#getValue()
	 * @see #getValueParameter()
	 * @generated
	 */
	EAttribute getValueParameter_Value();

	/**
	 * Returns the factory that creates the instances of the model.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the factory that creates the instances of the model.
	 * @generated
	 */
	LineagefunctionsFactory getLineagefunctionsFactory();

	/**
	 * <!-- begin-user-doc -->
	 * Defines literals for the meta objects that represent
	 * <ul>
	 *   <li>each class,</li>
	 *   <li>each feature of each class,</li>
	 *   <li>each operation of each class,</li>
	 *   <li>each enum,</li>
	 *   <li>and each data type</li>
	 * </ul>
	 * <!-- end-user-doc -->
	 * @generated
	 */
	interface Literals
	{
		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.MemberParameterImpl <em>Member Parameter</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.MemberParameterImpl
		 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.LineagefunctionsPackageImpl#getMemberParameter()
		 * @generated
		 */
		EClass MEMBER_PARAMETER = eINSTANCE.getMemberParameter();

		/**
		 * The meta object literal for the '<em><b>Param</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference MEMBER_PARAMETER__PARAM = eINSTANCE.getMemberParameter_Param();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.ResolvedCubeColumnParameterImpl <em>Resolved Cube Column Parameter</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.ResolvedCubeColumnParameterImpl
		 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.LineagefunctionsPackageImpl#getResolvedCubeColumnParameter()
		 * @generated
		 */
		EClass RESOLVED_CUBE_COLUMN_PARAMETER = eINSTANCE.getResolvedCubeColumnParameter();

		/**
		 * The meta object literal for the '<em><b>Cube Column</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference RESOLVED_CUBE_COLUMN_PARAMETER__CUBE_COLUMN = eINSTANCE.getResolvedCubeColumnParameter_CubeColumn();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.CubeColumnImpl <em>Cube Column</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.CubeColumnImpl
		 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.LineagefunctionsPackageImpl#getCubeColumn()
		 * @generated
		 */
		EClass CUBE_COLUMN = eINSTANCE.getCubeColumn();

		/**
		 * The meta object literal for the '<em><b>Column ID</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_COLUMN__COLUMN_ID = eINSTANCE.getCubeColumn_ColumnID();

		/**
		 * The meta object literal for the '<em><b>Cube</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_COLUMN__CUBE = eINSTANCE.getCubeColumn_Cube();

		/**
		 * The meta object literal for the '<em><b>Used In Subsets</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute CUBE_COLUMN__USED_IN_SUBSETS = eINSTANCE.getCubeColumn_UsedInSubsets();

		/**
		 * The meta object literal for the '<em><b>Variable</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference CUBE_COLUMN__VARIABLE = eINSTANCE.getCubeColumn_Variable();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.SpeculativeCubeColumnParameterImpl <em>Speculative Cube Column Parameter</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.SpeculativeCubeColumnParameterImpl
		 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.LineagefunctionsPackageImpl#getSpeculativeCubeColumnParameter()
		 * @generated
		 */
		EClass SPECULATIVE_CUBE_COLUMN_PARAMETER = eINSTANCE.getSpeculativeCubeColumnParameter();

		/**
		 * The meta object literal for the '<em><b>Column</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SPECULATIVE_CUBE_COLUMN_PARAMETER__COLUMN = eINSTANCE.getSpeculativeCubeColumnParameter_Column();

		/**
		 * The meta object literal for the '<em><b>Cube</b></em>' reference feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EReference SPECULATIVE_CUBE_COLUMN_PARAMETER__CUBE = eINSTANCE.getSpeculativeCubeColumnParameter_Cube();

		/**
		 * The meta object literal for the '{@link org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.ValueParameterImpl <em>Value Parameter</em>}' class.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.ValueParameterImpl
		 * @see org.eclipse.efbt.lineage.common.model.lineagefunctions.impl.LineagefunctionsPackageImpl#getValueParameter()
		 * @generated
		 */
		EClass VALUE_PARAMETER = eINSTANCE.getValueParameter();

		/**
		 * The meta object literal for the '<em><b>Data Type</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VALUE_PARAMETER__DATA_TYPE = eINSTANCE.getValueParameter_DataType();

		/**
		 * The meta object literal for the '<em><b>Value</b></em>' attribute feature.
		 * <!-- begin-user-doc -->
		 * <!-- end-user-doc -->
		 * @generated
		 */
		EAttribute VALUE_PARAMETER__VALUE = eINSTANCE.getValueParameter_Value();

	}

} //LineagefunctionsPackage
