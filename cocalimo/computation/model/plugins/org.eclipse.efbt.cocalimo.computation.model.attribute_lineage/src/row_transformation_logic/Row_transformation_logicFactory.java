/**
 */
package row_transformation_logic;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see row_transformation_logic.Row_transformation_logicPackage
 * @generated
 */
public interface Row_transformation_logicFactory extends EFactory {
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	Row_transformation_logicFactory eINSTANCE = row_transformation_logic.impl.Row_transformation_logicFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Custom Row Creation Approach</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Custom Row Creation Approach</em>'.
	 * @generated
	 */
	CustomRowCreationApproach createCustomRowCreationApproach();

	/**
	 * Returns a new object of class '<em>Filter And Group To One Row Creation Approach</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Filter And Group To One Row Creation Approach</em>'.
	 * @generated
	 */
	FilterAndGroupToOneRowCreationApproach createFilterAndGroupToOneRowCreationApproach();

	/**
	 * Returns a new object of class '<em>Row Creation Approach For Entity</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Row Creation Approach For Entity</em>'.
	 * @generated
	 */
	RowCreationApproachForEntity createRowCreationApproachForEntity();

	/**
	 * Returns a new object of class '<em>One To One Row Creation Approach</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>One To One Row Creation Approach</em>'.
	 * @generated
	 */
	OneToOneRowCreationApproach createOneToOneRowCreationApproach();

	/**
	 * Returns a new object of class '<em>Filter Row Creation Approach</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Filter Row Creation Approach</em>'.
	 * @generated
	 */
	FilterRowCreationApproach createFilterRowCreationApproach();

	/**
	 * Returns a new object of class '<em>Row Creation Approach</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Row Creation Approach</em>'.
	 * @generated
	 */
	RowCreationApproach createRowCreationApproach();

	/**
	 * Returns a new object of class '<em>Group By Row Creation Approach</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Group By Row Creation Approach</em>'.
	 * @generated
	 */
	GroupByRowCreationApproach createGroupByRowCreationApproach();

	/**
	 * Returns a new object of class '<em>Row Join Function</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Row Join Function</em>'.
	 * @generated
	 */
	RowJoinFunction createRowJoinFunction();

	/**
	 * Returns a new object of class '<em>Union Row Creation Approach</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Union Row Creation Approach</em>'.
	 * @generated
	 */
	UnionRowCreationApproach createUnionRowCreationApproach();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	Row_transformation_logicPackage getRow_transformation_logicPackage();

} //Row_transformation_logicFactory
