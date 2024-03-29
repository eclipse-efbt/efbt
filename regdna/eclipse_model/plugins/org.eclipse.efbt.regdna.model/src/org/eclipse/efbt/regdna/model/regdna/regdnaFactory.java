/**
 */
package org.eclipse.efbt.regdna.model.regdna;

import org.eclipse.emf.ecore.EFactory;

/**
 * <!-- begin-user-doc -->
 * The <b>Factory</b> for the model.
 * It provides a create method for each non-abstract class of the model.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage
 * @generated
 */
public interface regdnaFactory extends EFactory {
	/**
	 * The singleton instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	regdnaFactory eINSTANCE = org.eclipse.efbt.regdna.model.regdna.impl.regdnaFactoryImpl.init();

	/**
	 * Returns a new object of class '<em>Import</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Import</em>'.
	 * @generated
	 */
	Import createImport();

	/**
	 * Returns a new object of class '<em>Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Module</em>'.
	 * @generated
	 */
	Module createModule();

	/**
	 * Returns a new object of class '<em>Module List</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Module List</em>'.
	 * @generated
	 */
	ModuleList createModuleList();

	/**
	 * Returns a new object of class '<em>Rules For Report</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Rules For Report</em>'.
	 * @generated
	 */
	RulesForReport createRulesForReport();

	/**
	 * Returns a new object of class '<em>Rules For IL Table</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Rules For IL Table</em>'.
	 * @generated
	 */
	RulesForILTable createRulesForILTable();

	/**
	 * Returns a new object of class '<em>Select Column</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Select Column</em>'.
	 * @generated
	 */
	SelectColumn createSelectColumn();

	/**
	 * Returns a new object of class '<em>Select Column Member As</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Select Column Member As</em>'.
	 * @generated
	 */
	SelectColumnMemberAs createSelectColumnMemberAs();

	/**
	 * Returns a new object of class '<em>Select Column Attribute As</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Select Column Attribute As</em>'.
	 * @generated
	 */
	SelectColumnAttributeAs createSelectColumnAttributeAs();

	/**
	 * Returns a new object of class '<em>Select Derived Column As</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Select Derived Column As</em>'.
	 * @generated
	 */
	SelectDerivedColumnAs createSelectDerivedColumnAs();

	/**
	 * Returns a new object of class '<em>Select Value As</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Select Value As</em>'.
	 * @generated
	 */
	SelectValueAs createSelectValueAs();

	/**
	 * Returns a new object of class '<em>Table Filter</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Table Filter</em>'.
	 * @generated
	 */
	TableFilter createTableFilter();

	/**
	 * Returns a new object of class '<em>Generation Rules Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Generation Rules Module</em>'.
	 * @generated
	 */
	GenerationRulesModule createGenerationRulesModule();

	/**
	 * Returns a new object of class '<em>Rule For IL Table Part</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Rule For IL Table Part</em>'.
	 * @generated
	 */
	RuleForILTablePart createRuleForILTablePart();

	/**
	 * Returns a new object of class '<em>And Predicate</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>And Predicate</em>'.
	 * @generated
	 */
	AndPredicate createAndPredicate();

	/**
	 * Returns a new object of class '<em>Or Predicate</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Or Predicate</em>'.
	 * @generated
	 */
	OrPredicate createOrPredicate();

	/**
	 * Returns a new object of class '<em>Not Predicate</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Not Predicate</em>'.
	 * @generated
	 */
	NotPredicate createNotPredicate();

	/**
	 * Returns a new object of class '<em>Attribute Predicate</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Attribute Predicate</em>'.
	 * @generated
	 */
	AttributePredicate createAttributePredicate();

	/**
	 * Returns a new object of class '<em>EL Attribute</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>EL Attribute</em>'.
	 * @generated
	 */
	ELAttribute createELAttribute();

	/**
	 * Returns a new object of class '<em>EL Class</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>EL Class</em>'.
	 * @generated
	 */
	ELClass createELClass();

	/**
	 * Returns a new object of class '<em>EL Data Type</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>EL Data Type</em>'.
	 * @generated
	 */
	ELDataType createELDataType();

	/**
	 * Returns a new object of class '<em>EL Enum</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>EL Enum</em>'.
	 * @generated
	 */
	ELEnum createELEnum();

	/**
	 * Returns a new object of class '<em>EL Enum Literal</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>EL Enum Literal</em>'.
	 * @generated
	 */
	ELEnumLiteral createELEnumLiteral();

	/**
	 * Returns a new object of class '<em>EL Operation</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>EL Operation</em>'.
	 * @generated
	 */
	ELOperation createELOperation();

	/**
	 * Returns a new object of class '<em>EL Parameter</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>EL Parameter</em>'.
	 * @generated
	 */
	ELParameter createELParameter();

	/**
	 * Returns a new object of class '<em>EL Package</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>EL Package</em>'.
	 * @generated
	 */
	ELPackage createELPackage();

	/**
	 * Returns a new object of class '<em>EL Reference</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>EL Reference</em>'.
	 * @generated
	 */
	ELReference createELReference();

	/**
	 * Returns a new object of class '<em>EL Annotation</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>EL Annotation</em>'.
	 * @generated
	 */
	ELAnnotation createELAnnotation();

	/**
	 * Returns a new object of class '<em>EL Annotation Directive</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>EL Annotation Directive</em>'.
	 * @generated
	 */
	ELAnnotationDirective createELAnnotationDirective();

	/**
	 * Returns a new object of class '<em>EL String To String Map Entry</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>EL String To String Map Entry</em>'.
	 * @generated
	 */
	ELStringToStringMapEntry createELStringToStringMapEntry();

	/**
	 * Returns a new object of class '<em>Report</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Report</em>'.
	 * @generated
	 */
	Report createReport();

	/**
	 * Returns a new object of class '<em>Cell Based Report</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Cell Based Report</em>'.
	 * @generated
	 */
	CellBasedReport createCellBasedReport();

	/**
	 * Returns a new object of class '<em>Report Row</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Report Row</em>'.
	 * @generated
	 */
	ReportRow createReportRow();

	/**
	 * Returns a new object of class '<em>Report Column</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Report Column</em>'.
	 * @generated
	 */
	ReportColumn createReportColumn();

	/**
	 * Returns a new object of class '<em>Report Cell</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Report Cell</em>'.
	 * @generated
	 */
	ReportCell createReportCell();

	/**
	 * Returns a new object of class '<em>Filter</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Filter</em>'.
	 * @generated
	 */
	Filter createFilter();

	/**
	 * Returns a new object of class '<em>Report Module</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Report Module</em>'.
	 * @generated
	 */
	ReportModule createReportModule();

	/**
	 * Returns a new object of class '<em>Row Column Based Report</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Row Column Based Report</em>'.
	 * @generated
	 */
	RowColumnBasedReport createRowColumnBasedReport();

	/**
	 * Returns a new object of class '<em>Row Filters</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Row Filters</em>'.
	 * @generated
	 */
	RowFilters createRowFilters();

	/**
	 * Returns a new object of class '<em>Column Filters</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Column Filters</em>'.
	 * @generated
	 */
	ColumnFilters createColumnFilters();

	/**
	 * Returns a new object of class '<em>Whole Report Filters</em>'.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return a new object of class '<em>Whole Report Filters</em>'.
	 * @generated
	 */
	WholeReportFilters createWholeReportFilters();

	/**
	 * Returns the package supported by this factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the package supported by this factory.
	 * @generated
	 */
	regdnaPackage getregdnaPackage();

} //regdnaFactory
