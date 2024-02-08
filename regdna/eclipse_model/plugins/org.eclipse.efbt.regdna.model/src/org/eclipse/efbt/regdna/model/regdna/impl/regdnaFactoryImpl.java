/**
 */
package org.eclipse.efbt.regdna.model.regdna.impl;

import org.eclipse.efbt.regdna.model.regdna.AndPredicate;
import org.eclipse.efbt.regdna.model.regdna.AttributePredicate;
import org.eclipse.efbt.regdna.model.regdna.Comparitor;
import org.eclipse.efbt.regdna.model.regdna.ELAnnotation;
import org.eclipse.efbt.regdna.model.regdna.ELAnnotationDirective;
import org.eclipse.efbt.regdna.model.regdna.ELAttribute;
import org.eclipse.efbt.regdna.model.regdna.ELClass;
import org.eclipse.efbt.regdna.model.regdna.ELDataType;
import org.eclipse.efbt.regdna.model.regdna.ELEnum;
import org.eclipse.efbt.regdna.model.regdna.ELEnumLiteral;
import org.eclipse.efbt.regdna.model.regdna.ELOperation;
import org.eclipse.efbt.regdna.model.regdna.ELPackage;
import org.eclipse.efbt.regdna.model.regdna.ELParameter;
import org.eclipse.efbt.regdna.model.regdna.ELReference;
import org.eclipse.efbt.regdna.model.regdna.ELStringToStringMapEntry;
import org.eclipse.efbt.regdna.model.regdna.Filter;
import org.eclipse.efbt.regdna.model.regdna.GenerationRulesModule;
import org.eclipse.efbt.regdna.model.regdna.Import;
import org.eclipse.efbt.regdna.model.regdna.ModuleList;
import org.eclipse.efbt.regdna.model.regdna.NotPredicate;
import org.eclipse.efbt.regdna.model.regdna.OrPredicate;
import org.eclipse.efbt.regdna.model.regdna.Report;
import org.eclipse.efbt.regdna.model.regdna.ReportCell;
import org.eclipse.efbt.regdna.model.regdna.ReportColumn;
import org.eclipse.efbt.regdna.model.regdna.ReportModule;
import org.eclipse.efbt.regdna.model.regdna.ReportRow;
import org.eclipse.efbt.regdna.model.regdna.RuleForILTablePart;
import org.eclipse.efbt.regdna.model.regdna.RulesForILTable;
import org.eclipse.efbt.regdna.model.regdna.RulesForReport;
import org.eclipse.efbt.regdna.model.regdna.SelectColumn;
import org.eclipse.efbt.regdna.model.regdna.SelectColumnAttributeAs;
import org.eclipse.efbt.regdna.model.regdna.SelectColumnMemberAs;
import org.eclipse.efbt.regdna.model.regdna.SelectDerivedColumnAs;
import org.eclipse.efbt.regdna.model.regdna.SelectValueAs;
import org.eclipse.efbt.regdna.model.regdna.TableFilter;
import org.eclipse.efbt.regdna.model.regdna.regdnaFactory;
import org.eclipse.efbt.regdna.model.regdna.regdnaPackage;

import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EDataType;
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
public class regdnaFactoryImpl extends EFactoryImpl implements regdnaFactory {
	/**
	 * Creates the default factory implementation.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public static regdnaFactory init() {
		try {
			regdnaFactory theregdnaFactory = (regdnaFactory)EPackage.Registry.INSTANCE.getEFactory(regdnaPackage.eNS_URI);
			if (theregdnaFactory != null) {
				return theregdnaFactory;
			}
		}
		catch (Exception exception) {
			EcorePlugin.INSTANCE.log(exception);
		}
		return new regdnaFactoryImpl();
	}

	/**
	 * Creates an instance of the factory.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public regdnaFactoryImpl() {
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
			case regdnaPackage.IMPORT: return createImport();
			case regdnaPackage.MODULE: return createModule();
			case regdnaPackage.MODULE_LIST: return createModuleList();
			case regdnaPackage.RULES_FOR_REPORT: return createRulesForReport();
			case regdnaPackage.RULES_FOR_IL_TABLE: return createRulesForILTable();
			case regdnaPackage.SELECT_COLUMN: return createSelectColumn();
			case regdnaPackage.SELECT_COLUMN_MEMBER_AS: return createSelectColumnMemberAs();
			case regdnaPackage.SELECT_COLUMN_ATTRIBUTE_AS: return createSelectColumnAttributeAs();
			case regdnaPackage.SELECT_DERIVED_COLUMN_AS: return createSelectDerivedColumnAs();
			case regdnaPackage.SELECT_VALUE_AS: return createSelectValueAs();
			case regdnaPackage.TABLE_FILTER: return createTableFilter();
			case regdnaPackage.GENERATION_RULES_MODULE: return createGenerationRulesModule();
			case regdnaPackage.RULE_FOR_IL_TABLE_PART: return createRuleForILTablePart();
			case regdnaPackage.AND_PREDICATE: return createAndPredicate();
			case regdnaPackage.OR_PREDICATE: return createOrPredicate();
			case regdnaPackage.NOT_PREDICATE: return createNotPredicate();
			case regdnaPackage.ATTRIBUTE_PREDICATE: return createAttributePredicate();
			case regdnaPackage.EL_ATTRIBUTE: return createELAttribute();
			case regdnaPackage.EL_CLASS: return createELClass();
			case regdnaPackage.EL_DATA_TYPE: return createELDataType();
			case regdnaPackage.EL_ENUM: return createELEnum();
			case regdnaPackage.EL_ENUM_LITERAL: return createELEnumLiteral();
			case regdnaPackage.EL_OPERATION: return createELOperation();
			case regdnaPackage.EL_PARAMETER: return createELParameter();
			case regdnaPackage.EL_PACKAGE: return createELPackage();
			case regdnaPackage.EL_REFERENCE: return createELReference();
			case regdnaPackage.EL_ANNOTATION: return createELAnnotation();
			case regdnaPackage.EL_ANNOTATION_DIRECTIVE: return createELAnnotationDirective();
			case regdnaPackage.EL_STRING_TO_STRING_MAP_ENTRY: return createELStringToStringMapEntry();
			case regdnaPackage.REPORT: return createReport();
			case regdnaPackage.REPORT_ROW: return createReportRow();
			case regdnaPackage.REPORT_COLUMN: return createReportColumn();
			case regdnaPackage.REPORT_CELL: return createReportCell();
			case regdnaPackage.FILTER: return createFilter();
			case regdnaPackage.REPORT_MODULE: return createReportModule();
			default:
				throw new IllegalArgumentException("The class '" + eClass.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Object createFromString(EDataType eDataType, String initialValue) {
		switch (eDataType.getClassifierID()) {
			case regdnaPackage.COMPARITOR:
				return createComparitorFromString(eDataType, initialValue);
			default:
				throw new IllegalArgumentException("The datatype '" + eDataType.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public String convertToString(EDataType eDataType, Object instanceValue) {
		switch (eDataType.getClassifierID()) {
			case regdnaPackage.COMPARITOR:
				return convertComparitorToString(eDataType, instanceValue);
			default:
				throw new IllegalArgumentException("The datatype '" + eDataType.getName() + "' is not a valid classifier");
		}
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Import createImport() {
		ImportImpl import_ = new ImportImpl();
		return import_;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public org.eclipse.efbt.regdna.model.regdna.Module createModule() {
		ModuleImpl module = new ModuleImpl();
		return module;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ModuleList createModuleList() {
		ModuleListImpl moduleList = new ModuleListImpl();
		return moduleList;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public RulesForReport createRulesForReport() {
		RulesForReportImpl rulesForReport = new RulesForReportImpl();
		return rulesForReport;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public RulesForILTable createRulesForILTable() {
		RulesForILTableImpl rulesForILTable = new RulesForILTableImpl();
		return rulesForILTable;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public SelectColumn createSelectColumn() {
		SelectColumnImpl selectColumn = new SelectColumnImpl();
		return selectColumn;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public SelectColumnMemberAs createSelectColumnMemberAs() {
		SelectColumnMemberAsImpl selectColumnMemberAs = new SelectColumnMemberAsImpl();
		return selectColumnMemberAs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public SelectColumnAttributeAs createSelectColumnAttributeAs() {
		SelectColumnAttributeAsImpl selectColumnAttributeAs = new SelectColumnAttributeAsImpl();
		return selectColumnAttributeAs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public SelectDerivedColumnAs createSelectDerivedColumnAs() {
		SelectDerivedColumnAsImpl selectDerivedColumnAs = new SelectDerivedColumnAsImpl();
		return selectDerivedColumnAs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public SelectValueAs createSelectValueAs() {
		SelectValueAsImpl selectValueAs = new SelectValueAsImpl();
		return selectValueAs;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public TableFilter createTableFilter() {
		TableFilterImpl tableFilter = new TableFilterImpl();
		return tableFilter;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public GenerationRulesModule createGenerationRulesModule() {
		GenerationRulesModuleImpl generationRulesModule = new GenerationRulesModuleImpl();
		return generationRulesModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public RuleForILTablePart createRuleForILTablePart() {
		RuleForILTablePartImpl ruleForILTablePart = new RuleForILTablePartImpl();
		return ruleForILTablePart;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public AndPredicate createAndPredicate() {
		AndPredicateImpl andPredicate = new AndPredicateImpl();
		return andPredicate;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public OrPredicate createOrPredicate() {
		OrPredicateImpl orPredicate = new OrPredicateImpl();
		return orPredicate;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public NotPredicate createNotPredicate() {
		NotPredicateImpl notPredicate = new NotPredicateImpl();
		return notPredicate;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public AttributePredicate createAttributePredicate() {
		AttributePredicateImpl attributePredicate = new AttributePredicateImpl();
		return attributePredicate;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ELAttribute createELAttribute() {
		ELAttributeImpl elAttribute = new ELAttributeImpl();
		return elAttribute;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ELClass createELClass() {
		ELClassImpl elClass = new ELClassImpl();
		return elClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ELDataType createELDataType() {
		ELDataTypeImpl elDataType = new ELDataTypeImpl();
		return elDataType;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ELEnum createELEnum() {
		ELEnumImpl elEnum = new ELEnumImpl();
		return elEnum;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ELEnumLiteral createELEnumLiteral() {
		ELEnumLiteralImpl elEnumLiteral = new ELEnumLiteralImpl();
		return elEnumLiteral;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ELOperation createELOperation() {
		ELOperationImpl elOperation = new ELOperationImpl();
		return elOperation;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ELParameter createELParameter() {
		ELParameterImpl elParameter = new ELParameterImpl();
		return elParameter;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ELPackage createELPackage() {
		ELPackageImpl elPackage = new ELPackageImpl();
		return elPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ELReference createELReference() {
		ELReferenceImpl elReference = new ELReferenceImpl();
		return elReference;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ELAnnotation createELAnnotation() {
		ELAnnotationImpl elAnnotation = new ELAnnotationImpl();
		return elAnnotation;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ELAnnotationDirective createELAnnotationDirective() {
		ELAnnotationDirectiveImpl elAnnotationDirective = new ELAnnotationDirectiveImpl();
		return elAnnotationDirective;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ELStringToStringMapEntry createELStringToStringMapEntry() {
		ELStringToStringMapEntryImpl elStringToStringMapEntry = new ELStringToStringMapEntryImpl();
		return elStringToStringMapEntry;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Report createReport() {
		ReportImpl report = new ReportImpl();
		return report;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ReportRow createReportRow() {
		ReportRowImpl reportRow = new ReportRowImpl();
		return reportRow;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ReportColumn createReportColumn() {
		ReportColumnImpl reportColumn = new ReportColumnImpl();
		return reportColumn;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ReportCell createReportCell() {
		ReportCellImpl reportCell = new ReportCellImpl();
		return reportCell;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public Filter createFilter() {
		FilterImpl filter = new FilterImpl();
		return filter;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public ReportModule createReportModule() {
		ReportModuleImpl reportModule = new ReportModuleImpl();
		return reportModule;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Comparitor createComparitorFromString(EDataType eDataType, String initialValue) {
		Comparitor result = Comparitor.get(initialValue);
		if (result == null) throw new IllegalArgumentException("The value '" + initialValue + "' is not a valid enumerator of '" + eDataType.getName() + "'");
		return result;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public String convertComparitorToString(EDataType eDataType, Object instanceValue) {
		return instanceValue == null ? null : instanceValue.toString();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public regdnaPackage getregdnaPackage() {
		return (regdnaPackage)getEPackage();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @deprecated
	 * @generated
	 */
	@Deprecated
	public static regdnaPackage getPackage() {
		return regdnaPackage.eINSTANCE;
	}

} //regdnaFactoryImpl
