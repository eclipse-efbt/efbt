/**
 */
package org.eclipse.efbt.regdna.model.regdna.util;

import org.eclipse.efbt.regdna.model.regdna.AndPredicate;
import org.eclipse.efbt.regdna.model.regdna.AttributePredicate;
import org.eclipse.efbt.regdna.model.regdna.ELAnnotation;
import org.eclipse.efbt.regdna.model.regdna.ELAnnotationDirective;
import org.eclipse.efbt.regdna.model.regdna.ELAttribute;
import org.eclipse.efbt.regdna.model.regdna.ELClass;
import org.eclipse.efbt.regdna.model.regdna.ELClassifier;
import org.eclipse.efbt.regdna.model.regdna.ELDataType;
import org.eclipse.efbt.regdna.model.regdna.ELEnum;
import org.eclipse.efbt.regdna.model.regdna.ELEnumLiteral;
import org.eclipse.efbt.regdna.model.regdna.ELModelElement;
import org.eclipse.efbt.regdna.model.regdna.ELNamedElement;
import org.eclipse.efbt.regdna.model.regdna.ELOperation;
import org.eclipse.efbt.regdna.model.regdna.ELPackage;
import org.eclipse.efbt.regdna.model.regdna.ELParameter;
import org.eclipse.efbt.regdna.model.regdna.ELReference;
import org.eclipse.efbt.regdna.model.regdna.ELStringToStringMapEntry;
import org.eclipse.efbt.regdna.model.regdna.ELStructuralFeature;
import org.eclipse.efbt.regdna.model.regdna.ELTypedElement;
import org.eclipse.efbt.regdna.model.regdna.Filter;
import org.eclipse.efbt.regdna.model.regdna.GenerationRulesModule;
import org.eclipse.efbt.regdna.model.regdna.Import;
import org.eclipse.efbt.regdna.model.regdna.ModuleList;
import org.eclipse.efbt.regdna.model.regdna.NotPredicate;
import org.eclipse.efbt.regdna.model.regdna.OrPredicate;
import org.eclipse.efbt.regdna.model.regdna.Predicate;
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
import org.eclipse.efbt.regdna.model.regdna.regdnaPackage;

import org.eclipse.emf.ecore.EObject;
import org.eclipse.emf.ecore.EPackage;

import org.eclipse.emf.ecore.util.Switch;

/**
 * <!-- begin-user-doc -->
 * The <b>Switch</b> for the model's inheritance hierarchy.
 * It supports the call {@link #doSwitch(EObject) doSwitch(object)}
 * to invoke the <code>caseXXX</code> method for each class of the model,
 * starting with the actual class of the object
 * and proceeding up the inheritance hierarchy
 * until a non-null result is returned,
 * which is the result of the switch.
 * <!-- end-user-doc -->
 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage
 * @generated
 */
public class regdnaSwitch<T> extends Switch<T> {
	/**
	 * The cached model package
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected static regdnaPackage modelPackage;

	/**
	 * Creates an instance of the switch.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public regdnaSwitch() {
		if (modelPackage == null) {
			modelPackage = regdnaPackage.eINSTANCE;
		}
	}

	/**
	 * Checks whether this is a switch for the given package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @param ePackage the package in question.
	 * @return whether this is a switch for the given package.
	 * @generated
	 */
	@Override
	protected boolean isSwitchFor(EPackage ePackage) {
		return ePackage == modelPackage;
	}

	/**
	 * Calls <code>caseXXX</code> for each class of the model until one returns a non null result; it yields that result.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @return the first non-null result returned by a <code>caseXXX</code> call.
	 * @generated
	 */
	@Override
	protected T doSwitch(int classifierID, EObject theEObject) {
		switch (classifierID) {
			case regdnaPackage.IMPORT: {
				Import import_ = (Import)theEObject;
				T result = caseImport(import_);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.MODULE: {
				org.eclipse.efbt.regdna.model.regdna.Module module = (org.eclipse.efbt.regdna.model.regdna.Module)theEObject;
				T result = caseModule(module);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.MODULE_LIST: {
				ModuleList moduleList = (ModuleList)theEObject;
				T result = caseModuleList(moduleList);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.RULES_FOR_REPORT: {
				RulesForReport rulesForReport = (RulesForReport)theEObject;
				T result = caseRulesForReport(rulesForReport);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.RULES_FOR_IL_TABLE: {
				RulesForILTable rulesForILTable = (RulesForILTable)theEObject;
				T result = caseRulesForILTable(rulesForILTable);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.SELECT_COLUMN: {
				SelectColumn selectColumn = (SelectColumn)theEObject;
				T result = caseSelectColumn(selectColumn);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.SELECT_COLUMN_MEMBER_AS: {
				SelectColumnMemberAs selectColumnMemberAs = (SelectColumnMemberAs)theEObject;
				T result = caseSelectColumnMemberAs(selectColumnMemberAs);
				if (result == null) result = caseSelectColumn(selectColumnMemberAs);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.SELECT_COLUMN_ATTRIBUTE_AS: {
				SelectColumnAttributeAs selectColumnAttributeAs = (SelectColumnAttributeAs)theEObject;
				T result = caseSelectColumnAttributeAs(selectColumnAttributeAs);
				if (result == null) result = caseSelectColumn(selectColumnAttributeAs);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.SELECT_DERIVED_COLUMN_AS: {
				SelectDerivedColumnAs selectDerivedColumnAs = (SelectDerivedColumnAs)theEObject;
				T result = caseSelectDerivedColumnAs(selectDerivedColumnAs);
				if (result == null) result = caseSelectColumn(selectDerivedColumnAs);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.SELECT_VALUE_AS: {
				SelectValueAs selectValueAs = (SelectValueAs)theEObject;
				T result = caseSelectValueAs(selectValueAs);
				if (result == null) result = caseSelectColumn(selectValueAs);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.TABLE_FILTER: {
				TableFilter tableFilter = (TableFilter)theEObject;
				T result = caseTableFilter(tableFilter);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.GENERATION_RULES_MODULE: {
				GenerationRulesModule generationRulesModule = (GenerationRulesModule)theEObject;
				T result = caseGenerationRulesModule(generationRulesModule);
				if (result == null) result = caseModule(generationRulesModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.RULE_FOR_IL_TABLE_PART: {
				RuleForILTablePart ruleForILTablePart = (RuleForILTablePart)theEObject;
				T result = caseRuleForILTablePart(ruleForILTablePart);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.PREDICATE: {
				Predicate predicate = (Predicate)theEObject;
				T result = casePredicate(predicate);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.AND_PREDICATE: {
				AndPredicate andPredicate = (AndPredicate)theEObject;
				T result = caseAndPredicate(andPredicate);
				if (result == null) result = casePredicate(andPredicate);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.OR_PREDICATE: {
				OrPredicate orPredicate = (OrPredicate)theEObject;
				T result = caseOrPredicate(orPredicate);
				if (result == null) result = casePredicate(orPredicate);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.NOT_PREDICATE: {
				NotPredicate notPredicate = (NotPredicate)theEObject;
				T result = caseNotPredicate(notPredicate);
				if (result == null) result = casePredicate(notPredicate);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.ATTRIBUTE_PREDICATE: {
				AttributePredicate attributePredicate = (AttributePredicate)theEObject;
				T result = caseAttributePredicate(attributePredicate);
				if (result == null) result = casePredicate(attributePredicate);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.EL_ATTRIBUTE: {
				ELAttribute elAttribute = (ELAttribute)theEObject;
				T result = caseELAttribute(elAttribute);
				if (result == null) result = caseELStructuralFeature(elAttribute);
				if (result == null) result = caseELTypedElement(elAttribute);
				if (result == null) result = caseELNamedElement(elAttribute);
				if (result == null) result = caseELModelElement(elAttribute);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.EL_CLASS: {
				ELClass elClass = (ELClass)theEObject;
				T result = caseELClass(elClass);
				if (result == null) result = caseELClassifier(elClass);
				if (result == null) result = caseELNamedElement(elClass);
				if (result == null) result = caseELModelElement(elClass);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.EL_CLASSIFIER: {
				ELClassifier elClassifier = (ELClassifier)theEObject;
				T result = caseELClassifier(elClassifier);
				if (result == null) result = caseELNamedElement(elClassifier);
				if (result == null) result = caseELModelElement(elClassifier);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.EL_DATA_TYPE: {
				ELDataType elDataType = (ELDataType)theEObject;
				T result = caseELDataType(elDataType);
				if (result == null) result = caseELClassifier(elDataType);
				if (result == null) result = caseELNamedElement(elDataType);
				if (result == null) result = caseELModelElement(elDataType);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.EL_ENUM: {
				ELEnum elEnum = (ELEnum)theEObject;
				T result = caseELEnum(elEnum);
				if (result == null) result = caseELDataType(elEnum);
				if (result == null) result = caseELClassifier(elEnum);
				if (result == null) result = caseELNamedElement(elEnum);
				if (result == null) result = caseELModelElement(elEnum);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.EL_ENUM_LITERAL: {
				ELEnumLiteral elEnumLiteral = (ELEnumLiteral)theEObject;
				T result = caseELEnumLiteral(elEnumLiteral);
				if (result == null) result = caseELNamedElement(elEnumLiteral);
				if (result == null) result = caseELModelElement(elEnumLiteral);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.EL_MODEL_ELEMENT: {
				ELModelElement elModelElement = (ELModelElement)theEObject;
				T result = caseELModelElement(elModelElement);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.EL_NAMED_ELEMENT: {
				ELNamedElement elNamedElement = (ELNamedElement)theEObject;
				T result = caseELNamedElement(elNamedElement);
				if (result == null) result = caseELModelElement(elNamedElement);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.EL_OPERATION: {
				ELOperation elOperation = (ELOperation)theEObject;
				T result = caseELOperation(elOperation);
				if (result == null) result = caseELTypedElement(elOperation);
				if (result == null) result = caseELNamedElement(elOperation);
				if (result == null) result = caseELModelElement(elOperation);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.EL_PARAMETER: {
				ELParameter elParameter = (ELParameter)theEObject;
				T result = caseELParameter(elParameter);
				if (result == null) result = caseELTypedElement(elParameter);
				if (result == null) result = caseELNamedElement(elParameter);
				if (result == null) result = caseELModelElement(elParameter);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.EL_PACKAGE: {
				ELPackage elPackage = (ELPackage)theEObject;
				T result = caseELPackage(elPackage);
				if (result == null) result = caseModule(elPackage);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.EL_REFERENCE: {
				ELReference elReference = (ELReference)theEObject;
				T result = caseELReference(elReference);
				if (result == null) result = caseELStructuralFeature(elReference);
				if (result == null) result = caseELTypedElement(elReference);
				if (result == null) result = caseELNamedElement(elReference);
				if (result == null) result = caseELModelElement(elReference);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.EL_STRUCTURAL_FEATURE: {
				ELStructuralFeature elStructuralFeature = (ELStructuralFeature)theEObject;
				T result = caseELStructuralFeature(elStructuralFeature);
				if (result == null) result = caseELTypedElement(elStructuralFeature);
				if (result == null) result = caseELNamedElement(elStructuralFeature);
				if (result == null) result = caseELModelElement(elStructuralFeature);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.EL_TYPED_ELEMENT: {
				ELTypedElement elTypedElement = (ELTypedElement)theEObject;
				T result = caseELTypedElement(elTypedElement);
				if (result == null) result = caseELNamedElement(elTypedElement);
				if (result == null) result = caseELModelElement(elTypedElement);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.EL_ANNOTATION: {
				ELAnnotation elAnnotation = (ELAnnotation)theEObject;
				T result = caseELAnnotation(elAnnotation);
				if (result == null) result = caseELModelElement(elAnnotation);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.EL_ANNOTATION_DIRECTIVE: {
				ELAnnotationDirective elAnnotationDirective = (ELAnnotationDirective)theEObject;
				T result = caseELAnnotationDirective(elAnnotationDirective);
				if (result == null) result = caseELNamedElement(elAnnotationDirective);
				if (result == null) result = caseELModelElement(elAnnotationDirective);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.EL_STRING_TO_STRING_MAP_ENTRY: {
				ELStringToStringMapEntry elStringToStringMapEntry = (ELStringToStringMapEntry)theEObject;
				T result = caseELStringToStringMapEntry(elStringToStringMapEntry);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.REPORT: {
				Report report = (Report)theEObject;
				T result = caseReport(report);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.REPORT_ROW: {
				ReportRow reportRow = (ReportRow)theEObject;
				T result = caseReportRow(reportRow);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.REPORT_COLUMN: {
				ReportColumn reportColumn = (ReportColumn)theEObject;
				T result = caseReportColumn(reportColumn);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.REPORT_CELL: {
				ReportCell reportCell = (ReportCell)theEObject;
				T result = caseReportCell(reportCell);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.FILTER: {
				Filter filter = (Filter)theEObject;
				T result = caseFilter(filter);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			case regdnaPackage.REPORT_MODULE: {
				ReportModule reportModule = (ReportModule)theEObject;
				T result = caseReportModule(reportModule);
				if (result == null) result = caseModule(reportModule);
				if (result == null) result = defaultCase(theEObject);
				return result;
			}
			default: return defaultCase(theEObject);
		}
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Import</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Import</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseImport(Import object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseModule(org.eclipse.efbt.regdna.model.regdna.Module object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Module List</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Module List</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseModuleList(ModuleList object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Rules For Report</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Rules For Report</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseRulesForReport(RulesForReport object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Rules For IL Table</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Rules For IL Table</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseRulesForILTable(RulesForILTable object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Select Column</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Select Column</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseSelectColumn(SelectColumn object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Select Column Member As</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Select Column Member As</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseSelectColumnMemberAs(SelectColumnMemberAs object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Select Column Attribute As</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Select Column Attribute As</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseSelectColumnAttributeAs(SelectColumnAttributeAs object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Select Derived Column As</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Select Derived Column As</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseSelectDerivedColumnAs(SelectDerivedColumnAs object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Select Value As</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Select Value As</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseSelectValueAs(SelectValueAs object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Table Filter</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Table Filter</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseTableFilter(TableFilter object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Generation Rules Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Generation Rules Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseGenerationRulesModule(GenerationRulesModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Rule For IL Table Part</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Rule For IL Table Part</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseRuleForILTablePart(RuleForILTablePart object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Predicate</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Predicate</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T casePredicate(Predicate object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>And Predicate</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>And Predicate</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseAndPredicate(AndPredicate object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Or Predicate</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Or Predicate</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseOrPredicate(OrPredicate object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Not Predicate</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Not Predicate</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseNotPredicate(NotPredicate object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Attribute Predicate</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Attribute Predicate</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseAttributePredicate(AttributePredicate object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EL Attribute</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EL Attribute</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseELAttribute(ELAttribute object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EL Class</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EL Class</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseELClass(ELClass object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EL Classifier</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EL Classifier</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseELClassifier(ELClassifier object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EL Data Type</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EL Data Type</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseELDataType(ELDataType object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EL Enum</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EL Enum</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseELEnum(ELEnum object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EL Enum Literal</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EL Enum Literal</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseELEnumLiteral(ELEnumLiteral object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EL Model Element</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EL Model Element</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseELModelElement(ELModelElement object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EL Named Element</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EL Named Element</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseELNamedElement(ELNamedElement object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EL Operation</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EL Operation</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseELOperation(ELOperation object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EL Parameter</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EL Parameter</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseELParameter(ELParameter object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EL Package</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EL Package</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseELPackage(ELPackage object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EL Reference</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EL Reference</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseELReference(ELReference object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EL Structural Feature</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EL Structural Feature</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseELStructuralFeature(ELStructuralFeature object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EL Typed Element</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EL Typed Element</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseELTypedElement(ELTypedElement object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EL Annotation</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EL Annotation</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseELAnnotation(ELAnnotation object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EL Annotation Directive</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EL Annotation Directive</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseELAnnotationDirective(ELAnnotationDirective object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EL String To String Map Entry</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EL String To String Map Entry</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseELStringToStringMapEntry(ELStringToStringMapEntry object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Report</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Report</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseReport(Report object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Report Row</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Report Row</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseReportRow(ReportRow object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Report Column</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Report Column</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseReportColumn(ReportColumn object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Report Cell</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Report Cell</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseReportCell(ReportCell object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Filter</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Filter</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseFilter(Filter object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>Report Module</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>Report Module</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject) doSwitch(EObject)
	 * @generated
	 */
	public T caseReportModule(ReportModule object) {
		return null;
	}

	/**
	 * Returns the result of interpreting the object as an instance of '<em>EObject</em>'.
	 * <!-- begin-user-doc -->
	 * This implementation returns null;
	 * returning a non-null result will terminate the switch, but this is the last case anyway.
	 * <!-- end-user-doc -->
	 * @param object the target of the switch.
	 * @return the result of interpreting the object as an instance of '<em>EObject</em>'.
	 * @see #doSwitch(org.eclipse.emf.ecore.EObject)
	 * @generated
	 */
	@Override
	public T defaultCase(EObject object) {
		return null;
	}

} //regdnaSwitch
