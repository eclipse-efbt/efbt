/**
 */
package org.eclipse.efbt.regdna.model.regdna.impl;

import java.io.IOException;

import java.net.URL;

import org.eclipse.efbt.regdna.model.regdna.regdnaFactory;
import org.eclipse.efbt.regdna.model.regdna.regdnaPackage;

import org.eclipse.emf.common.util.URI;
import org.eclipse.emf.common.util.WrappedException;

import org.eclipse.emf.ecore.EAttribute;
import org.eclipse.emf.ecore.EClass;
import org.eclipse.emf.ecore.EClassifier;
import org.eclipse.emf.ecore.EEnum;
import org.eclipse.emf.ecore.EPackage;
import org.eclipse.emf.ecore.EReference;

import org.eclipse.emf.ecore.impl.EPackageImpl;

import org.eclipse.emf.ecore.resource.Resource;

import org.eclipse.emf.ecore.xmi.impl.EcoreResourceFactoryImpl;

/**
 * <!-- begin-user-doc -->
 * An implementation of the model <b>Package</b>.
 * <!-- end-user-doc -->
 * @generated
 */
public class regdnaPackageImpl extends EPackageImpl implements regdnaPackage {
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected String packageFilename = "regdna.ecore";

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass importEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass moduleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass moduleListEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass rulesForReportEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass rulesForILTableEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass selectColumnEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass selectColumnMemberAsEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass selectColumnAttributeAsEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass selectDerivedColumnAsEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass selectValueAsEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass tableFilterEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass generationRulesModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass ruleForILTablePartEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass predicateEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass andPredicateEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass orPredicateEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass notPredicateEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass attributePredicateEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass elAttributeEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass elClassEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass elClassifierEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass elDataTypeEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass elEnumEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass elEnumLiteralEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass elModelElementEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass elNamedElementEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass elOperationEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass elParameterEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass elPackageEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass elReferenceEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass elStructuralFeatureEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass elTypedElementEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass elAnnotationEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass elAnnotationDirectiveEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass elStringToStringMapEntryEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass reportEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass cellBasedReportEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass reportRowEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass reportColumnEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass reportCellEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass filterEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass reportModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass rowColumnBasedReportEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass rowFiltersEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass columnFiltersEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass wholeReportFiltersEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EEnum comparitorEEnum = null;

	/**
	 * Creates an instance of the model <b>Package</b>, registered with
	 * {@link org.eclipse.emf.ecore.EPackage.Registry EPackage.Registry} by the package
	 * package URI value.
	 * <p>Note: the correct way to create the package is via the static
	 * factory method {@link #init init()}, which also performs
	 * initialization of the package, or returns the registered package,
	 * if one already exists.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see org.eclipse.emf.ecore.EPackage.Registry
	 * @see org.eclipse.efbt.regdna.model.regdna.regdnaPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private regdnaPackageImpl() {
		super(eNS_URI, regdnaFactory.eINSTANCE);
	}
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private static boolean isInited = false;

	/**
	 * Creates, registers, and initializes the <b>Package</b> for this model, and for any others upon which it depends.
	 *
	 * <p>This method is used to initialize {@link regdnaPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @generated
	 */
	public static regdnaPackage init() {
		if (isInited) return (regdnaPackage)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredregdnaPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		regdnaPackageImpl theregdnaPackage = registeredregdnaPackage instanceof regdnaPackageImpl ? (regdnaPackageImpl)registeredregdnaPackage : new regdnaPackageImpl();

		isInited = true;

		// Load packages
		theregdnaPackage.loadPackage();

		// Fix loaded packages
		theregdnaPackage.fixPackageContents();

		// Mark meta-data to indicate it can't be changed
		theregdnaPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(regdnaPackage.eNS_URI, theregdnaPackage);
		return theregdnaPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getImport() {
		if (importEClass == null) {
			importEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(0);
		}
		return importEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getImport_ImportedNamespace() {
        return (EAttribute)getImport().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getModule() {
		if (moduleEClass == null) {
			moduleEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(1);
		}
		return moduleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getModule_Dependencies() {
        return (EReference)getModule().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getModule_TheDescription() {
        return (EAttribute)getModule().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getModule_License() {
        return (EAttribute)getModule().getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getModule_Name() {
        return (EAttribute)getModule().getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getModule_Version() {
        return (EAttribute)getModule().getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getModule_Imports() {
        return (EReference)getModule().getEStructuralFeatures().get(5);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getModule_AnnotationDirectives() {
        return (EReference)getModule().getEStructuralFeatures().get(6);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getModuleList() {
		if (moduleListEClass == null) {
			moduleListEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(2);
		}
		return moduleListEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getModuleList_Modules() {
        return (EReference)getModuleList().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getRulesForReport() {
		if (rulesForReportEClass == null) {
			rulesForReportEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(3);
		}
		return rulesForReportEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getRulesForReport_OutputLayerCube() {
        return (EReference)getRulesForReport().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getRulesForReport_RulesForTable() {
        return (EReference)getRulesForReport().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getRulesForILTable() {
		if (rulesForILTableEClass == null) {
			rulesForILTableEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(4);
		}
		return rulesForILTableEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getRulesForILTable_RulesForTablePart() {
        return (EReference)getRulesForILTable().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getRulesForILTable_InputLayerTable() {
        return (EReference)getRulesForILTable().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getSelectColumn() {
		if (selectColumnEClass == null) {
			selectColumnEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(5);
		}
		return selectColumnEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSelectColumn_AsAttribute() {
        return (EReference)getSelectColumn().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getSelectColumnMemberAs() {
		if (selectColumnMemberAsEClass == null) {
			selectColumnMemberAsEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(6);
		}
		return selectColumnMemberAsEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSelectColumnMemberAs_MemberAsConstant() {
        return (EReference)getSelectColumnMemberAs().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getSelectColumnAttributeAs() {
		if (selectColumnAttributeAsEClass == null) {
			selectColumnAttributeAsEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(7);
		}
		return selectColumnAttributeAsEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSelectColumnAttributeAs_Attribute() {
        return (EReference)getSelectColumnAttributeAs().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getSelectDerivedColumnAs() {
		if (selectDerivedColumnAsEClass == null) {
			selectDerivedColumnAsEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(8);
		}
		return selectDerivedColumnAsEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getSelectDerivedColumnAs_Attribute() {
        return (EReference)getSelectDerivedColumnAs().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getSelectValueAs() {
		if (selectValueAsEClass == null) {
			selectValueAsEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(9);
		}
		return selectValueAsEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getSelectValueAs_Value() {
        return (EAttribute)getSelectValueAs().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getTableFilter() {
		if (tableFilterEClass == null) {
			tableFilterEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(11);
		}
		return tableFilterEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getTableFilter_Predicate() {
        return (EReference)getTableFilter().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getGenerationRulesModule() {
		if (generationRulesModuleEClass == null) {
			generationRulesModuleEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(12);
		}
		return generationRulesModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getGenerationRulesModule_RulesForReport() {
        return (EReference)getGenerationRulesModule().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getRuleForILTablePart() {
		if (ruleForILTablePartEClass == null) {
			ruleForILTablePartEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(13);
		}
		return ruleForILTablePartEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getRuleForILTablePart_Name() {
        return (EAttribute)getRuleForILTablePart().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getRuleForILTablePart_Columns() {
        return (EReference)getRuleForILTablePart().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getRuleForILTablePart_WhereClause() {
        return (EReference)getRuleForILTablePart().getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getPredicate() {
		if (predicateEClass == null) {
			predicateEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(14);
		}
		return predicateEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getAndPredicate() {
		if (andPredicateEClass == null) {
			andPredicateEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(15);
		}
		return andPredicateEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getAndPredicate_Operands() {
        return (EReference)getAndPredicate().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getOrPredicate() {
		if (orPredicateEClass == null) {
			orPredicateEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(16);
		}
		return orPredicateEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getOrPredicate_Operands() {
        return (EReference)getOrPredicate().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getNotPredicate() {
		if (notPredicateEClass == null) {
			notPredicateEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(17);
		}
		return notPredicateEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getNotPredicate_Operand() {
        return (EReference)getNotPredicate().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getAttributePredicate() {
		if (attributePredicateEClass == null) {
			attributePredicateEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(18);
		}
		return attributePredicateEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getAttributePredicate_Attribute1() {
        return (EReference)getAttributePredicate().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getAttributePredicate_Comparitor() {
        return (EAttribute)getAttributePredicate().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getAttributePredicate_Member() {
        return (EReference)getAttributePredicate().getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getAttributePredicate_Value() {
        return (EAttribute)getAttributePredicate().getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getELAttribute() {
		if (elAttributeEClass == null) {
			elAttributeEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(19);
		}
		return elAttributeEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getELAttribute_ID() {
        return (EAttribute)getELAttribute().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getELAttribute_EAttributeType() {
        return (EReference)getELAttribute().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getELClass() {
		if (elClassEClass == null) {
			elClassEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(20);
		}
		return elClassEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getELClass_EAbstract() {
        return (EAttribute)getELClass().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getELClass_ESuperTypes() {
        return (EReference)getELClass().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getELClass_EStructuralFeatures() {
        return (EReference)getELClass().getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getELClass_EOperations() {
        return (EReference)getELClass().getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getELClassifier() {
		if (elClassifierEClass == null) {
			elClassifierEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(21);
		}
		return elClassifierEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getELClassifier_EPackage() {
        return (EReference)getELClassifier().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getELDataType() {
		if (elDataTypeEClass == null) {
			elDataTypeEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(22);
		}
		return elDataTypeEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getELDataType_IndustryName() {
        return (EAttribute)getELDataType().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getELEnum() {
		if (elEnumEClass == null) {
			elEnumEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(23);
		}
		return elEnumEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getELEnum_ELiterals() {
        return (EReference)getELEnum().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getELEnumLiteral() {
		if (elEnumLiteralEClass == null) {
			elEnumLiteralEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(24);
		}
		return elEnumLiteralEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getELEnumLiteral_Value() {
        return (EAttribute)getELEnumLiteral().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getELEnumLiteral_Literal() {
        return (EAttribute)getELEnumLiteral().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getELModelElement() {
		if (elModelElementEClass == null) {
			elModelElementEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(25);
		}
		return elModelElementEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getELModelElement_EAnnotations() {
        return (EReference)getELModelElement().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getELNamedElement() {
		if (elNamedElementEClass == null) {
			elNamedElementEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(26);
		}
		return elNamedElementEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getELNamedElement_Name() {
        return (EAttribute)getELNamedElement().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getELOperation() {
		if (elOperationEClass == null) {
			elOperationEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(27);
		}
		return elOperationEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getELOperation_Body() {
        return (EAttribute)getELOperation().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getELParameter() {
		if (elParameterEClass == null) {
			elParameterEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(28);
		}
		return elParameterEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getELPackage() {
		if (elPackageEClass == null) {
			elPackageEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(29);
		}
		return elPackageEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getELPackage_EClassifiers() {
        return (EReference)getELPackage().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getELPackage_NsURI() {
        return (EAttribute)getELPackage().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getELPackage_NsPrefix() {
        return (EAttribute)getELPackage().getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getELReference() {
		if (elReferenceEClass == null) {
			elReferenceEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(30);
		}
		return elReferenceEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getELReference_Containment() {
        return (EAttribute)getELReference().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getELReference_EReferenceType() {
        return (EReference)getELReference().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getELStructuralFeature() {
		if (elStructuralFeatureEClass == null) {
			elStructuralFeatureEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(31);
		}
		return elStructuralFeatureEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getELTypedElement() {
		if (elTypedElementEClass == null) {
			elTypedElementEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(32);
		}
		return elTypedElementEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getELTypedElement_EType() {
        return (EReference)getELTypedElement().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getELTypedElement_UpperBound() {
        return (EAttribute)getELTypedElement().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getELTypedElement_LowerBound() {
        return (EAttribute)getELTypedElement().getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getELAnnotation() {
		if (elAnnotationEClass == null) {
			elAnnotationEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(33);
		}
		return elAnnotationEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getELAnnotation_Details() {
        return (EReference)getELAnnotation().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getELAnnotation_Source() {
        return (EReference)getELAnnotation().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getELAnnotationDirective() {
		if (elAnnotationDirectiveEClass == null) {
			elAnnotationDirectiveEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(34);
		}
		return elAnnotationDirectiveEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getELAnnotationDirective_Module() {
        return (EReference)getELAnnotationDirective().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getELAnnotationDirective_SourceURI() {
        return (EAttribute)getELAnnotationDirective().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getELStringToStringMapEntry() {
		if (elStringToStringMapEntryEClass == null) {
			elStringToStringMapEntryEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(35);
		}
		return elStringToStringMapEntryEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getELStringToStringMapEntry_Key() {
        return (EAttribute)getELStringToStringMapEntry().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getELStringToStringMapEntry_Value() {
        return (EAttribute)getELStringToStringMapEntry().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getReport() {
		if (reportEClass == null) {
			reportEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(36);
		}
		return reportEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getReport_OutputLayer() {
        return (EReference)getReport().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getReport_Rows() {
        return (EReference)getReport().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getReport_Columns() {
        return (EReference)getReport().getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getCellBasedReport() {
		if (cellBasedReportEClass == null) {
			cellBasedReportEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(37);
		}
		return cellBasedReportEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getCellBasedReport_ReportCells() {
        return (EReference)getCellBasedReport().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getReportRow() {
		if (reportRowEClass == null) {
			reportRowEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(38);
		}
		return reportRowEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getReportRow_Name() {
        return (EAttribute)getReportRow().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getReportColumn() {
		if (reportColumnEClass == null) {
			reportColumnEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(39);
		}
		return reportColumnEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getReportColumn_Name() {
        return (EAttribute)getReportColumn().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getReportCell() {
		if (reportCellEClass == null) {
			reportCellEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(40);
		}
		return reportCellEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getReportCell_Row() {
        return (EReference)getReportCell().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getReportCell_Column() {
        return (EReference)getReportCell().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getReportCell_Filters() {
        return (EReference)getReportCell().getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getReportCell_Metric() {
        return (EReference)getReportCell().getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EAttribute getReportCell_DatapointID() {
        return (EAttribute)getReportCell().getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getFilter() {
		if (filterEClass == null) {
			filterEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(41);
		}
		return filterEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getFilter_OutputLayer() {
        return (EReference)getFilter().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getFilter_Operation() {
        return (EReference)getFilter().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getFilter_Member() {
        return (EReference)getFilter().getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getReportModule() {
		if (reportModuleEClass == null) {
			reportModuleEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(42);
		}
		return reportModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getReportModule_Reports() {
        return (EReference)getReportModule().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getRowColumnBasedReport() {
		if (rowColumnBasedReportEClass == null) {
			rowColumnBasedReportEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(43);
		}
		return rowColumnBasedReportEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getRowColumnBasedReport_ColumnFilters() {
        return (EReference)getRowColumnBasedReport().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getRowColumnBasedReport_RowFilters() {
        return (EReference)getRowColumnBasedReport().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getRowColumnBasedReport_WholeReportFilters() {
        return (EReference)getRowColumnBasedReport().getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getRowFilters() {
		if (rowFiltersEClass == null) {
			rowFiltersEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(44);
		}
		return rowFiltersEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getRowFilters_Row() {
        return (EReference)getRowFilters().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getRowFilters_Filters() {
        return (EReference)getRowFilters().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getRowFilters_Metric() {
        return (EReference)getRowFilters().getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getColumnFilters() {
		if (columnFiltersEClass == null) {
			columnFiltersEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(45);
		}
		return columnFiltersEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getColumnFilters_Column() {
        return (EReference)getColumnFilters().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getColumnFilters_Filters() {
        return (EReference)getColumnFilters().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getColumnFilters_Metric() {
        return (EReference)getColumnFilters().getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EClass getWholeReportFilters() {
		if (wholeReportFiltersEClass == null) {
			wholeReportFiltersEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(46);
		}
		return wholeReportFiltersEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EReference getWholeReportFilters_Filters() {
        return (EReference)getWholeReportFilters().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public EEnum getComparitor() {
		if (comparitorEEnum == null) {
			comparitorEEnum = (EEnum)EPackage.Registry.INSTANCE.getEPackage(regdnaPackage.eNS_URI).getEClassifiers().get(10);
		}
		return comparitorEEnum;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	public regdnaFactory getregdnaFactory() {
		return (regdnaFactory)getEFactoryInstance();
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private boolean isLoaded = false;

	/**
	 * Loads the package and any sub-packages from their serialized form.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void loadPackage() {
		if (isLoaded) return;
		isLoaded = true;

		URL url = getClass().getResource(packageFilename);
		if (url == null) {
			throw new RuntimeException("Missing serialized package: " + packageFilename);
		}
		URI uri = URI.createURI(url.toString());
		Resource resource = new EcoreResourceFactoryImpl().createResource(uri);
		try {
			resource.load(null);
		}
		catch (IOException exception) {
			throw new WrappedException(exception);
		}
		initializeFromLoadedEPackage(this, (EPackage)resource.getContents().get(0));
		createResource(eNS_URI);
	}


	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private boolean isFixed = false;

	/**
	 * Fixes up the loaded package, to make it appear as if it had been programmatically built.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public void fixPackageContents() {
		if (isFixed) return;
		isFixed = true;
		fixEClassifiers();
	}

	/**
	 * Sets the instance class on the given classifier.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	@Override
	protected void fixInstanceClass(EClassifier eClassifier) {
		if (eClassifier.getInstanceClassName() == null) {
			eClassifier.setInstanceClassName("org.eclipse.efbt.regdna.model.regdna." + eClassifier.getName());
			setGeneratedClassName(eClassifier);
		}
	}

} //regdnaPackageImpl
