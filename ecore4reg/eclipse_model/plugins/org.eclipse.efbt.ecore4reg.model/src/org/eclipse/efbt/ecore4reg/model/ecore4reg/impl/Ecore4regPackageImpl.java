/**
 */
package org.eclipse.efbt.ecore4reg.model.ecore4reg.impl;

import java.io.IOException;

import java.net.URL;

import org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regFactory;
import org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage;

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
public class Ecore4regPackageImpl extends EPackageImpl implements Ecore4regPackage {
	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	protected String packageFilename = "ecore4reg.ecore";

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
	private EClass allowedTypesEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass requirementsModuleEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass requirementsSectionEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass requirementsSectionImageEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass requirementsSectionLinkWithTextEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass requirementsSectionTextEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass requirementTypeEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass titledRequirementsSectionEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass tagEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass tagGroupEClass = null;

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
	private EClass elPublicOperationEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass elPrivateOperationEClass = null;

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
	private EClass elStringToStringMapEntryEClass = null;

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	private EClass operationTagEClass = null;

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
	 * @see org.eclipse.efbt.ecore4reg.model.ecore4reg.Ecore4regPackage#eNS_URI
	 * @see #init()
	 * @generated
	 */
	private Ecore4regPackageImpl() {
		super(eNS_URI, Ecore4regFactory.eINSTANCE);
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
	 * <p>This method is used to initialize {@link Ecore4regPackage#eINSTANCE} when that field is accessed.
	 * Clients should not invoke it directly. Instead, they should simply access that field to obtain the package.
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @see #eNS_URI
	 * @generated
	 */
	public static Ecore4regPackage init() {
		if (isInited) return (Ecore4regPackage)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI);

		// Obtain or create and register package
		Object registeredEcore4regPackage = EPackage.Registry.INSTANCE.get(eNS_URI);
		Ecore4regPackageImpl theEcore4regPackage = registeredEcore4regPackage instanceof Ecore4regPackageImpl ? (Ecore4regPackageImpl)registeredEcore4regPackage : new Ecore4regPackageImpl();

		isInited = true;

		// Load packages
		theEcore4regPackage.loadPackage();

		// Fix loaded packages
		theEcore4regPackage.fixPackageContents();

		// Mark meta-data to indicate it can't be changed
		theEcore4regPackage.freeze();

		// Update the registry and return the package
		EPackage.Registry.INSTANCE.put(Ecore4regPackage.eNS_URI, theEcore4regPackage);
		return theEcore4regPackage;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getImport() {
		if (importEClass == null) {
			importEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(0);
		}
		return importEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getImport_ImportedNamespace() {
        return (EAttribute)getImport().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getModule() {
		if (moduleEClass == null) {
			moduleEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(1);
		}
		return moduleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getModule_Dependencies() {
        return (EReference)getModule().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getModule_TheDescription() {
        return (EAttribute)getModule().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getModule_License() {
        return (EAttribute)getModule().getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getModule_Name() {
        return (EAttribute)getModule().getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getModule_Version() {
        return (EAttribute)getModule().getEStructuralFeatures().get(4);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getModule_Imports() {
        return (EReference)getModule().getEStructuralFeatures().get(5);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getModuleList() {
		if (moduleListEClass == null) {
			moduleListEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(2);
		}
		return moduleListEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getModuleList_Modules() {
        return (EReference)getModuleList().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getAllowedTypes() {
		if (allowedTypesEClass == null) {
			allowedTypesEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(3);
		}
		return allowedTypesEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getAllowedTypes_AllowedTypes() {
        return (EReference)getAllowedTypes().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getRequirementsModule() {
		if (requirementsModuleEClass == null) {
			requirementsModuleEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(4);
		}
		return requirementsModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getRequirementsModule_Rules() {
        return (EReference)getRequirementsModule().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getRequirementsModule_Allowedtypes() {
        return (EReference)getRequirementsModule().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getRequirementsSection() {
		if (requirementsSectionEClass == null) {
			requirementsSectionEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(5);
		}
		return requirementsSectionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getRequirementsSection_Name() {
        return (EAttribute)getRequirementsSection().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getRequirementsSectionImage() {
		if (requirementsSectionImageEClass == null) {
			requirementsSectionImageEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(6);
		}
		return requirementsSectionImageEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getRequirementsSectionImage_Style() {
        return (EAttribute)getRequirementsSectionImage().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getRequirementsSectionImage_Uri() {
        return (EAttribute)getRequirementsSectionImage().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getRequirementsSectionLinkWithText() {
		if (requirementsSectionLinkWithTextEClass == null) {
			requirementsSectionLinkWithTextEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(7);
		}
		return requirementsSectionLinkWithTextEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getRequirementsSectionLinkWithText_LinkedRuleSection() {
        return (EReference)getRequirementsSectionLinkWithText().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getRequirementsSectionLinkWithText_LinkText() {
        return (EAttribute)getRequirementsSectionLinkWithText().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getRequirementsSectionLinkWithText_Subsection() {
        return (EAttribute)getRequirementsSectionLinkWithText().getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getRequirementsSectionText() {
		if (requirementsSectionTextEClass == null) {
			requirementsSectionTextEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(8);
		}
		return requirementsSectionTextEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getRequirementsSectionText_Text() {
        return (EAttribute)getRequirementsSectionText().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getRequirementType() {
		if (requirementTypeEClass == null) {
			requirementTypeEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(9);
		}
		return requirementTypeEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getRequirementType_Name() {
        return (EAttribute)getRequirementType().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getTitledRequirementsSection() {
		if (titledRequirementsSectionEClass == null) {
			titledRequirementsSectionEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(10);
		}
		return titledRequirementsSectionEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getTitledRequirementsSection_Sections() {
        return (EReference)getTitledRequirementsSection().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getTitledRequirementsSection_ReferencingSections() {
        return (EReference)getTitledRequirementsSection().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getTitledRequirementsSection_RequirementsType() {
        return (EReference)getTitledRequirementsSection().getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getTitledRequirementsSection_Title() {
        return (EAttribute)getTitledRequirementsSection().getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getTag() {
		if (tagEClass == null) {
			tagEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(11);
		}
		return tagEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getTag_DisplayName() {
        return (EAttribute)getTag().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getTag_Name() {
        return (EAttribute)getTag().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getTag_Requirements() {
        return (EReference)getTag().getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getTagGroup() {
		if (tagGroupEClass == null) {
			tagGroupEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(12);
		}
		return tagGroupEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getTagGroup_Tags() {
        return (EReference)getTagGroup().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getRulesForReport() {
		if (rulesForReportEClass == null) {
			rulesForReportEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(13);
		}
		return rulesForReportEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getRulesForReport_OutputLayerCube() {
        return (EReference)getRulesForReport().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getRulesForReport_RulesForTable() {
        return (EReference)getRulesForReport().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getRulesForILTable() {
		if (rulesForILTableEClass == null) {
			rulesForILTableEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(14);
		}
		return rulesForILTableEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getRulesForILTable_RulesForTablePart() {
        return (EReference)getRulesForILTable().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getRulesForILTable_InputLayerTable() {
        return (EReference)getRulesForILTable().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getSelectColumn() {
		if (selectColumnEClass == null) {
			selectColumnEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(15);
		}
		return selectColumnEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getSelectColumn_AsAttribute() {
        return (EReference)getSelectColumn().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getSelectColumnMemberAs() {
		if (selectColumnMemberAsEClass == null) {
			selectColumnMemberAsEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(16);
		}
		return selectColumnMemberAsEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getSelectColumnMemberAs_MemberAsConstant() {
        return (EReference)getSelectColumnMemberAs().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getSelectColumnAttributeAs() {
		if (selectColumnAttributeAsEClass == null) {
			selectColumnAttributeAsEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(17);
		}
		return selectColumnAttributeAsEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getSelectColumnAttributeAs_Attribute() {
        return (EReference)getSelectColumnAttributeAs().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getSelectDerivedColumnAs() {
		if (selectDerivedColumnAsEClass == null) {
			selectDerivedColumnAsEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(18);
		}
		return selectDerivedColumnAsEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getSelectDerivedColumnAs_Attribute() {
        return (EReference)getSelectDerivedColumnAs().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getSelectValueAs() {
		if (selectValueAsEClass == null) {
			selectValueAsEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(19);
		}
		return selectValueAsEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getSelectValueAs_Value() {
        return (EAttribute)getSelectValueAs().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getTableFilter() {
		if (tableFilterEClass == null) {
			tableFilterEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(21);
		}
		return tableFilterEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getTableFilter_Predicate() {
        return (EReference)getTableFilter().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getGenerationRulesModule() {
		if (generationRulesModuleEClass == null) {
			generationRulesModuleEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(22);
		}
		return generationRulesModuleEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getGenerationRulesModule_RulesForReport() {
        return (EReference)getGenerationRulesModule().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getRuleForILTablePart() {
		if (ruleForILTablePartEClass == null) {
			ruleForILTablePartEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(23);
		}
		return ruleForILTablePartEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getRuleForILTablePart_Name() {
        return (EAttribute)getRuleForILTablePart().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getRuleForILTablePart_Columns() {
        return (EReference)getRuleForILTablePart().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getRuleForILTablePart_WhereClause() {
        return (EReference)getRuleForILTablePart().getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getPredicate() {
		if (predicateEClass == null) {
			predicateEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(24);
		}
		return predicateEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getAndPredicate() {
		if (andPredicateEClass == null) {
			andPredicateEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(25);
		}
		return andPredicateEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getAndPredicate_Operands() {
        return (EReference)getAndPredicate().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getOrPredicate() {
		if (orPredicateEClass == null) {
			orPredicateEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(26);
		}
		return orPredicateEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getOrPredicate_Operands() {
        return (EReference)getOrPredicate().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getNotPredicate() {
		if (notPredicateEClass == null) {
			notPredicateEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(27);
		}
		return notPredicateEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getNotPredicate_Operand() {
        return (EReference)getNotPredicate().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getAttributePredicate() {
		if (attributePredicateEClass == null) {
			attributePredicateEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(28);
		}
		return attributePredicateEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getAttributePredicate_Attribute1() {
        return (EReference)getAttributePredicate().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAttributePredicate_Comparitor() {
        return (EAttribute)getAttributePredicate().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getAttributePredicate_Member() {
        return (EReference)getAttributePredicate().getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getAttributePredicate_Value() {
        return (EAttribute)getAttributePredicate().getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getELAttribute() {
		if (elAttributeEClass == null) {
			elAttributeEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(29);
		}
		return elAttributeEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getELAttribute_ID() {
        return (EAttribute)getELAttribute().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getELAttribute_EAttributeType() {
        return (EReference)getELAttribute().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getELClass() {
		if (elClassEClass == null) {
			elClassEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(30);
		}
		return elClassEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getELClass_EAbstract() {
        return (EAttribute)getELClass().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getELClass_ESuperTypes() {
        return (EReference)getELClass().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getELClass_EStructuralFeatures() {
        return (EReference)getELClass().getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getELClass_EOperations() {
        return (EReference)getELClass().getEStructuralFeatures().get(3);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getELClassifier() {
		if (elClassifierEClass == null) {
			elClassifierEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(31);
		}
		return elClassifierEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getELClassifier_EPackage() {
        return (EReference)getELClassifier().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getELDataType() {
		if (elDataTypeEClass == null) {
			elDataTypeEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(32);
		}
		return elDataTypeEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getELDataType_IndustryName() {
        return (EAttribute)getELDataType().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getELEnum() {
		if (elEnumEClass == null) {
			elEnumEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(33);
		}
		return elEnumEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getELEnum_ELiterals() {
        return (EReference)getELEnum().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getELEnumLiteral() {
		if (elEnumLiteralEClass == null) {
			elEnumLiteralEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(34);
		}
		return elEnumLiteralEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getELEnumLiteral_Value() {
        return (EAttribute)getELEnumLiteral().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getELEnumLiteral_Literal() {
        return (EAttribute)getELEnumLiteral().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getELModelElement() {
		if (elModelElementEClass == null) {
			elModelElementEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(35);
		}
		return elModelElementEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getELModelElement_EAnnotations() {
        return (EReference)getELModelElement().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getELNamedElement() {
		if (elNamedElementEClass == null) {
			elNamedElementEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(36);
		}
		return elNamedElementEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getELNamedElement_Name() {
        return (EAttribute)getELNamedElement().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getELOperation() {
		if (elOperationEClass == null) {
			elOperationEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(37);
		}
		return elOperationEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getELOperation_Body() {
        return (EAttribute)getELOperation().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getELPublicOperation() {
		if (elPublicOperationEClass == null) {
			elPublicOperationEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(38);
		}
		return elPublicOperationEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getELPublicOperation_CalledPrivateOperations() {
        return (EReference)getELPublicOperation().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getELPrivateOperation() {
		if (elPrivateOperationEClass == null) {
			elPrivateOperationEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(39);
		}
		return elPrivateOperationEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getELPrivateOperation_EParameters() {
        return (EReference)getELPrivateOperation().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getELParameter() {
		if (elParameterEClass == null) {
			elParameterEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(40);
		}
		return elParameterEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getELPackage() {
		if (elPackageEClass == null) {
			elPackageEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(41);
		}
		return elPackageEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getELPackage_EClassifiers() {
        return (EReference)getELPackage().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getELPackage_NsURI() {
        return (EAttribute)getELPackage().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getELPackage_NsPrefix() {
        return (EAttribute)getELPackage().getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getELReference() {
		if (elReferenceEClass == null) {
			elReferenceEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(42);
		}
		return elReferenceEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getELReference_Containment() {
        return (EAttribute)getELReference().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getELReference_EReferenceType() {
        return (EReference)getELReference().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getELStructuralFeature() {
		if (elStructuralFeatureEClass == null) {
			elStructuralFeatureEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(43);
		}
		return elStructuralFeatureEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getELTypedElement() {
		if (elTypedElementEClass == null) {
			elTypedElementEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(44);
		}
		return elTypedElementEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getELTypedElement_EType() {
        return (EReference)getELTypedElement().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getELTypedElement_UpperBound() {
        return (EAttribute)getELTypedElement().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getELTypedElement_LowerBound() {
        return (EAttribute)getELTypedElement().getEStructuralFeatures().get(2);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getELAnnotation() {
		if (elAnnotationEClass == null) {
			elAnnotationEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(45);
		}
		return elAnnotationEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getELAnnotation_Details() {
        return (EReference)getELAnnotation().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getELAnnotation_Source() {
        return (EAttribute)getELAnnotation().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getELStringToStringMapEntry() {
		if (elStringToStringMapEntryEClass == null) {
			elStringToStringMapEntryEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(46);
		}
		return elStringToStringMapEntryEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getELStringToStringMapEntry_Key() {
        return (EAttribute)getELStringToStringMapEntry().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EAttribute getELStringToStringMapEntry_Value() {
        return (EAttribute)getELStringToStringMapEntry().getEStructuralFeatures().get(1);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EClass getOperationTag() {
		if (operationTagEClass == null) {
			operationTagEClass = (EClass)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(47);
		}
		return operationTagEClass;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EReference getOperationTag_Operation() {
        return (EReference)getOperationTag().getEStructuralFeatures().get(0);
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public EEnum getComparitor() {
		if (comparitorEEnum == null) {
			comparitorEEnum = (EEnum)EPackage.Registry.INSTANCE.getEPackage(Ecore4regPackage.eNS_URI).getEClassifiers().get(20);
		}
		return comparitorEEnum;
	}

	/**
	 * <!-- begin-user-doc -->
	 * <!-- end-user-doc -->
	 * @generated
	 */
	public Ecore4regFactory getEcore4regFactory() {
		return (Ecore4regFactory)getEFactoryInstance();
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
			eClassifier.setInstanceClassName("org.eclipse.efbt.ecore4reg.model.ecore4reg." + eClassifier.getName());
			setGeneratedClassName(eClassifier);
		}
	}

} //Ecore4regPackageImpl
