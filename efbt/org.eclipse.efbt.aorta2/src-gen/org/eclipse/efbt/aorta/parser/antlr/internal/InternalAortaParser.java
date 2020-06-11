package org.eclipse.efbt.aorta.parser.antlr.internal;

import org.eclipse.xtext.*;
import org.eclipse.xtext.parser.*;
import org.eclipse.xtext.parser.impl.*;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.xtext.parser.antlr.AbstractInternalAntlrParser;
import org.eclipse.xtext.parser.antlr.XtextTokenStream;
import org.eclipse.xtext.parser.antlr.XtextTokenStream.HiddenTokens;
import org.eclipse.xtext.parser.antlr.AntlrDatatypeRuleToken;
import org.eclipse.efbt.aorta.services.AortaGrammarAccess;



import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

@SuppressWarnings("all")
public class InternalAortaParser extends AbstractInternalAntlrParser {
    public static final String[] tokenNames = new String[] {
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "RULE_STRING", "RULE_ID", "RULE_INT", "RULE_ML_COMMENT", "RULE_SL_COMMENT", "RULE_WS", "RULE_ANY_OTHER", "'Program'", "'{'", "'domainModule'", "'requirements'", "'input_structures'", "'reportStructures'", "'testDefinitions'", "'tests'", "'requirementsArtefacts'", "'testTemplates'", "'testConstriants'", "'functionalityModules'", "'}'", "'ColumnDomainModule'", "'theDescription'", "'license'", "'version'", "'dependencies'", "'longName'", "'columnDomains'", "','", "'RequirementsModule'", "'rules'", "'allowedtypes'", "'ColumnStructureModule'", "'columnStructures'", "'RegReportModule'", "'reportGroups'", "'TestDefinitionModule'", "'TestModule'", "'executionEnvironment'", "'InputTypeModule'", "'inputTypes'", "'TestTemplateModule'", "'templates'", "'TestContraints'", "'template'", "'givenParams'", "'whenParams'", "'thenParams'", "'FunctionalityModuleModule'", "'FunctionalityModules'", "'ModuleDependencies'", "'theModules'", "'ModuleLongName'", "'isEnumerated'", "'ColumnDomain'", "'enumMembers'", "'dataType'", "'ModuleDependency'", "'moduleName'", "'moduleVersion'", "'theModule'", "'EnumMember'", "'code'", "'ColumnDataType'", "'TitledRequirementsSection'", "'title'", "'referencingSections'", "'requirementsType'", "'sections'", "'AllowedTypes'", "'allowedTypes'", "'RequirementsSectionLinkWithText'", "'linkText'", "'subsection'", "'linkedRuleSection'", "'RequirementType'", "'RequirementsSectionImage'", "'style'", "'uri'", "'RequirementsSectionText'", "'text'", "'ColumnStructuredEntity'", "'column'", "'isIdentifier'", "'isMandatory'", "'Column'", "'domain'", "'ReportGroup'", "'regReports'", "'RegReport'", "'cells'", "'RegReportCell'", "'TestDefinition'", "'testContraints'", "'given'", "'when'", "'then'", "'Given'", "'regulatoryTextLinks'", "'('", "')'", "'params'", "'When'", "'Then'", "'E2ETestDefinition'", "'UnitTestDefinition'", "'ClauseText'", "'Param'", "'param'", "'E2ETest'", "'testDefinition'", "'inputData'", "'expectedResults'", "'TestInputData'", "'Result'", "'TestColumnStructuredData'", "'sourceTableData'", "'TestStrucuredFileData'", "'testFileData'", "'BaseColumnStructuredData'", "'cube'", "'rows'", "'BaseRowData'", "'BaseCellWithEnumeratedValue'", "'value'", "'BaseCellWithValue'", "'StructuredFile'", "'fileLocation'", "'ReportCellResults'", "'reportCellValues'", "'DataSetResults'", "'RegReportCellValue'", "'cell'", "'ColumnStructuredData'", "'InputType'", "'TestTemplate'", "'givenText'", "'whenText'", "'thenText'", "'FunctionalityModule'", "'usedTransformationOrScheme'", "'scenario'", "'scenarioSet'", "'Scenario'", "'ScenarioSet'", "'scenarios'", "'DataProcessingFunctionalityModule'", "'LeafFunctionalityModule'", "'ParallelFunctionalityModule'", "'subTransformationsAndSchemes'", "'ScenarioSetFunctionalityModule'", "'scenarioSetChoice'", "'SerialFunctionalityModule'", "'FunctionalityModuleSet'", "'ReadDataSourceFunctionalityModule'", "'OutputCreationFunctionalityModule'", "'ValidationFunctionalityModule'", "'.'"
    };
    public static final int T__144=144;
    public static final int T__143=143;
    public static final int T__146=146;
    public static final int T__50=50;
    public static final int T__145=145;
    public static final int T__140=140;
    public static final int T__142=142;
    public static final int T__141=141;
    public static final int T__59=59;
    public static final int T__55=55;
    public static final int T__56=56;
    public static final int T__57=57;
    public static final int T__58=58;
    public static final int T__51=51;
    public static final int T__137=137;
    public static final int T__52=52;
    public static final int T__136=136;
    public static final int T__53=53;
    public static final int T__139=139;
    public static final int T__54=54;
    public static final int T__138=138;
    public static final int T__133=133;
    public static final int T__132=132;
    public static final int T__60=60;
    public static final int T__135=135;
    public static final int T__61=61;
    public static final int T__134=134;
    public static final int RULE_ID=5;
    public static final int T__131=131;
    public static final int T__130=130;
    public static final int RULE_INT=6;
    public static final int T__66=66;
    public static final int RULE_ML_COMMENT=7;
    public static final int T__67=67;
    public static final int T__129=129;
    public static final int T__68=68;
    public static final int T__69=69;
    public static final int T__62=62;
    public static final int T__126=126;
    public static final int T__63=63;
    public static final int T__125=125;
    public static final int T__64=64;
    public static final int T__128=128;
    public static final int T__65=65;
    public static final int T__127=127;
    public static final int T__160=160;
    public static final int T__37=37;
    public static final int T__38=38;
    public static final int T__39=39;
    public static final int T__33=33;
    public static final int T__34=34;
    public static final int T__35=35;
    public static final int T__36=36;
    public static final int T__159=159;
    public static final int T__30=30;
    public static final int T__158=158;
    public static final int T__31=31;
    public static final int T__32=32;
    public static final int T__155=155;
    public static final int T__154=154;
    public static final int T__157=157;
    public static final int T__156=156;
    public static final int T__151=151;
    public static final int T__150=150;
    public static final int T__153=153;
    public static final int T__152=152;
    public static final int T__48=48;
    public static final int T__49=49;
    public static final int T__44=44;
    public static final int T__45=45;
    public static final int T__46=46;
    public static final int T__47=47;
    public static final int T__40=40;
    public static final int T__148=148;
    public static final int T__41=41;
    public static final int T__147=147;
    public static final int T__42=42;
    public static final int T__43=43;
    public static final int T__149=149;
    public static final int T__91=91;
    public static final int T__100=100;
    public static final int T__92=92;
    public static final int T__93=93;
    public static final int T__102=102;
    public static final int T__94=94;
    public static final int T__101=101;
    public static final int T__90=90;
    public static final int T__19=19;
    public static final int T__15=15;
    public static final int T__16=16;
    public static final int T__17=17;
    public static final int T__18=18;
    public static final int T__11=11;
    public static final int T__99=99;
    public static final int T__12=12;
    public static final int T__13=13;
    public static final int T__14=14;
    public static final int T__95=95;
    public static final int T__96=96;
    public static final int T__97=97;
    public static final int T__98=98;
    public static final int T__26=26;
    public static final int T__27=27;
    public static final int T__28=28;
    public static final int T__29=29;
    public static final int T__22=22;
    public static final int T__23=23;
    public static final int T__24=24;
    public static final int T__25=25;
    public static final int T__20=20;
    public static final int T__21=21;
    public static final int T__122=122;
    public static final int T__70=70;
    public static final int T__121=121;
    public static final int T__71=71;
    public static final int T__124=124;
    public static final int T__72=72;
    public static final int T__123=123;
    public static final int T__120=120;
    public static final int RULE_STRING=4;
    public static final int RULE_SL_COMMENT=8;
    public static final int T__77=77;
    public static final int T__119=119;
    public static final int T__78=78;
    public static final int T__118=118;
    public static final int T__79=79;
    public static final int T__73=73;
    public static final int T__115=115;
    public static final int EOF=-1;
    public static final int T__74=74;
    public static final int T__114=114;
    public static final int T__75=75;
    public static final int T__117=117;
    public static final int T__76=76;
    public static final int T__116=116;
    public static final int T__80=80;
    public static final int T__111=111;
    public static final int T__81=81;
    public static final int T__110=110;
    public static final int T__82=82;
    public static final int T__113=113;
    public static final int T__83=83;
    public static final int T__112=112;
    public static final int RULE_WS=9;
    public static final int RULE_ANY_OTHER=10;
    public static final int T__88=88;
    public static final int T__108=108;
    public static final int T__89=89;
    public static final int T__107=107;
    public static final int T__109=109;
    public static final int T__84=84;
    public static final int T__104=104;
    public static final int T__85=85;
    public static final int T__103=103;
    public static final int T__86=86;
    public static final int T__106=106;
    public static final int T__87=87;
    public static final int T__105=105;

    // delegates
    // delegators


        public InternalAortaParser(TokenStream input) {
            this(input, new RecognizerSharedState());
        }
        public InternalAortaParser(TokenStream input, RecognizerSharedState state) {
            super(input, state);
             
        }
        

    public String[] getTokenNames() { return InternalAortaParser.tokenNames; }
    public String getGrammarFileName() { return "InternalAorta.g"; }



     	private AortaGrammarAccess grammarAccess;

        public InternalAortaParser(TokenStream input, AortaGrammarAccess grammarAccess) {
            this(input);
            this.grammarAccess = grammarAccess;
            registerRules(grammarAccess.getGrammar());
        }

        @Override
        protected String getFirstRuleName() {
        	return "Program";
       	}

       	@Override
       	protected AortaGrammarAccess getGrammarAccess() {
       		return grammarAccess;
       	}




    // $ANTLR start "entryRuleProgram"
    // InternalAorta.g:64:1: entryRuleProgram returns [EObject current=null] : iv_ruleProgram= ruleProgram EOF ;
    public final EObject entryRuleProgram() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleProgram = null;


        try {
            // InternalAorta.g:64:48: (iv_ruleProgram= ruleProgram EOF )
            // InternalAorta.g:65:2: iv_ruleProgram= ruleProgram EOF
            {
             newCompositeNode(grammarAccess.getProgramRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleProgram=ruleProgram();

            state._fsp--;

             current =iv_ruleProgram; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleProgram"


    // $ANTLR start "ruleProgram"
    // InternalAorta.g:71:1: ruleProgram returns [EObject current=null] : ( () otherlv_1= 'Program' otherlv_2= '{' (otherlv_3= 'domainModule' ( (lv_domainModule_4_0= ruleColumnDomainModule ) ) )? (otherlv_5= 'requirements' ( (lv_requirements_6_0= ruleRequirementsModule ) ) )? (otherlv_7= 'input_structures' ( (lv_input_structures_8_0= ruleColumnStructureModule ) ) )? (otherlv_9= 'reportStructures' ( (lv_reportStructures_10_0= ruleRegReportModule ) ) )? (otherlv_11= 'testDefinitions' ( (lv_testDefinitions_12_0= ruleTestDefinitionModule ) ) )? (otherlv_13= 'tests' ( (lv_tests_14_0= ruleTestModule ) ) )? (otherlv_15= 'requirementsArtefacts' ( (lv_requirementsArtefacts_16_0= ruleInputTypeModule ) ) )? (otherlv_17= 'testTemplates' ( (lv_testTemplates_18_0= ruleTestTemplateModule ) ) )? (otherlv_19= 'testConstriants' ( (lv_testConstriants_20_0= ruleTestContraints ) ) )? (otherlv_21= 'functionalityModules' ( (lv_functionalityModules_22_0= ruleFunctionalityModuleModule ) ) )? otherlv_23= '}' ) ;
    public final EObject ruleProgram() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_2=null;
        Token otherlv_3=null;
        Token otherlv_5=null;
        Token otherlv_7=null;
        Token otherlv_9=null;
        Token otherlv_11=null;
        Token otherlv_13=null;
        Token otherlv_15=null;
        Token otherlv_17=null;
        Token otherlv_19=null;
        Token otherlv_21=null;
        Token otherlv_23=null;
        EObject lv_domainModule_4_0 = null;

        EObject lv_requirements_6_0 = null;

        EObject lv_input_structures_8_0 = null;

        EObject lv_reportStructures_10_0 = null;

        EObject lv_testDefinitions_12_0 = null;

        EObject lv_tests_14_0 = null;

        EObject lv_requirementsArtefacts_16_0 = null;

        EObject lv_testTemplates_18_0 = null;

        EObject lv_testConstriants_20_0 = null;

        EObject lv_functionalityModules_22_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:77:2: ( ( () otherlv_1= 'Program' otherlv_2= '{' (otherlv_3= 'domainModule' ( (lv_domainModule_4_0= ruleColumnDomainModule ) ) )? (otherlv_5= 'requirements' ( (lv_requirements_6_0= ruleRequirementsModule ) ) )? (otherlv_7= 'input_structures' ( (lv_input_structures_8_0= ruleColumnStructureModule ) ) )? (otherlv_9= 'reportStructures' ( (lv_reportStructures_10_0= ruleRegReportModule ) ) )? (otherlv_11= 'testDefinitions' ( (lv_testDefinitions_12_0= ruleTestDefinitionModule ) ) )? (otherlv_13= 'tests' ( (lv_tests_14_0= ruleTestModule ) ) )? (otherlv_15= 'requirementsArtefacts' ( (lv_requirementsArtefacts_16_0= ruleInputTypeModule ) ) )? (otherlv_17= 'testTemplates' ( (lv_testTemplates_18_0= ruleTestTemplateModule ) ) )? (otherlv_19= 'testConstriants' ( (lv_testConstriants_20_0= ruleTestContraints ) ) )? (otherlv_21= 'functionalityModules' ( (lv_functionalityModules_22_0= ruleFunctionalityModuleModule ) ) )? otherlv_23= '}' ) )
            // InternalAorta.g:78:2: ( () otherlv_1= 'Program' otherlv_2= '{' (otherlv_3= 'domainModule' ( (lv_domainModule_4_0= ruleColumnDomainModule ) ) )? (otherlv_5= 'requirements' ( (lv_requirements_6_0= ruleRequirementsModule ) ) )? (otherlv_7= 'input_structures' ( (lv_input_structures_8_0= ruleColumnStructureModule ) ) )? (otherlv_9= 'reportStructures' ( (lv_reportStructures_10_0= ruleRegReportModule ) ) )? (otherlv_11= 'testDefinitions' ( (lv_testDefinitions_12_0= ruleTestDefinitionModule ) ) )? (otherlv_13= 'tests' ( (lv_tests_14_0= ruleTestModule ) ) )? (otherlv_15= 'requirementsArtefacts' ( (lv_requirementsArtefacts_16_0= ruleInputTypeModule ) ) )? (otherlv_17= 'testTemplates' ( (lv_testTemplates_18_0= ruleTestTemplateModule ) ) )? (otherlv_19= 'testConstriants' ( (lv_testConstriants_20_0= ruleTestContraints ) ) )? (otherlv_21= 'functionalityModules' ( (lv_functionalityModules_22_0= ruleFunctionalityModuleModule ) ) )? otherlv_23= '}' )
            {
            // InternalAorta.g:78:2: ( () otherlv_1= 'Program' otherlv_2= '{' (otherlv_3= 'domainModule' ( (lv_domainModule_4_0= ruleColumnDomainModule ) ) )? (otherlv_5= 'requirements' ( (lv_requirements_6_0= ruleRequirementsModule ) ) )? (otherlv_7= 'input_structures' ( (lv_input_structures_8_0= ruleColumnStructureModule ) ) )? (otherlv_9= 'reportStructures' ( (lv_reportStructures_10_0= ruleRegReportModule ) ) )? (otherlv_11= 'testDefinitions' ( (lv_testDefinitions_12_0= ruleTestDefinitionModule ) ) )? (otherlv_13= 'tests' ( (lv_tests_14_0= ruleTestModule ) ) )? (otherlv_15= 'requirementsArtefacts' ( (lv_requirementsArtefacts_16_0= ruleInputTypeModule ) ) )? (otherlv_17= 'testTemplates' ( (lv_testTemplates_18_0= ruleTestTemplateModule ) ) )? (otherlv_19= 'testConstriants' ( (lv_testConstriants_20_0= ruleTestContraints ) ) )? (otherlv_21= 'functionalityModules' ( (lv_functionalityModules_22_0= ruleFunctionalityModuleModule ) ) )? otherlv_23= '}' )
            // InternalAorta.g:79:3: () otherlv_1= 'Program' otherlv_2= '{' (otherlv_3= 'domainModule' ( (lv_domainModule_4_0= ruleColumnDomainModule ) ) )? (otherlv_5= 'requirements' ( (lv_requirements_6_0= ruleRequirementsModule ) ) )? (otherlv_7= 'input_structures' ( (lv_input_structures_8_0= ruleColumnStructureModule ) ) )? (otherlv_9= 'reportStructures' ( (lv_reportStructures_10_0= ruleRegReportModule ) ) )? (otherlv_11= 'testDefinitions' ( (lv_testDefinitions_12_0= ruleTestDefinitionModule ) ) )? (otherlv_13= 'tests' ( (lv_tests_14_0= ruleTestModule ) ) )? (otherlv_15= 'requirementsArtefacts' ( (lv_requirementsArtefacts_16_0= ruleInputTypeModule ) ) )? (otherlv_17= 'testTemplates' ( (lv_testTemplates_18_0= ruleTestTemplateModule ) ) )? (otherlv_19= 'testConstriants' ( (lv_testConstriants_20_0= ruleTestContraints ) ) )? (otherlv_21= 'functionalityModules' ( (lv_functionalityModules_22_0= ruleFunctionalityModuleModule ) ) )? otherlv_23= '}'
            {
            // InternalAorta.g:79:3: ()
            // InternalAorta.g:80:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getProgramAccess().getProgramAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,11,FOLLOW_3); 

            			newLeafNode(otherlv_1, grammarAccess.getProgramAccess().getProgramKeyword_1());
            		
            otherlv_2=(Token)match(input,12,FOLLOW_4); 

            			newLeafNode(otherlv_2, grammarAccess.getProgramAccess().getLeftCurlyBracketKeyword_2());
            		
            // InternalAorta.g:94:3: (otherlv_3= 'domainModule' ( (lv_domainModule_4_0= ruleColumnDomainModule ) ) )?
            int alt1=2;
            int LA1_0 = input.LA(1);

            if ( (LA1_0==13) ) {
                alt1=1;
            }
            switch (alt1) {
                case 1 :
                    // InternalAorta.g:95:4: otherlv_3= 'domainModule' ( (lv_domainModule_4_0= ruleColumnDomainModule ) )
                    {
                    otherlv_3=(Token)match(input,13,FOLLOW_5); 

                    				newLeafNode(otherlv_3, grammarAccess.getProgramAccess().getDomainModuleKeyword_3_0());
                    			
                    // InternalAorta.g:99:4: ( (lv_domainModule_4_0= ruleColumnDomainModule ) )
                    // InternalAorta.g:100:5: (lv_domainModule_4_0= ruleColumnDomainModule )
                    {
                    // InternalAorta.g:100:5: (lv_domainModule_4_0= ruleColumnDomainModule )
                    // InternalAorta.g:101:6: lv_domainModule_4_0= ruleColumnDomainModule
                    {

                    						newCompositeNode(grammarAccess.getProgramAccess().getDomainModuleColumnDomainModuleParserRuleCall_3_1_0());
                    					
                    pushFollow(FOLLOW_6);
                    lv_domainModule_4_0=ruleColumnDomainModule();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getProgramRule());
                    						}
                    						set(
                    							current,
                    							"domainModule",
                    							lv_domainModule_4_0,
                    							"org.eclipse.efbt.aorta.Aorta.ColumnDomainModule");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:119:3: (otherlv_5= 'requirements' ( (lv_requirements_6_0= ruleRequirementsModule ) ) )?
            int alt2=2;
            int LA2_0 = input.LA(1);

            if ( (LA2_0==14) ) {
                alt2=1;
            }
            switch (alt2) {
                case 1 :
                    // InternalAorta.g:120:4: otherlv_5= 'requirements' ( (lv_requirements_6_0= ruleRequirementsModule ) )
                    {
                    otherlv_5=(Token)match(input,14,FOLLOW_7); 

                    				newLeafNode(otherlv_5, grammarAccess.getProgramAccess().getRequirementsKeyword_4_0());
                    			
                    // InternalAorta.g:124:4: ( (lv_requirements_6_0= ruleRequirementsModule ) )
                    // InternalAorta.g:125:5: (lv_requirements_6_0= ruleRequirementsModule )
                    {
                    // InternalAorta.g:125:5: (lv_requirements_6_0= ruleRequirementsModule )
                    // InternalAorta.g:126:6: lv_requirements_6_0= ruleRequirementsModule
                    {

                    						newCompositeNode(grammarAccess.getProgramAccess().getRequirementsRequirementsModuleParserRuleCall_4_1_0());
                    					
                    pushFollow(FOLLOW_8);
                    lv_requirements_6_0=ruleRequirementsModule();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getProgramRule());
                    						}
                    						set(
                    							current,
                    							"requirements",
                    							lv_requirements_6_0,
                    							"org.eclipse.efbt.aorta.Aorta.RequirementsModule");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:144:3: (otherlv_7= 'input_structures' ( (lv_input_structures_8_0= ruleColumnStructureModule ) ) )?
            int alt3=2;
            int LA3_0 = input.LA(1);

            if ( (LA3_0==15) ) {
                alt3=1;
            }
            switch (alt3) {
                case 1 :
                    // InternalAorta.g:145:4: otherlv_7= 'input_structures' ( (lv_input_structures_8_0= ruleColumnStructureModule ) )
                    {
                    otherlv_7=(Token)match(input,15,FOLLOW_9); 

                    				newLeafNode(otherlv_7, grammarAccess.getProgramAccess().getInput_structuresKeyword_5_0());
                    			
                    // InternalAorta.g:149:4: ( (lv_input_structures_8_0= ruleColumnStructureModule ) )
                    // InternalAorta.g:150:5: (lv_input_structures_8_0= ruleColumnStructureModule )
                    {
                    // InternalAorta.g:150:5: (lv_input_structures_8_0= ruleColumnStructureModule )
                    // InternalAorta.g:151:6: lv_input_structures_8_0= ruleColumnStructureModule
                    {

                    						newCompositeNode(grammarAccess.getProgramAccess().getInput_structuresColumnStructureModuleParserRuleCall_5_1_0());
                    					
                    pushFollow(FOLLOW_10);
                    lv_input_structures_8_0=ruleColumnStructureModule();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getProgramRule());
                    						}
                    						set(
                    							current,
                    							"input_structures",
                    							lv_input_structures_8_0,
                    							"org.eclipse.efbt.aorta.Aorta.ColumnStructureModule");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:169:3: (otherlv_9= 'reportStructures' ( (lv_reportStructures_10_0= ruleRegReportModule ) ) )?
            int alt4=2;
            int LA4_0 = input.LA(1);

            if ( (LA4_0==16) ) {
                alt4=1;
            }
            switch (alt4) {
                case 1 :
                    // InternalAorta.g:170:4: otherlv_9= 'reportStructures' ( (lv_reportStructures_10_0= ruleRegReportModule ) )
                    {
                    otherlv_9=(Token)match(input,16,FOLLOW_11); 

                    				newLeafNode(otherlv_9, grammarAccess.getProgramAccess().getReportStructuresKeyword_6_0());
                    			
                    // InternalAorta.g:174:4: ( (lv_reportStructures_10_0= ruleRegReportModule ) )
                    // InternalAorta.g:175:5: (lv_reportStructures_10_0= ruleRegReportModule )
                    {
                    // InternalAorta.g:175:5: (lv_reportStructures_10_0= ruleRegReportModule )
                    // InternalAorta.g:176:6: lv_reportStructures_10_0= ruleRegReportModule
                    {

                    						newCompositeNode(grammarAccess.getProgramAccess().getReportStructuresRegReportModuleParserRuleCall_6_1_0());
                    					
                    pushFollow(FOLLOW_12);
                    lv_reportStructures_10_0=ruleRegReportModule();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getProgramRule());
                    						}
                    						set(
                    							current,
                    							"reportStructures",
                    							lv_reportStructures_10_0,
                    							"org.eclipse.efbt.aorta.Aorta.RegReportModule");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:194:3: (otherlv_11= 'testDefinitions' ( (lv_testDefinitions_12_0= ruleTestDefinitionModule ) ) )?
            int alt5=2;
            int LA5_0 = input.LA(1);

            if ( (LA5_0==17) ) {
                alt5=1;
            }
            switch (alt5) {
                case 1 :
                    // InternalAorta.g:195:4: otherlv_11= 'testDefinitions' ( (lv_testDefinitions_12_0= ruleTestDefinitionModule ) )
                    {
                    otherlv_11=(Token)match(input,17,FOLLOW_13); 

                    				newLeafNode(otherlv_11, grammarAccess.getProgramAccess().getTestDefinitionsKeyword_7_0());
                    			
                    // InternalAorta.g:199:4: ( (lv_testDefinitions_12_0= ruleTestDefinitionModule ) )
                    // InternalAorta.g:200:5: (lv_testDefinitions_12_0= ruleTestDefinitionModule )
                    {
                    // InternalAorta.g:200:5: (lv_testDefinitions_12_0= ruleTestDefinitionModule )
                    // InternalAorta.g:201:6: lv_testDefinitions_12_0= ruleTestDefinitionModule
                    {

                    						newCompositeNode(grammarAccess.getProgramAccess().getTestDefinitionsTestDefinitionModuleParserRuleCall_7_1_0());
                    					
                    pushFollow(FOLLOW_14);
                    lv_testDefinitions_12_0=ruleTestDefinitionModule();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getProgramRule());
                    						}
                    						set(
                    							current,
                    							"testDefinitions",
                    							lv_testDefinitions_12_0,
                    							"org.eclipse.efbt.aorta.Aorta.TestDefinitionModule");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:219:3: (otherlv_13= 'tests' ( (lv_tests_14_0= ruleTestModule ) ) )?
            int alt6=2;
            int LA6_0 = input.LA(1);

            if ( (LA6_0==18) ) {
                alt6=1;
            }
            switch (alt6) {
                case 1 :
                    // InternalAorta.g:220:4: otherlv_13= 'tests' ( (lv_tests_14_0= ruleTestModule ) )
                    {
                    otherlv_13=(Token)match(input,18,FOLLOW_15); 

                    				newLeafNode(otherlv_13, grammarAccess.getProgramAccess().getTestsKeyword_8_0());
                    			
                    // InternalAorta.g:224:4: ( (lv_tests_14_0= ruleTestModule ) )
                    // InternalAorta.g:225:5: (lv_tests_14_0= ruleTestModule )
                    {
                    // InternalAorta.g:225:5: (lv_tests_14_0= ruleTestModule )
                    // InternalAorta.g:226:6: lv_tests_14_0= ruleTestModule
                    {

                    						newCompositeNode(grammarAccess.getProgramAccess().getTestsTestModuleParserRuleCall_8_1_0());
                    					
                    pushFollow(FOLLOW_16);
                    lv_tests_14_0=ruleTestModule();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getProgramRule());
                    						}
                    						set(
                    							current,
                    							"tests",
                    							lv_tests_14_0,
                    							"org.eclipse.efbt.aorta.Aorta.TestModule");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:244:3: (otherlv_15= 'requirementsArtefacts' ( (lv_requirementsArtefacts_16_0= ruleInputTypeModule ) ) )?
            int alt7=2;
            int LA7_0 = input.LA(1);

            if ( (LA7_0==19) ) {
                alt7=1;
            }
            switch (alt7) {
                case 1 :
                    // InternalAorta.g:245:4: otherlv_15= 'requirementsArtefacts' ( (lv_requirementsArtefacts_16_0= ruleInputTypeModule ) )
                    {
                    otherlv_15=(Token)match(input,19,FOLLOW_17); 

                    				newLeafNode(otherlv_15, grammarAccess.getProgramAccess().getRequirementsArtefactsKeyword_9_0());
                    			
                    // InternalAorta.g:249:4: ( (lv_requirementsArtefacts_16_0= ruleInputTypeModule ) )
                    // InternalAorta.g:250:5: (lv_requirementsArtefacts_16_0= ruleInputTypeModule )
                    {
                    // InternalAorta.g:250:5: (lv_requirementsArtefacts_16_0= ruleInputTypeModule )
                    // InternalAorta.g:251:6: lv_requirementsArtefacts_16_0= ruleInputTypeModule
                    {

                    						newCompositeNode(grammarAccess.getProgramAccess().getRequirementsArtefactsInputTypeModuleParserRuleCall_9_1_0());
                    					
                    pushFollow(FOLLOW_18);
                    lv_requirementsArtefacts_16_0=ruleInputTypeModule();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getProgramRule());
                    						}
                    						set(
                    							current,
                    							"requirementsArtefacts",
                    							lv_requirementsArtefacts_16_0,
                    							"org.eclipse.efbt.aorta.Aorta.InputTypeModule");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:269:3: (otherlv_17= 'testTemplates' ( (lv_testTemplates_18_0= ruleTestTemplateModule ) ) )?
            int alt8=2;
            int LA8_0 = input.LA(1);

            if ( (LA8_0==20) ) {
                alt8=1;
            }
            switch (alt8) {
                case 1 :
                    // InternalAorta.g:270:4: otherlv_17= 'testTemplates' ( (lv_testTemplates_18_0= ruleTestTemplateModule ) )
                    {
                    otherlv_17=(Token)match(input,20,FOLLOW_19); 

                    				newLeafNode(otherlv_17, grammarAccess.getProgramAccess().getTestTemplatesKeyword_10_0());
                    			
                    // InternalAorta.g:274:4: ( (lv_testTemplates_18_0= ruleTestTemplateModule ) )
                    // InternalAorta.g:275:5: (lv_testTemplates_18_0= ruleTestTemplateModule )
                    {
                    // InternalAorta.g:275:5: (lv_testTemplates_18_0= ruleTestTemplateModule )
                    // InternalAorta.g:276:6: lv_testTemplates_18_0= ruleTestTemplateModule
                    {

                    						newCompositeNode(grammarAccess.getProgramAccess().getTestTemplatesTestTemplateModuleParserRuleCall_10_1_0());
                    					
                    pushFollow(FOLLOW_20);
                    lv_testTemplates_18_0=ruleTestTemplateModule();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getProgramRule());
                    						}
                    						set(
                    							current,
                    							"testTemplates",
                    							lv_testTemplates_18_0,
                    							"org.eclipse.efbt.aorta.Aorta.TestTemplateModule");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:294:3: (otherlv_19= 'testConstriants' ( (lv_testConstriants_20_0= ruleTestContraints ) ) )?
            int alt9=2;
            int LA9_0 = input.LA(1);

            if ( (LA9_0==21) ) {
                alt9=1;
            }
            switch (alt9) {
                case 1 :
                    // InternalAorta.g:295:4: otherlv_19= 'testConstriants' ( (lv_testConstriants_20_0= ruleTestContraints ) )
                    {
                    otherlv_19=(Token)match(input,21,FOLLOW_21); 

                    				newLeafNode(otherlv_19, grammarAccess.getProgramAccess().getTestConstriantsKeyword_11_0());
                    			
                    // InternalAorta.g:299:4: ( (lv_testConstriants_20_0= ruleTestContraints ) )
                    // InternalAorta.g:300:5: (lv_testConstriants_20_0= ruleTestContraints )
                    {
                    // InternalAorta.g:300:5: (lv_testConstriants_20_0= ruleTestContraints )
                    // InternalAorta.g:301:6: lv_testConstriants_20_0= ruleTestContraints
                    {

                    						newCompositeNode(grammarAccess.getProgramAccess().getTestConstriantsTestContraintsParserRuleCall_11_1_0());
                    					
                    pushFollow(FOLLOW_22);
                    lv_testConstriants_20_0=ruleTestContraints();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getProgramRule());
                    						}
                    						set(
                    							current,
                    							"testConstriants",
                    							lv_testConstriants_20_0,
                    							"org.eclipse.efbt.aorta.Aorta.TestContraints");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:319:3: (otherlv_21= 'functionalityModules' ( (lv_functionalityModules_22_0= ruleFunctionalityModuleModule ) ) )?
            int alt10=2;
            int LA10_0 = input.LA(1);

            if ( (LA10_0==22) ) {
                alt10=1;
            }
            switch (alt10) {
                case 1 :
                    // InternalAorta.g:320:4: otherlv_21= 'functionalityModules' ( (lv_functionalityModules_22_0= ruleFunctionalityModuleModule ) )
                    {
                    otherlv_21=(Token)match(input,22,FOLLOW_23); 

                    				newLeafNode(otherlv_21, grammarAccess.getProgramAccess().getFunctionalityModulesKeyword_12_0());
                    			
                    // InternalAorta.g:324:4: ( (lv_functionalityModules_22_0= ruleFunctionalityModuleModule ) )
                    // InternalAorta.g:325:5: (lv_functionalityModules_22_0= ruleFunctionalityModuleModule )
                    {
                    // InternalAorta.g:325:5: (lv_functionalityModules_22_0= ruleFunctionalityModuleModule )
                    // InternalAorta.g:326:6: lv_functionalityModules_22_0= ruleFunctionalityModuleModule
                    {

                    						newCompositeNode(grammarAccess.getProgramAccess().getFunctionalityModulesFunctionalityModuleModuleParserRuleCall_12_1_0());
                    					
                    pushFollow(FOLLOW_24);
                    lv_functionalityModules_22_0=ruleFunctionalityModuleModule();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getProgramRule());
                    						}
                    						set(
                    							current,
                    							"functionalityModules",
                    							lv_functionalityModules_22_0,
                    							"org.eclipse.efbt.aorta.Aorta.FunctionalityModuleModule");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            otherlv_23=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_23, grammarAccess.getProgramAccess().getRightCurlyBracketKeyword_13());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleProgram"


    // $ANTLR start "entryRuleTestDefinition"
    // InternalAorta.g:352:1: entryRuleTestDefinition returns [EObject current=null] : iv_ruleTestDefinition= ruleTestDefinition EOF ;
    public final EObject entryRuleTestDefinition() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleTestDefinition = null;


        try {
            // InternalAorta.g:352:55: (iv_ruleTestDefinition= ruleTestDefinition EOF )
            // InternalAorta.g:353:2: iv_ruleTestDefinition= ruleTestDefinition EOF
            {
             newCompositeNode(grammarAccess.getTestDefinitionRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleTestDefinition=ruleTestDefinition();

            state._fsp--;

             current =iv_ruleTestDefinition; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleTestDefinition"


    // $ANTLR start "ruleTestDefinition"
    // InternalAorta.g:359:1: ruleTestDefinition returns [EObject current=null] : (this_TestDefinition_Impl_0= ruleTestDefinition_Impl | this_E2ETestDefinition_1= ruleE2ETestDefinition | this_UnitTestDefinition_2= ruleUnitTestDefinition ) ;
    public final EObject ruleTestDefinition() throws RecognitionException {
        EObject current = null;

        EObject this_TestDefinition_Impl_0 = null;

        EObject this_E2ETestDefinition_1 = null;

        EObject this_UnitTestDefinition_2 = null;



        	enterRule();

        try {
            // InternalAorta.g:365:2: ( (this_TestDefinition_Impl_0= ruleTestDefinition_Impl | this_E2ETestDefinition_1= ruleE2ETestDefinition | this_UnitTestDefinition_2= ruleUnitTestDefinition ) )
            // InternalAorta.g:366:2: (this_TestDefinition_Impl_0= ruleTestDefinition_Impl | this_E2ETestDefinition_1= ruleE2ETestDefinition | this_UnitTestDefinition_2= ruleUnitTestDefinition )
            {
            // InternalAorta.g:366:2: (this_TestDefinition_Impl_0= ruleTestDefinition_Impl | this_E2ETestDefinition_1= ruleE2ETestDefinition | this_UnitTestDefinition_2= ruleUnitTestDefinition )
            int alt11=3;
            switch ( input.LA(1) ) {
            case 95:
                {
                alt11=1;
                }
                break;
            case 107:
                {
                alt11=2;
                }
                break;
            case 108:
                {
                alt11=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 11, 0, input);

                throw nvae;
            }

            switch (alt11) {
                case 1 :
                    // InternalAorta.g:367:3: this_TestDefinition_Impl_0= ruleTestDefinition_Impl
                    {

                    			newCompositeNode(grammarAccess.getTestDefinitionAccess().getTestDefinition_ImplParserRuleCall_0());
                    		
                    pushFollow(FOLLOW_2);
                    this_TestDefinition_Impl_0=ruleTestDefinition_Impl();

                    state._fsp--;


                    			current = this_TestDefinition_Impl_0;
                    			afterParserOrEnumRuleCall();
                    		

                    }
                    break;
                case 2 :
                    // InternalAorta.g:376:3: this_E2ETestDefinition_1= ruleE2ETestDefinition
                    {

                    			newCompositeNode(grammarAccess.getTestDefinitionAccess().getE2ETestDefinitionParserRuleCall_1());
                    		
                    pushFollow(FOLLOW_2);
                    this_E2ETestDefinition_1=ruleE2ETestDefinition();

                    state._fsp--;


                    			current = this_E2ETestDefinition_1;
                    			afterParserOrEnumRuleCall();
                    		

                    }
                    break;
                case 3 :
                    // InternalAorta.g:385:3: this_UnitTestDefinition_2= ruleUnitTestDefinition
                    {

                    			newCompositeNode(grammarAccess.getTestDefinitionAccess().getUnitTestDefinitionParserRuleCall_2());
                    		
                    pushFollow(FOLLOW_2);
                    this_UnitTestDefinition_2=ruleUnitTestDefinition();

                    state._fsp--;


                    			current = this_UnitTestDefinition_2;
                    			afterParserOrEnumRuleCall();
                    		

                    }
                    break;

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleTestDefinition"


    // $ANTLR start "entryRuleFunctionalityModule"
    // InternalAorta.g:397:1: entryRuleFunctionalityModule returns [EObject current=null] : iv_ruleFunctionalityModule= ruleFunctionalityModule EOF ;
    public final EObject entryRuleFunctionalityModule() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleFunctionalityModule = null;


        try {
            // InternalAorta.g:397:60: (iv_ruleFunctionalityModule= ruleFunctionalityModule EOF )
            // InternalAorta.g:398:2: iv_ruleFunctionalityModule= ruleFunctionalityModule EOF
            {
             newCompositeNode(grammarAccess.getFunctionalityModuleRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleFunctionalityModule=ruleFunctionalityModule();

            state._fsp--;

             current =iv_ruleFunctionalityModule; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleFunctionalityModule"


    // $ANTLR start "ruleFunctionalityModule"
    // InternalAorta.g:404:1: ruleFunctionalityModule returns [EObject current=null] : (this_FunctionalityModule_Impl_0= ruleFunctionalityModule_Impl | this_DataProcessingFunctionalityModule_1= ruleDataProcessingFunctionalityModule | this_LeafFunctionalityModule_Impl_2= ruleLeafFunctionalityModule_Impl | this_ParallelFunctionalityModule_3= ruleParallelFunctionalityModule | this_ScenarioSetFunctionalityModule_4= ruleScenarioSetFunctionalityModule | this_SerialFunctionalityModule_5= ruleSerialFunctionalityModule | this_FunctionalityModuleSet_Impl_6= ruleFunctionalityModuleSet_Impl | this_ReadDataSourceFunctionalityModule_7= ruleReadDataSourceFunctionalityModule | this_OutputCreationFunctionalityModule_8= ruleOutputCreationFunctionalityModule | this_ValidationFunctionalityModule_9= ruleValidationFunctionalityModule ) ;
    public final EObject ruleFunctionalityModule() throws RecognitionException {
        EObject current = null;

        EObject this_FunctionalityModule_Impl_0 = null;

        EObject this_DataProcessingFunctionalityModule_1 = null;

        EObject this_LeafFunctionalityModule_Impl_2 = null;

        EObject this_ParallelFunctionalityModule_3 = null;

        EObject this_ScenarioSetFunctionalityModule_4 = null;

        EObject this_SerialFunctionalityModule_5 = null;

        EObject this_FunctionalityModuleSet_Impl_6 = null;

        EObject this_ReadDataSourceFunctionalityModule_7 = null;

        EObject this_OutputCreationFunctionalityModule_8 = null;

        EObject this_ValidationFunctionalityModule_9 = null;



        	enterRule();

        try {
            // InternalAorta.g:410:2: ( (this_FunctionalityModule_Impl_0= ruleFunctionalityModule_Impl | this_DataProcessingFunctionalityModule_1= ruleDataProcessingFunctionalityModule | this_LeafFunctionalityModule_Impl_2= ruleLeafFunctionalityModule_Impl | this_ParallelFunctionalityModule_3= ruleParallelFunctionalityModule | this_ScenarioSetFunctionalityModule_4= ruleScenarioSetFunctionalityModule | this_SerialFunctionalityModule_5= ruleSerialFunctionalityModule | this_FunctionalityModuleSet_Impl_6= ruleFunctionalityModuleSet_Impl | this_ReadDataSourceFunctionalityModule_7= ruleReadDataSourceFunctionalityModule | this_OutputCreationFunctionalityModule_8= ruleOutputCreationFunctionalityModule | this_ValidationFunctionalityModule_9= ruleValidationFunctionalityModule ) )
            // InternalAorta.g:411:2: (this_FunctionalityModule_Impl_0= ruleFunctionalityModule_Impl | this_DataProcessingFunctionalityModule_1= ruleDataProcessingFunctionalityModule | this_LeafFunctionalityModule_Impl_2= ruleLeafFunctionalityModule_Impl | this_ParallelFunctionalityModule_3= ruleParallelFunctionalityModule | this_ScenarioSetFunctionalityModule_4= ruleScenarioSetFunctionalityModule | this_SerialFunctionalityModule_5= ruleSerialFunctionalityModule | this_FunctionalityModuleSet_Impl_6= ruleFunctionalityModuleSet_Impl | this_ReadDataSourceFunctionalityModule_7= ruleReadDataSourceFunctionalityModule | this_OutputCreationFunctionalityModule_8= ruleOutputCreationFunctionalityModule | this_ValidationFunctionalityModule_9= ruleValidationFunctionalityModule )
            {
            // InternalAorta.g:411:2: (this_FunctionalityModule_Impl_0= ruleFunctionalityModule_Impl | this_DataProcessingFunctionalityModule_1= ruleDataProcessingFunctionalityModule | this_LeafFunctionalityModule_Impl_2= ruleLeafFunctionalityModule_Impl | this_ParallelFunctionalityModule_3= ruleParallelFunctionalityModule | this_ScenarioSetFunctionalityModule_4= ruleScenarioSetFunctionalityModule | this_SerialFunctionalityModule_5= ruleSerialFunctionalityModule | this_FunctionalityModuleSet_Impl_6= ruleFunctionalityModuleSet_Impl | this_ReadDataSourceFunctionalityModule_7= ruleReadDataSourceFunctionalityModule | this_OutputCreationFunctionalityModule_8= ruleOutputCreationFunctionalityModule | this_ValidationFunctionalityModule_9= ruleValidationFunctionalityModule )
            int alt12=10;
            switch ( input.LA(1) ) {
            case 142:
                {
                alt12=1;
                }
                break;
            case 149:
                {
                alt12=2;
                }
                break;
            case 150:
                {
                alt12=3;
                }
                break;
            case 151:
                {
                alt12=4;
                }
                break;
            case 153:
                {
                alt12=5;
                }
                break;
            case 155:
                {
                alt12=6;
                }
                break;
            case 156:
                {
                alt12=7;
                }
                break;
            case 157:
                {
                alt12=8;
                }
                break;
            case 158:
                {
                alt12=9;
                }
                break;
            case 159:
                {
                alt12=10;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 12, 0, input);

                throw nvae;
            }

            switch (alt12) {
                case 1 :
                    // InternalAorta.g:412:3: this_FunctionalityModule_Impl_0= ruleFunctionalityModule_Impl
                    {

                    			newCompositeNode(grammarAccess.getFunctionalityModuleAccess().getFunctionalityModule_ImplParserRuleCall_0());
                    		
                    pushFollow(FOLLOW_2);
                    this_FunctionalityModule_Impl_0=ruleFunctionalityModule_Impl();

                    state._fsp--;


                    			current = this_FunctionalityModule_Impl_0;
                    			afterParserOrEnumRuleCall();
                    		

                    }
                    break;
                case 2 :
                    // InternalAorta.g:421:3: this_DataProcessingFunctionalityModule_1= ruleDataProcessingFunctionalityModule
                    {

                    			newCompositeNode(grammarAccess.getFunctionalityModuleAccess().getDataProcessingFunctionalityModuleParserRuleCall_1());
                    		
                    pushFollow(FOLLOW_2);
                    this_DataProcessingFunctionalityModule_1=ruleDataProcessingFunctionalityModule();

                    state._fsp--;


                    			current = this_DataProcessingFunctionalityModule_1;
                    			afterParserOrEnumRuleCall();
                    		

                    }
                    break;
                case 3 :
                    // InternalAorta.g:430:3: this_LeafFunctionalityModule_Impl_2= ruleLeafFunctionalityModule_Impl
                    {

                    			newCompositeNode(grammarAccess.getFunctionalityModuleAccess().getLeafFunctionalityModule_ImplParserRuleCall_2());
                    		
                    pushFollow(FOLLOW_2);
                    this_LeafFunctionalityModule_Impl_2=ruleLeafFunctionalityModule_Impl();

                    state._fsp--;


                    			current = this_LeafFunctionalityModule_Impl_2;
                    			afterParserOrEnumRuleCall();
                    		

                    }
                    break;
                case 4 :
                    // InternalAorta.g:439:3: this_ParallelFunctionalityModule_3= ruleParallelFunctionalityModule
                    {

                    			newCompositeNode(grammarAccess.getFunctionalityModuleAccess().getParallelFunctionalityModuleParserRuleCall_3());
                    		
                    pushFollow(FOLLOW_2);
                    this_ParallelFunctionalityModule_3=ruleParallelFunctionalityModule();

                    state._fsp--;


                    			current = this_ParallelFunctionalityModule_3;
                    			afterParserOrEnumRuleCall();
                    		

                    }
                    break;
                case 5 :
                    // InternalAorta.g:448:3: this_ScenarioSetFunctionalityModule_4= ruleScenarioSetFunctionalityModule
                    {

                    			newCompositeNode(grammarAccess.getFunctionalityModuleAccess().getScenarioSetFunctionalityModuleParserRuleCall_4());
                    		
                    pushFollow(FOLLOW_2);
                    this_ScenarioSetFunctionalityModule_4=ruleScenarioSetFunctionalityModule();

                    state._fsp--;


                    			current = this_ScenarioSetFunctionalityModule_4;
                    			afterParserOrEnumRuleCall();
                    		

                    }
                    break;
                case 6 :
                    // InternalAorta.g:457:3: this_SerialFunctionalityModule_5= ruleSerialFunctionalityModule
                    {

                    			newCompositeNode(grammarAccess.getFunctionalityModuleAccess().getSerialFunctionalityModuleParserRuleCall_5());
                    		
                    pushFollow(FOLLOW_2);
                    this_SerialFunctionalityModule_5=ruleSerialFunctionalityModule();

                    state._fsp--;


                    			current = this_SerialFunctionalityModule_5;
                    			afterParserOrEnumRuleCall();
                    		

                    }
                    break;
                case 7 :
                    // InternalAorta.g:466:3: this_FunctionalityModuleSet_Impl_6= ruleFunctionalityModuleSet_Impl
                    {

                    			newCompositeNode(grammarAccess.getFunctionalityModuleAccess().getFunctionalityModuleSet_ImplParserRuleCall_6());
                    		
                    pushFollow(FOLLOW_2);
                    this_FunctionalityModuleSet_Impl_6=ruleFunctionalityModuleSet_Impl();

                    state._fsp--;


                    			current = this_FunctionalityModuleSet_Impl_6;
                    			afterParserOrEnumRuleCall();
                    		

                    }
                    break;
                case 8 :
                    // InternalAorta.g:475:3: this_ReadDataSourceFunctionalityModule_7= ruleReadDataSourceFunctionalityModule
                    {

                    			newCompositeNode(grammarAccess.getFunctionalityModuleAccess().getReadDataSourceFunctionalityModuleParserRuleCall_7());
                    		
                    pushFollow(FOLLOW_2);
                    this_ReadDataSourceFunctionalityModule_7=ruleReadDataSourceFunctionalityModule();

                    state._fsp--;


                    			current = this_ReadDataSourceFunctionalityModule_7;
                    			afterParserOrEnumRuleCall();
                    		

                    }
                    break;
                case 9 :
                    // InternalAorta.g:484:3: this_OutputCreationFunctionalityModule_8= ruleOutputCreationFunctionalityModule
                    {

                    			newCompositeNode(grammarAccess.getFunctionalityModuleAccess().getOutputCreationFunctionalityModuleParserRuleCall_8());
                    		
                    pushFollow(FOLLOW_2);
                    this_OutputCreationFunctionalityModule_8=ruleOutputCreationFunctionalityModule();

                    state._fsp--;


                    			current = this_OutputCreationFunctionalityModule_8;
                    			afterParserOrEnumRuleCall();
                    		

                    }
                    break;
                case 10 :
                    // InternalAorta.g:493:3: this_ValidationFunctionalityModule_9= ruleValidationFunctionalityModule
                    {

                    			newCompositeNode(grammarAccess.getFunctionalityModuleAccess().getValidationFunctionalityModuleParserRuleCall_9());
                    		
                    pushFollow(FOLLOW_2);
                    this_ValidationFunctionalityModule_9=ruleValidationFunctionalityModule();

                    state._fsp--;


                    			current = this_ValidationFunctionalityModule_9;
                    			afterParserOrEnumRuleCall();
                    		

                    }
                    break;

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleFunctionalityModule"


    // $ANTLR start "entryRuleRequirementsSection"
    // InternalAorta.g:505:1: entryRuleRequirementsSection returns [EObject current=null] : iv_ruleRequirementsSection= ruleRequirementsSection EOF ;
    public final EObject entryRuleRequirementsSection() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleRequirementsSection = null;


        try {
            // InternalAorta.g:505:60: (iv_ruleRequirementsSection= ruleRequirementsSection EOF )
            // InternalAorta.g:506:2: iv_ruleRequirementsSection= ruleRequirementsSection EOF
            {
             newCompositeNode(grammarAccess.getRequirementsSectionRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleRequirementsSection=ruleRequirementsSection();

            state._fsp--;

             current =iv_ruleRequirementsSection; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleRequirementsSection"


    // $ANTLR start "ruleRequirementsSection"
    // InternalAorta.g:512:1: ruleRequirementsSection returns [EObject current=null] : (this_RequirementsSectionImage_0= ruleRequirementsSectionImage | this_RequirementsSectionLinkWithText_1= ruleRequirementsSectionLinkWithText | this_RequirementsSectionText_2= ruleRequirementsSectionText | this_TitledRequirementsSection_3= ruleTitledRequirementsSection ) ;
    public final EObject ruleRequirementsSection() throws RecognitionException {
        EObject current = null;

        EObject this_RequirementsSectionImage_0 = null;

        EObject this_RequirementsSectionLinkWithText_1 = null;

        EObject this_RequirementsSectionText_2 = null;

        EObject this_TitledRequirementsSection_3 = null;



        	enterRule();

        try {
            // InternalAorta.g:518:2: ( (this_RequirementsSectionImage_0= ruleRequirementsSectionImage | this_RequirementsSectionLinkWithText_1= ruleRequirementsSectionLinkWithText | this_RequirementsSectionText_2= ruleRequirementsSectionText | this_TitledRequirementsSection_3= ruleTitledRequirementsSection ) )
            // InternalAorta.g:519:2: (this_RequirementsSectionImage_0= ruleRequirementsSectionImage | this_RequirementsSectionLinkWithText_1= ruleRequirementsSectionLinkWithText | this_RequirementsSectionText_2= ruleRequirementsSectionText | this_TitledRequirementsSection_3= ruleTitledRequirementsSection )
            {
            // InternalAorta.g:519:2: (this_RequirementsSectionImage_0= ruleRequirementsSectionImage | this_RequirementsSectionLinkWithText_1= ruleRequirementsSectionLinkWithText | this_RequirementsSectionText_2= ruleRequirementsSectionText | this_TitledRequirementsSection_3= ruleTitledRequirementsSection )
            int alt13=4;
            switch ( input.LA(1) ) {
            case 79:
                {
                alt13=1;
                }
                break;
            case 74:
                {
                alt13=2;
                }
                break;
            case 82:
                {
                alt13=3;
                }
                break;
            case 67:
                {
                alt13=4;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 13, 0, input);

                throw nvae;
            }

            switch (alt13) {
                case 1 :
                    // InternalAorta.g:520:3: this_RequirementsSectionImage_0= ruleRequirementsSectionImage
                    {

                    			newCompositeNode(grammarAccess.getRequirementsSectionAccess().getRequirementsSectionImageParserRuleCall_0());
                    		
                    pushFollow(FOLLOW_2);
                    this_RequirementsSectionImage_0=ruleRequirementsSectionImage();

                    state._fsp--;


                    			current = this_RequirementsSectionImage_0;
                    			afterParserOrEnumRuleCall();
                    		

                    }
                    break;
                case 2 :
                    // InternalAorta.g:529:3: this_RequirementsSectionLinkWithText_1= ruleRequirementsSectionLinkWithText
                    {

                    			newCompositeNode(grammarAccess.getRequirementsSectionAccess().getRequirementsSectionLinkWithTextParserRuleCall_1());
                    		
                    pushFollow(FOLLOW_2);
                    this_RequirementsSectionLinkWithText_1=ruleRequirementsSectionLinkWithText();

                    state._fsp--;


                    			current = this_RequirementsSectionLinkWithText_1;
                    			afterParserOrEnumRuleCall();
                    		

                    }
                    break;
                case 3 :
                    // InternalAorta.g:538:3: this_RequirementsSectionText_2= ruleRequirementsSectionText
                    {

                    			newCompositeNode(grammarAccess.getRequirementsSectionAccess().getRequirementsSectionTextParserRuleCall_2());
                    		
                    pushFollow(FOLLOW_2);
                    this_RequirementsSectionText_2=ruleRequirementsSectionText();

                    state._fsp--;


                    			current = this_RequirementsSectionText_2;
                    			afterParserOrEnumRuleCall();
                    		

                    }
                    break;
                case 4 :
                    // InternalAorta.g:547:3: this_TitledRequirementsSection_3= ruleTitledRequirementsSection
                    {

                    			newCompositeNode(grammarAccess.getRequirementsSectionAccess().getTitledRequirementsSectionParserRuleCall_3());
                    		
                    pushFollow(FOLLOW_2);
                    this_TitledRequirementsSection_3=ruleTitledRequirementsSection();

                    state._fsp--;


                    			current = this_TitledRequirementsSection_3;
                    			afterParserOrEnumRuleCall();
                    		

                    }
                    break;

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleRequirementsSection"


    // $ANTLR start "entryRuleTestInputData"
    // InternalAorta.g:559:1: entryRuleTestInputData returns [EObject current=null] : iv_ruleTestInputData= ruleTestInputData EOF ;
    public final EObject entryRuleTestInputData() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleTestInputData = null;


        try {
            // InternalAorta.g:559:54: (iv_ruleTestInputData= ruleTestInputData EOF )
            // InternalAorta.g:560:2: iv_ruleTestInputData= ruleTestInputData EOF
            {
             newCompositeNode(grammarAccess.getTestInputDataRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleTestInputData=ruleTestInputData();

            state._fsp--;

             current =iv_ruleTestInputData; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleTestInputData"


    // $ANTLR start "ruleTestInputData"
    // InternalAorta.g:566:1: ruleTestInputData returns [EObject current=null] : (this_TestInputData_Impl_0= ruleTestInputData_Impl | this_TestColumnStructuredData_1= ruleTestColumnStructuredData | this_TestStrucuredFileData_2= ruleTestStrucuredFileData ) ;
    public final EObject ruleTestInputData() throws RecognitionException {
        EObject current = null;

        EObject this_TestInputData_Impl_0 = null;

        EObject this_TestColumnStructuredData_1 = null;

        EObject this_TestStrucuredFileData_2 = null;



        	enterRule();

        try {
            // InternalAorta.g:572:2: ( (this_TestInputData_Impl_0= ruleTestInputData_Impl | this_TestColumnStructuredData_1= ruleTestColumnStructuredData | this_TestStrucuredFileData_2= ruleTestStrucuredFileData ) )
            // InternalAorta.g:573:2: (this_TestInputData_Impl_0= ruleTestInputData_Impl | this_TestColumnStructuredData_1= ruleTestColumnStructuredData | this_TestStrucuredFileData_2= ruleTestStrucuredFileData )
            {
            // InternalAorta.g:573:2: (this_TestInputData_Impl_0= ruleTestInputData_Impl | this_TestColumnStructuredData_1= ruleTestColumnStructuredData | this_TestStrucuredFileData_2= ruleTestStrucuredFileData )
            int alt14=3;
            switch ( input.LA(1) ) {
            case 116:
                {
                alt14=1;
                }
                break;
            case 118:
                {
                alt14=2;
                }
                break;
            case 120:
                {
                alt14=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 14, 0, input);

                throw nvae;
            }

            switch (alt14) {
                case 1 :
                    // InternalAorta.g:574:3: this_TestInputData_Impl_0= ruleTestInputData_Impl
                    {

                    			newCompositeNode(grammarAccess.getTestInputDataAccess().getTestInputData_ImplParserRuleCall_0());
                    		
                    pushFollow(FOLLOW_2);
                    this_TestInputData_Impl_0=ruleTestInputData_Impl();

                    state._fsp--;


                    			current = this_TestInputData_Impl_0;
                    			afterParserOrEnumRuleCall();
                    		

                    }
                    break;
                case 2 :
                    // InternalAorta.g:583:3: this_TestColumnStructuredData_1= ruleTestColumnStructuredData
                    {

                    			newCompositeNode(grammarAccess.getTestInputDataAccess().getTestColumnStructuredDataParserRuleCall_1());
                    		
                    pushFollow(FOLLOW_2);
                    this_TestColumnStructuredData_1=ruleTestColumnStructuredData();

                    state._fsp--;


                    			current = this_TestColumnStructuredData_1;
                    			afterParserOrEnumRuleCall();
                    		

                    }
                    break;
                case 3 :
                    // InternalAorta.g:592:3: this_TestStrucuredFileData_2= ruleTestStrucuredFileData
                    {

                    			newCompositeNode(grammarAccess.getTestInputDataAccess().getTestStrucuredFileDataParserRuleCall_2());
                    		
                    pushFollow(FOLLOW_2);
                    this_TestStrucuredFileData_2=ruleTestStrucuredFileData();

                    state._fsp--;


                    			current = this_TestStrucuredFileData_2;
                    			afterParserOrEnumRuleCall();
                    		

                    }
                    break;

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleTestInputData"


    // $ANTLR start "entryRuleResult"
    // InternalAorta.g:604:1: entryRuleResult returns [EObject current=null] : iv_ruleResult= ruleResult EOF ;
    public final EObject entryRuleResult() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleResult = null;


        try {
            // InternalAorta.g:604:47: (iv_ruleResult= ruleResult EOF )
            // InternalAorta.g:605:2: iv_ruleResult= ruleResult EOF
            {
             newCompositeNode(grammarAccess.getResultRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleResult=ruleResult();

            state._fsp--;

             current =iv_ruleResult; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleResult"


    // $ANTLR start "ruleResult"
    // InternalAorta.g:611:1: ruleResult returns [EObject current=null] : (this_Result_Impl_0= ruleResult_Impl | this_ReportCellResults_1= ruleReportCellResults | this_DataSetResults_2= ruleDataSetResults ) ;
    public final EObject ruleResult() throws RecognitionException {
        EObject current = null;

        EObject this_Result_Impl_0 = null;

        EObject this_ReportCellResults_1 = null;

        EObject this_DataSetResults_2 = null;



        	enterRule();

        try {
            // InternalAorta.g:617:2: ( (this_Result_Impl_0= ruleResult_Impl | this_ReportCellResults_1= ruleReportCellResults | this_DataSetResults_2= ruleDataSetResults ) )
            // InternalAorta.g:618:2: (this_Result_Impl_0= ruleResult_Impl | this_ReportCellResults_1= ruleReportCellResults | this_DataSetResults_2= ruleDataSetResults )
            {
            // InternalAorta.g:618:2: (this_Result_Impl_0= ruleResult_Impl | this_ReportCellResults_1= ruleReportCellResults | this_DataSetResults_2= ruleDataSetResults )
            int alt15=3;
            switch ( input.LA(1) ) {
            case 117:
                {
                alt15=1;
                }
                break;
            case 131:
                {
                alt15=2;
                }
                break;
            case 133:
                {
                alt15=3;
                }
                break;
            default:
                NoViableAltException nvae =
                    new NoViableAltException("", 15, 0, input);

                throw nvae;
            }

            switch (alt15) {
                case 1 :
                    // InternalAorta.g:619:3: this_Result_Impl_0= ruleResult_Impl
                    {

                    			newCompositeNode(grammarAccess.getResultAccess().getResult_ImplParserRuleCall_0());
                    		
                    pushFollow(FOLLOW_2);
                    this_Result_Impl_0=ruleResult_Impl();

                    state._fsp--;


                    			current = this_Result_Impl_0;
                    			afterParserOrEnumRuleCall();
                    		

                    }
                    break;
                case 2 :
                    // InternalAorta.g:628:3: this_ReportCellResults_1= ruleReportCellResults
                    {

                    			newCompositeNode(grammarAccess.getResultAccess().getReportCellResultsParserRuleCall_1());
                    		
                    pushFollow(FOLLOW_2);
                    this_ReportCellResults_1=ruleReportCellResults();

                    state._fsp--;


                    			current = this_ReportCellResults_1;
                    			afterParserOrEnumRuleCall();
                    		

                    }
                    break;
                case 3 :
                    // InternalAorta.g:637:3: this_DataSetResults_2= ruleDataSetResults
                    {

                    			newCompositeNode(grammarAccess.getResultAccess().getDataSetResultsParserRuleCall_2());
                    		
                    pushFollow(FOLLOW_2);
                    this_DataSetResults_2=ruleDataSetResults();

                    state._fsp--;


                    			current = this_DataSetResults_2;
                    			afterParserOrEnumRuleCall();
                    		

                    }
                    break;

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleResult"


    // $ANTLR start "entryRuleBaseCell"
    // InternalAorta.g:649:1: entryRuleBaseCell returns [EObject current=null] : iv_ruleBaseCell= ruleBaseCell EOF ;
    public final EObject entryRuleBaseCell() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleBaseCell = null;


        try {
            // InternalAorta.g:649:49: (iv_ruleBaseCell= ruleBaseCell EOF )
            // InternalAorta.g:650:2: iv_ruleBaseCell= ruleBaseCell EOF
            {
             newCompositeNode(grammarAccess.getBaseCellRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleBaseCell=ruleBaseCell();

            state._fsp--;

             current =iv_ruleBaseCell; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleBaseCell"


    // $ANTLR start "ruleBaseCell"
    // InternalAorta.g:656:1: ruleBaseCell returns [EObject current=null] : (this_BaseCellWithEnumeratedValue_0= ruleBaseCellWithEnumeratedValue | this_BaseCellWithValue_1= ruleBaseCellWithValue ) ;
    public final EObject ruleBaseCell() throws RecognitionException {
        EObject current = null;

        EObject this_BaseCellWithEnumeratedValue_0 = null;

        EObject this_BaseCellWithValue_1 = null;



        	enterRule();

        try {
            // InternalAorta.g:662:2: ( (this_BaseCellWithEnumeratedValue_0= ruleBaseCellWithEnumeratedValue | this_BaseCellWithValue_1= ruleBaseCellWithValue ) )
            // InternalAorta.g:663:2: (this_BaseCellWithEnumeratedValue_0= ruleBaseCellWithEnumeratedValue | this_BaseCellWithValue_1= ruleBaseCellWithValue )
            {
            // InternalAorta.g:663:2: (this_BaseCellWithEnumeratedValue_0= ruleBaseCellWithEnumeratedValue | this_BaseCellWithValue_1= ruleBaseCellWithValue )
            int alt16=2;
            int LA16_0 = input.LA(1);

            if ( (LA16_0==126) ) {
                alt16=1;
            }
            else if ( (LA16_0==128) ) {
                alt16=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 16, 0, input);

                throw nvae;
            }
            switch (alt16) {
                case 1 :
                    // InternalAorta.g:664:3: this_BaseCellWithEnumeratedValue_0= ruleBaseCellWithEnumeratedValue
                    {

                    			newCompositeNode(grammarAccess.getBaseCellAccess().getBaseCellWithEnumeratedValueParserRuleCall_0());
                    		
                    pushFollow(FOLLOW_2);
                    this_BaseCellWithEnumeratedValue_0=ruleBaseCellWithEnumeratedValue();

                    state._fsp--;


                    			current = this_BaseCellWithEnumeratedValue_0;
                    			afterParserOrEnumRuleCall();
                    		

                    }
                    break;
                case 2 :
                    // InternalAorta.g:673:3: this_BaseCellWithValue_1= ruleBaseCellWithValue
                    {

                    			newCompositeNode(grammarAccess.getBaseCellAccess().getBaseCellWithValueParserRuleCall_1());
                    		
                    pushFollow(FOLLOW_2);
                    this_BaseCellWithValue_1=ruleBaseCellWithValue();

                    state._fsp--;


                    			current = this_BaseCellWithValue_1;
                    			afterParserOrEnumRuleCall();
                    		

                    }
                    break;

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleBaseCell"


    // $ANTLR start "entryRuleColumnDomainModule"
    // InternalAorta.g:685:1: entryRuleColumnDomainModule returns [EObject current=null] : iv_ruleColumnDomainModule= ruleColumnDomainModule EOF ;
    public final EObject entryRuleColumnDomainModule() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleColumnDomainModule = null;


        try {
            // InternalAorta.g:685:59: (iv_ruleColumnDomainModule= ruleColumnDomainModule EOF )
            // InternalAorta.g:686:2: iv_ruleColumnDomainModule= ruleColumnDomainModule EOF
            {
             newCompositeNode(grammarAccess.getColumnDomainModuleRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleColumnDomainModule=ruleColumnDomainModule();

            state._fsp--;

             current =iv_ruleColumnDomainModule; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleColumnDomainModule"


    // $ANTLR start "ruleColumnDomainModule"
    // InternalAorta.g:692:1: ruleColumnDomainModule returns [EObject current=null] : ( () otherlv_1= 'ColumnDomainModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'columnDomains' otherlv_15= '{' ( (lv_columnDomains_16_0= ruleColumnDomain ) ) (otherlv_17= ',' ( (lv_columnDomains_18_0= ruleColumnDomain ) ) )* otherlv_19= '}' )? otherlv_20= '}' ) ;
    public final EObject ruleColumnDomainModule() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_6=null;
        Token otherlv_8=null;
        Token otherlv_10=null;
        Token otherlv_12=null;
        Token otherlv_14=null;
        Token otherlv_15=null;
        Token otherlv_17=null;
        Token otherlv_19=null;
        Token otherlv_20=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;

        AntlrDatatypeRuleToken lv_theDescription_5_0 = null;

        AntlrDatatypeRuleToken lv_license_7_0 = null;

        AntlrDatatypeRuleToken lv_version_9_0 = null;

        EObject lv_dependencies_11_0 = null;

        EObject lv_longName_13_0 = null;

        EObject lv_columnDomains_16_0 = null;

        EObject lv_columnDomains_18_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:698:2: ( ( () otherlv_1= 'ColumnDomainModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'columnDomains' otherlv_15= '{' ( (lv_columnDomains_16_0= ruleColumnDomain ) ) (otherlv_17= ',' ( (lv_columnDomains_18_0= ruleColumnDomain ) ) )* otherlv_19= '}' )? otherlv_20= '}' ) )
            // InternalAorta.g:699:2: ( () otherlv_1= 'ColumnDomainModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'columnDomains' otherlv_15= '{' ( (lv_columnDomains_16_0= ruleColumnDomain ) ) (otherlv_17= ',' ( (lv_columnDomains_18_0= ruleColumnDomain ) ) )* otherlv_19= '}' )? otherlv_20= '}' )
            {
            // InternalAorta.g:699:2: ( () otherlv_1= 'ColumnDomainModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'columnDomains' otherlv_15= '{' ( (lv_columnDomains_16_0= ruleColumnDomain ) ) (otherlv_17= ',' ( (lv_columnDomains_18_0= ruleColumnDomain ) ) )* otherlv_19= '}' )? otherlv_20= '}' )
            // InternalAorta.g:700:3: () otherlv_1= 'ColumnDomainModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'columnDomains' otherlv_15= '{' ( (lv_columnDomains_16_0= ruleColumnDomain ) ) (otherlv_17= ',' ( (lv_columnDomains_18_0= ruleColumnDomain ) ) )* otherlv_19= '}' )? otherlv_20= '}'
            {
            // InternalAorta.g:700:3: ()
            // InternalAorta.g:701:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getColumnDomainModuleAccess().getColumnDomainModuleAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,24,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getColumnDomainModuleAccess().getColumnDomainModuleKeyword_1());
            		
            // InternalAorta.g:711:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:712:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:712:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:713:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getColumnDomainModuleAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getColumnDomainModuleRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_26); 

            			newLeafNode(otherlv_3, grammarAccess.getColumnDomainModuleAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:734:3: (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )?
            int alt17=2;
            int LA17_0 = input.LA(1);

            if ( (LA17_0==25) ) {
                alt17=1;
            }
            switch (alt17) {
                case 1 :
                    // InternalAorta.g:735:4: otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) )
                    {
                    otherlv_4=(Token)match(input,25,FOLLOW_25); 

                    				newLeafNode(otherlv_4, grammarAccess.getColumnDomainModuleAccess().getTheDescriptionKeyword_4_0());
                    			
                    // InternalAorta.g:739:4: ( (lv_theDescription_5_0= ruleEString ) )
                    // InternalAorta.g:740:5: (lv_theDescription_5_0= ruleEString )
                    {
                    // InternalAorta.g:740:5: (lv_theDescription_5_0= ruleEString )
                    // InternalAorta.g:741:6: lv_theDescription_5_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getColumnDomainModuleAccess().getTheDescriptionEStringParserRuleCall_4_1_0());
                    					
                    pushFollow(FOLLOW_27);
                    lv_theDescription_5_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getColumnDomainModuleRule());
                    						}
                    						set(
                    							current,
                    							"theDescription",
                    							lv_theDescription_5_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:759:3: (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )?
            int alt18=2;
            int LA18_0 = input.LA(1);

            if ( (LA18_0==26) ) {
                alt18=1;
            }
            switch (alt18) {
                case 1 :
                    // InternalAorta.g:760:4: otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) )
                    {
                    otherlv_6=(Token)match(input,26,FOLLOW_25); 

                    				newLeafNode(otherlv_6, grammarAccess.getColumnDomainModuleAccess().getLicenseKeyword_5_0());
                    			
                    // InternalAorta.g:764:4: ( (lv_license_7_0= ruleEString ) )
                    // InternalAorta.g:765:5: (lv_license_7_0= ruleEString )
                    {
                    // InternalAorta.g:765:5: (lv_license_7_0= ruleEString )
                    // InternalAorta.g:766:6: lv_license_7_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getColumnDomainModuleAccess().getLicenseEStringParserRuleCall_5_1_0());
                    					
                    pushFollow(FOLLOW_28);
                    lv_license_7_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getColumnDomainModuleRule());
                    						}
                    						set(
                    							current,
                    							"license",
                    							lv_license_7_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:784:3: (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )?
            int alt19=2;
            int LA19_0 = input.LA(1);

            if ( (LA19_0==27) ) {
                alt19=1;
            }
            switch (alt19) {
                case 1 :
                    // InternalAorta.g:785:4: otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) )
                    {
                    otherlv_8=(Token)match(input,27,FOLLOW_25); 

                    				newLeafNode(otherlv_8, grammarAccess.getColumnDomainModuleAccess().getVersionKeyword_6_0());
                    			
                    // InternalAorta.g:789:4: ( (lv_version_9_0= ruleEString ) )
                    // InternalAorta.g:790:5: (lv_version_9_0= ruleEString )
                    {
                    // InternalAorta.g:790:5: (lv_version_9_0= ruleEString )
                    // InternalAorta.g:791:6: lv_version_9_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getColumnDomainModuleAccess().getVersionEStringParserRuleCall_6_1_0());
                    					
                    pushFollow(FOLLOW_29);
                    lv_version_9_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getColumnDomainModuleRule());
                    						}
                    						set(
                    							current,
                    							"version",
                    							lv_version_9_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:809:3: (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )?
            int alt20=2;
            int LA20_0 = input.LA(1);

            if ( (LA20_0==28) ) {
                alt20=1;
            }
            switch (alt20) {
                case 1 :
                    // InternalAorta.g:810:4: otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) )
                    {
                    otherlv_10=(Token)match(input,28,FOLLOW_30); 

                    				newLeafNode(otherlv_10, grammarAccess.getColumnDomainModuleAccess().getDependenciesKeyword_7_0());
                    			
                    // InternalAorta.g:814:4: ( (lv_dependencies_11_0= ruleModuleDependencies ) )
                    // InternalAorta.g:815:5: (lv_dependencies_11_0= ruleModuleDependencies )
                    {
                    // InternalAorta.g:815:5: (lv_dependencies_11_0= ruleModuleDependencies )
                    // InternalAorta.g:816:6: lv_dependencies_11_0= ruleModuleDependencies
                    {

                    						newCompositeNode(grammarAccess.getColumnDomainModuleAccess().getDependenciesModuleDependenciesParserRuleCall_7_1_0());
                    					
                    pushFollow(FOLLOW_31);
                    lv_dependencies_11_0=ruleModuleDependencies();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getColumnDomainModuleRule());
                    						}
                    						set(
                    							current,
                    							"dependencies",
                    							lv_dependencies_11_0,
                    							"org.eclipse.efbt.aorta.Aorta.ModuleDependencies");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:834:3: (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )?
            int alt21=2;
            int LA21_0 = input.LA(1);

            if ( (LA21_0==29) ) {
                alt21=1;
            }
            switch (alt21) {
                case 1 :
                    // InternalAorta.g:835:4: otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) )
                    {
                    otherlv_12=(Token)match(input,29,FOLLOW_32); 

                    				newLeafNode(otherlv_12, grammarAccess.getColumnDomainModuleAccess().getLongNameKeyword_8_0());
                    			
                    // InternalAorta.g:839:4: ( (lv_longName_13_0= ruleModuleLongName ) )
                    // InternalAorta.g:840:5: (lv_longName_13_0= ruleModuleLongName )
                    {
                    // InternalAorta.g:840:5: (lv_longName_13_0= ruleModuleLongName )
                    // InternalAorta.g:841:6: lv_longName_13_0= ruleModuleLongName
                    {

                    						newCompositeNode(grammarAccess.getColumnDomainModuleAccess().getLongNameModuleLongNameParserRuleCall_8_1_0());
                    					
                    pushFollow(FOLLOW_33);
                    lv_longName_13_0=ruleModuleLongName();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getColumnDomainModuleRule());
                    						}
                    						set(
                    							current,
                    							"longName",
                    							lv_longName_13_0,
                    							"org.eclipse.efbt.aorta.Aorta.ModuleLongName");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:859:3: (otherlv_14= 'columnDomains' otherlv_15= '{' ( (lv_columnDomains_16_0= ruleColumnDomain ) ) (otherlv_17= ',' ( (lv_columnDomains_18_0= ruleColumnDomain ) ) )* otherlv_19= '}' )?
            int alt23=2;
            int LA23_0 = input.LA(1);

            if ( (LA23_0==30) ) {
                alt23=1;
            }
            switch (alt23) {
                case 1 :
                    // InternalAorta.g:860:4: otherlv_14= 'columnDomains' otherlv_15= '{' ( (lv_columnDomains_16_0= ruleColumnDomain ) ) (otherlv_17= ',' ( (lv_columnDomains_18_0= ruleColumnDomain ) ) )* otherlv_19= '}'
                    {
                    otherlv_14=(Token)match(input,30,FOLLOW_3); 

                    				newLeafNode(otherlv_14, grammarAccess.getColumnDomainModuleAccess().getColumnDomainsKeyword_9_0());
                    			
                    otherlv_15=(Token)match(input,12,FOLLOW_34); 

                    				newLeafNode(otherlv_15, grammarAccess.getColumnDomainModuleAccess().getLeftCurlyBracketKeyword_9_1());
                    			
                    // InternalAorta.g:868:4: ( (lv_columnDomains_16_0= ruleColumnDomain ) )
                    // InternalAorta.g:869:5: (lv_columnDomains_16_0= ruleColumnDomain )
                    {
                    // InternalAorta.g:869:5: (lv_columnDomains_16_0= ruleColumnDomain )
                    // InternalAorta.g:870:6: lv_columnDomains_16_0= ruleColumnDomain
                    {

                    						newCompositeNode(grammarAccess.getColumnDomainModuleAccess().getColumnDomainsColumnDomainParserRuleCall_9_2_0());
                    					
                    pushFollow(FOLLOW_35);
                    lv_columnDomains_16_0=ruleColumnDomain();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getColumnDomainModuleRule());
                    						}
                    						add(
                    							current,
                    							"columnDomains",
                    							lv_columnDomains_16_0,
                    							"org.eclipse.efbt.aorta.Aorta.ColumnDomain");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:887:4: (otherlv_17= ',' ( (lv_columnDomains_18_0= ruleColumnDomain ) ) )*
                    loop22:
                    do {
                        int alt22=2;
                        int LA22_0 = input.LA(1);

                        if ( (LA22_0==31) ) {
                            alt22=1;
                        }


                        switch (alt22) {
                    	case 1 :
                    	    // InternalAorta.g:888:5: otherlv_17= ',' ( (lv_columnDomains_18_0= ruleColumnDomain ) )
                    	    {
                    	    otherlv_17=(Token)match(input,31,FOLLOW_34); 

                    	    					newLeafNode(otherlv_17, grammarAccess.getColumnDomainModuleAccess().getCommaKeyword_9_3_0());
                    	    				
                    	    // InternalAorta.g:892:5: ( (lv_columnDomains_18_0= ruleColumnDomain ) )
                    	    // InternalAorta.g:893:6: (lv_columnDomains_18_0= ruleColumnDomain )
                    	    {
                    	    // InternalAorta.g:893:6: (lv_columnDomains_18_0= ruleColumnDomain )
                    	    // InternalAorta.g:894:7: lv_columnDomains_18_0= ruleColumnDomain
                    	    {

                    	    							newCompositeNode(grammarAccess.getColumnDomainModuleAccess().getColumnDomainsColumnDomainParserRuleCall_9_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_35);
                    	    lv_columnDomains_18_0=ruleColumnDomain();

                    	    state._fsp--;


                    	    							if (current==null) {
                    	    								current = createModelElementForParent(grammarAccess.getColumnDomainModuleRule());
                    	    							}
                    	    							add(
                    	    								current,
                    	    								"columnDomains",
                    	    								lv_columnDomains_18_0,
                    	    								"org.eclipse.efbt.aorta.Aorta.ColumnDomain");
                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop22;
                        }
                    } while (true);

                    otherlv_19=(Token)match(input,23,FOLLOW_24); 

                    				newLeafNode(otherlv_19, grammarAccess.getColumnDomainModuleAccess().getRightCurlyBracketKeyword_9_4());
                    			

                    }
                    break;

            }

            otherlv_20=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_20, grammarAccess.getColumnDomainModuleAccess().getRightCurlyBracketKeyword_10());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleColumnDomainModule"


    // $ANTLR start "entryRuleRequirementsModule"
    // InternalAorta.g:925:1: entryRuleRequirementsModule returns [EObject current=null] : iv_ruleRequirementsModule= ruleRequirementsModule EOF ;
    public final EObject entryRuleRequirementsModule() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleRequirementsModule = null;


        try {
            // InternalAorta.g:925:59: (iv_ruleRequirementsModule= ruleRequirementsModule EOF )
            // InternalAorta.g:926:2: iv_ruleRequirementsModule= ruleRequirementsModule EOF
            {
             newCompositeNode(grammarAccess.getRequirementsModuleRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleRequirementsModule=ruleRequirementsModule();

            state._fsp--;

             current =iv_ruleRequirementsModule; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleRequirementsModule"


    // $ANTLR start "ruleRequirementsModule"
    // InternalAorta.g:932:1: ruleRequirementsModule returns [EObject current=null] : ( () otherlv_1= 'RequirementsModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'rules' otherlv_15= '{' ( (lv_rules_16_0= ruleTitledRequirementsSection ) ) (otherlv_17= ',' ( (lv_rules_18_0= ruleTitledRequirementsSection ) ) )* otherlv_19= '}' )? (otherlv_20= 'allowedtypes' ( (lv_allowedtypes_21_0= ruleAllowedTypes ) ) )? otherlv_22= '}' ) ;
    public final EObject ruleRequirementsModule() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_6=null;
        Token otherlv_8=null;
        Token otherlv_10=null;
        Token otherlv_12=null;
        Token otherlv_14=null;
        Token otherlv_15=null;
        Token otherlv_17=null;
        Token otherlv_19=null;
        Token otherlv_20=null;
        Token otherlv_22=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;

        AntlrDatatypeRuleToken lv_theDescription_5_0 = null;

        AntlrDatatypeRuleToken lv_license_7_0 = null;

        AntlrDatatypeRuleToken lv_version_9_0 = null;

        EObject lv_dependencies_11_0 = null;

        EObject lv_longName_13_0 = null;

        EObject lv_rules_16_0 = null;

        EObject lv_rules_18_0 = null;

        EObject lv_allowedtypes_21_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:938:2: ( ( () otherlv_1= 'RequirementsModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'rules' otherlv_15= '{' ( (lv_rules_16_0= ruleTitledRequirementsSection ) ) (otherlv_17= ',' ( (lv_rules_18_0= ruleTitledRequirementsSection ) ) )* otherlv_19= '}' )? (otherlv_20= 'allowedtypes' ( (lv_allowedtypes_21_0= ruleAllowedTypes ) ) )? otherlv_22= '}' ) )
            // InternalAorta.g:939:2: ( () otherlv_1= 'RequirementsModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'rules' otherlv_15= '{' ( (lv_rules_16_0= ruleTitledRequirementsSection ) ) (otherlv_17= ',' ( (lv_rules_18_0= ruleTitledRequirementsSection ) ) )* otherlv_19= '}' )? (otherlv_20= 'allowedtypes' ( (lv_allowedtypes_21_0= ruleAllowedTypes ) ) )? otherlv_22= '}' )
            {
            // InternalAorta.g:939:2: ( () otherlv_1= 'RequirementsModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'rules' otherlv_15= '{' ( (lv_rules_16_0= ruleTitledRequirementsSection ) ) (otherlv_17= ',' ( (lv_rules_18_0= ruleTitledRequirementsSection ) ) )* otherlv_19= '}' )? (otherlv_20= 'allowedtypes' ( (lv_allowedtypes_21_0= ruleAllowedTypes ) ) )? otherlv_22= '}' )
            // InternalAorta.g:940:3: () otherlv_1= 'RequirementsModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'rules' otherlv_15= '{' ( (lv_rules_16_0= ruleTitledRequirementsSection ) ) (otherlv_17= ',' ( (lv_rules_18_0= ruleTitledRequirementsSection ) ) )* otherlv_19= '}' )? (otherlv_20= 'allowedtypes' ( (lv_allowedtypes_21_0= ruleAllowedTypes ) ) )? otherlv_22= '}'
            {
            // InternalAorta.g:940:3: ()
            // InternalAorta.g:941:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getRequirementsModuleAccess().getRequirementsModuleAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,32,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getRequirementsModuleAccess().getRequirementsModuleKeyword_1());
            		
            // InternalAorta.g:951:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:952:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:952:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:953:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getRequirementsModuleAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getRequirementsModuleRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_36); 

            			newLeafNode(otherlv_3, grammarAccess.getRequirementsModuleAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:974:3: (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )?
            int alt24=2;
            int LA24_0 = input.LA(1);

            if ( (LA24_0==25) ) {
                alt24=1;
            }
            switch (alt24) {
                case 1 :
                    // InternalAorta.g:975:4: otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) )
                    {
                    otherlv_4=(Token)match(input,25,FOLLOW_25); 

                    				newLeafNode(otherlv_4, grammarAccess.getRequirementsModuleAccess().getTheDescriptionKeyword_4_0());
                    			
                    // InternalAorta.g:979:4: ( (lv_theDescription_5_0= ruleEString ) )
                    // InternalAorta.g:980:5: (lv_theDescription_5_0= ruleEString )
                    {
                    // InternalAorta.g:980:5: (lv_theDescription_5_0= ruleEString )
                    // InternalAorta.g:981:6: lv_theDescription_5_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getRequirementsModuleAccess().getTheDescriptionEStringParserRuleCall_4_1_0());
                    					
                    pushFollow(FOLLOW_37);
                    lv_theDescription_5_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getRequirementsModuleRule());
                    						}
                    						set(
                    							current,
                    							"theDescription",
                    							lv_theDescription_5_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:999:3: (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )?
            int alt25=2;
            int LA25_0 = input.LA(1);

            if ( (LA25_0==26) ) {
                alt25=1;
            }
            switch (alt25) {
                case 1 :
                    // InternalAorta.g:1000:4: otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) )
                    {
                    otherlv_6=(Token)match(input,26,FOLLOW_25); 

                    				newLeafNode(otherlv_6, grammarAccess.getRequirementsModuleAccess().getLicenseKeyword_5_0());
                    			
                    // InternalAorta.g:1004:4: ( (lv_license_7_0= ruleEString ) )
                    // InternalAorta.g:1005:5: (lv_license_7_0= ruleEString )
                    {
                    // InternalAorta.g:1005:5: (lv_license_7_0= ruleEString )
                    // InternalAorta.g:1006:6: lv_license_7_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getRequirementsModuleAccess().getLicenseEStringParserRuleCall_5_1_0());
                    					
                    pushFollow(FOLLOW_38);
                    lv_license_7_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getRequirementsModuleRule());
                    						}
                    						set(
                    							current,
                    							"license",
                    							lv_license_7_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:1024:3: (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )?
            int alt26=2;
            int LA26_0 = input.LA(1);

            if ( (LA26_0==27) ) {
                alt26=1;
            }
            switch (alt26) {
                case 1 :
                    // InternalAorta.g:1025:4: otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) )
                    {
                    otherlv_8=(Token)match(input,27,FOLLOW_25); 

                    				newLeafNode(otherlv_8, grammarAccess.getRequirementsModuleAccess().getVersionKeyword_6_0());
                    			
                    // InternalAorta.g:1029:4: ( (lv_version_9_0= ruleEString ) )
                    // InternalAorta.g:1030:5: (lv_version_9_0= ruleEString )
                    {
                    // InternalAorta.g:1030:5: (lv_version_9_0= ruleEString )
                    // InternalAorta.g:1031:6: lv_version_9_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getRequirementsModuleAccess().getVersionEStringParserRuleCall_6_1_0());
                    					
                    pushFollow(FOLLOW_39);
                    lv_version_9_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getRequirementsModuleRule());
                    						}
                    						set(
                    							current,
                    							"version",
                    							lv_version_9_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:1049:3: (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )?
            int alt27=2;
            int LA27_0 = input.LA(1);

            if ( (LA27_0==28) ) {
                alt27=1;
            }
            switch (alt27) {
                case 1 :
                    // InternalAorta.g:1050:4: otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) )
                    {
                    otherlv_10=(Token)match(input,28,FOLLOW_30); 

                    				newLeafNode(otherlv_10, grammarAccess.getRequirementsModuleAccess().getDependenciesKeyword_7_0());
                    			
                    // InternalAorta.g:1054:4: ( (lv_dependencies_11_0= ruleModuleDependencies ) )
                    // InternalAorta.g:1055:5: (lv_dependencies_11_0= ruleModuleDependencies )
                    {
                    // InternalAorta.g:1055:5: (lv_dependencies_11_0= ruleModuleDependencies )
                    // InternalAorta.g:1056:6: lv_dependencies_11_0= ruleModuleDependencies
                    {

                    						newCompositeNode(grammarAccess.getRequirementsModuleAccess().getDependenciesModuleDependenciesParserRuleCall_7_1_0());
                    					
                    pushFollow(FOLLOW_40);
                    lv_dependencies_11_0=ruleModuleDependencies();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getRequirementsModuleRule());
                    						}
                    						set(
                    							current,
                    							"dependencies",
                    							lv_dependencies_11_0,
                    							"org.eclipse.efbt.aorta.Aorta.ModuleDependencies");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:1074:3: (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )?
            int alt28=2;
            int LA28_0 = input.LA(1);

            if ( (LA28_0==29) ) {
                alt28=1;
            }
            switch (alt28) {
                case 1 :
                    // InternalAorta.g:1075:4: otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) )
                    {
                    otherlv_12=(Token)match(input,29,FOLLOW_32); 

                    				newLeafNode(otherlv_12, grammarAccess.getRequirementsModuleAccess().getLongNameKeyword_8_0());
                    			
                    // InternalAorta.g:1079:4: ( (lv_longName_13_0= ruleModuleLongName ) )
                    // InternalAorta.g:1080:5: (lv_longName_13_0= ruleModuleLongName )
                    {
                    // InternalAorta.g:1080:5: (lv_longName_13_0= ruleModuleLongName )
                    // InternalAorta.g:1081:6: lv_longName_13_0= ruleModuleLongName
                    {

                    						newCompositeNode(grammarAccess.getRequirementsModuleAccess().getLongNameModuleLongNameParserRuleCall_8_1_0());
                    					
                    pushFollow(FOLLOW_41);
                    lv_longName_13_0=ruleModuleLongName();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getRequirementsModuleRule());
                    						}
                    						set(
                    							current,
                    							"longName",
                    							lv_longName_13_0,
                    							"org.eclipse.efbt.aorta.Aorta.ModuleLongName");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:1099:3: (otherlv_14= 'rules' otherlv_15= '{' ( (lv_rules_16_0= ruleTitledRequirementsSection ) ) (otherlv_17= ',' ( (lv_rules_18_0= ruleTitledRequirementsSection ) ) )* otherlv_19= '}' )?
            int alt30=2;
            int LA30_0 = input.LA(1);

            if ( (LA30_0==33) ) {
                alt30=1;
            }
            switch (alt30) {
                case 1 :
                    // InternalAorta.g:1100:4: otherlv_14= 'rules' otherlv_15= '{' ( (lv_rules_16_0= ruleTitledRequirementsSection ) ) (otherlv_17= ',' ( (lv_rules_18_0= ruleTitledRequirementsSection ) ) )* otherlv_19= '}'
                    {
                    otherlv_14=(Token)match(input,33,FOLLOW_3); 

                    				newLeafNode(otherlv_14, grammarAccess.getRequirementsModuleAccess().getRulesKeyword_9_0());
                    			
                    otherlv_15=(Token)match(input,12,FOLLOW_42); 

                    				newLeafNode(otherlv_15, grammarAccess.getRequirementsModuleAccess().getLeftCurlyBracketKeyword_9_1());
                    			
                    // InternalAorta.g:1108:4: ( (lv_rules_16_0= ruleTitledRequirementsSection ) )
                    // InternalAorta.g:1109:5: (lv_rules_16_0= ruleTitledRequirementsSection )
                    {
                    // InternalAorta.g:1109:5: (lv_rules_16_0= ruleTitledRequirementsSection )
                    // InternalAorta.g:1110:6: lv_rules_16_0= ruleTitledRequirementsSection
                    {

                    						newCompositeNode(grammarAccess.getRequirementsModuleAccess().getRulesTitledRequirementsSectionParserRuleCall_9_2_0());
                    					
                    pushFollow(FOLLOW_35);
                    lv_rules_16_0=ruleTitledRequirementsSection();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getRequirementsModuleRule());
                    						}
                    						add(
                    							current,
                    							"rules",
                    							lv_rules_16_0,
                    							"org.eclipse.efbt.aorta.Aorta.TitledRequirementsSection");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:1127:4: (otherlv_17= ',' ( (lv_rules_18_0= ruleTitledRequirementsSection ) ) )*
                    loop29:
                    do {
                        int alt29=2;
                        int LA29_0 = input.LA(1);

                        if ( (LA29_0==31) ) {
                            alt29=1;
                        }


                        switch (alt29) {
                    	case 1 :
                    	    // InternalAorta.g:1128:5: otherlv_17= ',' ( (lv_rules_18_0= ruleTitledRequirementsSection ) )
                    	    {
                    	    otherlv_17=(Token)match(input,31,FOLLOW_42); 

                    	    					newLeafNode(otherlv_17, grammarAccess.getRequirementsModuleAccess().getCommaKeyword_9_3_0());
                    	    				
                    	    // InternalAorta.g:1132:5: ( (lv_rules_18_0= ruleTitledRequirementsSection ) )
                    	    // InternalAorta.g:1133:6: (lv_rules_18_0= ruleTitledRequirementsSection )
                    	    {
                    	    // InternalAorta.g:1133:6: (lv_rules_18_0= ruleTitledRequirementsSection )
                    	    // InternalAorta.g:1134:7: lv_rules_18_0= ruleTitledRequirementsSection
                    	    {

                    	    							newCompositeNode(grammarAccess.getRequirementsModuleAccess().getRulesTitledRequirementsSectionParserRuleCall_9_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_35);
                    	    lv_rules_18_0=ruleTitledRequirementsSection();

                    	    state._fsp--;


                    	    							if (current==null) {
                    	    								current = createModelElementForParent(grammarAccess.getRequirementsModuleRule());
                    	    							}
                    	    							add(
                    	    								current,
                    	    								"rules",
                    	    								lv_rules_18_0,
                    	    								"org.eclipse.efbt.aorta.Aorta.TitledRequirementsSection");
                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop29;
                        }
                    } while (true);

                    otherlv_19=(Token)match(input,23,FOLLOW_43); 

                    				newLeafNode(otherlv_19, grammarAccess.getRequirementsModuleAccess().getRightCurlyBracketKeyword_9_4());
                    			

                    }
                    break;

            }

            // InternalAorta.g:1157:3: (otherlv_20= 'allowedtypes' ( (lv_allowedtypes_21_0= ruleAllowedTypes ) ) )?
            int alt31=2;
            int LA31_0 = input.LA(1);

            if ( (LA31_0==34) ) {
                alt31=1;
            }
            switch (alt31) {
                case 1 :
                    // InternalAorta.g:1158:4: otherlv_20= 'allowedtypes' ( (lv_allowedtypes_21_0= ruleAllowedTypes ) )
                    {
                    otherlv_20=(Token)match(input,34,FOLLOW_44); 

                    				newLeafNode(otherlv_20, grammarAccess.getRequirementsModuleAccess().getAllowedtypesKeyword_10_0());
                    			
                    // InternalAorta.g:1162:4: ( (lv_allowedtypes_21_0= ruleAllowedTypes ) )
                    // InternalAorta.g:1163:5: (lv_allowedtypes_21_0= ruleAllowedTypes )
                    {
                    // InternalAorta.g:1163:5: (lv_allowedtypes_21_0= ruleAllowedTypes )
                    // InternalAorta.g:1164:6: lv_allowedtypes_21_0= ruleAllowedTypes
                    {

                    						newCompositeNode(grammarAccess.getRequirementsModuleAccess().getAllowedtypesAllowedTypesParserRuleCall_10_1_0());
                    					
                    pushFollow(FOLLOW_24);
                    lv_allowedtypes_21_0=ruleAllowedTypes();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getRequirementsModuleRule());
                    						}
                    						set(
                    							current,
                    							"allowedtypes",
                    							lv_allowedtypes_21_0,
                    							"org.eclipse.efbt.aorta.Aorta.AllowedTypes");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            otherlv_22=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_22, grammarAccess.getRequirementsModuleAccess().getRightCurlyBracketKeyword_11());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleRequirementsModule"


    // $ANTLR start "entryRuleColumnStructureModule"
    // InternalAorta.g:1190:1: entryRuleColumnStructureModule returns [EObject current=null] : iv_ruleColumnStructureModule= ruleColumnStructureModule EOF ;
    public final EObject entryRuleColumnStructureModule() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleColumnStructureModule = null;


        try {
            // InternalAorta.g:1190:62: (iv_ruleColumnStructureModule= ruleColumnStructureModule EOF )
            // InternalAorta.g:1191:2: iv_ruleColumnStructureModule= ruleColumnStructureModule EOF
            {
             newCompositeNode(grammarAccess.getColumnStructureModuleRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleColumnStructureModule=ruleColumnStructureModule();

            state._fsp--;

             current =iv_ruleColumnStructureModule; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleColumnStructureModule"


    // $ANTLR start "ruleColumnStructureModule"
    // InternalAorta.g:1197:1: ruleColumnStructureModule returns [EObject current=null] : ( () otherlv_1= 'ColumnStructureModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'columnStructures' otherlv_15= '{' ( (lv_columnStructures_16_0= ruleColumnStructuredEntity ) ) (otherlv_17= ',' ( (lv_columnStructures_18_0= ruleColumnStructuredEntity ) ) )* otherlv_19= '}' )? otherlv_20= '}' ) ;
    public final EObject ruleColumnStructureModule() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_6=null;
        Token otherlv_8=null;
        Token otherlv_10=null;
        Token otherlv_12=null;
        Token otherlv_14=null;
        Token otherlv_15=null;
        Token otherlv_17=null;
        Token otherlv_19=null;
        Token otherlv_20=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;

        AntlrDatatypeRuleToken lv_theDescription_5_0 = null;

        AntlrDatatypeRuleToken lv_license_7_0 = null;

        AntlrDatatypeRuleToken lv_version_9_0 = null;

        EObject lv_dependencies_11_0 = null;

        EObject lv_longName_13_0 = null;

        EObject lv_columnStructures_16_0 = null;

        EObject lv_columnStructures_18_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:1203:2: ( ( () otherlv_1= 'ColumnStructureModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'columnStructures' otherlv_15= '{' ( (lv_columnStructures_16_0= ruleColumnStructuredEntity ) ) (otherlv_17= ',' ( (lv_columnStructures_18_0= ruleColumnStructuredEntity ) ) )* otherlv_19= '}' )? otherlv_20= '}' ) )
            // InternalAorta.g:1204:2: ( () otherlv_1= 'ColumnStructureModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'columnStructures' otherlv_15= '{' ( (lv_columnStructures_16_0= ruleColumnStructuredEntity ) ) (otherlv_17= ',' ( (lv_columnStructures_18_0= ruleColumnStructuredEntity ) ) )* otherlv_19= '}' )? otherlv_20= '}' )
            {
            // InternalAorta.g:1204:2: ( () otherlv_1= 'ColumnStructureModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'columnStructures' otherlv_15= '{' ( (lv_columnStructures_16_0= ruleColumnStructuredEntity ) ) (otherlv_17= ',' ( (lv_columnStructures_18_0= ruleColumnStructuredEntity ) ) )* otherlv_19= '}' )? otherlv_20= '}' )
            // InternalAorta.g:1205:3: () otherlv_1= 'ColumnStructureModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'columnStructures' otherlv_15= '{' ( (lv_columnStructures_16_0= ruleColumnStructuredEntity ) ) (otherlv_17= ',' ( (lv_columnStructures_18_0= ruleColumnStructuredEntity ) ) )* otherlv_19= '}' )? otherlv_20= '}'
            {
            // InternalAorta.g:1205:3: ()
            // InternalAorta.g:1206:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getColumnStructureModuleAccess().getColumnStructureModuleAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,35,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getColumnStructureModuleAccess().getColumnStructureModuleKeyword_1());
            		
            // InternalAorta.g:1216:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:1217:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:1217:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:1218:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getColumnStructureModuleAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getColumnStructureModuleRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_45); 

            			newLeafNode(otherlv_3, grammarAccess.getColumnStructureModuleAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:1239:3: (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )?
            int alt32=2;
            int LA32_0 = input.LA(1);

            if ( (LA32_0==25) ) {
                alt32=1;
            }
            switch (alt32) {
                case 1 :
                    // InternalAorta.g:1240:4: otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) )
                    {
                    otherlv_4=(Token)match(input,25,FOLLOW_25); 

                    				newLeafNode(otherlv_4, grammarAccess.getColumnStructureModuleAccess().getTheDescriptionKeyword_4_0());
                    			
                    // InternalAorta.g:1244:4: ( (lv_theDescription_5_0= ruleEString ) )
                    // InternalAorta.g:1245:5: (lv_theDescription_5_0= ruleEString )
                    {
                    // InternalAorta.g:1245:5: (lv_theDescription_5_0= ruleEString )
                    // InternalAorta.g:1246:6: lv_theDescription_5_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getColumnStructureModuleAccess().getTheDescriptionEStringParserRuleCall_4_1_0());
                    					
                    pushFollow(FOLLOW_46);
                    lv_theDescription_5_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getColumnStructureModuleRule());
                    						}
                    						set(
                    							current,
                    							"theDescription",
                    							lv_theDescription_5_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:1264:3: (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )?
            int alt33=2;
            int LA33_0 = input.LA(1);

            if ( (LA33_0==26) ) {
                alt33=1;
            }
            switch (alt33) {
                case 1 :
                    // InternalAorta.g:1265:4: otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) )
                    {
                    otherlv_6=(Token)match(input,26,FOLLOW_25); 

                    				newLeafNode(otherlv_6, grammarAccess.getColumnStructureModuleAccess().getLicenseKeyword_5_0());
                    			
                    // InternalAorta.g:1269:4: ( (lv_license_7_0= ruleEString ) )
                    // InternalAorta.g:1270:5: (lv_license_7_0= ruleEString )
                    {
                    // InternalAorta.g:1270:5: (lv_license_7_0= ruleEString )
                    // InternalAorta.g:1271:6: lv_license_7_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getColumnStructureModuleAccess().getLicenseEStringParserRuleCall_5_1_0());
                    					
                    pushFollow(FOLLOW_47);
                    lv_license_7_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getColumnStructureModuleRule());
                    						}
                    						set(
                    							current,
                    							"license",
                    							lv_license_7_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:1289:3: (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )?
            int alt34=2;
            int LA34_0 = input.LA(1);

            if ( (LA34_0==27) ) {
                alt34=1;
            }
            switch (alt34) {
                case 1 :
                    // InternalAorta.g:1290:4: otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) )
                    {
                    otherlv_8=(Token)match(input,27,FOLLOW_25); 

                    				newLeafNode(otherlv_8, grammarAccess.getColumnStructureModuleAccess().getVersionKeyword_6_0());
                    			
                    // InternalAorta.g:1294:4: ( (lv_version_9_0= ruleEString ) )
                    // InternalAorta.g:1295:5: (lv_version_9_0= ruleEString )
                    {
                    // InternalAorta.g:1295:5: (lv_version_9_0= ruleEString )
                    // InternalAorta.g:1296:6: lv_version_9_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getColumnStructureModuleAccess().getVersionEStringParserRuleCall_6_1_0());
                    					
                    pushFollow(FOLLOW_48);
                    lv_version_9_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getColumnStructureModuleRule());
                    						}
                    						set(
                    							current,
                    							"version",
                    							lv_version_9_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:1314:3: (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )?
            int alt35=2;
            int LA35_0 = input.LA(1);

            if ( (LA35_0==28) ) {
                alt35=1;
            }
            switch (alt35) {
                case 1 :
                    // InternalAorta.g:1315:4: otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) )
                    {
                    otherlv_10=(Token)match(input,28,FOLLOW_30); 

                    				newLeafNode(otherlv_10, grammarAccess.getColumnStructureModuleAccess().getDependenciesKeyword_7_0());
                    			
                    // InternalAorta.g:1319:4: ( (lv_dependencies_11_0= ruleModuleDependencies ) )
                    // InternalAorta.g:1320:5: (lv_dependencies_11_0= ruleModuleDependencies )
                    {
                    // InternalAorta.g:1320:5: (lv_dependencies_11_0= ruleModuleDependencies )
                    // InternalAorta.g:1321:6: lv_dependencies_11_0= ruleModuleDependencies
                    {

                    						newCompositeNode(grammarAccess.getColumnStructureModuleAccess().getDependenciesModuleDependenciesParserRuleCall_7_1_0());
                    					
                    pushFollow(FOLLOW_49);
                    lv_dependencies_11_0=ruleModuleDependencies();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getColumnStructureModuleRule());
                    						}
                    						set(
                    							current,
                    							"dependencies",
                    							lv_dependencies_11_0,
                    							"org.eclipse.efbt.aorta.Aorta.ModuleDependencies");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:1339:3: (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )?
            int alt36=2;
            int LA36_0 = input.LA(1);

            if ( (LA36_0==29) ) {
                alt36=1;
            }
            switch (alt36) {
                case 1 :
                    // InternalAorta.g:1340:4: otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) )
                    {
                    otherlv_12=(Token)match(input,29,FOLLOW_32); 

                    				newLeafNode(otherlv_12, grammarAccess.getColumnStructureModuleAccess().getLongNameKeyword_8_0());
                    			
                    // InternalAorta.g:1344:4: ( (lv_longName_13_0= ruleModuleLongName ) )
                    // InternalAorta.g:1345:5: (lv_longName_13_0= ruleModuleLongName )
                    {
                    // InternalAorta.g:1345:5: (lv_longName_13_0= ruleModuleLongName )
                    // InternalAorta.g:1346:6: lv_longName_13_0= ruleModuleLongName
                    {

                    						newCompositeNode(grammarAccess.getColumnStructureModuleAccess().getLongNameModuleLongNameParserRuleCall_8_1_0());
                    					
                    pushFollow(FOLLOW_50);
                    lv_longName_13_0=ruleModuleLongName();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getColumnStructureModuleRule());
                    						}
                    						set(
                    							current,
                    							"longName",
                    							lv_longName_13_0,
                    							"org.eclipse.efbt.aorta.Aorta.ModuleLongName");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:1364:3: (otherlv_14= 'columnStructures' otherlv_15= '{' ( (lv_columnStructures_16_0= ruleColumnStructuredEntity ) ) (otherlv_17= ',' ( (lv_columnStructures_18_0= ruleColumnStructuredEntity ) ) )* otherlv_19= '}' )?
            int alt38=2;
            int LA38_0 = input.LA(1);

            if ( (LA38_0==36) ) {
                alt38=1;
            }
            switch (alt38) {
                case 1 :
                    // InternalAorta.g:1365:4: otherlv_14= 'columnStructures' otherlv_15= '{' ( (lv_columnStructures_16_0= ruleColumnStructuredEntity ) ) (otherlv_17= ',' ( (lv_columnStructures_18_0= ruleColumnStructuredEntity ) ) )* otherlv_19= '}'
                    {
                    otherlv_14=(Token)match(input,36,FOLLOW_3); 

                    				newLeafNode(otherlv_14, grammarAccess.getColumnStructureModuleAccess().getColumnStructuresKeyword_9_0());
                    			
                    otherlv_15=(Token)match(input,12,FOLLOW_51); 

                    				newLeafNode(otherlv_15, grammarAccess.getColumnStructureModuleAccess().getLeftCurlyBracketKeyword_9_1());
                    			
                    // InternalAorta.g:1373:4: ( (lv_columnStructures_16_0= ruleColumnStructuredEntity ) )
                    // InternalAorta.g:1374:5: (lv_columnStructures_16_0= ruleColumnStructuredEntity )
                    {
                    // InternalAorta.g:1374:5: (lv_columnStructures_16_0= ruleColumnStructuredEntity )
                    // InternalAorta.g:1375:6: lv_columnStructures_16_0= ruleColumnStructuredEntity
                    {

                    						newCompositeNode(grammarAccess.getColumnStructureModuleAccess().getColumnStructuresColumnStructuredEntityParserRuleCall_9_2_0());
                    					
                    pushFollow(FOLLOW_35);
                    lv_columnStructures_16_0=ruleColumnStructuredEntity();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getColumnStructureModuleRule());
                    						}
                    						add(
                    							current,
                    							"columnStructures",
                    							lv_columnStructures_16_0,
                    							"org.eclipse.efbt.aorta.Aorta.ColumnStructuredEntity");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:1392:4: (otherlv_17= ',' ( (lv_columnStructures_18_0= ruleColumnStructuredEntity ) ) )*
                    loop37:
                    do {
                        int alt37=2;
                        int LA37_0 = input.LA(1);

                        if ( (LA37_0==31) ) {
                            alt37=1;
                        }


                        switch (alt37) {
                    	case 1 :
                    	    // InternalAorta.g:1393:5: otherlv_17= ',' ( (lv_columnStructures_18_0= ruleColumnStructuredEntity ) )
                    	    {
                    	    otherlv_17=(Token)match(input,31,FOLLOW_51); 

                    	    					newLeafNode(otherlv_17, grammarAccess.getColumnStructureModuleAccess().getCommaKeyword_9_3_0());
                    	    				
                    	    // InternalAorta.g:1397:5: ( (lv_columnStructures_18_0= ruleColumnStructuredEntity ) )
                    	    // InternalAorta.g:1398:6: (lv_columnStructures_18_0= ruleColumnStructuredEntity )
                    	    {
                    	    // InternalAorta.g:1398:6: (lv_columnStructures_18_0= ruleColumnStructuredEntity )
                    	    // InternalAorta.g:1399:7: lv_columnStructures_18_0= ruleColumnStructuredEntity
                    	    {

                    	    							newCompositeNode(grammarAccess.getColumnStructureModuleAccess().getColumnStructuresColumnStructuredEntityParserRuleCall_9_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_35);
                    	    lv_columnStructures_18_0=ruleColumnStructuredEntity();

                    	    state._fsp--;


                    	    							if (current==null) {
                    	    								current = createModelElementForParent(grammarAccess.getColumnStructureModuleRule());
                    	    							}
                    	    							add(
                    	    								current,
                    	    								"columnStructures",
                    	    								lv_columnStructures_18_0,
                    	    								"org.eclipse.efbt.aorta.Aorta.ColumnStructuredEntity");
                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop37;
                        }
                    } while (true);

                    otherlv_19=(Token)match(input,23,FOLLOW_24); 

                    				newLeafNode(otherlv_19, grammarAccess.getColumnStructureModuleAccess().getRightCurlyBracketKeyword_9_4());
                    			

                    }
                    break;

            }

            otherlv_20=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_20, grammarAccess.getColumnStructureModuleAccess().getRightCurlyBracketKeyword_10());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleColumnStructureModule"


    // $ANTLR start "entryRuleRegReportModule"
    // InternalAorta.g:1430:1: entryRuleRegReportModule returns [EObject current=null] : iv_ruleRegReportModule= ruleRegReportModule EOF ;
    public final EObject entryRuleRegReportModule() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleRegReportModule = null;


        try {
            // InternalAorta.g:1430:56: (iv_ruleRegReportModule= ruleRegReportModule EOF )
            // InternalAorta.g:1431:2: iv_ruleRegReportModule= ruleRegReportModule EOF
            {
             newCompositeNode(grammarAccess.getRegReportModuleRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleRegReportModule=ruleRegReportModule();

            state._fsp--;

             current =iv_ruleRegReportModule; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleRegReportModule"


    // $ANTLR start "ruleRegReportModule"
    // InternalAorta.g:1437:1: ruleRegReportModule returns [EObject current=null] : ( () otherlv_1= 'RegReportModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'reportGroups' otherlv_15= '{' ( (lv_reportGroups_16_0= ruleReportGroup ) ) (otherlv_17= ',' ( (lv_reportGroups_18_0= ruleReportGroup ) ) )* otherlv_19= '}' )? otherlv_20= '}' ) ;
    public final EObject ruleRegReportModule() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_6=null;
        Token otherlv_8=null;
        Token otherlv_10=null;
        Token otherlv_12=null;
        Token otherlv_14=null;
        Token otherlv_15=null;
        Token otherlv_17=null;
        Token otherlv_19=null;
        Token otherlv_20=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;

        AntlrDatatypeRuleToken lv_theDescription_5_0 = null;

        AntlrDatatypeRuleToken lv_license_7_0 = null;

        AntlrDatatypeRuleToken lv_version_9_0 = null;

        EObject lv_dependencies_11_0 = null;

        EObject lv_longName_13_0 = null;

        EObject lv_reportGroups_16_0 = null;

        EObject lv_reportGroups_18_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:1443:2: ( ( () otherlv_1= 'RegReportModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'reportGroups' otherlv_15= '{' ( (lv_reportGroups_16_0= ruleReportGroup ) ) (otherlv_17= ',' ( (lv_reportGroups_18_0= ruleReportGroup ) ) )* otherlv_19= '}' )? otherlv_20= '}' ) )
            // InternalAorta.g:1444:2: ( () otherlv_1= 'RegReportModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'reportGroups' otherlv_15= '{' ( (lv_reportGroups_16_0= ruleReportGroup ) ) (otherlv_17= ',' ( (lv_reportGroups_18_0= ruleReportGroup ) ) )* otherlv_19= '}' )? otherlv_20= '}' )
            {
            // InternalAorta.g:1444:2: ( () otherlv_1= 'RegReportModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'reportGroups' otherlv_15= '{' ( (lv_reportGroups_16_0= ruleReportGroup ) ) (otherlv_17= ',' ( (lv_reportGroups_18_0= ruleReportGroup ) ) )* otherlv_19= '}' )? otherlv_20= '}' )
            // InternalAorta.g:1445:3: () otherlv_1= 'RegReportModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'reportGroups' otherlv_15= '{' ( (lv_reportGroups_16_0= ruleReportGroup ) ) (otherlv_17= ',' ( (lv_reportGroups_18_0= ruleReportGroup ) ) )* otherlv_19= '}' )? otherlv_20= '}'
            {
            // InternalAorta.g:1445:3: ()
            // InternalAorta.g:1446:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getRegReportModuleAccess().getRegReportModuleAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,37,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getRegReportModuleAccess().getRegReportModuleKeyword_1());
            		
            // InternalAorta.g:1456:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:1457:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:1457:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:1458:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getRegReportModuleAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getRegReportModuleRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_52); 

            			newLeafNode(otherlv_3, grammarAccess.getRegReportModuleAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:1479:3: (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )?
            int alt39=2;
            int LA39_0 = input.LA(1);

            if ( (LA39_0==25) ) {
                alt39=1;
            }
            switch (alt39) {
                case 1 :
                    // InternalAorta.g:1480:4: otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) )
                    {
                    otherlv_4=(Token)match(input,25,FOLLOW_25); 

                    				newLeafNode(otherlv_4, grammarAccess.getRegReportModuleAccess().getTheDescriptionKeyword_4_0());
                    			
                    // InternalAorta.g:1484:4: ( (lv_theDescription_5_0= ruleEString ) )
                    // InternalAorta.g:1485:5: (lv_theDescription_5_0= ruleEString )
                    {
                    // InternalAorta.g:1485:5: (lv_theDescription_5_0= ruleEString )
                    // InternalAorta.g:1486:6: lv_theDescription_5_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getRegReportModuleAccess().getTheDescriptionEStringParserRuleCall_4_1_0());
                    					
                    pushFollow(FOLLOW_53);
                    lv_theDescription_5_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getRegReportModuleRule());
                    						}
                    						set(
                    							current,
                    							"theDescription",
                    							lv_theDescription_5_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:1504:3: (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )?
            int alt40=2;
            int LA40_0 = input.LA(1);

            if ( (LA40_0==26) ) {
                alt40=1;
            }
            switch (alt40) {
                case 1 :
                    // InternalAorta.g:1505:4: otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) )
                    {
                    otherlv_6=(Token)match(input,26,FOLLOW_25); 

                    				newLeafNode(otherlv_6, grammarAccess.getRegReportModuleAccess().getLicenseKeyword_5_0());
                    			
                    // InternalAorta.g:1509:4: ( (lv_license_7_0= ruleEString ) )
                    // InternalAorta.g:1510:5: (lv_license_7_0= ruleEString )
                    {
                    // InternalAorta.g:1510:5: (lv_license_7_0= ruleEString )
                    // InternalAorta.g:1511:6: lv_license_7_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getRegReportModuleAccess().getLicenseEStringParserRuleCall_5_1_0());
                    					
                    pushFollow(FOLLOW_54);
                    lv_license_7_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getRegReportModuleRule());
                    						}
                    						set(
                    							current,
                    							"license",
                    							lv_license_7_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:1529:3: (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )?
            int alt41=2;
            int LA41_0 = input.LA(1);

            if ( (LA41_0==27) ) {
                alt41=1;
            }
            switch (alt41) {
                case 1 :
                    // InternalAorta.g:1530:4: otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) )
                    {
                    otherlv_8=(Token)match(input,27,FOLLOW_25); 

                    				newLeafNode(otherlv_8, grammarAccess.getRegReportModuleAccess().getVersionKeyword_6_0());
                    			
                    // InternalAorta.g:1534:4: ( (lv_version_9_0= ruleEString ) )
                    // InternalAorta.g:1535:5: (lv_version_9_0= ruleEString )
                    {
                    // InternalAorta.g:1535:5: (lv_version_9_0= ruleEString )
                    // InternalAorta.g:1536:6: lv_version_9_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getRegReportModuleAccess().getVersionEStringParserRuleCall_6_1_0());
                    					
                    pushFollow(FOLLOW_55);
                    lv_version_9_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getRegReportModuleRule());
                    						}
                    						set(
                    							current,
                    							"version",
                    							lv_version_9_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:1554:3: (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )?
            int alt42=2;
            int LA42_0 = input.LA(1);

            if ( (LA42_0==28) ) {
                alt42=1;
            }
            switch (alt42) {
                case 1 :
                    // InternalAorta.g:1555:4: otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) )
                    {
                    otherlv_10=(Token)match(input,28,FOLLOW_30); 

                    				newLeafNode(otherlv_10, grammarAccess.getRegReportModuleAccess().getDependenciesKeyword_7_0());
                    			
                    // InternalAorta.g:1559:4: ( (lv_dependencies_11_0= ruleModuleDependencies ) )
                    // InternalAorta.g:1560:5: (lv_dependencies_11_0= ruleModuleDependencies )
                    {
                    // InternalAorta.g:1560:5: (lv_dependencies_11_0= ruleModuleDependencies )
                    // InternalAorta.g:1561:6: lv_dependencies_11_0= ruleModuleDependencies
                    {

                    						newCompositeNode(grammarAccess.getRegReportModuleAccess().getDependenciesModuleDependenciesParserRuleCall_7_1_0());
                    					
                    pushFollow(FOLLOW_56);
                    lv_dependencies_11_0=ruleModuleDependencies();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getRegReportModuleRule());
                    						}
                    						set(
                    							current,
                    							"dependencies",
                    							lv_dependencies_11_0,
                    							"org.eclipse.efbt.aorta.Aorta.ModuleDependencies");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:1579:3: (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )?
            int alt43=2;
            int LA43_0 = input.LA(1);

            if ( (LA43_0==29) ) {
                alt43=1;
            }
            switch (alt43) {
                case 1 :
                    // InternalAorta.g:1580:4: otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) )
                    {
                    otherlv_12=(Token)match(input,29,FOLLOW_32); 

                    				newLeafNode(otherlv_12, grammarAccess.getRegReportModuleAccess().getLongNameKeyword_8_0());
                    			
                    // InternalAorta.g:1584:4: ( (lv_longName_13_0= ruleModuleLongName ) )
                    // InternalAorta.g:1585:5: (lv_longName_13_0= ruleModuleLongName )
                    {
                    // InternalAorta.g:1585:5: (lv_longName_13_0= ruleModuleLongName )
                    // InternalAorta.g:1586:6: lv_longName_13_0= ruleModuleLongName
                    {

                    						newCompositeNode(grammarAccess.getRegReportModuleAccess().getLongNameModuleLongNameParserRuleCall_8_1_0());
                    					
                    pushFollow(FOLLOW_57);
                    lv_longName_13_0=ruleModuleLongName();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getRegReportModuleRule());
                    						}
                    						set(
                    							current,
                    							"longName",
                    							lv_longName_13_0,
                    							"org.eclipse.efbt.aorta.Aorta.ModuleLongName");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:1604:3: (otherlv_14= 'reportGroups' otherlv_15= '{' ( (lv_reportGroups_16_0= ruleReportGroup ) ) (otherlv_17= ',' ( (lv_reportGroups_18_0= ruleReportGroup ) ) )* otherlv_19= '}' )?
            int alt45=2;
            int LA45_0 = input.LA(1);

            if ( (LA45_0==38) ) {
                alt45=1;
            }
            switch (alt45) {
                case 1 :
                    // InternalAorta.g:1605:4: otherlv_14= 'reportGroups' otherlv_15= '{' ( (lv_reportGroups_16_0= ruleReportGroup ) ) (otherlv_17= ',' ( (lv_reportGroups_18_0= ruleReportGroup ) ) )* otherlv_19= '}'
                    {
                    otherlv_14=(Token)match(input,38,FOLLOW_3); 

                    				newLeafNode(otherlv_14, grammarAccess.getRegReportModuleAccess().getReportGroupsKeyword_9_0());
                    			
                    otherlv_15=(Token)match(input,12,FOLLOW_58); 

                    				newLeafNode(otherlv_15, grammarAccess.getRegReportModuleAccess().getLeftCurlyBracketKeyword_9_1());
                    			
                    // InternalAorta.g:1613:4: ( (lv_reportGroups_16_0= ruleReportGroup ) )
                    // InternalAorta.g:1614:5: (lv_reportGroups_16_0= ruleReportGroup )
                    {
                    // InternalAorta.g:1614:5: (lv_reportGroups_16_0= ruleReportGroup )
                    // InternalAorta.g:1615:6: lv_reportGroups_16_0= ruleReportGroup
                    {

                    						newCompositeNode(grammarAccess.getRegReportModuleAccess().getReportGroupsReportGroupParserRuleCall_9_2_0());
                    					
                    pushFollow(FOLLOW_35);
                    lv_reportGroups_16_0=ruleReportGroup();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getRegReportModuleRule());
                    						}
                    						add(
                    							current,
                    							"reportGroups",
                    							lv_reportGroups_16_0,
                    							"org.eclipse.efbt.aorta.Aorta.ReportGroup");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:1632:4: (otherlv_17= ',' ( (lv_reportGroups_18_0= ruleReportGroup ) ) )*
                    loop44:
                    do {
                        int alt44=2;
                        int LA44_0 = input.LA(1);

                        if ( (LA44_0==31) ) {
                            alt44=1;
                        }


                        switch (alt44) {
                    	case 1 :
                    	    // InternalAorta.g:1633:5: otherlv_17= ',' ( (lv_reportGroups_18_0= ruleReportGroup ) )
                    	    {
                    	    otherlv_17=(Token)match(input,31,FOLLOW_58); 

                    	    					newLeafNode(otherlv_17, grammarAccess.getRegReportModuleAccess().getCommaKeyword_9_3_0());
                    	    				
                    	    // InternalAorta.g:1637:5: ( (lv_reportGroups_18_0= ruleReportGroup ) )
                    	    // InternalAorta.g:1638:6: (lv_reportGroups_18_0= ruleReportGroup )
                    	    {
                    	    // InternalAorta.g:1638:6: (lv_reportGroups_18_0= ruleReportGroup )
                    	    // InternalAorta.g:1639:7: lv_reportGroups_18_0= ruleReportGroup
                    	    {

                    	    							newCompositeNode(grammarAccess.getRegReportModuleAccess().getReportGroupsReportGroupParserRuleCall_9_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_35);
                    	    lv_reportGroups_18_0=ruleReportGroup();

                    	    state._fsp--;


                    	    							if (current==null) {
                    	    								current = createModelElementForParent(grammarAccess.getRegReportModuleRule());
                    	    							}
                    	    							add(
                    	    								current,
                    	    								"reportGroups",
                    	    								lv_reportGroups_18_0,
                    	    								"org.eclipse.efbt.aorta.Aorta.ReportGroup");
                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop44;
                        }
                    } while (true);

                    otherlv_19=(Token)match(input,23,FOLLOW_24); 

                    				newLeafNode(otherlv_19, grammarAccess.getRegReportModuleAccess().getRightCurlyBracketKeyword_9_4());
                    			

                    }
                    break;

            }

            otherlv_20=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_20, grammarAccess.getRegReportModuleAccess().getRightCurlyBracketKeyword_10());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleRegReportModule"


    // $ANTLR start "entryRuleTestDefinitionModule"
    // InternalAorta.g:1670:1: entryRuleTestDefinitionModule returns [EObject current=null] : iv_ruleTestDefinitionModule= ruleTestDefinitionModule EOF ;
    public final EObject entryRuleTestDefinitionModule() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleTestDefinitionModule = null;


        try {
            // InternalAorta.g:1670:61: (iv_ruleTestDefinitionModule= ruleTestDefinitionModule EOF )
            // InternalAorta.g:1671:2: iv_ruleTestDefinitionModule= ruleTestDefinitionModule EOF
            {
             newCompositeNode(grammarAccess.getTestDefinitionModuleRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleTestDefinitionModule=ruleTestDefinitionModule();

            state._fsp--;

             current =iv_ruleTestDefinitionModule; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleTestDefinitionModule"


    // $ANTLR start "ruleTestDefinitionModule"
    // InternalAorta.g:1677:1: ruleTestDefinitionModule returns [EObject current=null] : ( () otherlv_1= 'TestDefinitionModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'testDefinitions' otherlv_15= '{' ( (lv_testDefinitions_16_0= ruleTestDefinition ) ) (otherlv_17= ',' ( (lv_testDefinitions_18_0= ruleTestDefinition ) ) )* otherlv_19= '}' )? otherlv_20= '}' ) ;
    public final EObject ruleTestDefinitionModule() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_6=null;
        Token otherlv_8=null;
        Token otherlv_10=null;
        Token otherlv_12=null;
        Token otherlv_14=null;
        Token otherlv_15=null;
        Token otherlv_17=null;
        Token otherlv_19=null;
        Token otherlv_20=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;

        AntlrDatatypeRuleToken lv_theDescription_5_0 = null;

        AntlrDatatypeRuleToken lv_license_7_0 = null;

        AntlrDatatypeRuleToken lv_version_9_0 = null;

        EObject lv_dependencies_11_0 = null;

        EObject lv_longName_13_0 = null;

        EObject lv_testDefinitions_16_0 = null;

        EObject lv_testDefinitions_18_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:1683:2: ( ( () otherlv_1= 'TestDefinitionModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'testDefinitions' otherlv_15= '{' ( (lv_testDefinitions_16_0= ruleTestDefinition ) ) (otherlv_17= ',' ( (lv_testDefinitions_18_0= ruleTestDefinition ) ) )* otherlv_19= '}' )? otherlv_20= '}' ) )
            // InternalAorta.g:1684:2: ( () otherlv_1= 'TestDefinitionModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'testDefinitions' otherlv_15= '{' ( (lv_testDefinitions_16_0= ruleTestDefinition ) ) (otherlv_17= ',' ( (lv_testDefinitions_18_0= ruleTestDefinition ) ) )* otherlv_19= '}' )? otherlv_20= '}' )
            {
            // InternalAorta.g:1684:2: ( () otherlv_1= 'TestDefinitionModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'testDefinitions' otherlv_15= '{' ( (lv_testDefinitions_16_0= ruleTestDefinition ) ) (otherlv_17= ',' ( (lv_testDefinitions_18_0= ruleTestDefinition ) ) )* otherlv_19= '}' )? otherlv_20= '}' )
            // InternalAorta.g:1685:3: () otherlv_1= 'TestDefinitionModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'testDefinitions' otherlv_15= '{' ( (lv_testDefinitions_16_0= ruleTestDefinition ) ) (otherlv_17= ',' ( (lv_testDefinitions_18_0= ruleTestDefinition ) ) )* otherlv_19= '}' )? otherlv_20= '}'
            {
            // InternalAorta.g:1685:3: ()
            // InternalAorta.g:1686:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getTestDefinitionModuleAccess().getTestDefinitionModuleAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,39,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getTestDefinitionModuleAccess().getTestDefinitionModuleKeyword_1());
            		
            // InternalAorta.g:1696:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:1697:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:1697:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:1698:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getTestDefinitionModuleAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getTestDefinitionModuleRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_59); 

            			newLeafNode(otherlv_3, grammarAccess.getTestDefinitionModuleAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:1719:3: (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )?
            int alt46=2;
            int LA46_0 = input.LA(1);

            if ( (LA46_0==25) ) {
                alt46=1;
            }
            switch (alt46) {
                case 1 :
                    // InternalAorta.g:1720:4: otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) )
                    {
                    otherlv_4=(Token)match(input,25,FOLLOW_25); 

                    				newLeafNode(otherlv_4, grammarAccess.getTestDefinitionModuleAccess().getTheDescriptionKeyword_4_0());
                    			
                    // InternalAorta.g:1724:4: ( (lv_theDescription_5_0= ruleEString ) )
                    // InternalAorta.g:1725:5: (lv_theDescription_5_0= ruleEString )
                    {
                    // InternalAorta.g:1725:5: (lv_theDescription_5_0= ruleEString )
                    // InternalAorta.g:1726:6: lv_theDescription_5_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getTestDefinitionModuleAccess().getTheDescriptionEStringParserRuleCall_4_1_0());
                    					
                    pushFollow(FOLLOW_60);
                    lv_theDescription_5_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getTestDefinitionModuleRule());
                    						}
                    						set(
                    							current,
                    							"theDescription",
                    							lv_theDescription_5_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:1744:3: (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )?
            int alt47=2;
            int LA47_0 = input.LA(1);

            if ( (LA47_0==26) ) {
                alt47=1;
            }
            switch (alt47) {
                case 1 :
                    // InternalAorta.g:1745:4: otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) )
                    {
                    otherlv_6=(Token)match(input,26,FOLLOW_25); 

                    				newLeafNode(otherlv_6, grammarAccess.getTestDefinitionModuleAccess().getLicenseKeyword_5_0());
                    			
                    // InternalAorta.g:1749:4: ( (lv_license_7_0= ruleEString ) )
                    // InternalAorta.g:1750:5: (lv_license_7_0= ruleEString )
                    {
                    // InternalAorta.g:1750:5: (lv_license_7_0= ruleEString )
                    // InternalAorta.g:1751:6: lv_license_7_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getTestDefinitionModuleAccess().getLicenseEStringParserRuleCall_5_1_0());
                    					
                    pushFollow(FOLLOW_61);
                    lv_license_7_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getTestDefinitionModuleRule());
                    						}
                    						set(
                    							current,
                    							"license",
                    							lv_license_7_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:1769:3: (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )?
            int alt48=2;
            int LA48_0 = input.LA(1);

            if ( (LA48_0==27) ) {
                alt48=1;
            }
            switch (alt48) {
                case 1 :
                    // InternalAorta.g:1770:4: otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) )
                    {
                    otherlv_8=(Token)match(input,27,FOLLOW_25); 

                    				newLeafNode(otherlv_8, grammarAccess.getTestDefinitionModuleAccess().getVersionKeyword_6_0());
                    			
                    // InternalAorta.g:1774:4: ( (lv_version_9_0= ruleEString ) )
                    // InternalAorta.g:1775:5: (lv_version_9_0= ruleEString )
                    {
                    // InternalAorta.g:1775:5: (lv_version_9_0= ruleEString )
                    // InternalAorta.g:1776:6: lv_version_9_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getTestDefinitionModuleAccess().getVersionEStringParserRuleCall_6_1_0());
                    					
                    pushFollow(FOLLOW_62);
                    lv_version_9_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getTestDefinitionModuleRule());
                    						}
                    						set(
                    							current,
                    							"version",
                    							lv_version_9_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:1794:3: (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )?
            int alt49=2;
            int LA49_0 = input.LA(1);

            if ( (LA49_0==28) ) {
                alt49=1;
            }
            switch (alt49) {
                case 1 :
                    // InternalAorta.g:1795:4: otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) )
                    {
                    otherlv_10=(Token)match(input,28,FOLLOW_30); 

                    				newLeafNode(otherlv_10, grammarAccess.getTestDefinitionModuleAccess().getDependenciesKeyword_7_0());
                    			
                    // InternalAorta.g:1799:4: ( (lv_dependencies_11_0= ruleModuleDependencies ) )
                    // InternalAorta.g:1800:5: (lv_dependencies_11_0= ruleModuleDependencies )
                    {
                    // InternalAorta.g:1800:5: (lv_dependencies_11_0= ruleModuleDependencies )
                    // InternalAorta.g:1801:6: lv_dependencies_11_0= ruleModuleDependencies
                    {

                    						newCompositeNode(grammarAccess.getTestDefinitionModuleAccess().getDependenciesModuleDependenciesParserRuleCall_7_1_0());
                    					
                    pushFollow(FOLLOW_63);
                    lv_dependencies_11_0=ruleModuleDependencies();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getTestDefinitionModuleRule());
                    						}
                    						set(
                    							current,
                    							"dependencies",
                    							lv_dependencies_11_0,
                    							"org.eclipse.efbt.aorta.Aorta.ModuleDependencies");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:1819:3: (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )?
            int alt50=2;
            int LA50_0 = input.LA(1);

            if ( (LA50_0==29) ) {
                alt50=1;
            }
            switch (alt50) {
                case 1 :
                    // InternalAorta.g:1820:4: otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) )
                    {
                    otherlv_12=(Token)match(input,29,FOLLOW_32); 

                    				newLeafNode(otherlv_12, grammarAccess.getTestDefinitionModuleAccess().getLongNameKeyword_8_0());
                    			
                    // InternalAorta.g:1824:4: ( (lv_longName_13_0= ruleModuleLongName ) )
                    // InternalAorta.g:1825:5: (lv_longName_13_0= ruleModuleLongName )
                    {
                    // InternalAorta.g:1825:5: (lv_longName_13_0= ruleModuleLongName )
                    // InternalAorta.g:1826:6: lv_longName_13_0= ruleModuleLongName
                    {

                    						newCompositeNode(grammarAccess.getTestDefinitionModuleAccess().getLongNameModuleLongNameParserRuleCall_8_1_0());
                    					
                    pushFollow(FOLLOW_64);
                    lv_longName_13_0=ruleModuleLongName();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getTestDefinitionModuleRule());
                    						}
                    						set(
                    							current,
                    							"longName",
                    							lv_longName_13_0,
                    							"org.eclipse.efbt.aorta.Aorta.ModuleLongName");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:1844:3: (otherlv_14= 'testDefinitions' otherlv_15= '{' ( (lv_testDefinitions_16_0= ruleTestDefinition ) ) (otherlv_17= ',' ( (lv_testDefinitions_18_0= ruleTestDefinition ) ) )* otherlv_19= '}' )?
            int alt52=2;
            int LA52_0 = input.LA(1);

            if ( (LA52_0==17) ) {
                alt52=1;
            }
            switch (alt52) {
                case 1 :
                    // InternalAorta.g:1845:4: otherlv_14= 'testDefinitions' otherlv_15= '{' ( (lv_testDefinitions_16_0= ruleTestDefinition ) ) (otherlv_17= ',' ( (lv_testDefinitions_18_0= ruleTestDefinition ) ) )* otherlv_19= '}'
                    {
                    otherlv_14=(Token)match(input,17,FOLLOW_3); 

                    				newLeafNode(otherlv_14, grammarAccess.getTestDefinitionModuleAccess().getTestDefinitionsKeyword_9_0());
                    			
                    otherlv_15=(Token)match(input,12,FOLLOW_65); 

                    				newLeafNode(otherlv_15, grammarAccess.getTestDefinitionModuleAccess().getLeftCurlyBracketKeyword_9_1());
                    			
                    // InternalAorta.g:1853:4: ( (lv_testDefinitions_16_0= ruleTestDefinition ) )
                    // InternalAorta.g:1854:5: (lv_testDefinitions_16_0= ruleTestDefinition )
                    {
                    // InternalAorta.g:1854:5: (lv_testDefinitions_16_0= ruleTestDefinition )
                    // InternalAorta.g:1855:6: lv_testDefinitions_16_0= ruleTestDefinition
                    {

                    						newCompositeNode(grammarAccess.getTestDefinitionModuleAccess().getTestDefinitionsTestDefinitionParserRuleCall_9_2_0());
                    					
                    pushFollow(FOLLOW_35);
                    lv_testDefinitions_16_0=ruleTestDefinition();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getTestDefinitionModuleRule());
                    						}
                    						add(
                    							current,
                    							"testDefinitions",
                    							lv_testDefinitions_16_0,
                    							"org.eclipse.efbt.aorta.Aorta.TestDefinition");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:1872:4: (otherlv_17= ',' ( (lv_testDefinitions_18_0= ruleTestDefinition ) ) )*
                    loop51:
                    do {
                        int alt51=2;
                        int LA51_0 = input.LA(1);

                        if ( (LA51_0==31) ) {
                            alt51=1;
                        }


                        switch (alt51) {
                    	case 1 :
                    	    // InternalAorta.g:1873:5: otherlv_17= ',' ( (lv_testDefinitions_18_0= ruleTestDefinition ) )
                    	    {
                    	    otherlv_17=(Token)match(input,31,FOLLOW_65); 

                    	    					newLeafNode(otherlv_17, grammarAccess.getTestDefinitionModuleAccess().getCommaKeyword_9_3_0());
                    	    				
                    	    // InternalAorta.g:1877:5: ( (lv_testDefinitions_18_0= ruleTestDefinition ) )
                    	    // InternalAorta.g:1878:6: (lv_testDefinitions_18_0= ruleTestDefinition )
                    	    {
                    	    // InternalAorta.g:1878:6: (lv_testDefinitions_18_0= ruleTestDefinition )
                    	    // InternalAorta.g:1879:7: lv_testDefinitions_18_0= ruleTestDefinition
                    	    {

                    	    							newCompositeNode(grammarAccess.getTestDefinitionModuleAccess().getTestDefinitionsTestDefinitionParserRuleCall_9_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_35);
                    	    lv_testDefinitions_18_0=ruleTestDefinition();

                    	    state._fsp--;


                    	    							if (current==null) {
                    	    								current = createModelElementForParent(grammarAccess.getTestDefinitionModuleRule());
                    	    							}
                    	    							add(
                    	    								current,
                    	    								"testDefinitions",
                    	    								lv_testDefinitions_18_0,
                    	    								"org.eclipse.efbt.aorta.Aorta.TestDefinition");
                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop51;
                        }
                    } while (true);

                    otherlv_19=(Token)match(input,23,FOLLOW_24); 

                    				newLeafNode(otherlv_19, grammarAccess.getTestDefinitionModuleAccess().getRightCurlyBracketKeyword_9_4());
                    			

                    }
                    break;

            }

            otherlv_20=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_20, grammarAccess.getTestDefinitionModuleAccess().getRightCurlyBracketKeyword_10());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleTestDefinitionModule"


    // $ANTLR start "entryRuleTestModule"
    // InternalAorta.g:1910:1: entryRuleTestModule returns [EObject current=null] : iv_ruleTestModule= ruleTestModule EOF ;
    public final EObject entryRuleTestModule() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleTestModule = null;


        try {
            // InternalAorta.g:1910:51: (iv_ruleTestModule= ruleTestModule EOF )
            // InternalAorta.g:1911:2: iv_ruleTestModule= ruleTestModule EOF
            {
             newCompositeNode(grammarAccess.getTestModuleRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleTestModule=ruleTestModule();

            state._fsp--;

             current =iv_ruleTestModule; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleTestModule"


    // $ANTLR start "ruleTestModule"
    // InternalAorta.g:1917:1: ruleTestModule returns [EObject current=null] : ( () otherlv_1= 'TestModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'executionEnvironment' ( ( ruleQualifiedName ) ) )? (otherlv_12= 'dependencies' ( (lv_dependencies_13_0= ruleModuleDependencies ) ) )? (otherlv_14= 'longName' ( (lv_longName_15_0= ruleModuleLongName ) ) )? (otherlv_16= 'tests' otherlv_17= '{' ( (lv_tests_18_0= ruleE2ETest ) ) (otherlv_19= ',' ( (lv_tests_20_0= ruleE2ETest ) ) )* otherlv_21= '}' )? otherlv_22= '}' ) ;
    public final EObject ruleTestModule() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_6=null;
        Token otherlv_8=null;
        Token otherlv_10=null;
        Token otherlv_12=null;
        Token otherlv_14=null;
        Token otherlv_16=null;
        Token otherlv_17=null;
        Token otherlv_19=null;
        Token otherlv_21=null;
        Token otherlv_22=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;

        AntlrDatatypeRuleToken lv_theDescription_5_0 = null;

        AntlrDatatypeRuleToken lv_license_7_0 = null;

        AntlrDatatypeRuleToken lv_version_9_0 = null;

        EObject lv_dependencies_13_0 = null;

        EObject lv_longName_15_0 = null;

        EObject lv_tests_18_0 = null;

        EObject lv_tests_20_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:1923:2: ( ( () otherlv_1= 'TestModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'executionEnvironment' ( ( ruleQualifiedName ) ) )? (otherlv_12= 'dependencies' ( (lv_dependencies_13_0= ruleModuleDependencies ) ) )? (otherlv_14= 'longName' ( (lv_longName_15_0= ruleModuleLongName ) ) )? (otherlv_16= 'tests' otherlv_17= '{' ( (lv_tests_18_0= ruleE2ETest ) ) (otherlv_19= ',' ( (lv_tests_20_0= ruleE2ETest ) ) )* otherlv_21= '}' )? otherlv_22= '}' ) )
            // InternalAorta.g:1924:2: ( () otherlv_1= 'TestModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'executionEnvironment' ( ( ruleQualifiedName ) ) )? (otherlv_12= 'dependencies' ( (lv_dependencies_13_0= ruleModuleDependencies ) ) )? (otherlv_14= 'longName' ( (lv_longName_15_0= ruleModuleLongName ) ) )? (otherlv_16= 'tests' otherlv_17= '{' ( (lv_tests_18_0= ruleE2ETest ) ) (otherlv_19= ',' ( (lv_tests_20_0= ruleE2ETest ) ) )* otherlv_21= '}' )? otherlv_22= '}' )
            {
            // InternalAorta.g:1924:2: ( () otherlv_1= 'TestModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'executionEnvironment' ( ( ruleQualifiedName ) ) )? (otherlv_12= 'dependencies' ( (lv_dependencies_13_0= ruleModuleDependencies ) ) )? (otherlv_14= 'longName' ( (lv_longName_15_0= ruleModuleLongName ) ) )? (otherlv_16= 'tests' otherlv_17= '{' ( (lv_tests_18_0= ruleE2ETest ) ) (otherlv_19= ',' ( (lv_tests_20_0= ruleE2ETest ) ) )* otherlv_21= '}' )? otherlv_22= '}' )
            // InternalAorta.g:1925:3: () otherlv_1= 'TestModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'executionEnvironment' ( ( ruleQualifiedName ) ) )? (otherlv_12= 'dependencies' ( (lv_dependencies_13_0= ruleModuleDependencies ) ) )? (otherlv_14= 'longName' ( (lv_longName_15_0= ruleModuleLongName ) ) )? (otherlv_16= 'tests' otherlv_17= '{' ( (lv_tests_18_0= ruleE2ETest ) ) (otherlv_19= ',' ( (lv_tests_20_0= ruleE2ETest ) ) )* otherlv_21= '}' )? otherlv_22= '}'
            {
            // InternalAorta.g:1925:3: ()
            // InternalAorta.g:1926:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getTestModuleAccess().getTestModuleAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,40,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getTestModuleAccess().getTestModuleKeyword_1());
            		
            // InternalAorta.g:1936:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:1937:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:1937:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:1938:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getTestModuleAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getTestModuleRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_66); 

            			newLeafNode(otherlv_3, grammarAccess.getTestModuleAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:1959:3: (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )?
            int alt53=2;
            int LA53_0 = input.LA(1);

            if ( (LA53_0==25) ) {
                alt53=1;
            }
            switch (alt53) {
                case 1 :
                    // InternalAorta.g:1960:4: otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) )
                    {
                    otherlv_4=(Token)match(input,25,FOLLOW_25); 

                    				newLeafNode(otherlv_4, grammarAccess.getTestModuleAccess().getTheDescriptionKeyword_4_0());
                    			
                    // InternalAorta.g:1964:4: ( (lv_theDescription_5_0= ruleEString ) )
                    // InternalAorta.g:1965:5: (lv_theDescription_5_0= ruleEString )
                    {
                    // InternalAorta.g:1965:5: (lv_theDescription_5_0= ruleEString )
                    // InternalAorta.g:1966:6: lv_theDescription_5_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getTestModuleAccess().getTheDescriptionEStringParserRuleCall_4_1_0());
                    					
                    pushFollow(FOLLOW_67);
                    lv_theDescription_5_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getTestModuleRule());
                    						}
                    						set(
                    							current,
                    							"theDescription",
                    							lv_theDescription_5_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:1984:3: (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )?
            int alt54=2;
            int LA54_0 = input.LA(1);

            if ( (LA54_0==26) ) {
                alt54=1;
            }
            switch (alt54) {
                case 1 :
                    // InternalAorta.g:1985:4: otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) )
                    {
                    otherlv_6=(Token)match(input,26,FOLLOW_25); 

                    				newLeafNode(otherlv_6, grammarAccess.getTestModuleAccess().getLicenseKeyword_5_0());
                    			
                    // InternalAorta.g:1989:4: ( (lv_license_7_0= ruleEString ) )
                    // InternalAorta.g:1990:5: (lv_license_7_0= ruleEString )
                    {
                    // InternalAorta.g:1990:5: (lv_license_7_0= ruleEString )
                    // InternalAorta.g:1991:6: lv_license_7_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getTestModuleAccess().getLicenseEStringParserRuleCall_5_1_0());
                    					
                    pushFollow(FOLLOW_68);
                    lv_license_7_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getTestModuleRule());
                    						}
                    						set(
                    							current,
                    							"license",
                    							lv_license_7_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:2009:3: (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )?
            int alt55=2;
            int LA55_0 = input.LA(1);

            if ( (LA55_0==27) ) {
                alt55=1;
            }
            switch (alt55) {
                case 1 :
                    // InternalAorta.g:2010:4: otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) )
                    {
                    otherlv_8=(Token)match(input,27,FOLLOW_25); 

                    				newLeafNode(otherlv_8, grammarAccess.getTestModuleAccess().getVersionKeyword_6_0());
                    			
                    // InternalAorta.g:2014:4: ( (lv_version_9_0= ruleEString ) )
                    // InternalAorta.g:2015:5: (lv_version_9_0= ruleEString )
                    {
                    // InternalAorta.g:2015:5: (lv_version_9_0= ruleEString )
                    // InternalAorta.g:2016:6: lv_version_9_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getTestModuleAccess().getVersionEStringParserRuleCall_6_1_0());
                    					
                    pushFollow(FOLLOW_69);
                    lv_version_9_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getTestModuleRule());
                    						}
                    						set(
                    							current,
                    							"version",
                    							lv_version_9_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:2034:3: (otherlv_10= 'executionEnvironment' ( ( ruleQualifiedName ) ) )?
            int alt56=2;
            int LA56_0 = input.LA(1);

            if ( (LA56_0==41) ) {
                alt56=1;
            }
            switch (alt56) {
                case 1 :
                    // InternalAorta.g:2035:4: otherlv_10= 'executionEnvironment' ( ( ruleQualifiedName ) )
                    {
                    otherlv_10=(Token)match(input,41,FOLLOW_70); 

                    				newLeafNode(otherlv_10, grammarAccess.getTestModuleAccess().getExecutionEnvironmentKeyword_7_0());
                    			
                    // InternalAorta.g:2039:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:2040:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:2040:5: ( ruleQualifiedName )
                    // InternalAorta.g:2041:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getTestModuleRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getTestModuleAccess().getExecutionEnvironmentTestExecutionEnvironmentCrossReference_7_1_0());
                    					
                    pushFollow(FOLLOW_71);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:2056:3: (otherlv_12= 'dependencies' ( (lv_dependencies_13_0= ruleModuleDependencies ) ) )?
            int alt57=2;
            int LA57_0 = input.LA(1);

            if ( (LA57_0==28) ) {
                alt57=1;
            }
            switch (alt57) {
                case 1 :
                    // InternalAorta.g:2057:4: otherlv_12= 'dependencies' ( (lv_dependencies_13_0= ruleModuleDependencies ) )
                    {
                    otherlv_12=(Token)match(input,28,FOLLOW_30); 

                    				newLeafNode(otherlv_12, grammarAccess.getTestModuleAccess().getDependenciesKeyword_8_0());
                    			
                    // InternalAorta.g:2061:4: ( (lv_dependencies_13_0= ruleModuleDependencies ) )
                    // InternalAorta.g:2062:5: (lv_dependencies_13_0= ruleModuleDependencies )
                    {
                    // InternalAorta.g:2062:5: (lv_dependencies_13_0= ruleModuleDependencies )
                    // InternalAorta.g:2063:6: lv_dependencies_13_0= ruleModuleDependencies
                    {

                    						newCompositeNode(grammarAccess.getTestModuleAccess().getDependenciesModuleDependenciesParserRuleCall_8_1_0());
                    					
                    pushFollow(FOLLOW_72);
                    lv_dependencies_13_0=ruleModuleDependencies();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getTestModuleRule());
                    						}
                    						set(
                    							current,
                    							"dependencies",
                    							lv_dependencies_13_0,
                    							"org.eclipse.efbt.aorta.Aorta.ModuleDependencies");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:2081:3: (otherlv_14= 'longName' ( (lv_longName_15_0= ruleModuleLongName ) ) )?
            int alt58=2;
            int LA58_0 = input.LA(1);

            if ( (LA58_0==29) ) {
                alt58=1;
            }
            switch (alt58) {
                case 1 :
                    // InternalAorta.g:2082:4: otherlv_14= 'longName' ( (lv_longName_15_0= ruleModuleLongName ) )
                    {
                    otherlv_14=(Token)match(input,29,FOLLOW_32); 

                    				newLeafNode(otherlv_14, grammarAccess.getTestModuleAccess().getLongNameKeyword_9_0());
                    			
                    // InternalAorta.g:2086:4: ( (lv_longName_15_0= ruleModuleLongName ) )
                    // InternalAorta.g:2087:5: (lv_longName_15_0= ruleModuleLongName )
                    {
                    // InternalAorta.g:2087:5: (lv_longName_15_0= ruleModuleLongName )
                    // InternalAorta.g:2088:6: lv_longName_15_0= ruleModuleLongName
                    {

                    						newCompositeNode(grammarAccess.getTestModuleAccess().getLongNameModuleLongNameParserRuleCall_9_1_0());
                    					
                    pushFollow(FOLLOW_73);
                    lv_longName_15_0=ruleModuleLongName();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getTestModuleRule());
                    						}
                    						set(
                    							current,
                    							"longName",
                    							lv_longName_15_0,
                    							"org.eclipse.efbt.aorta.Aorta.ModuleLongName");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:2106:3: (otherlv_16= 'tests' otherlv_17= '{' ( (lv_tests_18_0= ruleE2ETest ) ) (otherlv_19= ',' ( (lv_tests_20_0= ruleE2ETest ) ) )* otherlv_21= '}' )?
            int alt60=2;
            int LA60_0 = input.LA(1);

            if ( (LA60_0==18) ) {
                alt60=1;
            }
            switch (alt60) {
                case 1 :
                    // InternalAorta.g:2107:4: otherlv_16= 'tests' otherlv_17= '{' ( (lv_tests_18_0= ruleE2ETest ) ) (otherlv_19= ',' ( (lv_tests_20_0= ruleE2ETest ) ) )* otherlv_21= '}'
                    {
                    otherlv_16=(Token)match(input,18,FOLLOW_3); 

                    				newLeafNode(otherlv_16, grammarAccess.getTestModuleAccess().getTestsKeyword_10_0());
                    			
                    otherlv_17=(Token)match(input,12,FOLLOW_74); 

                    				newLeafNode(otherlv_17, grammarAccess.getTestModuleAccess().getLeftCurlyBracketKeyword_10_1());
                    			
                    // InternalAorta.g:2115:4: ( (lv_tests_18_0= ruleE2ETest ) )
                    // InternalAorta.g:2116:5: (lv_tests_18_0= ruleE2ETest )
                    {
                    // InternalAorta.g:2116:5: (lv_tests_18_0= ruleE2ETest )
                    // InternalAorta.g:2117:6: lv_tests_18_0= ruleE2ETest
                    {

                    						newCompositeNode(grammarAccess.getTestModuleAccess().getTestsE2ETestParserRuleCall_10_2_0());
                    					
                    pushFollow(FOLLOW_35);
                    lv_tests_18_0=ruleE2ETest();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getTestModuleRule());
                    						}
                    						add(
                    							current,
                    							"tests",
                    							lv_tests_18_0,
                    							"org.eclipse.efbt.aorta.Aorta.E2ETest");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:2134:4: (otherlv_19= ',' ( (lv_tests_20_0= ruleE2ETest ) ) )*
                    loop59:
                    do {
                        int alt59=2;
                        int LA59_0 = input.LA(1);

                        if ( (LA59_0==31) ) {
                            alt59=1;
                        }


                        switch (alt59) {
                    	case 1 :
                    	    // InternalAorta.g:2135:5: otherlv_19= ',' ( (lv_tests_20_0= ruleE2ETest ) )
                    	    {
                    	    otherlv_19=(Token)match(input,31,FOLLOW_74); 

                    	    					newLeafNode(otherlv_19, grammarAccess.getTestModuleAccess().getCommaKeyword_10_3_0());
                    	    				
                    	    // InternalAorta.g:2139:5: ( (lv_tests_20_0= ruleE2ETest ) )
                    	    // InternalAorta.g:2140:6: (lv_tests_20_0= ruleE2ETest )
                    	    {
                    	    // InternalAorta.g:2140:6: (lv_tests_20_0= ruleE2ETest )
                    	    // InternalAorta.g:2141:7: lv_tests_20_0= ruleE2ETest
                    	    {

                    	    							newCompositeNode(grammarAccess.getTestModuleAccess().getTestsE2ETestParserRuleCall_10_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_35);
                    	    lv_tests_20_0=ruleE2ETest();

                    	    state._fsp--;


                    	    							if (current==null) {
                    	    								current = createModelElementForParent(grammarAccess.getTestModuleRule());
                    	    							}
                    	    							add(
                    	    								current,
                    	    								"tests",
                    	    								lv_tests_20_0,
                    	    								"org.eclipse.efbt.aorta.Aorta.E2ETest");
                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop59;
                        }
                    } while (true);

                    otherlv_21=(Token)match(input,23,FOLLOW_24); 

                    				newLeafNode(otherlv_21, grammarAccess.getTestModuleAccess().getRightCurlyBracketKeyword_10_4());
                    			

                    }
                    break;

            }

            otherlv_22=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_22, grammarAccess.getTestModuleAccess().getRightCurlyBracketKeyword_11());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleTestModule"


    // $ANTLR start "entryRuleInputTypeModule"
    // InternalAorta.g:2172:1: entryRuleInputTypeModule returns [EObject current=null] : iv_ruleInputTypeModule= ruleInputTypeModule EOF ;
    public final EObject entryRuleInputTypeModule() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleInputTypeModule = null;


        try {
            // InternalAorta.g:2172:56: (iv_ruleInputTypeModule= ruleInputTypeModule EOF )
            // InternalAorta.g:2173:2: iv_ruleInputTypeModule= ruleInputTypeModule EOF
            {
             newCompositeNode(grammarAccess.getInputTypeModuleRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleInputTypeModule=ruleInputTypeModule();

            state._fsp--;

             current =iv_ruleInputTypeModule; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleInputTypeModule"


    // $ANTLR start "ruleInputTypeModule"
    // InternalAorta.g:2179:1: ruleInputTypeModule returns [EObject current=null] : ( () otherlv_1= 'InputTypeModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'inputTypes' otherlv_15= '{' ( (lv_inputTypes_16_0= ruleInputType ) ) (otherlv_17= ',' ( (lv_inputTypes_18_0= ruleInputType ) ) )* otherlv_19= '}' )? otherlv_20= '}' ) ;
    public final EObject ruleInputTypeModule() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_6=null;
        Token otherlv_8=null;
        Token otherlv_10=null;
        Token otherlv_12=null;
        Token otherlv_14=null;
        Token otherlv_15=null;
        Token otherlv_17=null;
        Token otherlv_19=null;
        Token otherlv_20=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;

        AntlrDatatypeRuleToken lv_theDescription_5_0 = null;

        AntlrDatatypeRuleToken lv_license_7_0 = null;

        AntlrDatatypeRuleToken lv_version_9_0 = null;

        EObject lv_dependencies_11_0 = null;

        EObject lv_longName_13_0 = null;

        EObject lv_inputTypes_16_0 = null;

        EObject lv_inputTypes_18_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:2185:2: ( ( () otherlv_1= 'InputTypeModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'inputTypes' otherlv_15= '{' ( (lv_inputTypes_16_0= ruleInputType ) ) (otherlv_17= ',' ( (lv_inputTypes_18_0= ruleInputType ) ) )* otherlv_19= '}' )? otherlv_20= '}' ) )
            // InternalAorta.g:2186:2: ( () otherlv_1= 'InputTypeModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'inputTypes' otherlv_15= '{' ( (lv_inputTypes_16_0= ruleInputType ) ) (otherlv_17= ',' ( (lv_inputTypes_18_0= ruleInputType ) ) )* otherlv_19= '}' )? otherlv_20= '}' )
            {
            // InternalAorta.g:2186:2: ( () otherlv_1= 'InputTypeModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'inputTypes' otherlv_15= '{' ( (lv_inputTypes_16_0= ruleInputType ) ) (otherlv_17= ',' ( (lv_inputTypes_18_0= ruleInputType ) ) )* otherlv_19= '}' )? otherlv_20= '}' )
            // InternalAorta.g:2187:3: () otherlv_1= 'InputTypeModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'inputTypes' otherlv_15= '{' ( (lv_inputTypes_16_0= ruleInputType ) ) (otherlv_17= ',' ( (lv_inputTypes_18_0= ruleInputType ) ) )* otherlv_19= '}' )? otherlv_20= '}'
            {
            // InternalAorta.g:2187:3: ()
            // InternalAorta.g:2188:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getInputTypeModuleAccess().getInputTypeModuleAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,42,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getInputTypeModuleAccess().getInputTypeModuleKeyword_1());
            		
            // InternalAorta.g:2198:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:2199:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:2199:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:2200:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getInputTypeModuleAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getInputTypeModuleRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_75); 

            			newLeafNode(otherlv_3, grammarAccess.getInputTypeModuleAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:2221:3: (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )?
            int alt61=2;
            int LA61_0 = input.LA(1);

            if ( (LA61_0==25) ) {
                alt61=1;
            }
            switch (alt61) {
                case 1 :
                    // InternalAorta.g:2222:4: otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) )
                    {
                    otherlv_4=(Token)match(input,25,FOLLOW_25); 

                    				newLeafNode(otherlv_4, grammarAccess.getInputTypeModuleAccess().getTheDescriptionKeyword_4_0());
                    			
                    // InternalAorta.g:2226:4: ( (lv_theDescription_5_0= ruleEString ) )
                    // InternalAorta.g:2227:5: (lv_theDescription_5_0= ruleEString )
                    {
                    // InternalAorta.g:2227:5: (lv_theDescription_5_0= ruleEString )
                    // InternalAorta.g:2228:6: lv_theDescription_5_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getInputTypeModuleAccess().getTheDescriptionEStringParserRuleCall_4_1_0());
                    					
                    pushFollow(FOLLOW_76);
                    lv_theDescription_5_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getInputTypeModuleRule());
                    						}
                    						set(
                    							current,
                    							"theDescription",
                    							lv_theDescription_5_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:2246:3: (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )?
            int alt62=2;
            int LA62_0 = input.LA(1);

            if ( (LA62_0==26) ) {
                alt62=1;
            }
            switch (alt62) {
                case 1 :
                    // InternalAorta.g:2247:4: otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) )
                    {
                    otherlv_6=(Token)match(input,26,FOLLOW_25); 

                    				newLeafNode(otherlv_6, grammarAccess.getInputTypeModuleAccess().getLicenseKeyword_5_0());
                    			
                    // InternalAorta.g:2251:4: ( (lv_license_7_0= ruleEString ) )
                    // InternalAorta.g:2252:5: (lv_license_7_0= ruleEString )
                    {
                    // InternalAorta.g:2252:5: (lv_license_7_0= ruleEString )
                    // InternalAorta.g:2253:6: lv_license_7_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getInputTypeModuleAccess().getLicenseEStringParserRuleCall_5_1_0());
                    					
                    pushFollow(FOLLOW_77);
                    lv_license_7_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getInputTypeModuleRule());
                    						}
                    						set(
                    							current,
                    							"license",
                    							lv_license_7_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:2271:3: (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )?
            int alt63=2;
            int LA63_0 = input.LA(1);

            if ( (LA63_0==27) ) {
                alt63=1;
            }
            switch (alt63) {
                case 1 :
                    // InternalAorta.g:2272:4: otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) )
                    {
                    otherlv_8=(Token)match(input,27,FOLLOW_25); 

                    				newLeafNode(otherlv_8, grammarAccess.getInputTypeModuleAccess().getVersionKeyword_6_0());
                    			
                    // InternalAorta.g:2276:4: ( (lv_version_9_0= ruleEString ) )
                    // InternalAorta.g:2277:5: (lv_version_9_0= ruleEString )
                    {
                    // InternalAorta.g:2277:5: (lv_version_9_0= ruleEString )
                    // InternalAorta.g:2278:6: lv_version_9_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getInputTypeModuleAccess().getVersionEStringParserRuleCall_6_1_0());
                    					
                    pushFollow(FOLLOW_78);
                    lv_version_9_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getInputTypeModuleRule());
                    						}
                    						set(
                    							current,
                    							"version",
                    							lv_version_9_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:2296:3: (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )?
            int alt64=2;
            int LA64_0 = input.LA(1);

            if ( (LA64_0==28) ) {
                alt64=1;
            }
            switch (alt64) {
                case 1 :
                    // InternalAorta.g:2297:4: otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) )
                    {
                    otherlv_10=(Token)match(input,28,FOLLOW_30); 

                    				newLeafNode(otherlv_10, grammarAccess.getInputTypeModuleAccess().getDependenciesKeyword_7_0());
                    			
                    // InternalAorta.g:2301:4: ( (lv_dependencies_11_0= ruleModuleDependencies ) )
                    // InternalAorta.g:2302:5: (lv_dependencies_11_0= ruleModuleDependencies )
                    {
                    // InternalAorta.g:2302:5: (lv_dependencies_11_0= ruleModuleDependencies )
                    // InternalAorta.g:2303:6: lv_dependencies_11_0= ruleModuleDependencies
                    {

                    						newCompositeNode(grammarAccess.getInputTypeModuleAccess().getDependenciesModuleDependenciesParserRuleCall_7_1_0());
                    					
                    pushFollow(FOLLOW_79);
                    lv_dependencies_11_0=ruleModuleDependencies();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getInputTypeModuleRule());
                    						}
                    						set(
                    							current,
                    							"dependencies",
                    							lv_dependencies_11_0,
                    							"org.eclipse.efbt.aorta.Aorta.ModuleDependencies");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:2321:3: (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )?
            int alt65=2;
            int LA65_0 = input.LA(1);

            if ( (LA65_0==29) ) {
                alt65=1;
            }
            switch (alt65) {
                case 1 :
                    // InternalAorta.g:2322:4: otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) )
                    {
                    otherlv_12=(Token)match(input,29,FOLLOW_32); 

                    				newLeafNode(otherlv_12, grammarAccess.getInputTypeModuleAccess().getLongNameKeyword_8_0());
                    			
                    // InternalAorta.g:2326:4: ( (lv_longName_13_0= ruleModuleLongName ) )
                    // InternalAorta.g:2327:5: (lv_longName_13_0= ruleModuleLongName )
                    {
                    // InternalAorta.g:2327:5: (lv_longName_13_0= ruleModuleLongName )
                    // InternalAorta.g:2328:6: lv_longName_13_0= ruleModuleLongName
                    {

                    						newCompositeNode(grammarAccess.getInputTypeModuleAccess().getLongNameModuleLongNameParserRuleCall_8_1_0());
                    					
                    pushFollow(FOLLOW_80);
                    lv_longName_13_0=ruleModuleLongName();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getInputTypeModuleRule());
                    						}
                    						set(
                    							current,
                    							"longName",
                    							lv_longName_13_0,
                    							"org.eclipse.efbt.aorta.Aorta.ModuleLongName");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:2346:3: (otherlv_14= 'inputTypes' otherlv_15= '{' ( (lv_inputTypes_16_0= ruleInputType ) ) (otherlv_17= ',' ( (lv_inputTypes_18_0= ruleInputType ) ) )* otherlv_19= '}' )?
            int alt67=2;
            int LA67_0 = input.LA(1);

            if ( (LA67_0==43) ) {
                alt67=1;
            }
            switch (alt67) {
                case 1 :
                    // InternalAorta.g:2347:4: otherlv_14= 'inputTypes' otherlv_15= '{' ( (lv_inputTypes_16_0= ruleInputType ) ) (otherlv_17= ',' ( (lv_inputTypes_18_0= ruleInputType ) ) )* otherlv_19= '}'
                    {
                    otherlv_14=(Token)match(input,43,FOLLOW_3); 

                    				newLeafNode(otherlv_14, grammarAccess.getInputTypeModuleAccess().getInputTypesKeyword_9_0());
                    			
                    otherlv_15=(Token)match(input,12,FOLLOW_81); 

                    				newLeafNode(otherlv_15, grammarAccess.getInputTypeModuleAccess().getLeftCurlyBracketKeyword_9_1());
                    			
                    // InternalAorta.g:2355:4: ( (lv_inputTypes_16_0= ruleInputType ) )
                    // InternalAorta.g:2356:5: (lv_inputTypes_16_0= ruleInputType )
                    {
                    // InternalAorta.g:2356:5: (lv_inputTypes_16_0= ruleInputType )
                    // InternalAorta.g:2357:6: lv_inputTypes_16_0= ruleInputType
                    {

                    						newCompositeNode(grammarAccess.getInputTypeModuleAccess().getInputTypesInputTypeParserRuleCall_9_2_0());
                    					
                    pushFollow(FOLLOW_35);
                    lv_inputTypes_16_0=ruleInputType();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getInputTypeModuleRule());
                    						}
                    						add(
                    							current,
                    							"inputTypes",
                    							lv_inputTypes_16_0,
                    							"org.eclipse.efbt.aorta.Aorta.InputType");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:2374:4: (otherlv_17= ',' ( (lv_inputTypes_18_0= ruleInputType ) ) )*
                    loop66:
                    do {
                        int alt66=2;
                        int LA66_0 = input.LA(1);

                        if ( (LA66_0==31) ) {
                            alt66=1;
                        }


                        switch (alt66) {
                    	case 1 :
                    	    // InternalAorta.g:2375:5: otherlv_17= ',' ( (lv_inputTypes_18_0= ruleInputType ) )
                    	    {
                    	    otherlv_17=(Token)match(input,31,FOLLOW_81); 

                    	    					newLeafNode(otherlv_17, grammarAccess.getInputTypeModuleAccess().getCommaKeyword_9_3_0());
                    	    				
                    	    // InternalAorta.g:2379:5: ( (lv_inputTypes_18_0= ruleInputType ) )
                    	    // InternalAorta.g:2380:6: (lv_inputTypes_18_0= ruleInputType )
                    	    {
                    	    // InternalAorta.g:2380:6: (lv_inputTypes_18_0= ruleInputType )
                    	    // InternalAorta.g:2381:7: lv_inputTypes_18_0= ruleInputType
                    	    {

                    	    							newCompositeNode(grammarAccess.getInputTypeModuleAccess().getInputTypesInputTypeParserRuleCall_9_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_35);
                    	    lv_inputTypes_18_0=ruleInputType();

                    	    state._fsp--;


                    	    							if (current==null) {
                    	    								current = createModelElementForParent(grammarAccess.getInputTypeModuleRule());
                    	    							}
                    	    							add(
                    	    								current,
                    	    								"inputTypes",
                    	    								lv_inputTypes_18_0,
                    	    								"org.eclipse.efbt.aorta.Aorta.InputType");
                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop66;
                        }
                    } while (true);

                    otherlv_19=(Token)match(input,23,FOLLOW_24); 

                    				newLeafNode(otherlv_19, grammarAccess.getInputTypeModuleAccess().getRightCurlyBracketKeyword_9_4());
                    			

                    }
                    break;

            }

            otherlv_20=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_20, grammarAccess.getInputTypeModuleAccess().getRightCurlyBracketKeyword_10());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleInputTypeModule"


    // $ANTLR start "entryRuleTestTemplateModule"
    // InternalAorta.g:2412:1: entryRuleTestTemplateModule returns [EObject current=null] : iv_ruleTestTemplateModule= ruleTestTemplateModule EOF ;
    public final EObject entryRuleTestTemplateModule() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleTestTemplateModule = null;


        try {
            // InternalAorta.g:2412:59: (iv_ruleTestTemplateModule= ruleTestTemplateModule EOF )
            // InternalAorta.g:2413:2: iv_ruleTestTemplateModule= ruleTestTemplateModule EOF
            {
             newCompositeNode(grammarAccess.getTestTemplateModuleRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleTestTemplateModule=ruleTestTemplateModule();

            state._fsp--;

             current =iv_ruleTestTemplateModule; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleTestTemplateModule"


    // $ANTLR start "ruleTestTemplateModule"
    // InternalAorta.g:2419:1: ruleTestTemplateModule returns [EObject current=null] : ( () otherlv_1= 'TestTemplateModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'templates' otherlv_15= '{' ( (lv_templates_16_0= ruleTestTemplate ) ) (otherlv_17= ',' ( (lv_templates_18_0= ruleTestTemplate ) ) )* otherlv_19= '}' )? otherlv_20= '}' ) ;
    public final EObject ruleTestTemplateModule() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_6=null;
        Token otherlv_8=null;
        Token otherlv_10=null;
        Token otherlv_12=null;
        Token otherlv_14=null;
        Token otherlv_15=null;
        Token otherlv_17=null;
        Token otherlv_19=null;
        Token otherlv_20=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;

        AntlrDatatypeRuleToken lv_theDescription_5_0 = null;

        AntlrDatatypeRuleToken lv_license_7_0 = null;

        AntlrDatatypeRuleToken lv_version_9_0 = null;

        EObject lv_dependencies_11_0 = null;

        EObject lv_longName_13_0 = null;

        EObject lv_templates_16_0 = null;

        EObject lv_templates_18_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:2425:2: ( ( () otherlv_1= 'TestTemplateModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'templates' otherlv_15= '{' ( (lv_templates_16_0= ruleTestTemplate ) ) (otherlv_17= ',' ( (lv_templates_18_0= ruleTestTemplate ) ) )* otherlv_19= '}' )? otherlv_20= '}' ) )
            // InternalAorta.g:2426:2: ( () otherlv_1= 'TestTemplateModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'templates' otherlv_15= '{' ( (lv_templates_16_0= ruleTestTemplate ) ) (otherlv_17= ',' ( (lv_templates_18_0= ruleTestTemplate ) ) )* otherlv_19= '}' )? otherlv_20= '}' )
            {
            // InternalAorta.g:2426:2: ( () otherlv_1= 'TestTemplateModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'templates' otherlv_15= '{' ( (lv_templates_16_0= ruleTestTemplate ) ) (otherlv_17= ',' ( (lv_templates_18_0= ruleTestTemplate ) ) )* otherlv_19= '}' )? otherlv_20= '}' )
            // InternalAorta.g:2427:3: () otherlv_1= 'TestTemplateModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'templates' otherlv_15= '{' ( (lv_templates_16_0= ruleTestTemplate ) ) (otherlv_17= ',' ( (lv_templates_18_0= ruleTestTemplate ) ) )* otherlv_19= '}' )? otherlv_20= '}'
            {
            // InternalAorta.g:2427:3: ()
            // InternalAorta.g:2428:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getTestTemplateModuleAccess().getTestTemplateModuleAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,44,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getTestTemplateModuleAccess().getTestTemplateModuleKeyword_1());
            		
            // InternalAorta.g:2438:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:2439:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:2439:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:2440:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getTestTemplateModuleAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getTestTemplateModuleRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_82); 

            			newLeafNode(otherlv_3, grammarAccess.getTestTemplateModuleAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:2461:3: (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )?
            int alt68=2;
            int LA68_0 = input.LA(1);

            if ( (LA68_0==25) ) {
                alt68=1;
            }
            switch (alt68) {
                case 1 :
                    // InternalAorta.g:2462:4: otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) )
                    {
                    otherlv_4=(Token)match(input,25,FOLLOW_25); 

                    				newLeafNode(otherlv_4, grammarAccess.getTestTemplateModuleAccess().getTheDescriptionKeyword_4_0());
                    			
                    // InternalAorta.g:2466:4: ( (lv_theDescription_5_0= ruleEString ) )
                    // InternalAorta.g:2467:5: (lv_theDescription_5_0= ruleEString )
                    {
                    // InternalAorta.g:2467:5: (lv_theDescription_5_0= ruleEString )
                    // InternalAorta.g:2468:6: lv_theDescription_5_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getTestTemplateModuleAccess().getTheDescriptionEStringParserRuleCall_4_1_0());
                    					
                    pushFollow(FOLLOW_83);
                    lv_theDescription_5_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getTestTemplateModuleRule());
                    						}
                    						set(
                    							current,
                    							"theDescription",
                    							lv_theDescription_5_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:2486:3: (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )?
            int alt69=2;
            int LA69_0 = input.LA(1);

            if ( (LA69_0==26) ) {
                alt69=1;
            }
            switch (alt69) {
                case 1 :
                    // InternalAorta.g:2487:4: otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) )
                    {
                    otherlv_6=(Token)match(input,26,FOLLOW_25); 

                    				newLeafNode(otherlv_6, grammarAccess.getTestTemplateModuleAccess().getLicenseKeyword_5_0());
                    			
                    // InternalAorta.g:2491:4: ( (lv_license_7_0= ruleEString ) )
                    // InternalAorta.g:2492:5: (lv_license_7_0= ruleEString )
                    {
                    // InternalAorta.g:2492:5: (lv_license_7_0= ruleEString )
                    // InternalAorta.g:2493:6: lv_license_7_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getTestTemplateModuleAccess().getLicenseEStringParserRuleCall_5_1_0());
                    					
                    pushFollow(FOLLOW_84);
                    lv_license_7_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getTestTemplateModuleRule());
                    						}
                    						set(
                    							current,
                    							"license",
                    							lv_license_7_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:2511:3: (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )?
            int alt70=2;
            int LA70_0 = input.LA(1);

            if ( (LA70_0==27) ) {
                alt70=1;
            }
            switch (alt70) {
                case 1 :
                    // InternalAorta.g:2512:4: otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) )
                    {
                    otherlv_8=(Token)match(input,27,FOLLOW_25); 

                    				newLeafNode(otherlv_8, grammarAccess.getTestTemplateModuleAccess().getVersionKeyword_6_0());
                    			
                    // InternalAorta.g:2516:4: ( (lv_version_9_0= ruleEString ) )
                    // InternalAorta.g:2517:5: (lv_version_9_0= ruleEString )
                    {
                    // InternalAorta.g:2517:5: (lv_version_9_0= ruleEString )
                    // InternalAorta.g:2518:6: lv_version_9_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getTestTemplateModuleAccess().getVersionEStringParserRuleCall_6_1_0());
                    					
                    pushFollow(FOLLOW_85);
                    lv_version_9_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getTestTemplateModuleRule());
                    						}
                    						set(
                    							current,
                    							"version",
                    							lv_version_9_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:2536:3: (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )?
            int alt71=2;
            int LA71_0 = input.LA(1);

            if ( (LA71_0==28) ) {
                alt71=1;
            }
            switch (alt71) {
                case 1 :
                    // InternalAorta.g:2537:4: otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) )
                    {
                    otherlv_10=(Token)match(input,28,FOLLOW_30); 

                    				newLeafNode(otherlv_10, grammarAccess.getTestTemplateModuleAccess().getDependenciesKeyword_7_0());
                    			
                    // InternalAorta.g:2541:4: ( (lv_dependencies_11_0= ruleModuleDependencies ) )
                    // InternalAorta.g:2542:5: (lv_dependencies_11_0= ruleModuleDependencies )
                    {
                    // InternalAorta.g:2542:5: (lv_dependencies_11_0= ruleModuleDependencies )
                    // InternalAorta.g:2543:6: lv_dependencies_11_0= ruleModuleDependencies
                    {

                    						newCompositeNode(grammarAccess.getTestTemplateModuleAccess().getDependenciesModuleDependenciesParserRuleCall_7_1_0());
                    					
                    pushFollow(FOLLOW_86);
                    lv_dependencies_11_0=ruleModuleDependencies();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getTestTemplateModuleRule());
                    						}
                    						set(
                    							current,
                    							"dependencies",
                    							lv_dependencies_11_0,
                    							"org.eclipse.efbt.aorta.Aorta.ModuleDependencies");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:2561:3: (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )?
            int alt72=2;
            int LA72_0 = input.LA(1);

            if ( (LA72_0==29) ) {
                alt72=1;
            }
            switch (alt72) {
                case 1 :
                    // InternalAorta.g:2562:4: otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) )
                    {
                    otherlv_12=(Token)match(input,29,FOLLOW_32); 

                    				newLeafNode(otherlv_12, grammarAccess.getTestTemplateModuleAccess().getLongNameKeyword_8_0());
                    			
                    // InternalAorta.g:2566:4: ( (lv_longName_13_0= ruleModuleLongName ) )
                    // InternalAorta.g:2567:5: (lv_longName_13_0= ruleModuleLongName )
                    {
                    // InternalAorta.g:2567:5: (lv_longName_13_0= ruleModuleLongName )
                    // InternalAorta.g:2568:6: lv_longName_13_0= ruleModuleLongName
                    {

                    						newCompositeNode(grammarAccess.getTestTemplateModuleAccess().getLongNameModuleLongNameParserRuleCall_8_1_0());
                    					
                    pushFollow(FOLLOW_87);
                    lv_longName_13_0=ruleModuleLongName();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getTestTemplateModuleRule());
                    						}
                    						set(
                    							current,
                    							"longName",
                    							lv_longName_13_0,
                    							"org.eclipse.efbt.aorta.Aorta.ModuleLongName");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:2586:3: (otherlv_14= 'templates' otherlv_15= '{' ( (lv_templates_16_0= ruleTestTemplate ) ) (otherlv_17= ',' ( (lv_templates_18_0= ruleTestTemplate ) ) )* otherlv_19= '}' )?
            int alt74=2;
            int LA74_0 = input.LA(1);

            if ( (LA74_0==45) ) {
                alt74=1;
            }
            switch (alt74) {
                case 1 :
                    // InternalAorta.g:2587:4: otherlv_14= 'templates' otherlv_15= '{' ( (lv_templates_16_0= ruleTestTemplate ) ) (otherlv_17= ',' ( (lv_templates_18_0= ruleTestTemplate ) ) )* otherlv_19= '}'
                    {
                    otherlv_14=(Token)match(input,45,FOLLOW_3); 

                    				newLeafNode(otherlv_14, grammarAccess.getTestTemplateModuleAccess().getTemplatesKeyword_9_0());
                    			
                    otherlv_15=(Token)match(input,12,FOLLOW_88); 

                    				newLeafNode(otherlv_15, grammarAccess.getTestTemplateModuleAccess().getLeftCurlyBracketKeyword_9_1());
                    			
                    // InternalAorta.g:2595:4: ( (lv_templates_16_0= ruleTestTemplate ) )
                    // InternalAorta.g:2596:5: (lv_templates_16_0= ruleTestTemplate )
                    {
                    // InternalAorta.g:2596:5: (lv_templates_16_0= ruleTestTemplate )
                    // InternalAorta.g:2597:6: lv_templates_16_0= ruleTestTemplate
                    {

                    						newCompositeNode(grammarAccess.getTestTemplateModuleAccess().getTemplatesTestTemplateParserRuleCall_9_2_0());
                    					
                    pushFollow(FOLLOW_35);
                    lv_templates_16_0=ruleTestTemplate();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getTestTemplateModuleRule());
                    						}
                    						add(
                    							current,
                    							"templates",
                    							lv_templates_16_0,
                    							"org.eclipse.efbt.aorta.Aorta.TestTemplate");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:2614:4: (otherlv_17= ',' ( (lv_templates_18_0= ruleTestTemplate ) ) )*
                    loop73:
                    do {
                        int alt73=2;
                        int LA73_0 = input.LA(1);

                        if ( (LA73_0==31) ) {
                            alt73=1;
                        }


                        switch (alt73) {
                    	case 1 :
                    	    // InternalAorta.g:2615:5: otherlv_17= ',' ( (lv_templates_18_0= ruleTestTemplate ) )
                    	    {
                    	    otherlv_17=(Token)match(input,31,FOLLOW_88); 

                    	    					newLeafNode(otherlv_17, grammarAccess.getTestTemplateModuleAccess().getCommaKeyword_9_3_0());
                    	    				
                    	    // InternalAorta.g:2619:5: ( (lv_templates_18_0= ruleTestTemplate ) )
                    	    // InternalAorta.g:2620:6: (lv_templates_18_0= ruleTestTemplate )
                    	    {
                    	    // InternalAorta.g:2620:6: (lv_templates_18_0= ruleTestTemplate )
                    	    // InternalAorta.g:2621:7: lv_templates_18_0= ruleTestTemplate
                    	    {

                    	    							newCompositeNode(grammarAccess.getTestTemplateModuleAccess().getTemplatesTestTemplateParserRuleCall_9_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_35);
                    	    lv_templates_18_0=ruleTestTemplate();

                    	    state._fsp--;


                    	    							if (current==null) {
                    	    								current = createModelElementForParent(grammarAccess.getTestTemplateModuleRule());
                    	    							}
                    	    							add(
                    	    								current,
                    	    								"templates",
                    	    								lv_templates_18_0,
                    	    								"org.eclipse.efbt.aorta.Aorta.TestTemplate");
                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop73;
                        }
                    } while (true);

                    otherlv_19=(Token)match(input,23,FOLLOW_24); 

                    				newLeafNode(otherlv_19, grammarAccess.getTestTemplateModuleAccess().getRightCurlyBracketKeyword_9_4());
                    			

                    }
                    break;

            }

            otherlv_20=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_20, grammarAccess.getTestTemplateModuleAccess().getRightCurlyBracketKeyword_10());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleTestTemplateModule"


    // $ANTLR start "entryRuleTestContraints"
    // InternalAorta.g:2652:1: entryRuleTestContraints returns [EObject current=null] : iv_ruleTestContraints= ruleTestContraints EOF ;
    public final EObject entryRuleTestContraints() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleTestContraints = null;


        try {
            // InternalAorta.g:2652:55: (iv_ruleTestContraints= ruleTestContraints EOF )
            // InternalAorta.g:2653:2: iv_ruleTestContraints= ruleTestContraints EOF
            {
             newCompositeNode(grammarAccess.getTestContraintsRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleTestContraints=ruleTestContraints();

            state._fsp--;

             current =iv_ruleTestContraints; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleTestContraints"


    // $ANTLR start "ruleTestContraints"
    // InternalAorta.g:2659:1: ruleTestContraints returns [EObject current=null] : ( () otherlv_1= 'TestContraints' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'template' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'givenParams' otherlv_7= '{' ( (lv_givenParams_8_0= ruleParam ) ) (otherlv_9= ',' ( (lv_givenParams_10_0= ruleParam ) ) )* otherlv_11= '}' )? (otherlv_12= 'whenParams' otherlv_13= '{' ( (lv_whenParams_14_0= ruleParam ) ) (otherlv_15= ',' ( (lv_whenParams_16_0= ruleParam ) ) )* otherlv_17= '}' )? (otherlv_18= 'thenParams' otherlv_19= '{' ( (lv_thenParams_20_0= ruleParam ) ) (otherlv_21= ',' ( (lv_thenParams_22_0= ruleParam ) ) )* otherlv_23= '}' )? otherlv_24= '}' ) ;
    public final EObject ruleTestContraints() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_6=null;
        Token otherlv_7=null;
        Token otherlv_9=null;
        Token otherlv_11=null;
        Token otherlv_12=null;
        Token otherlv_13=null;
        Token otherlv_15=null;
        Token otherlv_17=null;
        Token otherlv_18=null;
        Token otherlv_19=null;
        Token otherlv_21=null;
        Token otherlv_23=null;
        Token otherlv_24=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;

        EObject lv_givenParams_8_0 = null;

        EObject lv_givenParams_10_0 = null;

        EObject lv_whenParams_14_0 = null;

        EObject lv_whenParams_16_0 = null;

        EObject lv_thenParams_20_0 = null;

        EObject lv_thenParams_22_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:2665:2: ( ( () otherlv_1= 'TestContraints' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'template' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'givenParams' otherlv_7= '{' ( (lv_givenParams_8_0= ruleParam ) ) (otherlv_9= ',' ( (lv_givenParams_10_0= ruleParam ) ) )* otherlv_11= '}' )? (otherlv_12= 'whenParams' otherlv_13= '{' ( (lv_whenParams_14_0= ruleParam ) ) (otherlv_15= ',' ( (lv_whenParams_16_0= ruleParam ) ) )* otherlv_17= '}' )? (otherlv_18= 'thenParams' otherlv_19= '{' ( (lv_thenParams_20_0= ruleParam ) ) (otherlv_21= ',' ( (lv_thenParams_22_0= ruleParam ) ) )* otherlv_23= '}' )? otherlv_24= '}' ) )
            // InternalAorta.g:2666:2: ( () otherlv_1= 'TestContraints' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'template' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'givenParams' otherlv_7= '{' ( (lv_givenParams_8_0= ruleParam ) ) (otherlv_9= ',' ( (lv_givenParams_10_0= ruleParam ) ) )* otherlv_11= '}' )? (otherlv_12= 'whenParams' otherlv_13= '{' ( (lv_whenParams_14_0= ruleParam ) ) (otherlv_15= ',' ( (lv_whenParams_16_0= ruleParam ) ) )* otherlv_17= '}' )? (otherlv_18= 'thenParams' otherlv_19= '{' ( (lv_thenParams_20_0= ruleParam ) ) (otherlv_21= ',' ( (lv_thenParams_22_0= ruleParam ) ) )* otherlv_23= '}' )? otherlv_24= '}' )
            {
            // InternalAorta.g:2666:2: ( () otherlv_1= 'TestContraints' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'template' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'givenParams' otherlv_7= '{' ( (lv_givenParams_8_0= ruleParam ) ) (otherlv_9= ',' ( (lv_givenParams_10_0= ruleParam ) ) )* otherlv_11= '}' )? (otherlv_12= 'whenParams' otherlv_13= '{' ( (lv_whenParams_14_0= ruleParam ) ) (otherlv_15= ',' ( (lv_whenParams_16_0= ruleParam ) ) )* otherlv_17= '}' )? (otherlv_18= 'thenParams' otherlv_19= '{' ( (lv_thenParams_20_0= ruleParam ) ) (otherlv_21= ',' ( (lv_thenParams_22_0= ruleParam ) ) )* otherlv_23= '}' )? otherlv_24= '}' )
            // InternalAorta.g:2667:3: () otherlv_1= 'TestContraints' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'template' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'givenParams' otherlv_7= '{' ( (lv_givenParams_8_0= ruleParam ) ) (otherlv_9= ',' ( (lv_givenParams_10_0= ruleParam ) ) )* otherlv_11= '}' )? (otherlv_12= 'whenParams' otherlv_13= '{' ( (lv_whenParams_14_0= ruleParam ) ) (otherlv_15= ',' ( (lv_whenParams_16_0= ruleParam ) ) )* otherlv_17= '}' )? (otherlv_18= 'thenParams' otherlv_19= '{' ( (lv_thenParams_20_0= ruleParam ) ) (otherlv_21= ',' ( (lv_thenParams_22_0= ruleParam ) ) )* otherlv_23= '}' )? otherlv_24= '}'
            {
            // InternalAorta.g:2667:3: ()
            // InternalAorta.g:2668:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getTestContraintsAccess().getTestContraintsAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,46,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getTestContraintsAccess().getTestContraintsKeyword_1());
            		
            // InternalAorta.g:2678:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:2679:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:2679:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:2680:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getTestContraintsAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getTestContraintsRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_89); 

            			newLeafNode(otherlv_3, grammarAccess.getTestContraintsAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:2701:3: (otherlv_4= 'template' ( ( ruleQualifiedName ) ) )?
            int alt75=2;
            int LA75_0 = input.LA(1);

            if ( (LA75_0==47) ) {
                alt75=1;
            }
            switch (alt75) {
                case 1 :
                    // InternalAorta.g:2702:4: otherlv_4= 'template' ( ( ruleQualifiedName ) )
                    {
                    otherlv_4=(Token)match(input,47,FOLLOW_70); 

                    				newLeafNode(otherlv_4, grammarAccess.getTestContraintsAccess().getTemplateKeyword_4_0());
                    			
                    // InternalAorta.g:2706:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:2707:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:2707:5: ( ruleQualifiedName )
                    // InternalAorta.g:2708:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getTestContraintsRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getTestContraintsAccess().getTemplateTestTemplateCrossReference_4_1_0());
                    					
                    pushFollow(FOLLOW_90);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:2723:3: (otherlv_6= 'givenParams' otherlv_7= '{' ( (lv_givenParams_8_0= ruleParam ) ) (otherlv_9= ',' ( (lv_givenParams_10_0= ruleParam ) ) )* otherlv_11= '}' )?
            int alt77=2;
            int LA77_0 = input.LA(1);

            if ( (LA77_0==48) ) {
                alt77=1;
            }
            switch (alt77) {
                case 1 :
                    // InternalAorta.g:2724:4: otherlv_6= 'givenParams' otherlv_7= '{' ( (lv_givenParams_8_0= ruleParam ) ) (otherlv_9= ',' ( (lv_givenParams_10_0= ruleParam ) ) )* otherlv_11= '}'
                    {
                    otherlv_6=(Token)match(input,48,FOLLOW_3); 

                    				newLeafNode(otherlv_6, grammarAccess.getTestContraintsAccess().getGivenParamsKeyword_5_0());
                    			
                    otherlv_7=(Token)match(input,12,FOLLOW_91); 

                    				newLeafNode(otherlv_7, grammarAccess.getTestContraintsAccess().getLeftCurlyBracketKeyword_5_1());
                    			
                    // InternalAorta.g:2732:4: ( (lv_givenParams_8_0= ruleParam ) )
                    // InternalAorta.g:2733:5: (lv_givenParams_8_0= ruleParam )
                    {
                    // InternalAorta.g:2733:5: (lv_givenParams_8_0= ruleParam )
                    // InternalAorta.g:2734:6: lv_givenParams_8_0= ruleParam
                    {

                    						newCompositeNode(grammarAccess.getTestContraintsAccess().getGivenParamsParamParserRuleCall_5_2_0());
                    					
                    pushFollow(FOLLOW_35);
                    lv_givenParams_8_0=ruleParam();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getTestContraintsRule());
                    						}
                    						add(
                    							current,
                    							"givenParams",
                    							lv_givenParams_8_0,
                    							"org.eclipse.efbt.aorta.Aorta.Param");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:2751:4: (otherlv_9= ',' ( (lv_givenParams_10_0= ruleParam ) ) )*
                    loop76:
                    do {
                        int alt76=2;
                        int LA76_0 = input.LA(1);

                        if ( (LA76_0==31) ) {
                            alt76=1;
                        }


                        switch (alt76) {
                    	case 1 :
                    	    // InternalAorta.g:2752:5: otherlv_9= ',' ( (lv_givenParams_10_0= ruleParam ) )
                    	    {
                    	    otherlv_9=(Token)match(input,31,FOLLOW_91); 

                    	    					newLeafNode(otherlv_9, grammarAccess.getTestContraintsAccess().getCommaKeyword_5_3_0());
                    	    				
                    	    // InternalAorta.g:2756:5: ( (lv_givenParams_10_0= ruleParam ) )
                    	    // InternalAorta.g:2757:6: (lv_givenParams_10_0= ruleParam )
                    	    {
                    	    // InternalAorta.g:2757:6: (lv_givenParams_10_0= ruleParam )
                    	    // InternalAorta.g:2758:7: lv_givenParams_10_0= ruleParam
                    	    {

                    	    							newCompositeNode(grammarAccess.getTestContraintsAccess().getGivenParamsParamParserRuleCall_5_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_35);
                    	    lv_givenParams_10_0=ruleParam();

                    	    state._fsp--;


                    	    							if (current==null) {
                    	    								current = createModelElementForParent(grammarAccess.getTestContraintsRule());
                    	    							}
                    	    							add(
                    	    								current,
                    	    								"givenParams",
                    	    								lv_givenParams_10_0,
                    	    								"org.eclipse.efbt.aorta.Aorta.Param");
                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop76;
                        }
                    } while (true);

                    otherlv_11=(Token)match(input,23,FOLLOW_92); 

                    				newLeafNode(otherlv_11, grammarAccess.getTestContraintsAccess().getRightCurlyBracketKeyword_5_4());
                    			

                    }
                    break;

            }

            // InternalAorta.g:2781:3: (otherlv_12= 'whenParams' otherlv_13= '{' ( (lv_whenParams_14_0= ruleParam ) ) (otherlv_15= ',' ( (lv_whenParams_16_0= ruleParam ) ) )* otherlv_17= '}' )?
            int alt79=2;
            int LA79_0 = input.LA(1);

            if ( (LA79_0==49) ) {
                alt79=1;
            }
            switch (alt79) {
                case 1 :
                    // InternalAorta.g:2782:4: otherlv_12= 'whenParams' otherlv_13= '{' ( (lv_whenParams_14_0= ruleParam ) ) (otherlv_15= ',' ( (lv_whenParams_16_0= ruleParam ) ) )* otherlv_17= '}'
                    {
                    otherlv_12=(Token)match(input,49,FOLLOW_3); 

                    				newLeafNode(otherlv_12, grammarAccess.getTestContraintsAccess().getWhenParamsKeyword_6_0());
                    			
                    otherlv_13=(Token)match(input,12,FOLLOW_91); 

                    				newLeafNode(otherlv_13, grammarAccess.getTestContraintsAccess().getLeftCurlyBracketKeyword_6_1());
                    			
                    // InternalAorta.g:2790:4: ( (lv_whenParams_14_0= ruleParam ) )
                    // InternalAorta.g:2791:5: (lv_whenParams_14_0= ruleParam )
                    {
                    // InternalAorta.g:2791:5: (lv_whenParams_14_0= ruleParam )
                    // InternalAorta.g:2792:6: lv_whenParams_14_0= ruleParam
                    {

                    						newCompositeNode(grammarAccess.getTestContraintsAccess().getWhenParamsParamParserRuleCall_6_2_0());
                    					
                    pushFollow(FOLLOW_35);
                    lv_whenParams_14_0=ruleParam();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getTestContraintsRule());
                    						}
                    						add(
                    							current,
                    							"whenParams",
                    							lv_whenParams_14_0,
                    							"org.eclipse.efbt.aorta.Aorta.Param");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:2809:4: (otherlv_15= ',' ( (lv_whenParams_16_0= ruleParam ) ) )*
                    loop78:
                    do {
                        int alt78=2;
                        int LA78_0 = input.LA(1);

                        if ( (LA78_0==31) ) {
                            alt78=1;
                        }


                        switch (alt78) {
                    	case 1 :
                    	    // InternalAorta.g:2810:5: otherlv_15= ',' ( (lv_whenParams_16_0= ruleParam ) )
                    	    {
                    	    otherlv_15=(Token)match(input,31,FOLLOW_91); 

                    	    					newLeafNode(otherlv_15, grammarAccess.getTestContraintsAccess().getCommaKeyword_6_3_0());
                    	    				
                    	    // InternalAorta.g:2814:5: ( (lv_whenParams_16_0= ruleParam ) )
                    	    // InternalAorta.g:2815:6: (lv_whenParams_16_0= ruleParam )
                    	    {
                    	    // InternalAorta.g:2815:6: (lv_whenParams_16_0= ruleParam )
                    	    // InternalAorta.g:2816:7: lv_whenParams_16_0= ruleParam
                    	    {

                    	    							newCompositeNode(grammarAccess.getTestContraintsAccess().getWhenParamsParamParserRuleCall_6_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_35);
                    	    lv_whenParams_16_0=ruleParam();

                    	    state._fsp--;


                    	    							if (current==null) {
                    	    								current = createModelElementForParent(grammarAccess.getTestContraintsRule());
                    	    							}
                    	    							add(
                    	    								current,
                    	    								"whenParams",
                    	    								lv_whenParams_16_0,
                    	    								"org.eclipse.efbt.aorta.Aorta.Param");
                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop78;
                        }
                    } while (true);

                    otherlv_17=(Token)match(input,23,FOLLOW_93); 

                    				newLeafNode(otherlv_17, grammarAccess.getTestContraintsAccess().getRightCurlyBracketKeyword_6_4());
                    			

                    }
                    break;

            }

            // InternalAorta.g:2839:3: (otherlv_18= 'thenParams' otherlv_19= '{' ( (lv_thenParams_20_0= ruleParam ) ) (otherlv_21= ',' ( (lv_thenParams_22_0= ruleParam ) ) )* otherlv_23= '}' )?
            int alt81=2;
            int LA81_0 = input.LA(1);

            if ( (LA81_0==50) ) {
                alt81=1;
            }
            switch (alt81) {
                case 1 :
                    // InternalAorta.g:2840:4: otherlv_18= 'thenParams' otherlv_19= '{' ( (lv_thenParams_20_0= ruleParam ) ) (otherlv_21= ',' ( (lv_thenParams_22_0= ruleParam ) ) )* otherlv_23= '}'
                    {
                    otherlv_18=(Token)match(input,50,FOLLOW_3); 

                    				newLeafNode(otherlv_18, grammarAccess.getTestContraintsAccess().getThenParamsKeyword_7_0());
                    			
                    otherlv_19=(Token)match(input,12,FOLLOW_91); 

                    				newLeafNode(otherlv_19, grammarAccess.getTestContraintsAccess().getLeftCurlyBracketKeyword_7_1());
                    			
                    // InternalAorta.g:2848:4: ( (lv_thenParams_20_0= ruleParam ) )
                    // InternalAorta.g:2849:5: (lv_thenParams_20_0= ruleParam )
                    {
                    // InternalAorta.g:2849:5: (lv_thenParams_20_0= ruleParam )
                    // InternalAorta.g:2850:6: lv_thenParams_20_0= ruleParam
                    {

                    						newCompositeNode(grammarAccess.getTestContraintsAccess().getThenParamsParamParserRuleCall_7_2_0());
                    					
                    pushFollow(FOLLOW_35);
                    lv_thenParams_20_0=ruleParam();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getTestContraintsRule());
                    						}
                    						add(
                    							current,
                    							"thenParams",
                    							lv_thenParams_20_0,
                    							"org.eclipse.efbt.aorta.Aorta.Param");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:2867:4: (otherlv_21= ',' ( (lv_thenParams_22_0= ruleParam ) ) )*
                    loop80:
                    do {
                        int alt80=2;
                        int LA80_0 = input.LA(1);

                        if ( (LA80_0==31) ) {
                            alt80=1;
                        }


                        switch (alt80) {
                    	case 1 :
                    	    // InternalAorta.g:2868:5: otherlv_21= ',' ( (lv_thenParams_22_0= ruleParam ) )
                    	    {
                    	    otherlv_21=(Token)match(input,31,FOLLOW_91); 

                    	    					newLeafNode(otherlv_21, grammarAccess.getTestContraintsAccess().getCommaKeyword_7_3_0());
                    	    				
                    	    // InternalAorta.g:2872:5: ( (lv_thenParams_22_0= ruleParam ) )
                    	    // InternalAorta.g:2873:6: (lv_thenParams_22_0= ruleParam )
                    	    {
                    	    // InternalAorta.g:2873:6: (lv_thenParams_22_0= ruleParam )
                    	    // InternalAorta.g:2874:7: lv_thenParams_22_0= ruleParam
                    	    {

                    	    							newCompositeNode(grammarAccess.getTestContraintsAccess().getThenParamsParamParserRuleCall_7_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_35);
                    	    lv_thenParams_22_0=ruleParam();

                    	    state._fsp--;


                    	    							if (current==null) {
                    	    								current = createModelElementForParent(grammarAccess.getTestContraintsRule());
                    	    							}
                    	    							add(
                    	    								current,
                    	    								"thenParams",
                    	    								lv_thenParams_22_0,
                    	    								"org.eclipse.efbt.aorta.Aorta.Param");
                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop80;
                        }
                    } while (true);

                    otherlv_23=(Token)match(input,23,FOLLOW_24); 

                    				newLeafNode(otherlv_23, grammarAccess.getTestContraintsAccess().getRightCurlyBracketKeyword_7_4());
                    			

                    }
                    break;

            }

            otherlv_24=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_24, grammarAccess.getTestContraintsAccess().getRightCurlyBracketKeyword_8());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleTestContraints"


    // $ANTLR start "entryRuleFunctionalityModuleModule"
    // InternalAorta.g:2905:1: entryRuleFunctionalityModuleModule returns [EObject current=null] : iv_ruleFunctionalityModuleModule= ruleFunctionalityModuleModule EOF ;
    public final EObject entryRuleFunctionalityModuleModule() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleFunctionalityModuleModule = null;


        try {
            // InternalAorta.g:2905:66: (iv_ruleFunctionalityModuleModule= ruleFunctionalityModuleModule EOF )
            // InternalAorta.g:2906:2: iv_ruleFunctionalityModuleModule= ruleFunctionalityModuleModule EOF
            {
             newCompositeNode(grammarAccess.getFunctionalityModuleModuleRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleFunctionalityModuleModule=ruleFunctionalityModuleModule();

            state._fsp--;

             current =iv_ruleFunctionalityModuleModule; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleFunctionalityModuleModule"


    // $ANTLR start "ruleFunctionalityModuleModule"
    // InternalAorta.g:2912:1: ruleFunctionalityModuleModule returns [EObject current=null] : ( () otherlv_1= 'FunctionalityModuleModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'FunctionalityModules' otherlv_15= '{' ( (lv_FunctionalityModules_16_0= ruleFunctionalityModule ) ) (otherlv_17= ',' ( (lv_FunctionalityModules_18_0= ruleFunctionalityModule ) ) )* otherlv_19= '}' )? otherlv_20= '}' ) ;
    public final EObject ruleFunctionalityModuleModule() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_6=null;
        Token otherlv_8=null;
        Token otherlv_10=null;
        Token otherlv_12=null;
        Token otherlv_14=null;
        Token otherlv_15=null;
        Token otherlv_17=null;
        Token otherlv_19=null;
        Token otherlv_20=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;

        AntlrDatatypeRuleToken lv_theDescription_5_0 = null;

        AntlrDatatypeRuleToken lv_license_7_0 = null;

        AntlrDatatypeRuleToken lv_version_9_0 = null;

        EObject lv_dependencies_11_0 = null;

        EObject lv_longName_13_0 = null;

        EObject lv_FunctionalityModules_16_0 = null;

        EObject lv_FunctionalityModules_18_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:2918:2: ( ( () otherlv_1= 'FunctionalityModuleModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'FunctionalityModules' otherlv_15= '{' ( (lv_FunctionalityModules_16_0= ruleFunctionalityModule ) ) (otherlv_17= ',' ( (lv_FunctionalityModules_18_0= ruleFunctionalityModule ) ) )* otherlv_19= '}' )? otherlv_20= '}' ) )
            // InternalAorta.g:2919:2: ( () otherlv_1= 'FunctionalityModuleModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'FunctionalityModules' otherlv_15= '{' ( (lv_FunctionalityModules_16_0= ruleFunctionalityModule ) ) (otherlv_17= ',' ( (lv_FunctionalityModules_18_0= ruleFunctionalityModule ) ) )* otherlv_19= '}' )? otherlv_20= '}' )
            {
            // InternalAorta.g:2919:2: ( () otherlv_1= 'FunctionalityModuleModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'FunctionalityModules' otherlv_15= '{' ( (lv_FunctionalityModules_16_0= ruleFunctionalityModule ) ) (otherlv_17= ',' ( (lv_FunctionalityModules_18_0= ruleFunctionalityModule ) ) )* otherlv_19= '}' )? otherlv_20= '}' )
            // InternalAorta.g:2920:3: () otherlv_1= 'FunctionalityModuleModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )? (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )? (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )? (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )? (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )? (otherlv_14= 'FunctionalityModules' otherlv_15= '{' ( (lv_FunctionalityModules_16_0= ruleFunctionalityModule ) ) (otherlv_17= ',' ( (lv_FunctionalityModules_18_0= ruleFunctionalityModule ) ) )* otherlv_19= '}' )? otherlv_20= '}'
            {
            // InternalAorta.g:2920:3: ()
            // InternalAorta.g:2921:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getFunctionalityModuleModuleAccess().getFunctionalityModuleModuleAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,51,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getFunctionalityModuleModuleAccess().getFunctionalityModuleModuleKeyword_1());
            		
            // InternalAorta.g:2931:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:2932:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:2932:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:2933:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getFunctionalityModuleModuleAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getFunctionalityModuleModuleRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_94); 

            			newLeafNode(otherlv_3, grammarAccess.getFunctionalityModuleModuleAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:2954:3: (otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) ) )?
            int alt82=2;
            int LA82_0 = input.LA(1);

            if ( (LA82_0==25) ) {
                alt82=1;
            }
            switch (alt82) {
                case 1 :
                    // InternalAorta.g:2955:4: otherlv_4= 'theDescription' ( (lv_theDescription_5_0= ruleEString ) )
                    {
                    otherlv_4=(Token)match(input,25,FOLLOW_25); 

                    				newLeafNode(otherlv_4, grammarAccess.getFunctionalityModuleModuleAccess().getTheDescriptionKeyword_4_0());
                    			
                    // InternalAorta.g:2959:4: ( (lv_theDescription_5_0= ruleEString ) )
                    // InternalAorta.g:2960:5: (lv_theDescription_5_0= ruleEString )
                    {
                    // InternalAorta.g:2960:5: (lv_theDescription_5_0= ruleEString )
                    // InternalAorta.g:2961:6: lv_theDescription_5_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getFunctionalityModuleModuleAccess().getTheDescriptionEStringParserRuleCall_4_1_0());
                    					
                    pushFollow(FOLLOW_95);
                    lv_theDescription_5_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getFunctionalityModuleModuleRule());
                    						}
                    						set(
                    							current,
                    							"theDescription",
                    							lv_theDescription_5_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:2979:3: (otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) ) )?
            int alt83=2;
            int LA83_0 = input.LA(1);

            if ( (LA83_0==26) ) {
                alt83=1;
            }
            switch (alt83) {
                case 1 :
                    // InternalAorta.g:2980:4: otherlv_6= 'license' ( (lv_license_7_0= ruleEString ) )
                    {
                    otherlv_6=(Token)match(input,26,FOLLOW_25); 

                    				newLeafNode(otherlv_6, grammarAccess.getFunctionalityModuleModuleAccess().getLicenseKeyword_5_0());
                    			
                    // InternalAorta.g:2984:4: ( (lv_license_7_0= ruleEString ) )
                    // InternalAorta.g:2985:5: (lv_license_7_0= ruleEString )
                    {
                    // InternalAorta.g:2985:5: (lv_license_7_0= ruleEString )
                    // InternalAorta.g:2986:6: lv_license_7_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getFunctionalityModuleModuleAccess().getLicenseEStringParserRuleCall_5_1_0());
                    					
                    pushFollow(FOLLOW_96);
                    lv_license_7_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getFunctionalityModuleModuleRule());
                    						}
                    						set(
                    							current,
                    							"license",
                    							lv_license_7_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:3004:3: (otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) ) )?
            int alt84=2;
            int LA84_0 = input.LA(1);

            if ( (LA84_0==27) ) {
                alt84=1;
            }
            switch (alt84) {
                case 1 :
                    // InternalAorta.g:3005:4: otherlv_8= 'version' ( (lv_version_9_0= ruleEString ) )
                    {
                    otherlv_8=(Token)match(input,27,FOLLOW_25); 

                    				newLeafNode(otherlv_8, grammarAccess.getFunctionalityModuleModuleAccess().getVersionKeyword_6_0());
                    			
                    // InternalAorta.g:3009:4: ( (lv_version_9_0= ruleEString ) )
                    // InternalAorta.g:3010:5: (lv_version_9_0= ruleEString )
                    {
                    // InternalAorta.g:3010:5: (lv_version_9_0= ruleEString )
                    // InternalAorta.g:3011:6: lv_version_9_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getFunctionalityModuleModuleAccess().getVersionEStringParserRuleCall_6_1_0());
                    					
                    pushFollow(FOLLOW_97);
                    lv_version_9_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getFunctionalityModuleModuleRule());
                    						}
                    						set(
                    							current,
                    							"version",
                    							lv_version_9_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:3029:3: (otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) ) )?
            int alt85=2;
            int LA85_0 = input.LA(1);

            if ( (LA85_0==28) ) {
                alt85=1;
            }
            switch (alt85) {
                case 1 :
                    // InternalAorta.g:3030:4: otherlv_10= 'dependencies' ( (lv_dependencies_11_0= ruleModuleDependencies ) )
                    {
                    otherlv_10=(Token)match(input,28,FOLLOW_30); 

                    				newLeafNode(otherlv_10, grammarAccess.getFunctionalityModuleModuleAccess().getDependenciesKeyword_7_0());
                    			
                    // InternalAorta.g:3034:4: ( (lv_dependencies_11_0= ruleModuleDependencies ) )
                    // InternalAorta.g:3035:5: (lv_dependencies_11_0= ruleModuleDependencies )
                    {
                    // InternalAorta.g:3035:5: (lv_dependencies_11_0= ruleModuleDependencies )
                    // InternalAorta.g:3036:6: lv_dependencies_11_0= ruleModuleDependencies
                    {

                    						newCompositeNode(grammarAccess.getFunctionalityModuleModuleAccess().getDependenciesModuleDependenciesParserRuleCall_7_1_0());
                    					
                    pushFollow(FOLLOW_98);
                    lv_dependencies_11_0=ruleModuleDependencies();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getFunctionalityModuleModuleRule());
                    						}
                    						set(
                    							current,
                    							"dependencies",
                    							lv_dependencies_11_0,
                    							"org.eclipse.efbt.aorta.Aorta.ModuleDependencies");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:3054:3: (otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) ) )?
            int alt86=2;
            int LA86_0 = input.LA(1);

            if ( (LA86_0==29) ) {
                alt86=1;
            }
            switch (alt86) {
                case 1 :
                    // InternalAorta.g:3055:4: otherlv_12= 'longName' ( (lv_longName_13_0= ruleModuleLongName ) )
                    {
                    otherlv_12=(Token)match(input,29,FOLLOW_32); 

                    				newLeafNode(otherlv_12, grammarAccess.getFunctionalityModuleModuleAccess().getLongNameKeyword_8_0());
                    			
                    // InternalAorta.g:3059:4: ( (lv_longName_13_0= ruleModuleLongName ) )
                    // InternalAorta.g:3060:5: (lv_longName_13_0= ruleModuleLongName )
                    {
                    // InternalAorta.g:3060:5: (lv_longName_13_0= ruleModuleLongName )
                    // InternalAorta.g:3061:6: lv_longName_13_0= ruleModuleLongName
                    {

                    						newCompositeNode(grammarAccess.getFunctionalityModuleModuleAccess().getLongNameModuleLongNameParserRuleCall_8_1_0());
                    					
                    pushFollow(FOLLOW_99);
                    lv_longName_13_0=ruleModuleLongName();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getFunctionalityModuleModuleRule());
                    						}
                    						set(
                    							current,
                    							"longName",
                    							lv_longName_13_0,
                    							"org.eclipse.efbt.aorta.Aorta.ModuleLongName");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:3079:3: (otherlv_14= 'FunctionalityModules' otherlv_15= '{' ( (lv_FunctionalityModules_16_0= ruleFunctionalityModule ) ) (otherlv_17= ',' ( (lv_FunctionalityModules_18_0= ruleFunctionalityModule ) ) )* otherlv_19= '}' )?
            int alt88=2;
            int LA88_0 = input.LA(1);

            if ( (LA88_0==52) ) {
                alt88=1;
            }
            switch (alt88) {
                case 1 :
                    // InternalAorta.g:3080:4: otherlv_14= 'FunctionalityModules' otherlv_15= '{' ( (lv_FunctionalityModules_16_0= ruleFunctionalityModule ) ) (otherlv_17= ',' ( (lv_FunctionalityModules_18_0= ruleFunctionalityModule ) ) )* otherlv_19= '}'
                    {
                    otherlv_14=(Token)match(input,52,FOLLOW_3); 

                    				newLeafNode(otherlv_14, grammarAccess.getFunctionalityModuleModuleAccess().getFunctionalityModulesKeyword_9_0());
                    			
                    otherlv_15=(Token)match(input,12,FOLLOW_100); 

                    				newLeafNode(otherlv_15, grammarAccess.getFunctionalityModuleModuleAccess().getLeftCurlyBracketKeyword_9_1());
                    			
                    // InternalAorta.g:3088:4: ( (lv_FunctionalityModules_16_0= ruleFunctionalityModule ) )
                    // InternalAorta.g:3089:5: (lv_FunctionalityModules_16_0= ruleFunctionalityModule )
                    {
                    // InternalAorta.g:3089:5: (lv_FunctionalityModules_16_0= ruleFunctionalityModule )
                    // InternalAorta.g:3090:6: lv_FunctionalityModules_16_0= ruleFunctionalityModule
                    {

                    						newCompositeNode(grammarAccess.getFunctionalityModuleModuleAccess().getFunctionalityModulesFunctionalityModuleParserRuleCall_9_2_0());
                    					
                    pushFollow(FOLLOW_35);
                    lv_FunctionalityModules_16_0=ruleFunctionalityModule();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getFunctionalityModuleModuleRule());
                    						}
                    						add(
                    							current,
                    							"FunctionalityModules",
                    							lv_FunctionalityModules_16_0,
                    							"org.eclipse.efbt.aorta.Aorta.FunctionalityModule");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:3107:4: (otherlv_17= ',' ( (lv_FunctionalityModules_18_0= ruleFunctionalityModule ) ) )*
                    loop87:
                    do {
                        int alt87=2;
                        int LA87_0 = input.LA(1);

                        if ( (LA87_0==31) ) {
                            alt87=1;
                        }


                        switch (alt87) {
                    	case 1 :
                    	    // InternalAorta.g:3108:5: otherlv_17= ',' ( (lv_FunctionalityModules_18_0= ruleFunctionalityModule ) )
                    	    {
                    	    otherlv_17=(Token)match(input,31,FOLLOW_100); 

                    	    					newLeafNode(otherlv_17, grammarAccess.getFunctionalityModuleModuleAccess().getCommaKeyword_9_3_0());
                    	    				
                    	    // InternalAorta.g:3112:5: ( (lv_FunctionalityModules_18_0= ruleFunctionalityModule ) )
                    	    // InternalAorta.g:3113:6: (lv_FunctionalityModules_18_0= ruleFunctionalityModule )
                    	    {
                    	    // InternalAorta.g:3113:6: (lv_FunctionalityModules_18_0= ruleFunctionalityModule )
                    	    // InternalAorta.g:3114:7: lv_FunctionalityModules_18_0= ruleFunctionalityModule
                    	    {

                    	    							newCompositeNode(grammarAccess.getFunctionalityModuleModuleAccess().getFunctionalityModulesFunctionalityModuleParserRuleCall_9_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_35);
                    	    lv_FunctionalityModules_18_0=ruleFunctionalityModule();

                    	    state._fsp--;


                    	    							if (current==null) {
                    	    								current = createModelElementForParent(grammarAccess.getFunctionalityModuleModuleRule());
                    	    							}
                    	    							add(
                    	    								current,
                    	    								"FunctionalityModules",
                    	    								lv_FunctionalityModules_18_0,
                    	    								"org.eclipse.efbt.aorta.Aorta.FunctionalityModule");
                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop87;
                        }
                    } while (true);

                    otherlv_19=(Token)match(input,23,FOLLOW_24); 

                    				newLeafNode(otherlv_19, grammarAccess.getFunctionalityModuleModuleAccess().getRightCurlyBracketKeyword_9_4());
                    			

                    }
                    break;

            }

            otherlv_20=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_20, grammarAccess.getFunctionalityModuleModuleAccess().getRightCurlyBracketKeyword_10());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleFunctionalityModuleModule"


    // $ANTLR start "entryRuleModuleDependencies"
    // InternalAorta.g:3145:1: entryRuleModuleDependencies returns [EObject current=null] : iv_ruleModuleDependencies= ruleModuleDependencies EOF ;
    public final EObject entryRuleModuleDependencies() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleModuleDependencies = null;


        try {
            // InternalAorta.g:3145:59: (iv_ruleModuleDependencies= ruleModuleDependencies EOF )
            // InternalAorta.g:3146:2: iv_ruleModuleDependencies= ruleModuleDependencies EOF
            {
             newCompositeNode(grammarAccess.getModuleDependenciesRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleModuleDependencies=ruleModuleDependencies();

            state._fsp--;

             current =iv_ruleModuleDependencies; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleModuleDependencies"


    // $ANTLR start "ruleModuleDependencies"
    // InternalAorta.g:3152:1: ruleModuleDependencies returns [EObject current=null] : ( () otherlv_1= 'ModuleDependencies' otherlv_2= '{' (otherlv_3= 'theModules' otherlv_4= '{' ( (lv_theModules_5_0= ruleModuleDependency ) ) (otherlv_6= ',' ( (lv_theModules_7_0= ruleModuleDependency ) ) )* otherlv_8= '}' )? otherlv_9= '}' ) ;
    public final EObject ruleModuleDependencies() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_2=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_6=null;
        Token otherlv_8=null;
        Token otherlv_9=null;
        EObject lv_theModules_5_0 = null;

        EObject lv_theModules_7_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:3158:2: ( ( () otherlv_1= 'ModuleDependencies' otherlv_2= '{' (otherlv_3= 'theModules' otherlv_4= '{' ( (lv_theModules_5_0= ruleModuleDependency ) ) (otherlv_6= ',' ( (lv_theModules_7_0= ruleModuleDependency ) ) )* otherlv_8= '}' )? otherlv_9= '}' ) )
            // InternalAorta.g:3159:2: ( () otherlv_1= 'ModuleDependencies' otherlv_2= '{' (otherlv_3= 'theModules' otherlv_4= '{' ( (lv_theModules_5_0= ruleModuleDependency ) ) (otherlv_6= ',' ( (lv_theModules_7_0= ruleModuleDependency ) ) )* otherlv_8= '}' )? otherlv_9= '}' )
            {
            // InternalAorta.g:3159:2: ( () otherlv_1= 'ModuleDependencies' otherlv_2= '{' (otherlv_3= 'theModules' otherlv_4= '{' ( (lv_theModules_5_0= ruleModuleDependency ) ) (otherlv_6= ',' ( (lv_theModules_7_0= ruleModuleDependency ) ) )* otherlv_8= '}' )? otherlv_9= '}' )
            // InternalAorta.g:3160:3: () otherlv_1= 'ModuleDependencies' otherlv_2= '{' (otherlv_3= 'theModules' otherlv_4= '{' ( (lv_theModules_5_0= ruleModuleDependency ) ) (otherlv_6= ',' ( (lv_theModules_7_0= ruleModuleDependency ) ) )* otherlv_8= '}' )? otherlv_9= '}'
            {
            // InternalAorta.g:3160:3: ()
            // InternalAorta.g:3161:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getModuleDependenciesAccess().getModuleDependenciesAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,53,FOLLOW_3); 

            			newLeafNode(otherlv_1, grammarAccess.getModuleDependenciesAccess().getModuleDependenciesKeyword_1());
            		
            otherlv_2=(Token)match(input,12,FOLLOW_101); 

            			newLeafNode(otherlv_2, grammarAccess.getModuleDependenciesAccess().getLeftCurlyBracketKeyword_2());
            		
            // InternalAorta.g:3175:3: (otherlv_3= 'theModules' otherlv_4= '{' ( (lv_theModules_5_0= ruleModuleDependency ) ) (otherlv_6= ',' ( (lv_theModules_7_0= ruleModuleDependency ) ) )* otherlv_8= '}' )?
            int alt90=2;
            int LA90_0 = input.LA(1);

            if ( (LA90_0==54) ) {
                alt90=1;
            }
            switch (alt90) {
                case 1 :
                    // InternalAorta.g:3176:4: otherlv_3= 'theModules' otherlv_4= '{' ( (lv_theModules_5_0= ruleModuleDependency ) ) (otherlv_6= ',' ( (lv_theModules_7_0= ruleModuleDependency ) ) )* otherlv_8= '}'
                    {
                    otherlv_3=(Token)match(input,54,FOLLOW_3); 

                    				newLeafNode(otherlv_3, grammarAccess.getModuleDependenciesAccess().getTheModulesKeyword_3_0());
                    			
                    otherlv_4=(Token)match(input,12,FOLLOW_102); 

                    				newLeafNode(otherlv_4, grammarAccess.getModuleDependenciesAccess().getLeftCurlyBracketKeyword_3_1());
                    			
                    // InternalAorta.g:3184:4: ( (lv_theModules_5_0= ruleModuleDependency ) )
                    // InternalAorta.g:3185:5: (lv_theModules_5_0= ruleModuleDependency )
                    {
                    // InternalAorta.g:3185:5: (lv_theModules_5_0= ruleModuleDependency )
                    // InternalAorta.g:3186:6: lv_theModules_5_0= ruleModuleDependency
                    {

                    						newCompositeNode(grammarAccess.getModuleDependenciesAccess().getTheModulesModuleDependencyParserRuleCall_3_2_0());
                    					
                    pushFollow(FOLLOW_35);
                    lv_theModules_5_0=ruleModuleDependency();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getModuleDependenciesRule());
                    						}
                    						add(
                    							current,
                    							"theModules",
                    							lv_theModules_5_0,
                    							"org.eclipse.efbt.aorta.Aorta.ModuleDependency");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:3203:4: (otherlv_6= ',' ( (lv_theModules_7_0= ruleModuleDependency ) ) )*
                    loop89:
                    do {
                        int alt89=2;
                        int LA89_0 = input.LA(1);

                        if ( (LA89_0==31) ) {
                            alt89=1;
                        }


                        switch (alt89) {
                    	case 1 :
                    	    // InternalAorta.g:3204:5: otherlv_6= ',' ( (lv_theModules_7_0= ruleModuleDependency ) )
                    	    {
                    	    otherlv_6=(Token)match(input,31,FOLLOW_102); 

                    	    					newLeafNode(otherlv_6, grammarAccess.getModuleDependenciesAccess().getCommaKeyword_3_3_0());
                    	    				
                    	    // InternalAorta.g:3208:5: ( (lv_theModules_7_0= ruleModuleDependency ) )
                    	    // InternalAorta.g:3209:6: (lv_theModules_7_0= ruleModuleDependency )
                    	    {
                    	    // InternalAorta.g:3209:6: (lv_theModules_7_0= ruleModuleDependency )
                    	    // InternalAorta.g:3210:7: lv_theModules_7_0= ruleModuleDependency
                    	    {

                    	    							newCompositeNode(grammarAccess.getModuleDependenciesAccess().getTheModulesModuleDependencyParserRuleCall_3_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_35);
                    	    lv_theModules_7_0=ruleModuleDependency();

                    	    state._fsp--;


                    	    							if (current==null) {
                    	    								current = createModelElementForParent(grammarAccess.getModuleDependenciesRule());
                    	    							}
                    	    							add(
                    	    								current,
                    	    								"theModules",
                    	    								lv_theModules_7_0,
                    	    								"org.eclipse.efbt.aorta.Aorta.ModuleDependency");
                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop89;
                        }
                    } while (true);

                    otherlv_8=(Token)match(input,23,FOLLOW_24); 

                    				newLeafNode(otherlv_8, grammarAccess.getModuleDependenciesAccess().getRightCurlyBracketKeyword_3_4());
                    			

                    }
                    break;

            }

            otherlv_9=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_9, grammarAccess.getModuleDependenciesAccess().getRightCurlyBracketKeyword_4());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleModuleDependencies"


    // $ANTLR start "entryRuleEString"
    // InternalAorta.g:3241:1: entryRuleEString returns [String current=null] : iv_ruleEString= ruleEString EOF ;
    public final String entryRuleEString() throws RecognitionException {
        String current = null;

        AntlrDatatypeRuleToken iv_ruleEString = null;


        try {
            // InternalAorta.g:3241:47: (iv_ruleEString= ruleEString EOF )
            // InternalAorta.g:3242:2: iv_ruleEString= ruleEString EOF
            {
             newCompositeNode(grammarAccess.getEStringRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleEString=ruleEString();

            state._fsp--;

             current =iv_ruleEString.getText(); 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleEString"


    // $ANTLR start "ruleEString"
    // InternalAorta.g:3248:1: ruleEString returns [AntlrDatatypeRuleToken current=new AntlrDatatypeRuleToken()] : (this_STRING_0= RULE_STRING | this_ID_1= RULE_ID ) ;
    public final AntlrDatatypeRuleToken ruleEString() throws RecognitionException {
        AntlrDatatypeRuleToken current = new AntlrDatatypeRuleToken();

        Token this_STRING_0=null;
        Token this_ID_1=null;


        	enterRule();

        try {
            // InternalAorta.g:3254:2: ( (this_STRING_0= RULE_STRING | this_ID_1= RULE_ID ) )
            // InternalAorta.g:3255:2: (this_STRING_0= RULE_STRING | this_ID_1= RULE_ID )
            {
            // InternalAorta.g:3255:2: (this_STRING_0= RULE_STRING | this_ID_1= RULE_ID )
            int alt91=2;
            int LA91_0 = input.LA(1);

            if ( (LA91_0==RULE_STRING) ) {
                alt91=1;
            }
            else if ( (LA91_0==RULE_ID) ) {
                alt91=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 91, 0, input);

                throw nvae;
            }
            switch (alt91) {
                case 1 :
                    // InternalAorta.g:3256:3: this_STRING_0= RULE_STRING
                    {
                    this_STRING_0=(Token)match(input,RULE_STRING,FOLLOW_2); 

                    			current.merge(this_STRING_0);
                    		

                    			newLeafNode(this_STRING_0, grammarAccess.getEStringAccess().getSTRINGTerminalRuleCall_0());
                    		

                    }
                    break;
                case 2 :
                    // InternalAorta.g:3264:3: this_ID_1= RULE_ID
                    {
                    this_ID_1=(Token)match(input,RULE_ID,FOLLOW_2); 

                    			current.merge(this_ID_1);
                    		

                    			newLeafNode(this_ID_1, grammarAccess.getEStringAccess().getIDTerminalRuleCall_1());
                    		

                    }
                    break;

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleEString"


    // $ANTLR start "entryRuleModuleLongName"
    // InternalAorta.g:3275:1: entryRuleModuleLongName returns [EObject current=null] : iv_ruleModuleLongName= ruleModuleLongName EOF ;
    public final EObject entryRuleModuleLongName() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleModuleLongName = null;


        try {
            // InternalAorta.g:3275:55: (iv_ruleModuleLongName= ruleModuleLongName EOF )
            // InternalAorta.g:3276:2: iv_ruleModuleLongName= ruleModuleLongName EOF
            {
             newCompositeNode(grammarAccess.getModuleLongNameRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleModuleLongName=ruleModuleLongName();

            state._fsp--;

             current =iv_ruleModuleLongName; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleModuleLongName"


    // $ANTLR start "ruleModuleLongName"
    // InternalAorta.g:3282:1: ruleModuleLongName returns [EObject current=null] : ( () otherlv_1= 'ModuleLongName' ( (lv_name_2_0= ruleEString ) ) ) ;
    public final EObject ruleModuleLongName() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:3288:2: ( ( () otherlv_1= 'ModuleLongName' ( (lv_name_2_0= ruleEString ) ) ) )
            // InternalAorta.g:3289:2: ( () otherlv_1= 'ModuleLongName' ( (lv_name_2_0= ruleEString ) ) )
            {
            // InternalAorta.g:3289:2: ( () otherlv_1= 'ModuleLongName' ( (lv_name_2_0= ruleEString ) ) )
            // InternalAorta.g:3290:3: () otherlv_1= 'ModuleLongName' ( (lv_name_2_0= ruleEString ) )
            {
            // InternalAorta.g:3290:3: ()
            // InternalAorta.g:3291:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getModuleLongNameAccess().getModuleLongNameAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,55,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getModuleLongNameAccess().getModuleLongNameKeyword_1());
            		
            // InternalAorta.g:3301:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:3302:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:3302:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:3303:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getModuleLongNameAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_2);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getModuleLongNameRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }


            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleModuleLongName"


    // $ANTLR start "entryRuleColumnDomain"
    // InternalAorta.g:3324:1: entryRuleColumnDomain returns [EObject current=null] : iv_ruleColumnDomain= ruleColumnDomain EOF ;
    public final EObject entryRuleColumnDomain() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleColumnDomain = null;


        try {
            // InternalAorta.g:3324:53: (iv_ruleColumnDomain= ruleColumnDomain EOF )
            // InternalAorta.g:3325:2: iv_ruleColumnDomain= ruleColumnDomain EOF
            {
             newCompositeNode(grammarAccess.getColumnDomainRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleColumnDomain=ruleColumnDomain();

            state._fsp--;

             current =iv_ruleColumnDomain; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleColumnDomain"


    // $ANTLR start "ruleColumnDomain"
    // InternalAorta.g:3331:1: ruleColumnDomain returns [EObject current=null] : ( () ( (lv_isEnumerated_1_0= 'isEnumerated' ) )? otherlv_2= 'ColumnDomain' ( (lv_name_3_0= ruleEString ) ) otherlv_4= '{' (otherlv_5= 'enumMembers' otherlv_6= '{' ( (lv_enumMembers_7_0= ruleEnumMember ) ) (otherlv_8= ',' ( (lv_enumMembers_9_0= ruleEnumMember ) ) )* otherlv_10= '}' )? (otherlv_11= 'dataType' ( (lv_dataType_12_0= ruleColumnDataType ) ) )? otherlv_13= '}' ) ;
    public final EObject ruleColumnDomain() throws RecognitionException {
        EObject current = null;

        Token lv_isEnumerated_1_0=null;
        Token otherlv_2=null;
        Token otherlv_4=null;
        Token otherlv_5=null;
        Token otherlv_6=null;
        Token otherlv_8=null;
        Token otherlv_10=null;
        Token otherlv_11=null;
        Token otherlv_13=null;
        AntlrDatatypeRuleToken lv_name_3_0 = null;

        EObject lv_enumMembers_7_0 = null;

        EObject lv_enumMembers_9_0 = null;

        EObject lv_dataType_12_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:3337:2: ( ( () ( (lv_isEnumerated_1_0= 'isEnumerated' ) )? otherlv_2= 'ColumnDomain' ( (lv_name_3_0= ruleEString ) ) otherlv_4= '{' (otherlv_5= 'enumMembers' otherlv_6= '{' ( (lv_enumMembers_7_0= ruleEnumMember ) ) (otherlv_8= ',' ( (lv_enumMembers_9_0= ruleEnumMember ) ) )* otherlv_10= '}' )? (otherlv_11= 'dataType' ( (lv_dataType_12_0= ruleColumnDataType ) ) )? otherlv_13= '}' ) )
            // InternalAorta.g:3338:2: ( () ( (lv_isEnumerated_1_0= 'isEnumerated' ) )? otherlv_2= 'ColumnDomain' ( (lv_name_3_0= ruleEString ) ) otherlv_4= '{' (otherlv_5= 'enumMembers' otherlv_6= '{' ( (lv_enumMembers_7_0= ruleEnumMember ) ) (otherlv_8= ',' ( (lv_enumMembers_9_0= ruleEnumMember ) ) )* otherlv_10= '}' )? (otherlv_11= 'dataType' ( (lv_dataType_12_0= ruleColumnDataType ) ) )? otherlv_13= '}' )
            {
            // InternalAorta.g:3338:2: ( () ( (lv_isEnumerated_1_0= 'isEnumerated' ) )? otherlv_2= 'ColumnDomain' ( (lv_name_3_0= ruleEString ) ) otherlv_4= '{' (otherlv_5= 'enumMembers' otherlv_6= '{' ( (lv_enumMembers_7_0= ruleEnumMember ) ) (otherlv_8= ',' ( (lv_enumMembers_9_0= ruleEnumMember ) ) )* otherlv_10= '}' )? (otherlv_11= 'dataType' ( (lv_dataType_12_0= ruleColumnDataType ) ) )? otherlv_13= '}' )
            // InternalAorta.g:3339:3: () ( (lv_isEnumerated_1_0= 'isEnumerated' ) )? otherlv_2= 'ColumnDomain' ( (lv_name_3_0= ruleEString ) ) otherlv_4= '{' (otherlv_5= 'enumMembers' otherlv_6= '{' ( (lv_enumMembers_7_0= ruleEnumMember ) ) (otherlv_8= ',' ( (lv_enumMembers_9_0= ruleEnumMember ) ) )* otherlv_10= '}' )? (otherlv_11= 'dataType' ( (lv_dataType_12_0= ruleColumnDataType ) ) )? otherlv_13= '}'
            {
            // InternalAorta.g:3339:3: ()
            // InternalAorta.g:3340:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getColumnDomainAccess().getColumnDomainAction_0(),
            					current);
            			

            }

            // InternalAorta.g:3346:3: ( (lv_isEnumerated_1_0= 'isEnumerated' ) )?
            int alt92=2;
            int LA92_0 = input.LA(1);

            if ( (LA92_0==56) ) {
                alt92=1;
            }
            switch (alt92) {
                case 1 :
                    // InternalAorta.g:3347:4: (lv_isEnumerated_1_0= 'isEnumerated' )
                    {
                    // InternalAorta.g:3347:4: (lv_isEnumerated_1_0= 'isEnumerated' )
                    // InternalAorta.g:3348:5: lv_isEnumerated_1_0= 'isEnumerated'
                    {
                    lv_isEnumerated_1_0=(Token)match(input,56,FOLLOW_103); 

                    					newLeafNode(lv_isEnumerated_1_0, grammarAccess.getColumnDomainAccess().getIsEnumeratedIsEnumeratedKeyword_1_0());
                    				

                    					if (current==null) {
                    						current = createModelElement(grammarAccess.getColumnDomainRule());
                    					}
                    					setWithLastConsumed(current, "isEnumerated", true, "isEnumerated");
                    				

                    }


                    }
                    break;

            }

            otherlv_2=(Token)match(input,57,FOLLOW_25); 

            			newLeafNode(otherlv_2, grammarAccess.getColumnDomainAccess().getColumnDomainKeyword_2());
            		
            // InternalAorta.g:3364:3: ( (lv_name_3_0= ruleEString ) )
            // InternalAorta.g:3365:4: (lv_name_3_0= ruleEString )
            {
            // InternalAorta.g:3365:4: (lv_name_3_0= ruleEString )
            // InternalAorta.g:3366:5: lv_name_3_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getColumnDomainAccess().getNameEStringParserRuleCall_3_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_3_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getColumnDomainRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_3_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_4=(Token)match(input,12,FOLLOW_104); 

            			newLeafNode(otherlv_4, grammarAccess.getColumnDomainAccess().getLeftCurlyBracketKeyword_4());
            		
            // InternalAorta.g:3387:3: (otherlv_5= 'enumMembers' otherlv_6= '{' ( (lv_enumMembers_7_0= ruleEnumMember ) ) (otherlv_8= ',' ( (lv_enumMembers_9_0= ruleEnumMember ) ) )* otherlv_10= '}' )?
            int alt94=2;
            int LA94_0 = input.LA(1);

            if ( (LA94_0==58) ) {
                alt94=1;
            }
            switch (alt94) {
                case 1 :
                    // InternalAorta.g:3388:4: otherlv_5= 'enumMembers' otherlv_6= '{' ( (lv_enumMembers_7_0= ruleEnumMember ) ) (otherlv_8= ',' ( (lv_enumMembers_9_0= ruleEnumMember ) ) )* otherlv_10= '}'
                    {
                    otherlv_5=(Token)match(input,58,FOLLOW_3); 

                    				newLeafNode(otherlv_5, grammarAccess.getColumnDomainAccess().getEnumMembersKeyword_5_0());
                    			
                    otherlv_6=(Token)match(input,12,FOLLOW_105); 

                    				newLeafNode(otherlv_6, grammarAccess.getColumnDomainAccess().getLeftCurlyBracketKeyword_5_1());
                    			
                    // InternalAorta.g:3396:4: ( (lv_enumMembers_7_0= ruleEnumMember ) )
                    // InternalAorta.g:3397:5: (lv_enumMembers_7_0= ruleEnumMember )
                    {
                    // InternalAorta.g:3397:5: (lv_enumMembers_7_0= ruleEnumMember )
                    // InternalAorta.g:3398:6: lv_enumMembers_7_0= ruleEnumMember
                    {

                    						newCompositeNode(grammarAccess.getColumnDomainAccess().getEnumMembersEnumMemberParserRuleCall_5_2_0());
                    					
                    pushFollow(FOLLOW_35);
                    lv_enumMembers_7_0=ruleEnumMember();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getColumnDomainRule());
                    						}
                    						add(
                    							current,
                    							"enumMembers",
                    							lv_enumMembers_7_0,
                    							"org.eclipse.efbt.aorta.Aorta.EnumMember");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:3415:4: (otherlv_8= ',' ( (lv_enumMembers_9_0= ruleEnumMember ) ) )*
                    loop93:
                    do {
                        int alt93=2;
                        int LA93_0 = input.LA(1);

                        if ( (LA93_0==31) ) {
                            alt93=1;
                        }


                        switch (alt93) {
                    	case 1 :
                    	    // InternalAorta.g:3416:5: otherlv_8= ',' ( (lv_enumMembers_9_0= ruleEnumMember ) )
                    	    {
                    	    otherlv_8=(Token)match(input,31,FOLLOW_105); 

                    	    					newLeafNode(otherlv_8, grammarAccess.getColumnDomainAccess().getCommaKeyword_5_3_0());
                    	    				
                    	    // InternalAorta.g:3420:5: ( (lv_enumMembers_9_0= ruleEnumMember ) )
                    	    // InternalAorta.g:3421:6: (lv_enumMembers_9_0= ruleEnumMember )
                    	    {
                    	    // InternalAorta.g:3421:6: (lv_enumMembers_9_0= ruleEnumMember )
                    	    // InternalAorta.g:3422:7: lv_enumMembers_9_0= ruleEnumMember
                    	    {

                    	    							newCompositeNode(grammarAccess.getColumnDomainAccess().getEnumMembersEnumMemberParserRuleCall_5_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_35);
                    	    lv_enumMembers_9_0=ruleEnumMember();

                    	    state._fsp--;


                    	    							if (current==null) {
                    	    								current = createModelElementForParent(grammarAccess.getColumnDomainRule());
                    	    							}
                    	    							add(
                    	    								current,
                    	    								"enumMembers",
                    	    								lv_enumMembers_9_0,
                    	    								"org.eclipse.efbt.aorta.Aorta.EnumMember");
                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop93;
                        }
                    } while (true);

                    otherlv_10=(Token)match(input,23,FOLLOW_106); 

                    				newLeafNode(otherlv_10, grammarAccess.getColumnDomainAccess().getRightCurlyBracketKeyword_5_4());
                    			

                    }
                    break;

            }

            // InternalAorta.g:3445:3: (otherlv_11= 'dataType' ( (lv_dataType_12_0= ruleColumnDataType ) ) )?
            int alt95=2;
            int LA95_0 = input.LA(1);

            if ( (LA95_0==59) ) {
                alt95=1;
            }
            switch (alt95) {
                case 1 :
                    // InternalAorta.g:3446:4: otherlv_11= 'dataType' ( (lv_dataType_12_0= ruleColumnDataType ) )
                    {
                    otherlv_11=(Token)match(input,59,FOLLOW_107); 

                    				newLeafNode(otherlv_11, grammarAccess.getColumnDomainAccess().getDataTypeKeyword_6_0());
                    			
                    // InternalAorta.g:3450:4: ( (lv_dataType_12_0= ruleColumnDataType ) )
                    // InternalAorta.g:3451:5: (lv_dataType_12_0= ruleColumnDataType )
                    {
                    // InternalAorta.g:3451:5: (lv_dataType_12_0= ruleColumnDataType )
                    // InternalAorta.g:3452:6: lv_dataType_12_0= ruleColumnDataType
                    {

                    						newCompositeNode(grammarAccess.getColumnDomainAccess().getDataTypeColumnDataTypeParserRuleCall_6_1_0());
                    					
                    pushFollow(FOLLOW_24);
                    lv_dataType_12_0=ruleColumnDataType();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getColumnDomainRule());
                    						}
                    						set(
                    							current,
                    							"dataType",
                    							lv_dataType_12_0,
                    							"org.eclipse.efbt.aorta.Aorta.ColumnDataType");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            otherlv_13=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_13, grammarAccess.getColumnDomainAccess().getRightCurlyBracketKeyword_7());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleColumnDomain"


    // $ANTLR start "entryRuleModuleDependency"
    // InternalAorta.g:3478:1: entryRuleModuleDependency returns [EObject current=null] : iv_ruleModuleDependency= ruleModuleDependency EOF ;
    public final EObject entryRuleModuleDependency() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleModuleDependency = null;


        try {
            // InternalAorta.g:3478:57: (iv_ruleModuleDependency= ruleModuleDependency EOF )
            // InternalAorta.g:3479:2: iv_ruleModuleDependency= ruleModuleDependency EOF
            {
             newCompositeNode(grammarAccess.getModuleDependencyRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleModuleDependency=ruleModuleDependency();

            state._fsp--;

             current =iv_ruleModuleDependency; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleModuleDependency"


    // $ANTLR start "ruleModuleDependency"
    // InternalAorta.g:3485:1: ruleModuleDependency returns [EObject current=null] : ( () otherlv_1= 'ModuleDependency' otherlv_2= '{' (otherlv_3= 'moduleName' ( (lv_moduleName_4_0= ruleEString ) ) )? (otherlv_5= 'moduleVersion' ( (lv_moduleVersion_6_0= ruleEString ) ) )? (otherlv_7= 'theModule' ( ( ruleQualifiedName ) ) )? (otherlv_9= 'longName' ( ( ruleQualifiedName ) ) )? otherlv_11= '}' ) ;
    public final EObject ruleModuleDependency() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_2=null;
        Token otherlv_3=null;
        Token otherlv_5=null;
        Token otherlv_7=null;
        Token otherlv_9=null;
        Token otherlv_11=null;
        AntlrDatatypeRuleToken lv_moduleName_4_0 = null;

        AntlrDatatypeRuleToken lv_moduleVersion_6_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:3491:2: ( ( () otherlv_1= 'ModuleDependency' otherlv_2= '{' (otherlv_3= 'moduleName' ( (lv_moduleName_4_0= ruleEString ) ) )? (otherlv_5= 'moduleVersion' ( (lv_moduleVersion_6_0= ruleEString ) ) )? (otherlv_7= 'theModule' ( ( ruleQualifiedName ) ) )? (otherlv_9= 'longName' ( ( ruleQualifiedName ) ) )? otherlv_11= '}' ) )
            // InternalAorta.g:3492:2: ( () otherlv_1= 'ModuleDependency' otherlv_2= '{' (otherlv_3= 'moduleName' ( (lv_moduleName_4_0= ruleEString ) ) )? (otherlv_5= 'moduleVersion' ( (lv_moduleVersion_6_0= ruleEString ) ) )? (otherlv_7= 'theModule' ( ( ruleQualifiedName ) ) )? (otherlv_9= 'longName' ( ( ruleQualifiedName ) ) )? otherlv_11= '}' )
            {
            // InternalAorta.g:3492:2: ( () otherlv_1= 'ModuleDependency' otherlv_2= '{' (otherlv_3= 'moduleName' ( (lv_moduleName_4_0= ruleEString ) ) )? (otherlv_5= 'moduleVersion' ( (lv_moduleVersion_6_0= ruleEString ) ) )? (otherlv_7= 'theModule' ( ( ruleQualifiedName ) ) )? (otherlv_9= 'longName' ( ( ruleQualifiedName ) ) )? otherlv_11= '}' )
            // InternalAorta.g:3493:3: () otherlv_1= 'ModuleDependency' otherlv_2= '{' (otherlv_3= 'moduleName' ( (lv_moduleName_4_0= ruleEString ) ) )? (otherlv_5= 'moduleVersion' ( (lv_moduleVersion_6_0= ruleEString ) ) )? (otherlv_7= 'theModule' ( ( ruleQualifiedName ) ) )? (otherlv_9= 'longName' ( ( ruleQualifiedName ) ) )? otherlv_11= '}'
            {
            // InternalAorta.g:3493:3: ()
            // InternalAorta.g:3494:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getModuleDependencyAccess().getModuleDependencyAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,60,FOLLOW_3); 

            			newLeafNode(otherlv_1, grammarAccess.getModuleDependencyAccess().getModuleDependencyKeyword_1());
            		
            otherlv_2=(Token)match(input,12,FOLLOW_108); 

            			newLeafNode(otherlv_2, grammarAccess.getModuleDependencyAccess().getLeftCurlyBracketKeyword_2());
            		
            // InternalAorta.g:3508:3: (otherlv_3= 'moduleName' ( (lv_moduleName_4_0= ruleEString ) ) )?
            int alt96=2;
            int LA96_0 = input.LA(1);

            if ( (LA96_0==61) ) {
                alt96=1;
            }
            switch (alt96) {
                case 1 :
                    // InternalAorta.g:3509:4: otherlv_3= 'moduleName' ( (lv_moduleName_4_0= ruleEString ) )
                    {
                    otherlv_3=(Token)match(input,61,FOLLOW_25); 

                    				newLeafNode(otherlv_3, grammarAccess.getModuleDependencyAccess().getModuleNameKeyword_3_0());
                    			
                    // InternalAorta.g:3513:4: ( (lv_moduleName_4_0= ruleEString ) )
                    // InternalAorta.g:3514:5: (lv_moduleName_4_0= ruleEString )
                    {
                    // InternalAorta.g:3514:5: (lv_moduleName_4_0= ruleEString )
                    // InternalAorta.g:3515:6: lv_moduleName_4_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getModuleDependencyAccess().getModuleNameEStringParserRuleCall_3_1_0());
                    					
                    pushFollow(FOLLOW_109);
                    lv_moduleName_4_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getModuleDependencyRule());
                    						}
                    						set(
                    							current,
                    							"moduleName",
                    							lv_moduleName_4_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:3533:3: (otherlv_5= 'moduleVersion' ( (lv_moduleVersion_6_0= ruleEString ) ) )?
            int alt97=2;
            int LA97_0 = input.LA(1);

            if ( (LA97_0==62) ) {
                alt97=1;
            }
            switch (alt97) {
                case 1 :
                    // InternalAorta.g:3534:4: otherlv_5= 'moduleVersion' ( (lv_moduleVersion_6_0= ruleEString ) )
                    {
                    otherlv_5=(Token)match(input,62,FOLLOW_25); 

                    				newLeafNode(otherlv_5, grammarAccess.getModuleDependencyAccess().getModuleVersionKeyword_4_0());
                    			
                    // InternalAorta.g:3538:4: ( (lv_moduleVersion_6_0= ruleEString ) )
                    // InternalAorta.g:3539:5: (lv_moduleVersion_6_0= ruleEString )
                    {
                    // InternalAorta.g:3539:5: (lv_moduleVersion_6_0= ruleEString )
                    // InternalAorta.g:3540:6: lv_moduleVersion_6_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getModuleDependencyAccess().getModuleVersionEStringParserRuleCall_4_1_0());
                    					
                    pushFollow(FOLLOW_110);
                    lv_moduleVersion_6_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getModuleDependencyRule());
                    						}
                    						set(
                    							current,
                    							"moduleVersion",
                    							lv_moduleVersion_6_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:3558:3: (otherlv_7= 'theModule' ( ( ruleQualifiedName ) ) )?
            int alt98=2;
            int LA98_0 = input.LA(1);

            if ( (LA98_0==63) ) {
                alt98=1;
            }
            switch (alt98) {
                case 1 :
                    // InternalAorta.g:3559:4: otherlv_7= 'theModule' ( ( ruleQualifiedName ) )
                    {
                    otherlv_7=(Token)match(input,63,FOLLOW_70); 

                    				newLeafNode(otherlv_7, grammarAccess.getModuleDependencyAccess().getTheModuleKeyword_5_0());
                    			
                    // InternalAorta.g:3563:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:3564:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:3564:5: ( ruleQualifiedName )
                    // InternalAorta.g:3565:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getModuleDependencyRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getModuleDependencyAccess().getTheModuleModuleCrossReference_5_1_0());
                    					
                    pushFollow(FOLLOW_111);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:3580:3: (otherlv_9= 'longName' ( ( ruleQualifiedName ) ) )?
            int alt99=2;
            int LA99_0 = input.LA(1);

            if ( (LA99_0==29) ) {
                alt99=1;
            }
            switch (alt99) {
                case 1 :
                    // InternalAorta.g:3581:4: otherlv_9= 'longName' ( ( ruleQualifiedName ) )
                    {
                    otherlv_9=(Token)match(input,29,FOLLOW_70); 

                    				newLeafNode(otherlv_9, grammarAccess.getModuleDependencyAccess().getLongNameKeyword_6_0());
                    			
                    // InternalAorta.g:3585:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:3586:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:3586:5: ( ruleQualifiedName )
                    // InternalAorta.g:3587:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getModuleDependencyRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getModuleDependencyAccess().getLongNameModuleLongNameCrossReference_6_1_0());
                    					
                    pushFollow(FOLLOW_24);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            otherlv_11=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_11, grammarAccess.getModuleDependencyAccess().getRightCurlyBracketKeyword_7());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleModuleDependency"


    // $ANTLR start "entryRuleEnumMember"
    // InternalAorta.g:3610:1: entryRuleEnumMember returns [EObject current=null] : iv_ruleEnumMember= ruleEnumMember EOF ;
    public final EObject entryRuleEnumMember() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleEnumMember = null;


        try {
            // InternalAorta.g:3610:51: (iv_ruleEnumMember= ruleEnumMember EOF )
            // InternalAorta.g:3611:2: iv_ruleEnumMember= ruleEnumMember EOF
            {
             newCompositeNode(grammarAccess.getEnumMemberRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleEnumMember=ruleEnumMember();

            state._fsp--;

             current =iv_ruleEnumMember; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleEnumMember"


    // $ANTLR start "ruleEnumMember"
    // InternalAorta.g:3617:1: ruleEnumMember returns [EObject current=null] : ( () otherlv_1= 'EnumMember' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'code' ( (lv_code_5_0= ruleEString ) ) )? otherlv_6= '}' ) ;
    public final EObject ruleEnumMember() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_6=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;

        AntlrDatatypeRuleToken lv_code_5_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:3623:2: ( ( () otherlv_1= 'EnumMember' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'code' ( (lv_code_5_0= ruleEString ) ) )? otherlv_6= '}' ) )
            // InternalAorta.g:3624:2: ( () otherlv_1= 'EnumMember' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'code' ( (lv_code_5_0= ruleEString ) ) )? otherlv_6= '}' )
            {
            // InternalAorta.g:3624:2: ( () otherlv_1= 'EnumMember' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'code' ( (lv_code_5_0= ruleEString ) ) )? otherlv_6= '}' )
            // InternalAorta.g:3625:3: () otherlv_1= 'EnumMember' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'code' ( (lv_code_5_0= ruleEString ) ) )? otherlv_6= '}'
            {
            // InternalAorta.g:3625:3: ()
            // InternalAorta.g:3626:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getEnumMemberAccess().getEnumMemberAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,64,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getEnumMemberAccess().getEnumMemberKeyword_1());
            		
            // InternalAorta.g:3636:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:3637:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:3637:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:3638:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getEnumMemberAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getEnumMemberRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_112); 

            			newLeafNode(otherlv_3, grammarAccess.getEnumMemberAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:3659:3: (otherlv_4= 'code' ( (lv_code_5_0= ruleEString ) ) )?
            int alt100=2;
            int LA100_0 = input.LA(1);

            if ( (LA100_0==65) ) {
                alt100=1;
            }
            switch (alt100) {
                case 1 :
                    // InternalAorta.g:3660:4: otherlv_4= 'code' ( (lv_code_5_0= ruleEString ) )
                    {
                    otherlv_4=(Token)match(input,65,FOLLOW_25); 

                    				newLeafNode(otherlv_4, grammarAccess.getEnumMemberAccess().getCodeKeyword_4_0());
                    			
                    // InternalAorta.g:3664:4: ( (lv_code_5_0= ruleEString ) )
                    // InternalAorta.g:3665:5: (lv_code_5_0= ruleEString )
                    {
                    // InternalAorta.g:3665:5: (lv_code_5_0= ruleEString )
                    // InternalAorta.g:3666:6: lv_code_5_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getEnumMemberAccess().getCodeEStringParserRuleCall_4_1_0());
                    					
                    pushFollow(FOLLOW_24);
                    lv_code_5_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getEnumMemberRule());
                    						}
                    						set(
                    							current,
                    							"code",
                    							lv_code_5_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            otherlv_6=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_6, grammarAccess.getEnumMemberAccess().getRightCurlyBracketKeyword_5());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleEnumMember"


    // $ANTLR start "entryRuleColumnDataType"
    // InternalAorta.g:3692:1: entryRuleColumnDataType returns [EObject current=null] : iv_ruleColumnDataType= ruleColumnDataType EOF ;
    public final EObject entryRuleColumnDataType() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleColumnDataType = null;


        try {
            // InternalAorta.g:3692:55: (iv_ruleColumnDataType= ruleColumnDataType EOF )
            // InternalAorta.g:3693:2: iv_ruleColumnDataType= ruleColumnDataType EOF
            {
             newCompositeNode(grammarAccess.getColumnDataTypeRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleColumnDataType=ruleColumnDataType();

            state._fsp--;

             current =iv_ruleColumnDataType; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleColumnDataType"


    // $ANTLR start "ruleColumnDataType"
    // InternalAorta.g:3699:1: ruleColumnDataType returns [EObject current=null] : ( () otherlv_1= 'ColumnDataType' ( (lv_name_2_0= ruleEString ) ) ) ;
    public final EObject ruleColumnDataType() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:3705:2: ( ( () otherlv_1= 'ColumnDataType' ( (lv_name_2_0= ruleEString ) ) ) )
            // InternalAorta.g:3706:2: ( () otherlv_1= 'ColumnDataType' ( (lv_name_2_0= ruleEString ) ) )
            {
            // InternalAorta.g:3706:2: ( () otherlv_1= 'ColumnDataType' ( (lv_name_2_0= ruleEString ) ) )
            // InternalAorta.g:3707:3: () otherlv_1= 'ColumnDataType' ( (lv_name_2_0= ruleEString ) )
            {
            // InternalAorta.g:3707:3: ()
            // InternalAorta.g:3708:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getColumnDataTypeAccess().getColumnDataTypeAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,66,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getColumnDataTypeAccess().getColumnDataTypeKeyword_1());
            		
            // InternalAorta.g:3718:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:3719:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:3719:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:3720:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getColumnDataTypeAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_2);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getColumnDataTypeRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }


            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleColumnDataType"


    // $ANTLR start "entryRuleTitledRequirementsSection"
    // InternalAorta.g:3741:1: entryRuleTitledRequirementsSection returns [EObject current=null] : iv_ruleTitledRequirementsSection= ruleTitledRequirementsSection EOF ;
    public final EObject entryRuleTitledRequirementsSection() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleTitledRequirementsSection = null;


        try {
            // InternalAorta.g:3741:66: (iv_ruleTitledRequirementsSection= ruleTitledRequirementsSection EOF )
            // InternalAorta.g:3742:2: iv_ruleTitledRequirementsSection= ruleTitledRequirementsSection EOF
            {
             newCompositeNode(grammarAccess.getTitledRequirementsSectionRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleTitledRequirementsSection=ruleTitledRequirementsSection();

            state._fsp--;

             current =iv_ruleTitledRequirementsSection; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleTitledRequirementsSection"


    // $ANTLR start "ruleTitledRequirementsSection"
    // InternalAorta.g:3748:1: ruleTitledRequirementsSection returns [EObject current=null] : (otherlv_0= 'TitledRequirementsSection' ( (lv_name_1_0= ruleEString ) ) otherlv_2= '{' (otherlv_3= 'title' ( (lv_title_4_0= ruleEString ) ) )? (otherlv_5= 'referencingSections' ( ( ruleQualifiedName ) ) )? otherlv_7= 'requirementsType' ( ( ruleQualifiedName ) ) (otherlv_9= 'sections' otherlv_10= '{' ( (lv_sections_11_0= ruleRequirementsSection ) ) (otherlv_12= ',' ( (lv_sections_13_0= ruleRequirementsSection ) ) )* otherlv_14= '}' )? otherlv_15= '}' ) ;
    public final EObject ruleTitledRequirementsSection() throws RecognitionException {
        EObject current = null;

        Token otherlv_0=null;
        Token otherlv_2=null;
        Token otherlv_3=null;
        Token otherlv_5=null;
        Token otherlv_7=null;
        Token otherlv_9=null;
        Token otherlv_10=null;
        Token otherlv_12=null;
        Token otherlv_14=null;
        Token otherlv_15=null;
        AntlrDatatypeRuleToken lv_name_1_0 = null;

        AntlrDatatypeRuleToken lv_title_4_0 = null;

        EObject lv_sections_11_0 = null;

        EObject lv_sections_13_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:3754:2: ( (otherlv_0= 'TitledRequirementsSection' ( (lv_name_1_0= ruleEString ) ) otherlv_2= '{' (otherlv_3= 'title' ( (lv_title_4_0= ruleEString ) ) )? (otherlv_5= 'referencingSections' ( ( ruleQualifiedName ) ) )? otherlv_7= 'requirementsType' ( ( ruleQualifiedName ) ) (otherlv_9= 'sections' otherlv_10= '{' ( (lv_sections_11_0= ruleRequirementsSection ) ) (otherlv_12= ',' ( (lv_sections_13_0= ruleRequirementsSection ) ) )* otherlv_14= '}' )? otherlv_15= '}' ) )
            // InternalAorta.g:3755:2: (otherlv_0= 'TitledRequirementsSection' ( (lv_name_1_0= ruleEString ) ) otherlv_2= '{' (otherlv_3= 'title' ( (lv_title_4_0= ruleEString ) ) )? (otherlv_5= 'referencingSections' ( ( ruleQualifiedName ) ) )? otherlv_7= 'requirementsType' ( ( ruleQualifiedName ) ) (otherlv_9= 'sections' otherlv_10= '{' ( (lv_sections_11_0= ruleRequirementsSection ) ) (otherlv_12= ',' ( (lv_sections_13_0= ruleRequirementsSection ) ) )* otherlv_14= '}' )? otherlv_15= '}' )
            {
            // InternalAorta.g:3755:2: (otherlv_0= 'TitledRequirementsSection' ( (lv_name_1_0= ruleEString ) ) otherlv_2= '{' (otherlv_3= 'title' ( (lv_title_4_0= ruleEString ) ) )? (otherlv_5= 'referencingSections' ( ( ruleQualifiedName ) ) )? otherlv_7= 'requirementsType' ( ( ruleQualifiedName ) ) (otherlv_9= 'sections' otherlv_10= '{' ( (lv_sections_11_0= ruleRequirementsSection ) ) (otherlv_12= ',' ( (lv_sections_13_0= ruleRequirementsSection ) ) )* otherlv_14= '}' )? otherlv_15= '}' )
            // InternalAorta.g:3756:3: otherlv_0= 'TitledRequirementsSection' ( (lv_name_1_0= ruleEString ) ) otherlv_2= '{' (otherlv_3= 'title' ( (lv_title_4_0= ruleEString ) ) )? (otherlv_5= 'referencingSections' ( ( ruleQualifiedName ) ) )? otherlv_7= 'requirementsType' ( ( ruleQualifiedName ) ) (otherlv_9= 'sections' otherlv_10= '{' ( (lv_sections_11_0= ruleRequirementsSection ) ) (otherlv_12= ',' ( (lv_sections_13_0= ruleRequirementsSection ) ) )* otherlv_14= '}' )? otherlv_15= '}'
            {
            otherlv_0=(Token)match(input,67,FOLLOW_25); 

            			newLeafNode(otherlv_0, grammarAccess.getTitledRequirementsSectionAccess().getTitledRequirementsSectionKeyword_0());
            		
            // InternalAorta.g:3760:3: ( (lv_name_1_0= ruleEString ) )
            // InternalAorta.g:3761:4: (lv_name_1_0= ruleEString )
            {
            // InternalAorta.g:3761:4: (lv_name_1_0= ruleEString )
            // InternalAorta.g:3762:5: lv_name_1_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getTitledRequirementsSectionAccess().getNameEStringParserRuleCall_1_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_1_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getTitledRequirementsSectionRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_1_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_2=(Token)match(input,12,FOLLOW_113); 

            			newLeafNode(otherlv_2, grammarAccess.getTitledRequirementsSectionAccess().getLeftCurlyBracketKeyword_2());
            		
            // InternalAorta.g:3783:3: (otherlv_3= 'title' ( (lv_title_4_0= ruleEString ) ) )?
            int alt101=2;
            int LA101_0 = input.LA(1);

            if ( (LA101_0==68) ) {
                alt101=1;
            }
            switch (alt101) {
                case 1 :
                    // InternalAorta.g:3784:4: otherlv_3= 'title' ( (lv_title_4_0= ruleEString ) )
                    {
                    otherlv_3=(Token)match(input,68,FOLLOW_25); 

                    				newLeafNode(otherlv_3, grammarAccess.getTitledRequirementsSectionAccess().getTitleKeyword_3_0());
                    			
                    // InternalAorta.g:3788:4: ( (lv_title_4_0= ruleEString ) )
                    // InternalAorta.g:3789:5: (lv_title_4_0= ruleEString )
                    {
                    // InternalAorta.g:3789:5: (lv_title_4_0= ruleEString )
                    // InternalAorta.g:3790:6: lv_title_4_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getTitledRequirementsSectionAccess().getTitleEStringParserRuleCall_3_1_0());
                    					
                    pushFollow(FOLLOW_114);
                    lv_title_4_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getTitledRequirementsSectionRule());
                    						}
                    						set(
                    							current,
                    							"title",
                    							lv_title_4_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:3808:3: (otherlv_5= 'referencingSections' ( ( ruleQualifiedName ) ) )?
            int alt102=2;
            int LA102_0 = input.LA(1);

            if ( (LA102_0==69) ) {
                alt102=1;
            }
            switch (alt102) {
                case 1 :
                    // InternalAorta.g:3809:4: otherlv_5= 'referencingSections' ( ( ruleQualifiedName ) )
                    {
                    otherlv_5=(Token)match(input,69,FOLLOW_70); 

                    				newLeafNode(otherlv_5, grammarAccess.getTitledRequirementsSectionAccess().getReferencingSectionsKeyword_4_0());
                    			
                    // InternalAorta.g:3813:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:3814:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:3814:5: ( ruleQualifiedName )
                    // InternalAorta.g:3815:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getTitledRequirementsSectionRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getTitledRequirementsSectionAccess().getReferencingSectionsRequirementsSectionLinkWithTextCrossReference_4_1_0());
                    					
                    pushFollow(FOLLOW_115);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            otherlv_7=(Token)match(input,70,FOLLOW_70); 

            			newLeafNode(otherlv_7, grammarAccess.getTitledRequirementsSectionAccess().getRequirementsTypeKeyword_5());
            		
            // InternalAorta.g:3834:3: ( ( ruleQualifiedName ) )
            // InternalAorta.g:3835:4: ( ruleQualifiedName )
            {
            // InternalAorta.g:3835:4: ( ruleQualifiedName )
            // InternalAorta.g:3836:5: ruleQualifiedName
            {

            					if (current==null) {
            						current = createModelElement(grammarAccess.getTitledRequirementsSectionRule());
            					}
            				

            					newCompositeNode(grammarAccess.getTitledRequirementsSectionAccess().getRequirementsTypeRequirementTypeCrossReference_6_0());
            				
            pushFollow(FOLLOW_116);
            ruleQualifiedName();

            state._fsp--;


            					afterParserOrEnumRuleCall();
            				

            }


            }

            // InternalAorta.g:3850:3: (otherlv_9= 'sections' otherlv_10= '{' ( (lv_sections_11_0= ruleRequirementsSection ) ) (otherlv_12= ',' ( (lv_sections_13_0= ruleRequirementsSection ) ) )* otherlv_14= '}' )?
            int alt104=2;
            int LA104_0 = input.LA(1);

            if ( (LA104_0==71) ) {
                alt104=1;
            }
            switch (alt104) {
                case 1 :
                    // InternalAorta.g:3851:4: otherlv_9= 'sections' otherlv_10= '{' ( (lv_sections_11_0= ruleRequirementsSection ) ) (otherlv_12= ',' ( (lv_sections_13_0= ruleRequirementsSection ) ) )* otherlv_14= '}'
                    {
                    otherlv_9=(Token)match(input,71,FOLLOW_3); 

                    				newLeafNode(otherlv_9, grammarAccess.getTitledRequirementsSectionAccess().getSectionsKeyword_7_0());
                    			
                    otherlv_10=(Token)match(input,12,FOLLOW_117); 

                    				newLeafNode(otherlv_10, grammarAccess.getTitledRequirementsSectionAccess().getLeftCurlyBracketKeyword_7_1());
                    			
                    // InternalAorta.g:3859:4: ( (lv_sections_11_0= ruleRequirementsSection ) )
                    // InternalAorta.g:3860:5: (lv_sections_11_0= ruleRequirementsSection )
                    {
                    // InternalAorta.g:3860:5: (lv_sections_11_0= ruleRequirementsSection )
                    // InternalAorta.g:3861:6: lv_sections_11_0= ruleRequirementsSection
                    {

                    						newCompositeNode(grammarAccess.getTitledRequirementsSectionAccess().getSectionsRequirementsSectionParserRuleCall_7_2_0());
                    					
                    pushFollow(FOLLOW_35);
                    lv_sections_11_0=ruleRequirementsSection();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getTitledRequirementsSectionRule());
                    						}
                    						add(
                    							current,
                    							"sections",
                    							lv_sections_11_0,
                    							"org.eclipse.efbt.aorta.Aorta.RequirementsSection");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:3878:4: (otherlv_12= ',' ( (lv_sections_13_0= ruleRequirementsSection ) ) )*
                    loop103:
                    do {
                        int alt103=2;
                        int LA103_0 = input.LA(1);

                        if ( (LA103_0==31) ) {
                            alt103=1;
                        }


                        switch (alt103) {
                    	case 1 :
                    	    // InternalAorta.g:3879:5: otherlv_12= ',' ( (lv_sections_13_0= ruleRequirementsSection ) )
                    	    {
                    	    otherlv_12=(Token)match(input,31,FOLLOW_117); 

                    	    					newLeafNode(otherlv_12, grammarAccess.getTitledRequirementsSectionAccess().getCommaKeyword_7_3_0());
                    	    				
                    	    // InternalAorta.g:3883:5: ( (lv_sections_13_0= ruleRequirementsSection ) )
                    	    // InternalAorta.g:3884:6: (lv_sections_13_0= ruleRequirementsSection )
                    	    {
                    	    // InternalAorta.g:3884:6: (lv_sections_13_0= ruleRequirementsSection )
                    	    // InternalAorta.g:3885:7: lv_sections_13_0= ruleRequirementsSection
                    	    {

                    	    							newCompositeNode(grammarAccess.getTitledRequirementsSectionAccess().getSectionsRequirementsSectionParserRuleCall_7_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_35);
                    	    lv_sections_13_0=ruleRequirementsSection();

                    	    state._fsp--;


                    	    							if (current==null) {
                    	    								current = createModelElementForParent(grammarAccess.getTitledRequirementsSectionRule());
                    	    							}
                    	    							add(
                    	    								current,
                    	    								"sections",
                    	    								lv_sections_13_0,
                    	    								"org.eclipse.efbt.aorta.Aorta.RequirementsSection");
                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop103;
                        }
                    } while (true);

                    otherlv_14=(Token)match(input,23,FOLLOW_24); 

                    				newLeafNode(otherlv_14, grammarAccess.getTitledRequirementsSectionAccess().getRightCurlyBracketKeyword_7_4());
                    			

                    }
                    break;

            }

            otherlv_15=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_15, grammarAccess.getTitledRequirementsSectionAccess().getRightCurlyBracketKeyword_8());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleTitledRequirementsSection"


    // $ANTLR start "entryRuleAllowedTypes"
    // InternalAorta.g:3916:1: entryRuleAllowedTypes returns [EObject current=null] : iv_ruleAllowedTypes= ruleAllowedTypes EOF ;
    public final EObject entryRuleAllowedTypes() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleAllowedTypes = null;


        try {
            // InternalAorta.g:3916:53: (iv_ruleAllowedTypes= ruleAllowedTypes EOF )
            // InternalAorta.g:3917:2: iv_ruleAllowedTypes= ruleAllowedTypes EOF
            {
             newCompositeNode(grammarAccess.getAllowedTypesRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleAllowedTypes=ruleAllowedTypes();

            state._fsp--;

             current =iv_ruleAllowedTypes; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleAllowedTypes"


    // $ANTLR start "ruleAllowedTypes"
    // InternalAorta.g:3923:1: ruleAllowedTypes returns [EObject current=null] : ( () otherlv_1= 'AllowedTypes' otherlv_2= '{' (otherlv_3= 'allowedTypes' otherlv_4= '{' ( (lv_allowedTypes_5_0= ruleRequirementType ) ) (otherlv_6= ',' ( (lv_allowedTypes_7_0= ruleRequirementType ) ) )* otherlv_8= '}' )? otherlv_9= '}' ) ;
    public final EObject ruleAllowedTypes() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_2=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_6=null;
        Token otherlv_8=null;
        Token otherlv_9=null;
        EObject lv_allowedTypes_5_0 = null;

        EObject lv_allowedTypes_7_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:3929:2: ( ( () otherlv_1= 'AllowedTypes' otherlv_2= '{' (otherlv_3= 'allowedTypes' otherlv_4= '{' ( (lv_allowedTypes_5_0= ruleRequirementType ) ) (otherlv_6= ',' ( (lv_allowedTypes_7_0= ruleRequirementType ) ) )* otherlv_8= '}' )? otherlv_9= '}' ) )
            // InternalAorta.g:3930:2: ( () otherlv_1= 'AllowedTypes' otherlv_2= '{' (otherlv_3= 'allowedTypes' otherlv_4= '{' ( (lv_allowedTypes_5_0= ruleRequirementType ) ) (otherlv_6= ',' ( (lv_allowedTypes_7_0= ruleRequirementType ) ) )* otherlv_8= '}' )? otherlv_9= '}' )
            {
            // InternalAorta.g:3930:2: ( () otherlv_1= 'AllowedTypes' otherlv_2= '{' (otherlv_3= 'allowedTypes' otherlv_4= '{' ( (lv_allowedTypes_5_0= ruleRequirementType ) ) (otherlv_6= ',' ( (lv_allowedTypes_7_0= ruleRequirementType ) ) )* otherlv_8= '}' )? otherlv_9= '}' )
            // InternalAorta.g:3931:3: () otherlv_1= 'AllowedTypes' otherlv_2= '{' (otherlv_3= 'allowedTypes' otherlv_4= '{' ( (lv_allowedTypes_5_0= ruleRequirementType ) ) (otherlv_6= ',' ( (lv_allowedTypes_7_0= ruleRequirementType ) ) )* otherlv_8= '}' )? otherlv_9= '}'
            {
            // InternalAorta.g:3931:3: ()
            // InternalAorta.g:3932:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getAllowedTypesAccess().getAllowedTypesAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,72,FOLLOW_3); 

            			newLeafNode(otherlv_1, grammarAccess.getAllowedTypesAccess().getAllowedTypesKeyword_1());
            		
            otherlv_2=(Token)match(input,12,FOLLOW_118); 

            			newLeafNode(otherlv_2, grammarAccess.getAllowedTypesAccess().getLeftCurlyBracketKeyword_2());
            		
            // InternalAorta.g:3946:3: (otherlv_3= 'allowedTypes' otherlv_4= '{' ( (lv_allowedTypes_5_0= ruleRequirementType ) ) (otherlv_6= ',' ( (lv_allowedTypes_7_0= ruleRequirementType ) ) )* otherlv_8= '}' )?
            int alt106=2;
            int LA106_0 = input.LA(1);

            if ( (LA106_0==73) ) {
                alt106=1;
            }
            switch (alt106) {
                case 1 :
                    // InternalAorta.g:3947:4: otherlv_3= 'allowedTypes' otherlv_4= '{' ( (lv_allowedTypes_5_0= ruleRequirementType ) ) (otherlv_6= ',' ( (lv_allowedTypes_7_0= ruleRequirementType ) ) )* otherlv_8= '}'
                    {
                    otherlv_3=(Token)match(input,73,FOLLOW_3); 

                    				newLeafNode(otherlv_3, grammarAccess.getAllowedTypesAccess().getAllowedTypesKeyword_3_0());
                    			
                    otherlv_4=(Token)match(input,12,FOLLOW_119); 

                    				newLeafNode(otherlv_4, grammarAccess.getAllowedTypesAccess().getLeftCurlyBracketKeyword_3_1());
                    			
                    // InternalAorta.g:3955:4: ( (lv_allowedTypes_5_0= ruleRequirementType ) )
                    // InternalAorta.g:3956:5: (lv_allowedTypes_5_0= ruleRequirementType )
                    {
                    // InternalAorta.g:3956:5: (lv_allowedTypes_5_0= ruleRequirementType )
                    // InternalAorta.g:3957:6: lv_allowedTypes_5_0= ruleRequirementType
                    {

                    						newCompositeNode(grammarAccess.getAllowedTypesAccess().getAllowedTypesRequirementTypeParserRuleCall_3_2_0());
                    					
                    pushFollow(FOLLOW_35);
                    lv_allowedTypes_5_0=ruleRequirementType();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getAllowedTypesRule());
                    						}
                    						add(
                    							current,
                    							"allowedTypes",
                    							lv_allowedTypes_5_0,
                    							"org.eclipse.efbt.aorta.Aorta.RequirementType");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:3974:4: (otherlv_6= ',' ( (lv_allowedTypes_7_0= ruleRequirementType ) ) )*
                    loop105:
                    do {
                        int alt105=2;
                        int LA105_0 = input.LA(1);

                        if ( (LA105_0==31) ) {
                            alt105=1;
                        }


                        switch (alt105) {
                    	case 1 :
                    	    // InternalAorta.g:3975:5: otherlv_6= ',' ( (lv_allowedTypes_7_0= ruleRequirementType ) )
                    	    {
                    	    otherlv_6=(Token)match(input,31,FOLLOW_119); 

                    	    					newLeafNode(otherlv_6, grammarAccess.getAllowedTypesAccess().getCommaKeyword_3_3_0());
                    	    				
                    	    // InternalAorta.g:3979:5: ( (lv_allowedTypes_7_0= ruleRequirementType ) )
                    	    // InternalAorta.g:3980:6: (lv_allowedTypes_7_0= ruleRequirementType )
                    	    {
                    	    // InternalAorta.g:3980:6: (lv_allowedTypes_7_0= ruleRequirementType )
                    	    // InternalAorta.g:3981:7: lv_allowedTypes_7_0= ruleRequirementType
                    	    {

                    	    							newCompositeNode(grammarAccess.getAllowedTypesAccess().getAllowedTypesRequirementTypeParserRuleCall_3_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_35);
                    	    lv_allowedTypes_7_0=ruleRequirementType();

                    	    state._fsp--;


                    	    							if (current==null) {
                    	    								current = createModelElementForParent(grammarAccess.getAllowedTypesRule());
                    	    							}
                    	    							add(
                    	    								current,
                    	    								"allowedTypes",
                    	    								lv_allowedTypes_7_0,
                    	    								"org.eclipse.efbt.aorta.Aorta.RequirementType");
                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop105;
                        }
                    } while (true);

                    otherlv_8=(Token)match(input,23,FOLLOW_24); 

                    				newLeafNode(otherlv_8, grammarAccess.getAllowedTypesAccess().getRightCurlyBracketKeyword_3_4());
                    			

                    }
                    break;

            }

            otherlv_9=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_9, grammarAccess.getAllowedTypesAccess().getRightCurlyBracketKeyword_4());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleAllowedTypes"


    // $ANTLR start "entryRuleRequirementsSectionLinkWithText"
    // InternalAorta.g:4012:1: entryRuleRequirementsSectionLinkWithText returns [EObject current=null] : iv_ruleRequirementsSectionLinkWithText= ruleRequirementsSectionLinkWithText EOF ;
    public final EObject entryRuleRequirementsSectionLinkWithText() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleRequirementsSectionLinkWithText = null;


        try {
            // InternalAorta.g:4012:72: (iv_ruleRequirementsSectionLinkWithText= ruleRequirementsSectionLinkWithText EOF )
            // InternalAorta.g:4013:2: iv_ruleRequirementsSectionLinkWithText= ruleRequirementsSectionLinkWithText EOF
            {
             newCompositeNode(grammarAccess.getRequirementsSectionLinkWithTextRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleRequirementsSectionLinkWithText=ruleRequirementsSectionLinkWithText();

            state._fsp--;

             current =iv_ruleRequirementsSectionLinkWithText; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleRequirementsSectionLinkWithText"


    // $ANTLR start "ruleRequirementsSectionLinkWithText"
    // InternalAorta.g:4019:1: ruleRequirementsSectionLinkWithText returns [EObject current=null] : ( () otherlv_1= 'RequirementsSectionLinkWithText' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'linkText' ( (lv_linkText_5_0= ruleEString ) ) )? (otherlv_6= 'subsection' ( (lv_subsection_7_0= ruleEString ) ) )? (otherlv_8= 'linkedRuleSection' ( ( ruleQualifiedName ) ) )? otherlv_10= '}' ) ;
    public final EObject ruleRequirementsSectionLinkWithText() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_6=null;
        Token otherlv_8=null;
        Token otherlv_10=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;

        AntlrDatatypeRuleToken lv_linkText_5_0 = null;

        AntlrDatatypeRuleToken lv_subsection_7_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:4025:2: ( ( () otherlv_1= 'RequirementsSectionLinkWithText' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'linkText' ( (lv_linkText_5_0= ruleEString ) ) )? (otherlv_6= 'subsection' ( (lv_subsection_7_0= ruleEString ) ) )? (otherlv_8= 'linkedRuleSection' ( ( ruleQualifiedName ) ) )? otherlv_10= '}' ) )
            // InternalAorta.g:4026:2: ( () otherlv_1= 'RequirementsSectionLinkWithText' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'linkText' ( (lv_linkText_5_0= ruleEString ) ) )? (otherlv_6= 'subsection' ( (lv_subsection_7_0= ruleEString ) ) )? (otherlv_8= 'linkedRuleSection' ( ( ruleQualifiedName ) ) )? otherlv_10= '}' )
            {
            // InternalAorta.g:4026:2: ( () otherlv_1= 'RequirementsSectionLinkWithText' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'linkText' ( (lv_linkText_5_0= ruleEString ) ) )? (otherlv_6= 'subsection' ( (lv_subsection_7_0= ruleEString ) ) )? (otherlv_8= 'linkedRuleSection' ( ( ruleQualifiedName ) ) )? otherlv_10= '}' )
            // InternalAorta.g:4027:3: () otherlv_1= 'RequirementsSectionLinkWithText' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'linkText' ( (lv_linkText_5_0= ruleEString ) ) )? (otherlv_6= 'subsection' ( (lv_subsection_7_0= ruleEString ) ) )? (otherlv_8= 'linkedRuleSection' ( ( ruleQualifiedName ) ) )? otherlv_10= '}'
            {
            // InternalAorta.g:4027:3: ()
            // InternalAorta.g:4028:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getRequirementsSectionLinkWithTextAccess().getRequirementsSectionLinkWithTextAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,74,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getRequirementsSectionLinkWithTextAccess().getRequirementsSectionLinkWithTextKeyword_1());
            		
            // InternalAorta.g:4038:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:4039:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:4039:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:4040:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getRequirementsSectionLinkWithTextAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getRequirementsSectionLinkWithTextRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_120); 

            			newLeafNode(otherlv_3, grammarAccess.getRequirementsSectionLinkWithTextAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:4061:3: (otherlv_4= 'linkText' ( (lv_linkText_5_0= ruleEString ) ) )?
            int alt107=2;
            int LA107_0 = input.LA(1);

            if ( (LA107_0==75) ) {
                alt107=1;
            }
            switch (alt107) {
                case 1 :
                    // InternalAorta.g:4062:4: otherlv_4= 'linkText' ( (lv_linkText_5_0= ruleEString ) )
                    {
                    otherlv_4=(Token)match(input,75,FOLLOW_25); 

                    				newLeafNode(otherlv_4, grammarAccess.getRequirementsSectionLinkWithTextAccess().getLinkTextKeyword_4_0());
                    			
                    // InternalAorta.g:4066:4: ( (lv_linkText_5_0= ruleEString ) )
                    // InternalAorta.g:4067:5: (lv_linkText_5_0= ruleEString )
                    {
                    // InternalAorta.g:4067:5: (lv_linkText_5_0= ruleEString )
                    // InternalAorta.g:4068:6: lv_linkText_5_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getRequirementsSectionLinkWithTextAccess().getLinkTextEStringParserRuleCall_4_1_0());
                    					
                    pushFollow(FOLLOW_121);
                    lv_linkText_5_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getRequirementsSectionLinkWithTextRule());
                    						}
                    						set(
                    							current,
                    							"linkText",
                    							lv_linkText_5_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:4086:3: (otherlv_6= 'subsection' ( (lv_subsection_7_0= ruleEString ) ) )?
            int alt108=2;
            int LA108_0 = input.LA(1);

            if ( (LA108_0==76) ) {
                alt108=1;
            }
            switch (alt108) {
                case 1 :
                    // InternalAorta.g:4087:4: otherlv_6= 'subsection' ( (lv_subsection_7_0= ruleEString ) )
                    {
                    otherlv_6=(Token)match(input,76,FOLLOW_25); 

                    				newLeafNode(otherlv_6, grammarAccess.getRequirementsSectionLinkWithTextAccess().getSubsectionKeyword_5_0());
                    			
                    // InternalAorta.g:4091:4: ( (lv_subsection_7_0= ruleEString ) )
                    // InternalAorta.g:4092:5: (lv_subsection_7_0= ruleEString )
                    {
                    // InternalAorta.g:4092:5: (lv_subsection_7_0= ruleEString )
                    // InternalAorta.g:4093:6: lv_subsection_7_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getRequirementsSectionLinkWithTextAccess().getSubsectionEStringParserRuleCall_5_1_0());
                    					
                    pushFollow(FOLLOW_122);
                    lv_subsection_7_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getRequirementsSectionLinkWithTextRule());
                    						}
                    						set(
                    							current,
                    							"subsection",
                    							lv_subsection_7_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:4111:3: (otherlv_8= 'linkedRuleSection' ( ( ruleQualifiedName ) ) )?
            int alt109=2;
            int LA109_0 = input.LA(1);

            if ( (LA109_0==77) ) {
                alt109=1;
            }
            switch (alt109) {
                case 1 :
                    // InternalAorta.g:4112:4: otherlv_8= 'linkedRuleSection' ( ( ruleQualifiedName ) )
                    {
                    otherlv_8=(Token)match(input,77,FOLLOW_70); 

                    				newLeafNode(otherlv_8, grammarAccess.getRequirementsSectionLinkWithTextAccess().getLinkedRuleSectionKeyword_6_0());
                    			
                    // InternalAorta.g:4116:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:4117:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:4117:5: ( ruleQualifiedName )
                    // InternalAorta.g:4118:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getRequirementsSectionLinkWithTextRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getRequirementsSectionLinkWithTextAccess().getLinkedRuleSectionTitledRequirementsSectionCrossReference_6_1_0());
                    					
                    pushFollow(FOLLOW_24);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            otherlv_10=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_10, grammarAccess.getRequirementsSectionLinkWithTextAccess().getRightCurlyBracketKeyword_7());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleRequirementsSectionLinkWithText"


    // $ANTLR start "entryRuleRequirementType"
    // InternalAorta.g:4141:1: entryRuleRequirementType returns [EObject current=null] : iv_ruleRequirementType= ruleRequirementType EOF ;
    public final EObject entryRuleRequirementType() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleRequirementType = null;


        try {
            // InternalAorta.g:4141:56: (iv_ruleRequirementType= ruleRequirementType EOF )
            // InternalAorta.g:4142:2: iv_ruleRequirementType= ruleRequirementType EOF
            {
             newCompositeNode(grammarAccess.getRequirementTypeRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleRequirementType=ruleRequirementType();

            state._fsp--;

             current =iv_ruleRequirementType; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleRequirementType"


    // $ANTLR start "ruleRequirementType"
    // InternalAorta.g:4148:1: ruleRequirementType returns [EObject current=null] : ( () otherlv_1= 'RequirementType' ( (lv_name_2_0= ruleEString ) ) ) ;
    public final EObject ruleRequirementType() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:4154:2: ( ( () otherlv_1= 'RequirementType' ( (lv_name_2_0= ruleEString ) ) ) )
            // InternalAorta.g:4155:2: ( () otherlv_1= 'RequirementType' ( (lv_name_2_0= ruleEString ) ) )
            {
            // InternalAorta.g:4155:2: ( () otherlv_1= 'RequirementType' ( (lv_name_2_0= ruleEString ) ) )
            // InternalAorta.g:4156:3: () otherlv_1= 'RequirementType' ( (lv_name_2_0= ruleEString ) )
            {
            // InternalAorta.g:4156:3: ()
            // InternalAorta.g:4157:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getRequirementTypeAccess().getRequirementTypeAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,78,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getRequirementTypeAccess().getRequirementTypeKeyword_1());
            		
            // InternalAorta.g:4167:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:4168:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:4168:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:4169:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getRequirementTypeAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_2);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getRequirementTypeRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }


            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleRequirementType"


    // $ANTLR start "entryRuleRequirementsSectionImage"
    // InternalAorta.g:4190:1: entryRuleRequirementsSectionImage returns [EObject current=null] : iv_ruleRequirementsSectionImage= ruleRequirementsSectionImage EOF ;
    public final EObject entryRuleRequirementsSectionImage() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleRequirementsSectionImage = null;


        try {
            // InternalAorta.g:4190:65: (iv_ruleRequirementsSectionImage= ruleRequirementsSectionImage EOF )
            // InternalAorta.g:4191:2: iv_ruleRequirementsSectionImage= ruleRequirementsSectionImage EOF
            {
             newCompositeNode(grammarAccess.getRequirementsSectionImageRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleRequirementsSectionImage=ruleRequirementsSectionImage();

            state._fsp--;

             current =iv_ruleRequirementsSectionImage; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleRequirementsSectionImage"


    // $ANTLR start "ruleRequirementsSectionImage"
    // InternalAorta.g:4197:1: ruleRequirementsSectionImage returns [EObject current=null] : ( () otherlv_1= 'RequirementsSectionImage' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'style' ( (lv_style_5_0= ruleEString ) ) )? (otherlv_6= 'uri' ( (lv_uri_7_0= ruleEString ) ) )? otherlv_8= '}' ) ;
    public final EObject ruleRequirementsSectionImage() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_6=null;
        Token otherlv_8=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;

        AntlrDatatypeRuleToken lv_style_5_0 = null;

        AntlrDatatypeRuleToken lv_uri_7_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:4203:2: ( ( () otherlv_1= 'RequirementsSectionImage' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'style' ( (lv_style_5_0= ruleEString ) ) )? (otherlv_6= 'uri' ( (lv_uri_7_0= ruleEString ) ) )? otherlv_8= '}' ) )
            // InternalAorta.g:4204:2: ( () otherlv_1= 'RequirementsSectionImage' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'style' ( (lv_style_5_0= ruleEString ) ) )? (otherlv_6= 'uri' ( (lv_uri_7_0= ruleEString ) ) )? otherlv_8= '}' )
            {
            // InternalAorta.g:4204:2: ( () otherlv_1= 'RequirementsSectionImage' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'style' ( (lv_style_5_0= ruleEString ) ) )? (otherlv_6= 'uri' ( (lv_uri_7_0= ruleEString ) ) )? otherlv_8= '}' )
            // InternalAorta.g:4205:3: () otherlv_1= 'RequirementsSectionImage' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'style' ( (lv_style_5_0= ruleEString ) ) )? (otherlv_6= 'uri' ( (lv_uri_7_0= ruleEString ) ) )? otherlv_8= '}'
            {
            // InternalAorta.g:4205:3: ()
            // InternalAorta.g:4206:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getRequirementsSectionImageAccess().getRequirementsSectionImageAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,79,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getRequirementsSectionImageAccess().getRequirementsSectionImageKeyword_1());
            		
            // InternalAorta.g:4216:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:4217:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:4217:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:4218:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getRequirementsSectionImageAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getRequirementsSectionImageRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_123); 

            			newLeafNode(otherlv_3, grammarAccess.getRequirementsSectionImageAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:4239:3: (otherlv_4= 'style' ( (lv_style_5_0= ruleEString ) ) )?
            int alt110=2;
            int LA110_0 = input.LA(1);

            if ( (LA110_0==80) ) {
                alt110=1;
            }
            switch (alt110) {
                case 1 :
                    // InternalAorta.g:4240:4: otherlv_4= 'style' ( (lv_style_5_0= ruleEString ) )
                    {
                    otherlv_4=(Token)match(input,80,FOLLOW_25); 

                    				newLeafNode(otherlv_4, grammarAccess.getRequirementsSectionImageAccess().getStyleKeyword_4_0());
                    			
                    // InternalAorta.g:4244:4: ( (lv_style_5_0= ruleEString ) )
                    // InternalAorta.g:4245:5: (lv_style_5_0= ruleEString )
                    {
                    // InternalAorta.g:4245:5: (lv_style_5_0= ruleEString )
                    // InternalAorta.g:4246:6: lv_style_5_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getRequirementsSectionImageAccess().getStyleEStringParserRuleCall_4_1_0());
                    					
                    pushFollow(FOLLOW_124);
                    lv_style_5_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getRequirementsSectionImageRule());
                    						}
                    						set(
                    							current,
                    							"style",
                    							lv_style_5_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:4264:3: (otherlv_6= 'uri' ( (lv_uri_7_0= ruleEString ) ) )?
            int alt111=2;
            int LA111_0 = input.LA(1);

            if ( (LA111_0==81) ) {
                alt111=1;
            }
            switch (alt111) {
                case 1 :
                    // InternalAorta.g:4265:4: otherlv_6= 'uri' ( (lv_uri_7_0= ruleEString ) )
                    {
                    otherlv_6=(Token)match(input,81,FOLLOW_25); 

                    				newLeafNode(otherlv_6, grammarAccess.getRequirementsSectionImageAccess().getUriKeyword_5_0());
                    			
                    // InternalAorta.g:4269:4: ( (lv_uri_7_0= ruleEString ) )
                    // InternalAorta.g:4270:5: (lv_uri_7_0= ruleEString )
                    {
                    // InternalAorta.g:4270:5: (lv_uri_7_0= ruleEString )
                    // InternalAorta.g:4271:6: lv_uri_7_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getRequirementsSectionImageAccess().getUriEStringParserRuleCall_5_1_0());
                    					
                    pushFollow(FOLLOW_24);
                    lv_uri_7_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getRequirementsSectionImageRule());
                    						}
                    						set(
                    							current,
                    							"uri",
                    							lv_uri_7_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            otherlv_8=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_8, grammarAccess.getRequirementsSectionImageAccess().getRightCurlyBracketKeyword_6());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleRequirementsSectionImage"


    // $ANTLR start "entryRuleRequirementsSectionText"
    // InternalAorta.g:4297:1: entryRuleRequirementsSectionText returns [EObject current=null] : iv_ruleRequirementsSectionText= ruleRequirementsSectionText EOF ;
    public final EObject entryRuleRequirementsSectionText() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleRequirementsSectionText = null;


        try {
            // InternalAorta.g:4297:64: (iv_ruleRequirementsSectionText= ruleRequirementsSectionText EOF )
            // InternalAorta.g:4298:2: iv_ruleRequirementsSectionText= ruleRequirementsSectionText EOF
            {
             newCompositeNode(grammarAccess.getRequirementsSectionTextRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleRequirementsSectionText=ruleRequirementsSectionText();

            state._fsp--;

             current =iv_ruleRequirementsSectionText; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleRequirementsSectionText"


    // $ANTLR start "ruleRequirementsSectionText"
    // InternalAorta.g:4304:1: ruleRequirementsSectionText returns [EObject current=null] : ( () otherlv_1= 'RequirementsSectionText' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'text' ( (lv_text_5_0= ruleEString ) ) )? otherlv_6= '}' ) ;
    public final EObject ruleRequirementsSectionText() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_6=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;

        AntlrDatatypeRuleToken lv_text_5_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:4310:2: ( ( () otherlv_1= 'RequirementsSectionText' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'text' ( (lv_text_5_0= ruleEString ) ) )? otherlv_6= '}' ) )
            // InternalAorta.g:4311:2: ( () otherlv_1= 'RequirementsSectionText' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'text' ( (lv_text_5_0= ruleEString ) ) )? otherlv_6= '}' )
            {
            // InternalAorta.g:4311:2: ( () otherlv_1= 'RequirementsSectionText' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'text' ( (lv_text_5_0= ruleEString ) ) )? otherlv_6= '}' )
            // InternalAorta.g:4312:3: () otherlv_1= 'RequirementsSectionText' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'text' ( (lv_text_5_0= ruleEString ) ) )? otherlv_6= '}'
            {
            // InternalAorta.g:4312:3: ()
            // InternalAorta.g:4313:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getRequirementsSectionTextAccess().getRequirementsSectionTextAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,82,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getRequirementsSectionTextAccess().getRequirementsSectionTextKeyword_1());
            		
            // InternalAorta.g:4323:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:4324:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:4324:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:4325:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getRequirementsSectionTextAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getRequirementsSectionTextRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_125); 

            			newLeafNode(otherlv_3, grammarAccess.getRequirementsSectionTextAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:4346:3: (otherlv_4= 'text' ( (lv_text_5_0= ruleEString ) ) )?
            int alt112=2;
            int LA112_0 = input.LA(1);

            if ( (LA112_0==83) ) {
                alt112=1;
            }
            switch (alt112) {
                case 1 :
                    // InternalAorta.g:4347:4: otherlv_4= 'text' ( (lv_text_5_0= ruleEString ) )
                    {
                    otherlv_4=(Token)match(input,83,FOLLOW_25); 

                    				newLeafNode(otherlv_4, grammarAccess.getRequirementsSectionTextAccess().getTextKeyword_4_0());
                    			
                    // InternalAorta.g:4351:4: ( (lv_text_5_0= ruleEString ) )
                    // InternalAorta.g:4352:5: (lv_text_5_0= ruleEString )
                    {
                    // InternalAorta.g:4352:5: (lv_text_5_0= ruleEString )
                    // InternalAorta.g:4353:6: lv_text_5_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getRequirementsSectionTextAccess().getTextEStringParserRuleCall_4_1_0());
                    					
                    pushFollow(FOLLOW_24);
                    lv_text_5_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getRequirementsSectionTextRule());
                    						}
                    						set(
                    							current,
                    							"text",
                    							lv_text_5_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            otherlv_6=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_6, grammarAccess.getRequirementsSectionTextAccess().getRightCurlyBracketKeyword_5());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleRequirementsSectionText"


    // $ANTLR start "entryRuleColumnStructuredEntity"
    // InternalAorta.g:4379:1: entryRuleColumnStructuredEntity returns [EObject current=null] : iv_ruleColumnStructuredEntity= ruleColumnStructuredEntity EOF ;
    public final EObject entryRuleColumnStructuredEntity() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleColumnStructuredEntity = null;


        try {
            // InternalAorta.g:4379:63: (iv_ruleColumnStructuredEntity= ruleColumnStructuredEntity EOF )
            // InternalAorta.g:4380:2: iv_ruleColumnStructuredEntity= ruleColumnStructuredEntity EOF
            {
             newCompositeNode(grammarAccess.getColumnStructuredEntityRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleColumnStructuredEntity=ruleColumnStructuredEntity();

            state._fsp--;

             current =iv_ruleColumnStructuredEntity; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleColumnStructuredEntity"


    // $ANTLR start "ruleColumnStructuredEntity"
    // InternalAorta.g:4386:1: ruleColumnStructuredEntity returns [EObject current=null] : ( () otherlv_1= 'ColumnStructuredEntity' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'column' otherlv_5= '{' ( (lv_column_6_0= ruleColumn ) ) (otherlv_7= ',' ( (lv_column_8_0= ruleColumn ) ) )* otherlv_9= '}' )? otherlv_10= '}' ) ;
    public final EObject ruleColumnStructuredEntity() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_5=null;
        Token otherlv_7=null;
        Token otherlv_9=null;
        Token otherlv_10=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;

        EObject lv_column_6_0 = null;

        EObject lv_column_8_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:4392:2: ( ( () otherlv_1= 'ColumnStructuredEntity' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'column' otherlv_5= '{' ( (lv_column_6_0= ruleColumn ) ) (otherlv_7= ',' ( (lv_column_8_0= ruleColumn ) ) )* otherlv_9= '}' )? otherlv_10= '}' ) )
            // InternalAorta.g:4393:2: ( () otherlv_1= 'ColumnStructuredEntity' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'column' otherlv_5= '{' ( (lv_column_6_0= ruleColumn ) ) (otherlv_7= ',' ( (lv_column_8_0= ruleColumn ) ) )* otherlv_9= '}' )? otherlv_10= '}' )
            {
            // InternalAorta.g:4393:2: ( () otherlv_1= 'ColumnStructuredEntity' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'column' otherlv_5= '{' ( (lv_column_6_0= ruleColumn ) ) (otherlv_7= ',' ( (lv_column_8_0= ruleColumn ) ) )* otherlv_9= '}' )? otherlv_10= '}' )
            // InternalAorta.g:4394:3: () otherlv_1= 'ColumnStructuredEntity' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'column' otherlv_5= '{' ( (lv_column_6_0= ruleColumn ) ) (otherlv_7= ',' ( (lv_column_8_0= ruleColumn ) ) )* otherlv_9= '}' )? otherlv_10= '}'
            {
            // InternalAorta.g:4394:3: ()
            // InternalAorta.g:4395:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getColumnStructuredEntityAccess().getColumnStructuredEntityAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,84,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getColumnStructuredEntityAccess().getColumnStructuredEntityKeyword_1());
            		
            // InternalAorta.g:4405:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:4406:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:4406:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:4407:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getColumnStructuredEntityAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getColumnStructuredEntityRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_126); 

            			newLeafNode(otherlv_3, grammarAccess.getColumnStructuredEntityAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:4428:3: (otherlv_4= 'column' otherlv_5= '{' ( (lv_column_6_0= ruleColumn ) ) (otherlv_7= ',' ( (lv_column_8_0= ruleColumn ) ) )* otherlv_9= '}' )?
            int alt114=2;
            int LA114_0 = input.LA(1);

            if ( (LA114_0==85) ) {
                alt114=1;
            }
            switch (alt114) {
                case 1 :
                    // InternalAorta.g:4429:4: otherlv_4= 'column' otherlv_5= '{' ( (lv_column_6_0= ruleColumn ) ) (otherlv_7= ',' ( (lv_column_8_0= ruleColumn ) ) )* otherlv_9= '}'
                    {
                    otherlv_4=(Token)match(input,85,FOLLOW_3); 

                    				newLeafNode(otherlv_4, grammarAccess.getColumnStructuredEntityAccess().getColumnKeyword_4_0());
                    			
                    otherlv_5=(Token)match(input,12,FOLLOW_127); 

                    				newLeafNode(otherlv_5, grammarAccess.getColumnStructuredEntityAccess().getLeftCurlyBracketKeyword_4_1());
                    			
                    // InternalAorta.g:4437:4: ( (lv_column_6_0= ruleColumn ) )
                    // InternalAorta.g:4438:5: (lv_column_6_0= ruleColumn )
                    {
                    // InternalAorta.g:4438:5: (lv_column_6_0= ruleColumn )
                    // InternalAorta.g:4439:6: lv_column_6_0= ruleColumn
                    {

                    						newCompositeNode(grammarAccess.getColumnStructuredEntityAccess().getColumnColumnParserRuleCall_4_2_0());
                    					
                    pushFollow(FOLLOW_35);
                    lv_column_6_0=ruleColumn();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getColumnStructuredEntityRule());
                    						}
                    						add(
                    							current,
                    							"column",
                    							lv_column_6_0,
                    							"org.eclipse.efbt.aorta.Aorta.Column");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:4456:4: (otherlv_7= ',' ( (lv_column_8_0= ruleColumn ) ) )*
                    loop113:
                    do {
                        int alt113=2;
                        int LA113_0 = input.LA(1);

                        if ( (LA113_0==31) ) {
                            alt113=1;
                        }


                        switch (alt113) {
                    	case 1 :
                    	    // InternalAorta.g:4457:5: otherlv_7= ',' ( (lv_column_8_0= ruleColumn ) )
                    	    {
                    	    otherlv_7=(Token)match(input,31,FOLLOW_127); 

                    	    					newLeafNode(otherlv_7, grammarAccess.getColumnStructuredEntityAccess().getCommaKeyword_4_3_0());
                    	    				
                    	    // InternalAorta.g:4461:5: ( (lv_column_8_0= ruleColumn ) )
                    	    // InternalAorta.g:4462:6: (lv_column_8_0= ruleColumn )
                    	    {
                    	    // InternalAorta.g:4462:6: (lv_column_8_0= ruleColumn )
                    	    // InternalAorta.g:4463:7: lv_column_8_0= ruleColumn
                    	    {

                    	    							newCompositeNode(grammarAccess.getColumnStructuredEntityAccess().getColumnColumnParserRuleCall_4_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_35);
                    	    lv_column_8_0=ruleColumn();

                    	    state._fsp--;


                    	    							if (current==null) {
                    	    								current = createModelElementForParent(grammarAccess.getColumnStructuredEntityRule());
                    	    							}
                    	    							add(
                    	    								current,
                    	    								"column",
                    	    								lv_column_8_0,
                    	    								"org.eclipse.efbt.aorta.Aorta.Column");
                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop113;
                        }
                    } while (true);

                    otherlv_9=(Token)match(input,23,FOLLOW_24); 

                    				newLeafNode(otherlv_9, grammarAccess.getColumnStructuredEntityAccess().getRightCurlyBracketKeyword_4_4());
                    			

                    }
                    break;

            }

            otherlv_10=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_10, grammarAccess.getColumnStructuredEntityAccess().getRightCurlyBracketKeyword_5());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleColumnStructuredEntity"


    // $ANTLR start "entryRuleColumn"
    // InternalAorta.g:4494:1: entryRuleColumn returns [EObject current=null] : iv_ruleColumn= ruleColumn EOF ;
    public final EObject entryRuleColumn() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleColumn = null;


        try {
            // InternalAorta.g:4494:47: (iv_ruleColumn= ruleColumn EOF )
            // InternalAorta.g:4495:2: iv_ruleColumn= ruleColumn EOF
            {
             newCompositeNode(grammarAccess.getColumnRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleColumn=ruleColumn();

            state._fsp--;

             current =iv_ruleColumn; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleColumn"


    // $ANTLR start "ruleColumn"
    // InternalAorta.g:4501:1: ruleColumn returns [EObject current=null] : ( () ( (lv_isIdentifier_1_0= 'isIdentifier' ) )? ( (lv_isMandatory_2_0= 'isMandatory' ) )? otherlv_3= 'Column' ( (lv_name_4_0= ruleEString ) ) otherlv_5= '{' (otherlv_6= 'domain' ( ( ruleQualifiedName ) ) )? otherlv_8= '}' ) ;
    public final EObject ruleColumn() throws RecognitionException {
        EObject current = null;

        Token lv_isIdentifier_1_0=null;
        Token lv_isMandatory_2_0=null;
        Token otherlv_3=null;
        Token otherlv_5=null;
        Token otherlv_6=null;
        Token otherlv_8=null;
        AntlrDatatypeRuleToken lv_name_4_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:4507:2: ( ( () ( (lv_isIdentifier_1_0= 'isIdentifier' ) )? ( (lv_isMandatory_2_0= 'isMandatory' ) )? otherlv_3= 'Column' ( (lv_name_4_0= ruleEString ) ) otherlv_5= '{' (otherlv_6= 'domain' ( ( ruleQualifiedName ) ) )? otherlv_8= '}' ) )
            // InternalAorta.g:4508:2: ( () ( (lv_isIdentifier_1_0= 'isIdentifier' ) )? ( (lv_isMandatory_2_0= 'isMandatory' ) )? otherlv_3= 'Column' ( (lv_name_4_0= ruleEString ) ) otherlv_5= '{' (otherlv_6= 'domain' ( ( ruleQualifiedName ) ) )? otherlv_8= '}' )
            {
            // InternalAorta.g:4508:2: ( () ( (lv_isIdentifier_1_0= 'isIdentifier' ) )? ( (lv_isMandatory_2_0= 'isMandatory' ) )? otherlv_3= 'Column' ( (lv_name_4_0= ruleEString ) ) otherlv_5= '{' (otherlv_6= 'domain' ( ( ruleQualifiedName ) ) )? otherlv_8= '}' )
            // InternalAorta.g:4509:3: () ( (lv_isIdentifier_1_0= 'isIdentifier' ) )? ( (lv_isMandatory_2_0= 'isMandatory' ) )? otherlv_3= 'Column' ( (lv_name_4_0= ruleEString ) ) otherlv_5= '{' (otherlv_6= 'domain' ( ( ruleQualifiedName ) ) )? otherlv_8= '}'
            {
            // InternalAorta.g:4509:3: ()
            // InternalAorta.g:4510:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getColumnAccess().getColumnAction_0(),
            					current);
            			

            }

            // InternalAorta.g:4516:3: ( (lv_isIdentifier_1_0= 'isIdentifier' ) )?
            int alt115=2;
            int LA115_0 = input.LA(1);

            if ( (LA115_0==86) ) {
                alt115=1;
            }
            switch (alt115) {
                case 1 :
                    // InternalAorta.g:4517:4: (lv_isIdentifier_1_0= 'isIdentifier' )
                    {
                    // InternalAorta.g:4517:4: (lv_isIdentifier_1_0= 'isIdentifier' )
                    // InternalAorta.g:4518:5: lv_isIdentifier_1_0= 'isIdentifier'
                    {
                    lv_isIdentifier_1_0=(Token)match(input,86,FOLLOW_128); 

                    					newLeafNode(lv_isIdentifier_1_0, grammarAccess.getColumnAccess().getIsIdentifierIsIdentifierKeyword_1_0());
                    				

                    					if (current==null) {
                    						current = createModelElement(grammarAccess.getColumnRule());
                    					}
                    					setWithLastConsumed(current, "isIdentifier", true, "isIdentifier");
                    				

                    }


                    }
                    break;

            }

            // InternalAorta.g:4530:3: ( (lv_isMandatory_2_0= 'isMandatory' ) )?
            int alt116=2;
            int LA116_0 = input.LA(1);

            if ( (LA116_0==87) ) {
                alt116=1;
            }
            switch (alt116) {
                case 1 :
                    // InternalAorta.g:4531:4: (lv_isMandatory_2_0= 'isMandatory' )
                    {
                    // InternalAorta.g:4531:4: (lv_isMandatory_2_0= 'isMandatory' )
                    // InternalAorta.g:4532:5: lv_isMandatory_2_0= 'isMandatory'
                    {
                    lv_isMandatory_2_0=(Token)match(input,87,FOLLOW_129); 

                    					newLeafNode(lv_isMandatory_2_0, grammarAccess.getColumnAccess().getIsMandatoryIsMandatoryKeyword_2_0());
                    				

                    					if (current==null) {
                    						current = createModelElement(grammarAccess.getColumnRule());
                    					}
                    					setWithLastConsumed(current, "isMandatory", true, "isMandatory");
                    				

                    }


                    }
                    break;

            }

            otherlv_3=(Token)match(input,88,FOLLOW_25); 

            			newLeafNode(otherlv_3, grammarAccess.getColumnAccess().getColumnKeyword_3());
            		
            // InternalAorta.g:4548:3: ( (lv_name_4_0= ruleEString ) )
            // InternalAorta.g:4549:4: (lv_name_4_0= ruleEString )
            {
            // InternalAorta.g:4549:4: (lv_name_4_0= ruleEString )
            // InternalAorta.g:4550:5: lv_name_4_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getColumnAccess().getNameEStringParserRuleCall_4_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_4_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getColumnRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_4_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_5=(Token)match(input,12,FOLLOW_130); 

            			newLeafNode(otherlv_5, grammarAccess.getColumnAccess().getLeftCurlyBracketKeyword_5());
            		
            // InternalAorta.g:4571:3: (otherlv_6= 'domain' ( ( ruleQualifiedName ) ) )?
            int alt117=2;
            int LA117_0 = input.LA(1);

            if ( (LA117_0==89) ) {
                alt117=1;
            }
            switch (alt117) {
                case 1 :
                    // InternalAorta.g:4572:4: otherlv_6= 'domain' ( ( ruleQualifiedName ) )
                    {
                    otherlv_6=(Token)match(input,89,FOLLOW_70); 

                    				newLeafNode(otherlv_6, grammarAccess.getColumnAccess().getDomainKeyword_6_0());
                    			
                    // InternalAorta.g:4576:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:4577:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:4577:5: ( ruleQualifiedName )
                    // InternalAorta.g:4578:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getColumnRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getColumnAccess().getDomainColumnDomainCrossReference_6_1_0());
                    					
                    pushFollow(FOLLOW_24);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            otherlv_8=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_8, grammarAccess.getColumnAccess().getRightCurlyBracketKeyword_7());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleColumn"


    // $ANTLR start "entryRuleReportGroup"
    // InternalAorta.g:4601:1: entryRuleReportGroup returns [EObject current=null] : iv_ruleReportGroup= ruleReportGroup EOF ;
    public final EObject entryRuleReportGroup() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleReportGroup = null;


        try {
            // InternalAorta.g:4601:52: (iv_ruleReportGroup= ruleReportGroup EOF )
            // InternalAorta.g:4602:2: iv_ruleReportGroup= ruleReportGroup EOF
            {
             newCompositeNode(grammarAccess.getReportGroupRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleReportGroup=ruleReportGroup();

            state._fsp--;

             current =iv_ruleReportGroup; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleReportGroup"


    // $ANTLR start "ruleReportGroup"
    // InternalAorta.g:4608:1: ruleReportGroup returns [EObject current=null] : ( () otherlv_1= 'ReportGroup' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'regReports' otherlv_5= '{' ( (lv_regReports_6_0= ruleRegReport ) ) (otherlv_7= ',' ( (lv_regReports_8_0= ruleRegReport ) ) )* otherlv_9= '}' )? otherlv_10= '}' ) ;
    public final EObject ruleReportGroup() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_5=null;
        Token otherlv_7=null;
        Token otherlv_9=null;
        Token otherlv_10=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;

        EObject lv_regReports_6_0 = null;

        EObject lv_regReports_8_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:4614:2: ( ( () otherlv_1= 'ReportGroup' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'regReports' otherlv_5= '{' ( (lv_regReports_6_0= ruleRegReport ) ) (otherlv_7= ',' ( (lv_regReports_8_0= ruleRegReport ) ) )* otherlv_9= '}' )? otherlv_10= '}' ) )
            // InternalAorta.g:4615:2: ( () otherlv_1= 'ReportGroup' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'regReports' otherlv_5= '{' ( (lv_regReports_6_0= ruleRegReport ) ) (otherlv_7= ',' ( (lv_regReports_8_0= ruleRegReport ) ) )* otherlv_9= '}' )? otherlv_10= '}' )
            {
            // InternalAorta.g:4615:2: ( () otherlv_1= 'ReportGroup' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'regReports' otherlv_5= '{' ( (lv_regReports_6_0= ruleRegReport ) ) (otherlv_7= ',' ( (lv_regReports_8_0= ruleRegReport ) ) )* otherlv_9= '}' )? otherlv_10= '}' )
            // InternalAorta.g:4616:3: () otherlv_1= 'ReportGroup' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'regReports' otherlv_5= '{' ( (lv_regReports_6_0= ruleRegReport ) ) (otherlv_7= ',' ( (lv_regReports_8_0= ruleRegReport ) ) )* otherlv_9= '}' )? otherlv_10= '}'
            {
            // InternalAorta.g:4616:3: ()
            // InternalAorta.g:4617:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getReportGroupAccess().getReportGroupAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,90,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getReportGroupAccess().getReportGroupKeyword_1());
            		
            // InternalAorta.g:4627:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:4628:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:4628:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:4629:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getReportGroupAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getReportGroupRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_131); 

            			newLeafNode(otherlv_3, grammarAccess.getReportGroupAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:4650:3: (otherlv_4= 'regReports' otherlv_5= '{' ( (lv_regReports_6_0= ruleRegReport ) ) (otherlv_7= ',' ( (lv_regReports_8_0= ruleRegReport ) ) )* otherlv_9= '}' )?
            int alt119=2;
            int LA119_0 = input.LA(1);

            if ( (LA119_0==91) ) {
                alt119=1;
            }
            switch (alt119) {
                case 1 :
                    // InternalAorta.g:4651:4: otherlv_4= 'regReports' otherlv_5= '{' ( (lv_regReports_6_0= ruleRegReport ) ) (otherlv_7= ',' ( (lv_regReports_8_0= ruleRegReport ) ) )* otherlv_9= '}'
                    {
                    otherlv_4=(Token)match(input,91,FOLLOW_3); 

                    				newLeafNode(otherlv_4, grammarAccess.getReportGroupAccess().getRegReportsKeyword_4_0());
                    			
                    otherlv_5=(Token)match(input,12,FOLLOW_132); 

                    				newLeafNode(otherlv_5, grammarAccess.getReportGroupAccess().getLeftCurlyBracketKeyword_4_1());
                    			
                    // InternalAorta.g:4659:4: ( (lv_regReports_6_0= ruleRegReport ) )
                    // InternalAorta.g:4660:5: (lv_regReports_6_0= ruleRegReport )
                    {
                    // InternalAorta.g:4660:5: (lv_regReports_6_0= ruleRegReport )
                    // InternalAorta.g:4661:6: lv_regReports_6_0= ruleRegReport
                    {

                    						newCompositeNode(grammarAccess.getReportGroupAccess().getRegReportsRegReportParserRuleCall_4_2_0());
                    					
                    pushFollow(FOLLOW_35);
                    lv_regReports_6_0=ruleRegReport();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getReportGroupRule());
                    						}
                    						add(
                    							current,
                    							"regReports",
                    							lv_regReports_6_0,
                    							"org.eclipse.efbt.aorta.Aorta.RegReport");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:4678:4: (otherlv_7= ',' ( (lv_regReports_8_0= ruleRegReport ) ) )*
                    loop118:
                    do {
                        int alt118=2;
                        int LA118_0 = input.LA(1);

                        if ( (LA118_0==31) ) {
                            alt118=1;
                        }


                        switch (alt118) {
                    	case 1 :
                    	    // InternalAorta.g:4679:5: otherlv_7= ',' ( (lv_regReports_8_0= ruleRegReport ) )
                    	    {
                    	    otherlv_7=(Token)match(input,31,FOLLOW_132); 

                    	    					newLeafNode(otherlv_7, grammarAccess.getReportGroupAccess().getCommaKeyword_4_3_0());
                    	    				
                    	    // InternalAorta.g:4683:5: ( (lv_regReports_8_0= ruleRegReport ) )
                    	    // InternalAorta.g:4684:6: (lv_regReports_8_0= ruleRegReport )
                    	    {
                    	    // InternalAorta.g:4684:6: (lv_regReports_8_0= ruleRegReport )
                    	    // InternalAorta.g:4685:7: lv_regReports_8_0= ruleRegReport
                    	    {

                    	    							newCompositeNode(grammarAccess.getReportGroupAccess().getRegReportsRegReportParserRuleCall_4_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_35);
                    	    lv_regReports_8_0=ruleRegReport();

                    	    state._fsp--;


                    	    							if (current==null) {
                    	    								current = createModelElementForParent(grammarAccess.getReportGroupRule());
                    	    							}
                    	    							add(
                    	    								current,
                    	    								"regReports",
                    	    								lv_regReports_8_0,
                    	    								"org.eclipse.efbt.aorta.Aorta.RegReport");
                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop118;
                        }
                    } while (true);

                    otherlv_9=(Token)match(input,23,FOLLOW_24); 

                    				newLeafNode(otherlv_9, grammarAccess.getReportGroupAccess().getRightCurlyBracketKeyword_4_4());
                    			

                    }
                    break;

            }

            otherlv_10=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_10, grammarAccess.getReportGroupAccess().getRightCurlyBracketKeyword_5());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleReportGroup"


    // $ANTLR start "entryRuleRegReport"
    // InternalAorta.g:4716:1: entryRuleRegReport returns [EObject current=null] : iv_ruleRegReport= ruleRegReport EOF ;
    public final EObject entryRuleRegReport() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleRegReport = null;


        try {
            // InternalAorta.g:4716:50: (iv_ruleRegReport= ruleRegReport EOF )
            // InternalAorta.g:4717:2: iv_ruleRegReport= ruleRegReport EOF
            {
             newCompositeNode(grammarAccess.getRegReportRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleRegReport=ruleRegReport();

            state._fsp--;

             current =iv_ruleRegReport; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleRegReport"


    // $ANTLR start "ruleRegReport"
    // InternalAorta.g:4723:1: ruleRegReport returns [EObject current=null] : ( () otherlv_1= 'RegReport' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'cells' otherlv_5= '{' ( (lv_cells_6_0= ruleRegReportCell ) ) (otherlv_7= ',' ( (lv_cells_8_0= ruleRegReportCell ) ) )* otherlv_9= '}' )? otherlv_10= '}' ) ;
    public final EObject ruleRegReport() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_5=null;
        Token otherlv_7=null;
        Token otherlv_9=null;
        Token otherlv_10=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;

        EObject lv_cells_6_0 = null;

        EObject lv_cells_8_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:4729:2: ( ( () otherlv_1= 'RegReport' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'cells' otherlv_5= '{' ( (lv_cells_6_0= ruleRegReportCell ) ) (otherlv_7= ',' ( (lv_cells_8_0= ruleRegReportCell ) ) )* otherlv_9= '}' )? otherlv_10= '}' ) )
            // InternalAorta.g:4730:2: ( () otherlv_1= 'RegReport' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'cells' otherlv_5= '{' ( (lv_cells_6_0= ruleRegReportCell ) ) (otherlv_7= ',' ( (lv_cells_8_0= ruleRegReportCell ) ) )* otherlv_9= '}' )? otherlv_10= '}' )
            {
            // InternalAorta.g:4730:2: ( () otherlv_1= 'RegReport' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'cells' otherlv_5= '{' ( (lv_cells_6_0= ruleRegReportCell ) ) (otherlv_7= ',' ( (lv_cells_8_0= ruleRegReportCell ) ) )* otherlv_9= '}' )? otherlv_10= '}' )
            // InternalAorta.g:4731:3: () otherlv_1= 'RegReport' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'cells' otherlv_5= '{' ( (lv_cells_6_0= ruleRegReportCell ) ) (otherlv_7= ',' ( (lv_cells_8_0= ruleRegReportCell ) ) )* otherlv_9= '}' )? otherlv_10= '}'
            {
            // InternalAorta.g:4731:3: ()
            // InternalAorta.g:4732:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getRegReportAccess().getRegReportAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,92,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getRegReportAccess().getRegReportKeyword_1());
            		
            // InternalAorta.g:4742:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:4743:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:4743:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:4744:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getRegReportAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getRegReportRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_133); 

            			newLeafNode(otherlv_3, grammarAccess.getRegReportAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:4765:3: (otherlv_4= 'cells' otherlv_5= '{' ( (lv_cells_6_0= ruleRegReportCell ) ) (otherlv_7= ',' ( (lv_cells_8_0= ruleRegReportCell ) ) )* otherlv_9= '}' )?
            int alt121=2;
            int LA121_0 = input.LA(1);

            if ( (LA121_0==93) ) {
                alt121=1;
            }
            switch (alt121) {
                case 1 :
                    // InternalAorta.g:4766:4: otherlv_4= 'cells' otherlv_5= '{' ( (lv_cells_6_0= ruleRegReportCell ) ) (otherlv_7= ',' ( (lv_cells_8_0= ruleRegReportCell ) ) )* otherlv_9= '}'
                    {
                    otherlv_4=(Token)match(input,93,FOLLOW_3); 

                    				newLeafNode(otherlv_4, grammarAccess.getRegReportAccess().getCellsKeyword_4_0());
                    			
                    otherlv_5=(Token)match(input,12,FOLLOW_134); 

                    				newLeafNode(otherlv_5, grammarAccess.getRegReportAccess().getLeftCurlyBracketKeyword_4_1());
                    			
                    // InternalAorta.g:4774:4: ( (lv_cells_6_0= ruleRegReportCell ) )
                    // InternalAorta.g:4775:5: (lv_cells_6_0= ruleRegReportCell )
                    {
                    // InternalAorta.g:4775:5: (lv_cells_6_0= ruleRegReportCell )
                    // InternalAorta.g:4776:6: lv_cells_6_0= ruleRegReportCell
                    {

                    						newCompositeNode(grammarAccess.getRegReportAccess().getCellsRegReportCellParserRuleCall_4_2_0());
                    					
                    pushFollow(FOLLOW_35);
                    lv_cells_6_0=ruleRegReportCell();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getRegReportRule());
                    						}
                    						add(
                    							current,
                    							"cells",
                    							lv_cells_6_0,
                    							"org.eclipse.efbt.aorta.Aorta.RegReportCell");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:4793:4: (otherlv_7= ',' ( (lv_cells_8_0= ruleRegReportCell ) ) )*
                    loop120:
                    do {
                        int alt120=2;
                        int LA120_0 = input.LA(1);

                        if ( (LA120_0==31) ) {
                            alt120=1;
                        }


                        switch (alt120) {
                    	case 1 :
                    	    // InternalAorta.g:4794:5: otherlv_7= ',' ( (lv_cells_8_0= ruleRegReportCell ) )
                    	    {
                    	    otherlv_7=(Token)match(input,31,FOLLOW_134); 

                    	    					newLeafNode(otherlv_7, grammarAccess.getRegReportAccess().getCommaKeyword_4_3_0());
                    	    				
                    	    // InternalAorta.g:4798:5: ( (lv_cells_8_0= ruleRegReportCell ) )
                    	    // InternalAorta.g:4799:6: (lv_cells_8_0= ruleRegReportCell )
                    	    {
                    	    // InternalAorta.g:4799:6: (lv_cells_8_0= ruleRegReportCell )
                    	    // InternalAorta.g:4800:7: lv_cells_8_0= ruleRegReportCell
                    	    {

                    	    							newCompositeNode(grammarAccess.getRegReportAccess().getCellsRegReportCellParserRuleCall_4_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_35);
                    	    lv_cells_8_0=ruleRegReportCell();

                    	    state._fsp--;


                    	    							if (current==null) {
                    	    								current = createModelElementForParent(grammarAccess.getRegReportRule());
                    	    							}
                    	    							add(
                    	    								current,
                    	    								"cells",
                    	    								lv_cells_8_0,
                    	    								"org.eclipse.efbt.aorta.Aorta.RegReportCell");
                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop120;
                        }
                    } while (true);

                    otherlv_9=(Token)match(input,23,FOLLOW_24); 

                    				newLeafNode(otherlv_9, grammarAccess.getRegReportAccess().getRightCurlyBracketKeyword_4_4());
                    			

                    }
                    break;

            }

            otherlv_10=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_10, grammarAccess.getRegReportAccess().getRightCurlyBracketKeyword_5());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleRegReport"


    // $ANTLR start "entryRuleRegReportCell"
    // InternalAorta.g:4831:1: entryRuleRegReportCell returns [EObject current=null] : iv_ruleRegReportCell= ruleRegReportCell EOF ;
    public final EObject entryRuleRegReportCell() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleRegReportCell = null;


        try {
            // InternalAorta.g:4831:54: (iv_ruleRegReportCell= ruleRegReportCell EOF )
            // InternalAorta.g:4832:2: iv_ruleRegReportCell= ruleRegReportCell EOF
            {
             newCompositeNode(grammarAccess.getRegReportCellRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleRegReportCell=ruleRegReportCell();

            state._fsp--;

             current =iv_ruleRegReportCell; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleRegReportCell"


    // $ANTLR start "ruleRegReportCell"
    // InternalAorta.g:4838:1: ruleRegReportCell returns [EObject current=null] : ( () otherlv_1= 'RegReportCell' ( (lv_name_2_0= ruleEString ) ) ) ;
    public final EObject ruleRegReportCell() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:4844:2: ( ( () otherlv_1= 'RegReportCell' ( (lv_name_2_0= ruleEString ) ) ) )
            // InternalAorta.g:4845:2: ( () otherlv_1= 'RegReportCell' ( (lv_name_2_0= ruleEString ) ) )
            {
            // InternalAorta.g:4845:2: ( () otherlv_1= 'RegReportCell' ( (lv_name_2_0= ruleEString ) ) )
            // InternalAorta.g:4846:3: () otherlv_1= 'RegReportCell' ( (lv_name_2_0= ruleEString ) )
            {
            // InternalAorta.g:4846:3: ()
            // InternalAorta.g:4847:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getRegReportCellAccess().getRegReportCellAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,94,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getRegReportCellAccess().getRegReportCellKeyword_1());
            		
            // InternalAorta.g:4857:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:4858:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:4858:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:4859:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getRegReportCellAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_2);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getRegReportCellRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }


            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleRegReportCell"


    // $ANTLR start "entryRuleTestDefinition_Impl"
    // InternalAorta.g:4880:1: entryRuleTestDefinition_Impl returns [EObject current=null] : iv_ruleTestDefinition_Impl= ruleTestDefinition_Impl EOF ;
    public final EObject entryRuleTestDefinition_Impl() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleTestDefinition_Impl = null;


        try {
            // InternalAorta.g:4880:60: (iv_ruleTestDefinition_Impl= ruleTestDefinition_Impl EOF )
            // InternalAorta.g:4881:2: iv_ruleTestDefinition_Impl= ruleTestDefinition_Impl EOF
            {
             newCompositeNode(grammarAccess.getTestDefinition_ImplRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleTestDefinition_Impl=ruleTestDefinition_Impl();

            state._fsp--;

             current =iv_ruleTestDefinition_Impl; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleTestDefinition_Impl"


    // $ANTLR start "ruleTestDefinition_Impl"
    // InternalAorta.g:4887:1: ruleTestDefinition_Impl returns [EObject current=null] : ( () otherlv_1= 'TestDefinition' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'testContraints' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'given' ( (lv_given_7_0= ruleGiven ) ) )? (otherlv_8= 'when' ( (lv_when_9_0= ruleWhen ) ) )? (otherlv_10= 'then' ( (lv_then_11_0= ruleThen ) ) )? otherlv_12= '}' ) ;
    public final EObject ruleTestDefinition_Impl() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_6=null;
        Token otherlv_8=null;
        Token otherlv_10=null;
        Token otherlv_12=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;

        EObject lv_given_7_0 = null;

        EObject lv_when_9_0 = null;

        EObject lv_then_11_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:4893:2: ( ( () otherlv_1= 'TestDefinition' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'testContraints' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'given' ( (lv_given_7_0= ruleGiven ) ) )? (otherlv_8= 'when' ( (lv_when_9_0= ruleWhen ) ) )? (otherlv_10= 'then' ( (lv_then_11_0= ruleThen ) ) )? otherlv_12= '}' ) )
            // InternalAorta.g:4894:2: ( () otherlv_1= 'TestDefinition' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'testContraints' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'given' ( (lv_given_7_0= ruleGiven ) ) )? (otherlv_8= 'when' ( (lv_when_9_0= ruleWhen ) ) )? (otherlv_10= 'then' ( (lv_then_11_0= ruleThen ) ) )? otherlv_12= '}' )
            {
            // InternalAorta.g:4894:2: ( () otherlv_1= 'TestDefinition' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'testContraints' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'given' ( (lv_given_7_0= ruleGiven ) ) )? (otherlv_8= 'when' ( (lv_when_9_0= ruleWhen ) ) )? (otherlv_10= 'then' ( (lv_then_11_0= ruleThen ) ) )? otherlv_12= '}' )
            // InternalAorta.g:4895:3: () otherlv_1= 'TestDefinition' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'testContraints' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'given' ( (lv_given_7_0= ruleGiven ) ) )? (otherlv_8= 'when' ( (lv_when_9_0= ruleWhen ) ) )? (otherlv_10= 'then' ( (lv_then_11_0= ruleThen ) ) )? otherlv_12= '}'
            {
            // InternalAorta.g:4895:3: ()
            // InternalAorta.g:4896:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getTestDefinition_ImplAccess().getTestDefinitionAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,95,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getTestDefinition_ImplAccess().getTestDefinitionKeyword_1());
            		
            // InternalAorta.g:4906:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:4907:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:4907:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:4908:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getTestDefinition_ImplAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getTestDefinition_ImplRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_135); 

            			newLeafNode(otherlv_3, grammarAccess.getTestDefinition_ImplAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:4929:3: (otherlv_4= 'testContraints' ( ( ruleQualifiedName ) ) )?
            int alt122=2;
            int LA122_0 = input.LA(1);

            if ( (LA122_0==96) ) {
                alt122=1;
            }
            switch (alt122) {
                case 1 :
                    // InternalAorta.g:4930:4: otherlv_4= 'testContraints' ( ( ruleQualifiedName ) )
                    {
                    otherlv_4=(Token)match(input,96,FOLLOW_70); 

                    				newLeafNode(otherlv_4, grammarAccess.getTestDefinition_ImplAccess().getTestContraintsKeyword_4_0());
                    			
                    // InternalAorta.g:4934:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:4935:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:4935:5: ( ruleQualifiedName )
                    // InternalAorta.g:4936:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getTestDefinition_ImplRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getTestDefinition_ImplAccess().getTestContraintsTestContraintsCrossReference_4_1_0());
                    					
                    pushFollow(FOLLOW_136);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:4951:3: (otherlv_6= 'given' ( (lv_given_7_0= ruleGiven ) ) )?
            int alt123=2;
            int LA123_0 = input.LA(1);

            if ( (LA123_0==97) ) {
                alt123=1;
            }
            switch (alt123) {
                case 1 :
                    // InternalAorta.g:4952:4: otherlv_6= 'given' ( (lv_given_7_0= ruleGiven ) )
                    {
                    otherlv_6=(Token)match(input,97,FOLLOW_137); 

                    				newLeafNode(otherlv_6, grammarAccess.getTestDefinition_ImplAccess().getGivenKeyword_5_0());
                    			
                    // InternalAorta.g:4956:4: ( (lv_given_7_0= ruleGiven ) )
                    // InternalAorta.g:4957:5: (lv_given_7_0= ruleGiven )
                    {
                    // InternalAorta.g:4957:5: (lv_given_7_0= ruleGiven )
                    // InternalAorta.g:4958:6: lv_given_7_0= ruleGiven
                    {

                    						newCompositeNode(grammarAccess.getTestDefinition_ImplAccess().getGivenGivenParserRuleCall_5_1_0());
                    					
                    pushFollow(FOLLOW_138);
                    lv_given_7_0=ruleGiven();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getTestDefinition_ImplRule());
                    						}
                    						set(
                    							current,
                    							"given",
                    							lv_given_7_0,
                    							"org.eclipse.efbt.aorta.Aorta.Given");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:4976:3: (otherlv_8= 'when' ( (lv_when_9_0= ruleWhen ) ) )?
            int alt124=2;
            int LA124_0 = input.LA(1);

            if ( (LA124_0==98) ) {
                alt124=1;
            }
            switch (alt124) {
                case 1 :
                    // InternalAorta.g:4977:4: otherlv_8= 'when' ( (lv_when_9_0= ruleWhen ) )
                    {
                    otherlv_8=(Token)match(input,98,FOLLOW_139); 

                    				newLeafNode(otherlv_8, grammarAccess.getTestDefinition_ImplAccess().getWhenKeyword_6_0());
                    			
                    // InternalAorta.g:4981:4: ( (lv_when_9_0= ruleWhen ) )
                    // InternalAorta.g:4982:5: (lv_when_9_0= ruleWhen )
                    {
                    // InternalAorta.g:4982:5: (lv_when_9_0= ruleWhen )
                    // InternalAorta.g:4983:6: lv_when_9_0= ruleWhen
                    {

                    						newCompositeNode(grammarAccess.getTestDefinition_ImplAccess().getWhenWhenParserRuleCall_6_1_0());
                    					
                    pushFollow(FOLLOW_140);
                    lv_when_9_0=ruleWhen();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getTestDefinition_ImplRule());
                    						}
                    						set(
                    							current,
                    							"when",
                    							lv_when_9_0,
                    							"org.eclipse.efbt.aorta.Aorta.When");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:5001:3: (otherlv_10= 'then' ( (lv_then_11_0= ruleThen ) ) )?
            int alt125=2;
            int LA125_0 = input.LA(1);

            if ( (LA125_0==99) ) {
                alt125=1;
            }
            switch (alt125) {
                case 1 :
                    // InternalAorta.g:5002:4: otherlv_10= 'then' ( (lv_then_11_0= ruleThen ) )
                    {
                    otherlv_10=(Token)match(input,99,FOLLOW_141); 

                    				newLeafNode(otherlv_10, grammarAccess.getTestDefinition_ImplAccess().getThenKeyword_7_0());
                    			
                    // InternalAorta.g:5006:4: ( (lv_then_11_0= ruleThen ) )
                    // InternalAorta.g:5007:5: (lv_then_11_0= ruleThen )
                    {
                    // InternalAorta.g:5007:5: (lv_then_11_0= ruleThen )
                    // InternalAorta.g:5008:6: lv_then_11_0= ruleThen
                    {

                    						newCompositeNode(grammarAccess.getTestDefinition_ImplAccess().getThenThenParserRuleCall_7_1_0());
                    					
                    pushFollow(FOLLOW_24);
                    lv_then_11_0=ruleThen();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getTestDefinition_ImplRule());
                    						}
                    						set(
                    							current,
                    							"then",
                    							lv_then_11_0,
                    							"org.eclipse.efbt.aorta.Aorta.Then");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            otherlv_12=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_12, grammarAccess.getTestDefinition_ImplAccess().getRightCurlyBracketKeyword_8());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleTestDefinition_Impl"


    // $ANTLR start "entryRuleGiven"
    // InternalAorta.g:5034:1: entryRuleGiven returns [EObject current=null] : iv_ruleGiven= ruleGiven EOF ;
    public final EObject entryRuleGiven() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleGiven = null;


        try {
            // InternalAorta.g:5034:46: (iv_ruleGiven= ruleGiven EOF )
            // InternalAorta.g:5035:2: iv_ruleGiven= ruleGiven EOF
            {
             newCompositeNode(grammarAccess.getGivenRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleGiven=ruleGiven();

            state._fsp--;

             current =iv_ruleGiven; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleGiven"


    // $ANTLR start "ruleGiven"
    // InternalAorta.g:5041:1: ruleGiven returns [EObject current=null] : ( () otherlv_1= 'Given' otherlv_2= '{' (otherlv_3= 'text' ( ( ruleQualifiedName ) ) )? (otherlv_5= 'regulatoryTextLinks' otherlv_6= '(' ( ( ruleQualifiedName ) ) (otherlv_8= ',' ( ( ruleQualifiedName ) ) )* otherlv_10= ')' )? (otherlv_11= 'params' otherlv_12= '{' ( (lv_params_13_0= ruleParam ) ) (otherlv_14= ',' ( (lv_params_15_0= ruleParam ) ) )* otherlv_16= '}' )? otherlv_17= '}' ) ;
    public final EObject ruleGiven() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_2=null;
        Token otherlv_3=null;
        Token otherlv_5=null;
        Token otherlv_6=null;
        Token otherlv_8=null;
        Token otherlv_10=null;
        Token otherlv_11=null;
        Token otherlv_12=null;
        Token otherlv_14=null;
        Token otherlv_16=null;
        Token otherlv_17=null;
        EObject lv_params_13_0 = null;

        EObject lv_params_15_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:5047:2: ( ( () otherlv_1= 'Given' otherlv_2= '{' (otherlv_3= 'text' ( ( ruleQualifiedName ) ) )? (otherlv_5= 'regulatoryTextLinks' otherlv_6= '(' ( ( ruleQualifiedName ) ) (otherlv_8= ',' ( ( ruleQualifiedName ) ) )* otherlv_10= ')' )? (otherlv_11= 'params' otherlv_12= '{' ( (lv_params_13_0= ruleParam ) ) (otherlv_14= ',' ( (lv_params_15_0= ruleParam ) ) )* otherlv_16= '}' )? otherlv_17= '}' ) )
            // InternalAorta.g:5048:2: ( () otherlv_1= 'Given' otherlv_2= '{' (otherlv_3= 'text' ( ( ruleQualifiedName ) ) )? (otherlv_5= 'regulatoryTextLinks' otherlv_6= '(' ( ( ruleQualifiedName ) ) (otherlv_8= ',' ( ( ruleQualifiedName ) ) )* otherlv_10= ')' )? (otherlv_11= 'params' otherlv_12= '{' ( (lv_params_13_0= ruleParam ) ) (otherlv_14= ',' ( (lv_params_15_0= ruleParam ) ) )* otherlv_16= '}' )? otherlv_17= '}' )
            {
            // InternalAorta.g:5048:2: ( () otherlv_1= 'Given' otherlv_2= '{' (otherlv_3= 'text' ( ( ruleQualifiedName ) ) )? (otherlv_5= 'regulatoryTextLinks' otherlv_6= '(' ( ( ruleQualifiedName ) ) (otherlv_8= ',' ( ( ruleQualifiedName ) ) )* otherlv_10= ')' )? (otherlv_11= 'params' otherlv_12= '{' ( (lv_params_13_0= ruleParam ) ) (otherlv_14= ',' ( (lv_params_15_0= ruleParam ) ) )* otherlv_16= '}' )? otherlv_17= '}' )
            // InternalAorta.g:5049:3: () otherlv_1= 'Given' otherlv_2= '{' (otherlv_3= 'text' ( ( ruleQualifiedName ) ) )? (otherlv_5= 'regulatoryTextLinks' otherlv_6= '(' ( ( ruleQualifiedName ) ) (otherlv_8= ',' ( ( ruleQualifiedName ) ) )* otherlv_10= ')' )? (otherlv_11= 'params' otherlv_12= '{' ( (lv_params_13_0= ruleParam ) ) (otherlv_14= ',' ( (lv_params_15_0= ruleParam ) ) )* otherlv_16= '}' )? otherlv_17= '}'
            {
            // InternalAorta.g:5049:3: ()
            // InternalAorta.g:5050:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getGivenAccess().getGivenAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,100,FOLLOW_3); 

            			newLeafNode(otherlv_1, grammarAccess.getGivenAccess().getGivenKeyword_1());
            		
            otherlv_2=(Token)match(input,12,FOLLOW_142); 

            			newLeafNode(otherlv_2, grammarAccess.getGivenAccess().getLeftCurlyBracketKeyword_2());
            		
            // InternalAorta.g:5064:3: (otherlv_3= 'text' ( ( ruleQualifiedName ) ) )?
            int alt126=2;
            int LA126_0 = input.LA(1);

            if ( (LA126_0==83) ) {
                alt126=1;
            }
            switch (alt126) {
                case 1 :
                    // InternalAorta.g:5065:4: otherlv_3= 'text' ( ( ruleQualifiedName ) )
                    {
                    otherlv_3=(Token)match(input,83,FOLLOW_70); 

                    				newLeafNode(otherlv_3, grammarAccess.getGivenAccess().getTextKeyword_3_0());
                    			
                    // InternalAorta.g:5069:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:5070:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:5070:5: ( ruleQualifiedName )
                    // InternalAorta.g:5071:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getGivenRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getGivenAccess().getTextClauseTextCrossReference_3_1_0());
                    					
                    pushFollow(FOLLOW_143);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:5086:3: (otherlv_5= 'regulatoryTextLinks' otherlv_6= '(' ( ( ruleQualifiedName ) ) (otherlv_8= ',' ( ( ruleQualifiedName ) ) )* otherlv_10= ')' )?
            int alt128=2;
            int LA128_0 = input.LA(1);

            if ( (LA128_0==101) ) {
                alt128=1;
            }
            switch (alt128) {
                case 1 :
                    // InternalAorta.g:5087:4: otherlv_5= 'regulatoryTextLinks' otherlv_6= '(' ( ( ruleQualifiedName ) ) (otherlv_8= ',' ( ( ruleQualifiedName ) ) )* otherlv_10= ')'
                    {
                    otherlv_5=(Token)match(input,101,FOLLOW_144); 

                    				newLeafNode(otherlv_5, grammarAccess.getGivenAccess().getRegulatoryTextLinksKeyword_4_0());
                    			
                    otherlv_6=(Token)match(input,102,FOLLOW_70); 

                    				newLeafNode(otherlv_6, grammarAccess.getGivenAccess().getLeftParenthesisKeyword_4_1());
                    			
                    // InternalAorta.g:5095:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:5096:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:5096:5: ( ruleQualifiedName )
                    // InternalAorta.g:5097:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getGivenRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getGivenAccess().getRegulatoryTextLinksTitledRequirementsSectionCrossReference_4_2_0());
                    					
                    pushFollow(FOLLOW_145);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:5111:4: (otherlv_8= ',' ( ( ruleQualifiedName ) ) )*
                    loop127:
                    do {
                        int alt127=2;
                        int LA127_0 = input.LA(1);

                        if ( (LA127_0==31) ) {
                            alt127=1;
                        }


                        switch (alt127) {
                    	case 1 :
                    	    // InternalAorta.g:5112:5: otherlv_8= ',' ( ( ruleQualifiedName ) )
                    	    {
                    	    otherlv_8=(Token)match(input,31,FOLLOW_70); 

                    	    					newLeafNode(otherlv_8, grammarAccess.getGivenAccess().getCommaKeyword_4_3_0());
                    	    				
                    	    // InternalAorta.g:5116:5: ( ( ruleQualifiedName ) )
                    	    // InternalAorta.g:5117:6: ( ruleQualifiedName )
                    	    {
                    	    // InternalAorta.g:5117:6: ( ruleQualifiedName )
                    	    // InternalAorta.g:5118:7: ruleQualifiedName
                    	    {

                    	    							if (current==null) {
                    	    								current = createModelElement(grammarAccess.getGivenRule());
                    	    							}
                    	    						

                    	    							newCompositeNode(grammarAccess.getGivenAccess().getRegulatoryTextLinksTitledRequirementsSectionCrossReference_4_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_145);
                    	    ruleQualifiedName();

                    	    state._fsp--;


                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop127;
                        }
                    } while (true);

                    otherlv_10=(Token)match(input,103,FOLLOW_146); 

                    				newLeafNode(otherlv_10, grammarAccess.getGivenAccess().getRightParenthesisKeyword_4_4());
                    			

                    }
                    break;

            }

            // InternalAorta.g:5138:3: (otherlv_11= 'params' otherlv_12= '{' ( (lv_params_13_0= ruleParam ) ) (otherlv_14= ',' ( (lv_params_15_0= ruleParam ) ) )* otherlv_16= '}' )?
            int alt130=2;
            int LA130_0 = input.LA(1);

            if ( (LA130_0==104) ) {
                alt130=1;
            }
            switch (alt130) {
                case 1 :
                    // InternalAorta.g:5139:4: otherlv_11= 'params' otherlv_12= '{' ( (lv_params_13_0= ruleParam ) ) (otherlv_14= ',' ( (lv_params_15_0= ruleParam ) ) )* otherlv_16= '}'
                    {
                    otherlv_11=(Token)match(input,104,FOLLOW_3); 

                    				newLeafNode(otherlv_11, grammarAccess.getGivenAccess().getParamsKeyword_5_0());
                    			
                    otherlv_12=(Token)match(input,12,FOLLOW_91); 

                    				newLeafNode(otherlv_12, grammarAccess.getGivenAccess().getLeftCurlyBracketKeyword_5_1());
                    			
                    // InternalAorta.g:5147:4: ( (lv_params_13_0= ruleParam ) )
                    // InternalAorta.g:5148:5: (lv_params_13_0= ruleParam )
                    {
                    // InternalAorta.g:5148:5: (lv_params_13_0= ruleParam )
                    // InternalAorta.g:5149:6: lv_params_13_0= ruleParam
                    {

                    						newCompositeNode(grammarAccess.getGivenAccess().getParamsParamParserRuleCall_5_2_0());
                    					
                    pushFollow(FOLLOW_35);
                    lv_params_13_0=ruleParam();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getGivenRule());
                    						}
                    						add(
                    							current,
                    							"params",
                    							lv_params_13_0,
                    							"org.eclipse.efbt.aorta.Aorta.Param");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:5166:4: (otherlv_14= ',' ( (lv_params_15_0= ruleParam ) ) )*
                    loop129:
                    do {
                        int alt129=2;
                        int LA129_0 = input.LA(1);

                        if ( (LA129_0==31) ) {
                            alt129=1;
                        }


                        switch (alt129) {
                    	case 1 :
                    	    // InternalAorta.g:5167:5: otherlv_14= ',' ( (lv_params_15_0= ruleParam ) )
                    	    {
                    	    otherlv_14=(Token)match(input,31,FOLLOW_91); 

                    	    					newLeafNode(otherlv_14, grammarAccess.getGivenAccess().getCommaKeyword_5_3_0());
                    	    				
                    	    // InternalAorta.g:5171:5: ( (lv_params_15_0= ruleParam ) )
                    	    // InternalAorta.g:5172:6: (lv_params_15_0= ruleParam )
                    	    {
                    	    // InternalAorta.g:5172:6: (lv_params_15_0= ruleParam )
                    	    // InternalAorta.g:5173:7: lv_params_15_0= ruleParam
                    	    {

                    	    							newCompositeNode(grammarAccess.getGivenAccess().getParamsParamParserRuleCall_5_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_35);
                    	    lv_params_15_0=ruleParam();

                    	    state._fsp--;


                    	    							if (current==null) {
                    	    								current = createModelElementForParent(grammarAccess.getGivenRule());
                    	    							}
                    	    							add(
                    	    								current,
                    	    								"params",
                    	    								lv_params_15_0,
                    	    								"org.eclipse.efbt.aorta.Aorta.Param");
                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop129;
                        }
                    } while (true);

                    otherlv_16=(Token)match(input,23,FOLLOW_24); 

                    				newLeafNode(otherlv_16, grammarAccess.getGivenAccess().getRightCurlyBracketKeyword_5_4());
                    			

                    }
                    break;

            }

            otherlv_17=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_17, grammarAccess.getGivenAccess().getRightCurlyBracketKeyword_6());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleGiven"


    // $ANTLR start "entryRuleWhen"
    // InternalAorta.g:5204:1: entryRuleWhen returns [EObject current=null] : iv_ruleWhen= ruleWhen EOF ;
    public final EObject entryRuleWhen() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleWhen = null;


        try {
            // InternalAorta.g:5204:45: (iv_ruleWhen= ruleWhen EOF )
            // InternalAorta.g:5205:2: iv_ruleWhen= ruleWhen EOF
            {
             newCompositeNode(grammarAccess.getWhenRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleWhen=ruleWhen();

            state._fsp--;

             current =iv_ruleWhen; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleWhen"


    // $ANTLR start "ruleWhen"
    // InternalAorta.g:5211:1: ruleWhen returns [EObject current=null] : ( () otherlv_1= 'When' otherlv_2= '{' (otherlv_3= 'text' ( ( ruleQualifiedName ) ) )? (otherlv_5= 'regulatoryTextLinks' otherlv_6= '(' ( ( ruleQualifiedName ) ) (otherlv_8= ',' ( ( ruleQualifiedName ) ) )* otherlv_10= ')' )? (otherlv_11= 'params' otherlv_12= '{' ( (lv_params_13_0= ruleParam ) ) (otherlv_14= ',' ( (lv_params_15_0= ruleParam ) ) )* otherlv_16= '}' )? otherlv_17= '}' ) ;
    public final EObject ruleWhen() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_2=null;
        Token otherlv_3=null;
        Token otherlv_5=null;
        Token otherlv_6=null;
        Token otherlv_8=null;
        Token otherlv_10=null;
        Token otherlv_11=null;
        Token otherlv_12=null;
        Token otherlv_14=null;
        Token otherlv_16=null;
        Token otherlv_17=null;
        EObject lv_params_13_0 = null;

        EObject lv_params_15_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:5217:2: ( ( () otherlv_1= 'When' otherlv_2= '{' (otherlv_3= 'text' ( ( ruleQualifiedName ) ) )? (otherlv_5= 'regulatoryTextLinks' otherlv_6= '(' ( ( ruleQualifiedName ) ) (otherlv_8= ',' ( ( ruleQualifiedName ) ) )* otherlv_10= ')' )? (otherlv_11= 'params' otherlv_12= '{' ( (lv_params_13_0= ruleParam ) ) (otherlv_14= ',' ( (lv_params_15_0= ruleParam ) ) )* otherlv_16= '}' )? otherlv_17= '}' ) )
            // InternalAorta.g:5218:2: ( () otherlv_1= 'When' otherlv_2= '{' (otherlv_3= 'text' ( ( ruleQualifiedName ) ) )? (otherlv_5= 'regulatoryTextLinks' otherlv_6= '(' ( ( ruleQualifiedName ) ) (otherlv_8= ',' ( ( ruleQualifiedName ) ) )* otherlv_10= ')' )? (otherlv_11= 'params' otherlv_12= '{' ( (lv_params_13_0= ruleParam ) ) (otherlv_14= ',' ( (lv_params_15_0= ruleParam ) ) )* otherlv_16= '}' )? otherlv_17= '}' )
            {
            // InternalAorta.g:5218:2: ( () otherlv_1= 'When' otherlv_2= '{' (otherlv_3= 'text' ( ( ruleQualifiedName ) ) )? (otherlv_5= 'regulatoryTextLinks' otherlv_6= '(' ( ( ruleQualifiedName ) ) (otherlv_8= ',' ( ( ruleQualifiedName ) ) )* otherlv_10= ')' )? (otherlv_11= 'params' otherlv_12= '{' ( (lv_params_13_0= ruleParam ) ) (otherlv_14= ',' ( (lv_params_15_0= ruleParam ) ) )* otherlv_16= '}' )? otherlv_17= '}' )
            // InternalAorta.g:5219:3: () otherlv_1= 'When' otherlv_2= '{' (otherlv_3= 'text' ( ( ruleQualifiedName ) ) )? (otherlv_5= 'regulatoryTextLinks' otherlv_6= '(' ( ( ruleQualifiedName ) ) (otherlv_8= ',' ( ( ruleQualifiedName ) ) )* otherlv_10= ')' )? (otherlv_11= 'params' otherlv_12= '{' ( (lv_params_13_0= ruleParam ) ) (otherlv_14= ',' ( (lv_params_15_0= ruleParam ) ) )* otherlv_16= '}' )? otherlv_17= '}'
            {
            // InternalAorta.g:5219:3: ()
            // InternalAorta.g:5220:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getWhenAccess().getWhenAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,105,FOLLOW_3); 

            			newLeafNode(otherlv_1, grammarAccess.getWhenAccess().getWhenKeyword_1());
            		
            otherlv_2=(Token)match(input,12,FOLLOW_142); 

            			newLeafNode(otherlv_2, grammarAccess.getWhenAccess().getLeftCurlyBracketKeyword_2());
            		
            // InternalAorta.g:5234:3: (otherlv_3= 'text' ( ( ruleQualifiedName ) ) )?
            int alt131=2;
            int LA131_0 = input.LA(1);

            if ( (LA131_0==83) ) {
                alt131=1;
            }
            switch (alt131) {
                case 1 :
                    // InternalAorta.g:5235:4: otherlv_3= 'text' ( ( ruleQualifiedName ) )
                    {
                    otherlv_3=(Token)match(input,83,FOLLOW_70); 

                    				newLeafNode(otherlv_3, grammarAccess.getWhenAccess().getTextKeyword_3_0());
                    			
                    // InternalAorta.g:5239:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:5240:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:5240:5: ( ruleQualifiedName )
                    // InternalAorta.g:5241:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getWhenRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getWhenAccess().getTextClauseTextCrossReference_3_1_0());
                    					
                    pushFollow(FOLLOW_143);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:5256:3: (otherlv_5= 'regulatoryTextLinks' otherlv_6= '(' ( ( ruleQualifiedName ) ) (otherlv_8= ',' ( ( ruleQualifiedName ) ) )* otherlv_10= ')' )?
            int alt133=2;
            int LA133_0 = input.LA(1);

            if ( (LA133_0==101) ) {
                alt133=1;
            }
            switch (alt133) {
                case 1 :
                    // InternalAorta.g:5257:4: otherlv_5= 'regulatoryTextLinks' otherlv_6= '(' ( ( ruleQualifiedName ) ) (otherlv_8= ',' ( ( ruleQualifiedName ) ) )* otherlv_10= ')'
                    {
                    otherlv_5=(Token)match(input,101,FOLLOW_144); 

                    				newLeafNode(otherlv_5, grammarAccess.getWhenAccess().getRegulatoryTextLinksKeyword_4_0());
                    			
                    otherlv_6=(Token)match(input,102,FOLLOW_70); 

                    				newLeafNode(otherlv_6, grammarAccess.getWhenAccess().getLeftParenthesisKeyword_4_1());
                    			
                    // InternalAorta.g:5265:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:5266:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:5266:5: ( ruleQualifiedName )
                    // InternalAorta.g:5267:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getWhenRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getWhenAccess().getRegulatoryTextLinksTitledRequirementsSectionCrossReference_4_2_0());
                    					
                    pushFollow(FOLLOW_145);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:5281:4: (otherlv_8= ',' ( ( ruleQualifiedName ) ) )*
                    loop132:
                    do {
                        int alt132=2;
                        int LA132_0 = input.LA(1);

                        if ( (LA132_0==31) ) {
                            alt132=1;
                        }


                        switch (alt132) {
                    	case 1 :
                    	    // InternalAorta.g:5282:5: otherlv_8= ',' ( ( ruleQualifiedName ) )
                    	    {
                    	    otherlv_8=(Token)match(input,31,FOLLOW_70); 

                    	    					newLeafNode(otherlv_8, grammarAccess.getWhenAccess().getCommaKeyword_4_3_0());
                    	    				
                    	    // InternalAorta.g:5286:5: ( ( ruleQualifiedName ) )
                    	    // InternalAorta.g:5287:6: ( ruleQualifiedName )
                    	    {
                    	    // InternalAorta.g:5287:6: ( ruleQualifiedName )
                    	    // InternalAorta.g:5288:7: ruleQualifiedName
                    	    {

                    	    							if (current==null) {
                    	    								current = createModelElement(grammarAccess.getWhenRule());
                    	    							}
                    	    						

                    	    							newCompositeNode(grammarAccess.getWhenAccess().getRegulatoryTextLinksTitledRequirementsSectionCrossReference_4_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_145);
                    	    ruleQualifiedName();

                    	    state._fsp--;


                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop132;
                        }
                    } while (true);

                    otherlv_10=(Token)match(input,103,FOLLOW_146); 

                    				newLeafNode(otherlv_10, grammarAccess.getWhenAccess().getRightParenthesisKeyword_4_4());
                    			

                    }
                    break;

            }

            // InternalAorta.g:5308:3: (otherlv_11= 'params' otherlv_12= '{' ( (lv_params_13_0= ruleParam ) ) (otherlv_14= ',' ( (lv_params_15_0= ruleParam ) ) )* otherlv_16= '}' )?
            int alt135=2;
            int LA135_0 = input.LA(1);

            if ( (LA135_0==104) ) {
                alt135=1;
            }
            switch (alt135) {
                case 1 :
                    // InternalAorta.g:5309:4: otherlv_11= 'params' otherlv_12= '{' ( (lv_params_13_0= ruleParam ) ) (otherlv_14= ',' ( (lv_params_15_0= ruleParam ) ) )* otherlv_16= '}'
                    {
                    otherlv_11=(Token)match(input,104,FOLLOW_3); 

                    				newLeafNode(otherlv_11, grammarAccess.getWhenAccess().getParamsKeyword_5_0());
                    			
                    otherlv_12=(Token)match(input,12,FOLLOW_91); 

                    				newLeafNode(otherlv_12, grammarAccess.getWhenAccess().getLeftCurlyBracketKeyword_5_1());
                    			
                    // InternalAorta.g:5317:4: ( (lv_params_13_0= ruleParam ) )
                    // InternalAorta.g:5318:5: (lv_params_13_0= ruleParam )
                    {
                    // InternalAorta.g:5318:5: (lv_params_13_0= ruleParam )
                    // InternalAorta.g:5319:6: lv_params_13_0= ruleParam
                    {

                    						newCompositeNode(grammarAccess.getWhenAccess().getParamsParamParserRuleCall_5_2_0());
                    					
                    pushFollow(FOLLOW_35);
                    lv_params_13_0=ruleParam();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getWhenRule());
                    						}
                    						add(
                    							current,
                    							"params",
                    							lv_params_13_0,
                    							"org.eclipse.efbt.aorta.Aorta.Param");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:5336:4: (otherlv_14= ',' ( (lv_params_15_0= ruleParam ) ) )*
                    loop134:
                    do {
                        int alt134=2;
                        int LA134_0 = input.LA(1);

                        if ( (LA134_0==31) ) {
                            alt134=1;
                        }


                        switch (alt134) {
                    	case 1 :
                    	    // InternalAorta.g:5337:5: otherlv_14= ',' ( (lv_params_15_0= ruleParam ) )
                    	    {
                    	    otherlv_14=(Token)match(input,31,FOLLOW_91); 

                    	    					newLeafNode(otherlv_14, grammarAccess.getWhenAccess().getCommaKeyword_5_3_0());
                    	    				
                    	    // InternalAorta.g:5341:5: ( (lv_params_15_0= ruleParam ) )
                    	    // InternalAorta.g:5342:6: (lv_params_15_0= ruleParam )
                    	    {
                    	    // InternalAorta.g:5342:6: (lv_params_15_0= ruleParam )
                    	    // InternalAorta.g:5343:7: lv_params_15_0= ruleParam
                    	    {

                    	    							newCompositeNode(grammarAccess.getWhenAccess().getParamsParamParserRuleCall_5_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_35);
                    	    lv_params_15_0=ruleParam();

                    	    state._fsp--;


                    	    							if (current==null) {
                    	    								current = createModelElementForParent(grammarAccess.getWhenRule());
                    	    							}
                    	    							add(
                    	    								current,
                    	    								"params",
                    	    								lv_params_15_0,
                    	    								"org.eclipse.efbt.aorta.Aorta.Param");
                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop134;
                        }
                    } while (true);

                    otherlv_16=(Token)match(input,23,FOLLOW_24); 

                    				newLeafNode(otherlv_16, grammarAccess.getWhenAccess().getRightCurlyBracketKeyword_5_4());
                    			

                    }
                    break;

            }

            otherlv_17=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_17, grammarAccess.getWhenAccess().getRightCurlyBracketKeyword_6());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleWhen"


    // $ANTLR start "entryRuleThen"
    // InternalAorta.g:5374:1: entryRuleThen returns [EObject current=null] : iv_ruleThen= ruleThen EOF ;
    public final EObject entryRuleThen() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleThen = null;


        try {
            // InternalAorta.g:5374:45: (iv_ruleThen= ruleThen EOF )
            // InternalAorta.g:5375:2: iv_ruleThen= ruleThen EOF
            {
             newCompositeNode(grammarAccess.getThenRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleThen=ruleThen();

            state._fsp--;

             current =iv_ruleThen; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleThen"


    // $ANTLR start "ruleThen"
    // InternalAorta.g:5381:1: ruleThen returns [EObject current=null] : ( () otherlv_1= 'Then' otherlv_2= '{' (otherlv_3= 'text' ( ( ruleQualifiedName ) ) )? (otherlv_5= 'regulatoryTextLinks' otherlv_6= '(' ( ( ruleQualifiedName ) ) (otherlv_8= ',' ( ( ruleQualifiedName ) ) )* otherlv_10= ')' )? (otherlv_11= 'params' otherlv_12= '{' ( (lv_params_13_0= ruleParam ) ) (otherlv_14= ',' ( (lv_params_15_0= ruleParam ) ) )* otherlv_16= '}' )? otherlv_17= '}' ) ;
    public final EObject ruleThen() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_2=null;
        Token otherlv_3=null;
        Token otherlv_5=null;
        Token otherlv_6=null;
        Token otherlv_8=null;
        Token otherlv_10=null;
        Token otherlv_11=null;
        Token otherlv_12=null;
        Token otherlv_14=null;
        Token otherlv_16=null;
        Token otherlv_17=null;
        EObject lv_params_13_0 = null;

        EObject lv_params_15_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:5387:2: ( ( () otherlv_1= 'Then' otherlv_2= '{' (otherlv_3= 'text' ( ( ruleQualifiedName ) ) )? (otherlv_5= 'regulatoryTextLinks' otherlv_6= '(' ( ( ruleQualifiedName ) ) (otherlv_8= ',' ( ( ruleQualifiedName ) ) )* otherlv_10= ')' )? (otherlv_11= 'params' otherlv_12= '{' ( (lv_params_13_0= ruleParam ) ) (otherlv_14= ',' ( (lv_params_15_0= ruleParam ) ) )* otherlv_16= '}' )? otherlv_17= '}' ) )
            // InternalAorta.g:5388:2: ( () otherlv_1= 'Then' otherlv_2= '{' (otherlv_3= 'text' ( ( ruleQualifiedName ) ) )? (otherlv_5= 'regulatoryTextLinks' otherlv_6= '(' ( ( ruleQualifiedName ) ) (otherlv_8= ',' ( ( ruleQualifiedName ) ) )* otherlv_10= ')' )? (otherlv_11= 'params' otherlv_12= '{' ( (lv_params_13_0= ruleParam ) ) (otherlv_14= ',' ( (lv_params_15_0= ruleParam ) ) )* otherlv_16= '}' )? otherlv_17= '}' )
            {
            // InternalAorta.g:5388:2: ( () otherlv_1= 'Then' otherlv_2= '{' (otherlv_3= 'text' ( ( ruleQualifiedName ) ) )? (otherlv_5= 'regulatoryTextLinks' otherlv_6= '(' ( ( ruleQualifiedName ) ) (otherlv_8= ',' ( ( ruleQualifiedName ) ) )* otherlv_10= ')' )? (otherlv_11= 'params' otherlv_12= '{' ( (lv_params_13_0= ruleParam ) ) (otherlv_14= ',' ( (lv_params_15_0= ruleParam ) ) )* otherlv_16= '}' )? otherlv_17= '}' )
            // InternalAorta.g:5389:3: () otherlv_1= 'Then' otherlv_2= '{' (otherlv_3= 'text' ( ( ruleQualifiedName ) ) )? (otherlv_5= 'regulatoryTextLinks' otherlv_6= '(' ( ( ruleQualifiedName ) ) (otherlv_8= ',' ( ( ruleQualifiedName ) ) )* otherlv_10= ')' )? (otherlv_11= 'params' otherlv_12= '{' ( (lv_params_13_0= ruleParam ) ) (otherlv_14= ',' ( (lv_params_15_0= ruleParam ) ) )* otherlv_16= '}' )? otherlv_17= '}'
            {
            // InternalAorta.g:5389:3: ()
            // InternalAorta.g:5390:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getThenAccess().getThenAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,106,FOLLOW_3); 

            			newLeafNode(otherlv_1, grammarAccess.getThenAccess().getThenKeyword_1());
            		
            otherlv_2=(Token)match(input,12,FOLLOW_142); 

            			newLeafNode(otherlv_2, grammarAccess.getThenAccess().getLeftCurlyBracketKeyword_2());
            		
            // InternalAorta.g:5404:3: (otherlv_3= 'text' ( ( ruleQualifiedName ) ) )?
            int alt136=2;
            int LA136_0 = input.LA(1);

            if ( (LA136_0==83) ) {
                alt136=1;
            }
            switch (alt136) {
                case 1 :
                    // InternalAorta.g:5405:4: otherlv_3= 'text' ( ( ruleQualifiedName ) )
                    {
                    otherlv_3=(Token)match(input,83,FOLLOW_70); 

                    				newLeafNode(otherlv_3, grammarAccess.getThenAccess().getTextKeyword_3_0());
                    			
                    // InternalAorta.g:5409:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:5410:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:5410:5: ( ruleQualifiedName )
                    // InternalAorta.g:5411:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getThenRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getThenAccess().getTextClauseTextCrossReference_3_1_0());
                    					
                    pushFollow(FOLLOW_143);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:5426:3: (otherlv_5= 'regulatoryTextLinks' otherlv_6= '(' ( ( ruleQualifiedName ) ) (otherlv_8= ',' ( ( ruleQualifiedName ) ) )* otherlv_10= ')' )?
            int alt138=2;
            int LA138_0 = input.LA(1);

            if ( (LA138_0==101) ) {
                alt138=1;
            }
            switch (alt138) {
                case 1 :
                    // InternalAorta.g:5427:4: otherlv_5= 'regulatoryTextLinks' otherlv_6= '(' ( ( ruleQualifiedName ) ) (otherlv_8= ',' ( ( ruleQualifiedName ) ) )* otherlv_10= ')'
                    {
                    otherlv_5=(Token)match(input,101,FOLLOW_144); 

                    				newLeafNode(otherlv_5, grammarAccess.getThenAccess().getRegulatoryTextLinksKeyword_4_0());
                    			
                    otherlv_6=(Token)match(input,102,FOLLOW_70); 

                    				newLeafNode(otherlv_6, grammarAccess.getThenAccess().getLeftParenthesisKeyword_4_1());
                    			
                    // InternalAorta.g:5435:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:5436:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:5436:5: ( ruleQualifiedName )
                    // InternalAorta.g:5437:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getThenRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getThenAccess().getRegulatoryTextLinksTitledRequirementsSectionCrossReference_4_2_0());
                    					
                    pushFollow(FOLLOW_145);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:5451:4: (otherlv_8= ',' ( ( ruleQualifiedName ) ) )*
                    loop137:
                    do {
                        int alt137=2;
                        int LA137_0 = input.LA(1);

                        if ( (LA137_0==31) ) {
                            alt137=1;
                        }


                        switch (alt137) {
                    	case 1 :
                    	    // InternalAorta.g:5452:5: otherlv_8= ',' ( ( ruleQualifiedName ) )
                    	    {
                    	    otherlv_8=(Token)match(input,31,FOLLOW_70); 

                    	    					newLeafNode(otherlv_8, grammarAccess.getThenAccess().getCommaKeyword_4_3_0());
                    	    				
                    	    // InternalAorta.g:5456:5: ( ( ruleQualifiedName ) )
                    	    // InternalAorta.g:5457:6: ( ruleQualifiedName )
                    	    {
                    	    // InternalAorta.g:5457:6: ( ruleQualifiedName )
                    	    // InternalAorta.g:5458:7: ruleQualifiedName
                    	    {

                    	    							if (current==null) {
                    	    								current = createModelElement(grammarAccess.getThenRule());
                    	    							}
                    	    						

                    	    							newCompositeNode(grammarAccess.getThenAccess().getRegulatoryTextLinksTitledRequirementsSectionCrossReference_4_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_145);
                    	    ruleQualifiedName();

                    	    state._fsp--;


                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop137;
                        }
                    } while (true);

                    otherlv_10=(Token)match(input,103,FOLLOW_146); 

                    				newLeafNode(otherlv_10, grammarAccess.getThenAccess().getRightParenthesisKeyword_4_4());
                    			

                    }
                    break;

            }

            // InternalAorta.g:5478:3: (otherlv_11= 'params' otherlv_12= '{' ( (lv_params_13_0= ruleParam ) ) (otherlv_14= ',' ( (lv_params_15_0= ruleParam ) ) )* otherlv_16= '}' )?
            int alt140=2;
            int LA140_0 = input.LA(1);

            if ( (LA140_0==104) ) {
                alt140=1;
            }
            switch (alt140) {
                case 1 :
                    // InternalAorta.g:5479:4: otherlv_11= 'params' otherlv_12= '{' ( (lv_params_13_0= ruleParam ) ) (otherlv_14= ',' ( (lv_params_15_0= ruleParam ) ) )* otherlv_16= '}'
                    {
                    otherlv_11=(Token)match(input,104,FOLLOW_3); 

                    				newLeafNode(otherlv_11, grammarAccess.getThenAccess().getParamsKeyword_5_0());
                    			
                    otherlv_12=(Token)match(input,12,FOLLOW_91); 

                    				newLeafNode(otherlv_12, grammarAccess.getThenAccess().getLeftCurlyBracketKeyword_5_1());
                    			
                    // InternalAorta.g:5487:4: ( (lv_params_13_0= ruleParam ) )
                    // InternalAorta.g:5488:5: (lv_params_13_0= ruleParam )
                    {
                    // InternalAorta.g:5488:5: (lv_params_13_0= ruleParam )
                    // InternalAorta.g:5489:6: lv_params_13_0= ruleParam
                    {

                    						newCompositeNode(grammarAccess.getThenAccess().getParamsParamParserRuleCall_5_2_0());
                    					
                    pushFollow(FOLLOW_35);
                    lv_params_13_0=ruleParam();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getThenRule());
                    						}
                    						add(
                    							current,
                    							"params",
                    							lv_params_13_0,
                    							"org.eclipse.efbt.aorta.Aorta.Param");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:5506:4: (otherlv_14= ',' ( (lv_params_15_0= ruleParam ) ) )*
                    loop139:
                    do {
                        int alt139=2;
                        int LA139_0 = input.LA(1);

                        if ( (LA139_0==31) ) {
                            alt139=1;
                        }


                        switch (alt139) {
                    	case 1 :
                    	    // InternalAorta.g:5507:5: otherlv_14= ',' ( (lv_params_15_0= ruleParam ) )
                    	    {
                    	    otherlv_14=(Token)match(input,31,FOLLOW_91); 

                    	    					newLeafNode(otherlv_14, grammarAccess.getThenAccess().getCommaKeyword_5_3_0());
                    	    				
                    	    // InternalAorta.g:5511:5: ( (lv_params_15_0= ruleParam ) )
                    	    // InternalAorta.g:5512:6: (lv_params_15_0= ruleParam )
                    	    {
                    	    // InternalAorta.g:5512:6: (lv_params_15_0= ruleParam )
                    	    // InternalAorta.g:5513:7: lv_params_15_0= ruleParam
                    	    {

                    	    							newCompositeNode(grammarAccess.getThenAccess().getParamsParamParserRuleCall_5_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_35);
                    	    lv_params_15_0=ruleParam();

                    	    state._fsp--;


                    	    							if (current==null) {
                    	    								current = createModelElementForParent(grammarAccess.getThenRule());
                    	    							}
                    	    							add(
                    	    								current,
                    	    								"params",
                    	    								lv_params_15_0,
                    	    								"org.eclipse.efbt.aorta.Aorta.Param");
                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop139;
                        }
                    } while (true);

                    otherlv_16=(Token)match(input,23,FOLLOW_24); 

                    				newLeafNode(otherlv_16, grammarAccess.getThenAccess().getRightCurlyBracketKeyword_5_4());
                    			

                    }
                    break;

            }

            otherlv_17=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_17, grammarAccess.getThenAccess().getRightCurlyBracketKeyword_6());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleThen"


    // $ANTLR start "entryRuleE2ETestDefinition"
    // InternalAorta.g:5544:1: entryRuleE2ETestDefinition returns [EObject current=null] : iv_ruleE2ETestDefinition= ruleE2ETestDefinition EOF ;
    public final EObject entryRuleE2ETestDefinition() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleE2ETestDefinition = null;


        try {
            // InternalAorta.g:5544:58: (iv_ruleE2ETestDefinition= ruleE2ETestDefinition EOF )
            // InternalAorta.g:5545:2: iv_ruleE2ETestDefinition= ruleE2ETestDefinition EOF
            {
             newCompositeNode(grammarAccess.getE2ETestDefinitionRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleE2ETestDefinition=ruleE2ETestDefinition();

            state._fsp--;

             current =iv_ruleE2ETestDefinition; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleE2ETestDefinition"


    // $ANTLR start "ruleE2ETestDefinition"
    // InternalAorta.g:5551:1: ruleE2ETestDefinition returns [EObject current=null] : ( () otherlv_1= 'E2ETestDefinition' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'testContraints' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'given' ( (lv_given_7_0= ruleGiven ) ) )? (otherlv_8= 'when' ( (lv_when_9_0= ruleWhen ) ) )? (otherlv_10= 'then' ( (lv_then_11_0= ruleThen ) ) )? otherlv_12= '}' ) ;
    public final EObject ruleE2ETestDefinition() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_6=null;
        Token otherlv_8=null;
        Token otherlv_10=null;
        Token otherlv_12=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;

        EObject lv_given_7_0 = null;

        EObject lv_when_9_0 = null;

        EObject lv_then_11_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:5557:2: ( ( () otherlv_1= 'E2ETestDefinition' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'testContraints' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'given' ( (lv_given_7_0= ruleGiven ) ) )? (otherlv_8= 'when' ( (lv_when_9_0= ruleWhen ) ) )? (otherlv_10= 'then' ( (lv_then_11_0= ruleThen ) ) )? otherlv_12= '}' ) )
            // InternalAorta.g:5558:2: ( () otherlv_1= 'E2ETestDefinition' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'testContraints' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'given' ( (lv_given_7_0= ruleGiven ) ) )? (otherlv_8= 'when' ( (lv_when_9_0= ruleWhen ) ) )? (otherlv_10= 'then' ( (lv_then_11_0= ruleThen ) ) )? otherlv_12= '}' )
            {
            // InternalAorta.g:5558:2: ( () otherlv_1= 'E2ETestDefinition' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'testContraints' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'given' ( (lv_given_7_0= ruleGiven ) ) )? (otherlv_8= 'when' ( (lv_when_9_0= ruleWhen ) ) )? (otherlv_10= 'then' ( (lv_then_11_0= ruleThen ) ) )? otherlv_12= '}' )
            // InternalAorta.g:5559:3: () otherlv_1= 'E2ETestDefinition' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'testContraints' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'given' ( (lv_given_7_0= ruleGiven ) ) )? (otherlv_8= 'when' ( (lv_when_9_0= ruleWhen ) ) )? (otherlv_10= 'then' ( (lv_then_11_0= ruleThen ) ) )? otherlv_12= '}'
            {
            // InternalAorta.g:5559:3: ()
            // InternalAorta.g:5560:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getE2ETestDefinitionAccess().getE2ETestDefinitionAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,107,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getE2ETestDefinitionAccess().getE2ETestDefinitionKeyword_1());
            		
            // InternalAorta.g:5570:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:5571:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:5571:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:5572:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getE2ETestDefinitionAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getE2ETestDefinitionRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_135); 

            			newLeafNode(otherlv_3, grammarAccess.getE2ETestDefinitionAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:5593:3: (otherlv_4= 'testContraints' ( ( ruleQualifiedName ) ) )?
            int alt141=2;
            int LA141_0 = input.LA(1);

            if ( (LA141_0==96) ) {
                alt141=1;
            }
            switch (alt141) {
                case 1 :
                    // InternalAorta.g:5594:4: otherlv_4= 'testContraints' ( ( ruleQualifiedName ) )
                    {
                    otherlv_4=(Token)match(input,96,FOLLOW_70); 

                    				newLeafNode(otherlv_4, grammarAccess.getE2ETestDefinitionAccess().getTestContraintsKeyword_4_0());
                    			
                    // InternalAorta.g:5598:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:5599:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:5599:5: ( ruleQualifiedName )
                    // InternalAorta.g:5600:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getE2ETestDefinitionRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getE2ETestDefinitionAccess().getTestContraintsTestContraintsCrossReference_4_1_0());
                    					
                    pushFollow(FOLLOW_136);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:5615:3: (otherlv_6= 'given' ( (lv_given_7_0= ruleGiven ) ) )?
            int alt142=2;
            int LA142_0 = input.LA(1);

            if ( (LA142_0==97) ) {
                alt142=1;
            }
            switch (alt142) {
                case 1 :
                    // InternalAorta.g:5616:4: otherlv_6= 'given' ( (lv_given_7_0= ruleGiven ) )
                    {
                    otherlv_6=(Token)match(input,97,FOLLOW_137); 

                    				newLeafNode(otherlv_6, grammarAccess.getE2ETestDefinitionAccess().getGivenKeyword_5_0());
                    			
                    // InternalAorta.g:5620:4: ( (lv_given_7_0= ruleGiven ) )
                    // InternalAorta.g:5621:5: (lv_given_7_0= ruleGiven )
                    {
                    // InternalAorta.g:5621:5: (lv_given_7_0= ruleGiven )
                    // InternalAorta.g:5622:6: lv_given_7_0= ruleGiven
                    {

                    						newCompositeNode(grammarAccess.getE2ETestDefinitionAccess().getGivenGivenParserRuleCall_5_1_0());
                    					
                    pushFollow(FOLLOW_138);
                    lv_given_7_0=ruleGiven();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getE2ETestDefinitionRule());
                    						}
                    						set(
                    							current,
                    							"given",
                    							lv_given_7_0,
                    							"org.eclipse.efbt.aorta.Aorta.Given");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:5640:3: (otherlv_8= 'when' ( (lv_when_9_0= ruleWhen ) ) )?
            int alt143=2;
            int LA143_0 = input.LA(1);

            if ( (LA143_0==98) ) {
                alt143=1;
            }
            switch (alt143) {
                case 1 :
                    // InternalAorta.g:5641:4: otherlv_8= 'when' ( (lv_when_9_0= ruleWhen ) )
                    {
                    otherlv_8=(Token)match(input,98,FOLLOW_139); 

                    				newLeafNode(otherlv_8, grammarAccess.getE2ETestDefinitionAccess().getWhenKeyword_6_0());
                    			
                    // InternalAorta.g:5645:4: ( (lv_when_9_0= ruleWhen ) )
                    // InternalAorta.g:5646:5: (lv_when_9_0= ruleWhen )
                    {
                    // InternalAorta.g:5646:5: (lv_when_9_0= ruleWhen )
                    // InternalAorta.g:5647:6: lv_when_9_0= ruleWhen
                    {

                    						newCompositeNode(grammarAccess.getE2ETestDefinitionAccess().getWhenWhenParserRuleCall_6_1_0());
                    					
                    pushFollow(FOLLOW_140);
                    lv_when_9_0=ruleWhen();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getE2ETestDefinitionRule());
                    						}
                    						set(
                    							current,
                    							"when",
                    							lv_when_9_0,
                    							"org.eclipse.efbt.aorta.Aorta.When");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:5665:3: (otherlv_10= 'then' ( (lv_then_11_0= ruleThen ) ) )?
            int alt144=2;
            int LA144_0 = input.LA(1);

            if ( (LA144_0==99) ) {
                alt144=1;
            }
            switch (alt144) {
                case 1 :
                    // InternalAorta.g:5666:4: otherlv_10= 'then' ( (lv_then_11_0= ruleThen ) )
                    {
                    otherlv_10=(Token)match(input,99,FOLLOW_141); 

                    				newLeafNode(otherlv_10, grammarAccess.getE2ETestDefinitionAccess().getThenKeyword_7_0());
                    			
                    // InternalAorta.g:5670:4: ( (lv_then_11_0= ruleThen ) )
                    // InternalAorta.g:5671:5: (lv_then_11_0= ruleThen )
                    {
                    // InternalAorta.g:5671:5: (lv_then_11_0= ruleThen )
                    // InternalAorta.g:5672:6: lv_then_11_0= ruleThen
                    {

                    						newCompositeNode(grammarAccess.getE2ETestDefinitionAccess().getThenThenParserRuleCall_7_1_0());
                    					
                    pushFollow(FOLLOW_24);
                    lv_then_11_0=ruleThen();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getE2ETestDefinitionRule());
                    						}
                    						set(
                    							current,
                    							"then",
                    							lv_then_11_0,
                    							"org.eclipse.efbt.aorta.Aorta.Then");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            otherlv_12=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_12, grammarAccess.getE2ETestDefinitionAccess().getRightCurlyBracketKeyword_8());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleE2ETestDefinition"


    // $ANTLR start "entryRuleUnitTestDefinition"
    // InternalAorta.g:5698:1: entryRuleUnitTestDefinition returns [EObject current=null] : iv_ruleUnitTestDefinition= ruleUnitTestDefinition EOF ;
    public final EObject entryRuleUnitTestDefinition() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleUnitTestDefinition = null;


        try {
            // InternalAorta.g:5698:59: (iv_ruleUnitTestDefinition= ruleUnitTestDefinition EOF )
            // InternalAorta.g:5699:2: iv_ruleUnitTestDefinition= ruleUnitTestDefinition EOF
            {
             newCompositeNode(grammarAccess.getUnitTestDefinitionRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleUnitTestDefinition=ruleUnitTestDefinition();

            state._fsp--;

             current =iv_ruleUnitTestDefinition; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleUnitTestDefinition"


    // $ANTLR start "ruleUnitTestDefinition"
    // InternalAorta.g:5705:1: ruleUnitTestDefinition returns [EObject current=null] : ( () otherlv_1= 'UnitTestDefinition' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'testContraints' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'given' ( (lv_given_7_0= ruleGiven ) ) )? (otherlv_8= 'when' ( (lv_when_9_0= ruleWhen ) ) )? (otherlv_10= 'then' ( (lv_then_11_0= ruleThen ) ) )? otherlv_12= '}' ) ;
    public final EObject ruleUnitTestDefinition() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_6=null;
        Token otherlv_8=null;
        Token otherlv_10=null;
        Token otherlv_12=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;

        EObject lv_given_7_0 = null;

        EObject lv_when_9_0 = null;

        EObject lv_then_11_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:5711:2: ( ( () otherlv_1= 'UnitTestDefinition' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'testContraints' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'given' ( (lv_given_7_0= ruleGiven ) ) )? (otherlv_8= 'when' ( (lv_when_9_0= ruleWhen ) ) )? (otherlv_10= 'then' ( (lv_then_11_0= ruleThen ) ) )? otherlv_12= '}' ) )
            // InternalAorta.g:5712:2: ( () otherlv_1= 'UnitTestDefinition' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'testContraints' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'given' ( (lv_given_7_0= ruleGiven ) ) )? (otherlv_8= 'when' ( (lv_when_9_0= ruleWhen ) ) )? (otherlv_10= 'then' ( (lv_then_11_0= ruleThen ) ) )? otherlv_12= '}' )
            {
            // InternalAorta.g:5712:2: ( () otherlv_1= 'UnitTestDefinition' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'testContraints' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'given' ( (lv_given_7_0= ruleGiven ) ) )? (otherlv_8= 'when' ( (lv_when_9_0= ruleWhen ) ) )? (otherlv_10= 'then' ( (lv_then_11_0= ruleThen ) ) )? otherlv_12= '}' )
            // InternalAorta.g:5713:3: () otherlv_1= 'UnitTestDefinition' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'testContraints' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'given' ( (lv_given_7_0= ruleGiven ) ) )? (otherlv_8= 'when' ( (lv_when_9_0= ruleWhen ) ) )? (otherlv_10= 'then' ( (lv_then_11_0= ruleThen ) ) )? otherlv_12= '}'
            {
            // InternalAorta.g:5713:3: ()
            // InternalAorta.g:5714:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getUnitTestDefinitionAccess().getUnitTestDefinitionAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,108,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getUnitTestDefinitionAccess().getUnitTestDefinitionKeyword_1());
            		
            // InternalAorta.g:5724:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:5725:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:5725:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:5726:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getUnitTestDefinitionAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getUnitTestDefinitionRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_135); 

            			newLeafNode(otherlv_3, grammarAccess.getUnitTestDefinitionAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:5747:3: (otherlv_4= 'testContraints' ( ( ruleQualifiedName ) ) )?
            int alt145=2;
            int LA145_0 = input.LA(1);

            if ( (LA145_0==96) ) {
                alt145=1;
            }
            switch (alt145) {
                case 1 :
                    // InternalAorta.g:5748:4: otherlv_4= 'testContraints' ( ( ruleQualifiedName ) )
                    {
                    otherlv_4=(Token)match(input,96,FOLLOW_70); 

                    				newLeafNode(otherlv_4, grammarAccess.getUnitTestDefinitionAccess().getTestContraintsKeyword_4_0());
                    			
                    // InternalAorta.g:5752:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:5753:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:5753:5: ( ruleQualifiedName )
                    // InternalAorta.g:5754:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getUnitTestDefinitionRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getUnitTestDefinitionAccess().getTestContraintsTestContraintsCrossReference_4_1_0());
                    					
                    pushFollow(FOLLOW_136);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:5769:3: (otherlv_6= 'given' ( (lv_given_7_0= ruleGiven ) ) )?
            int alt146=2;
            int LA146_0 = input.LA(1);

            if ( (LA146_0==97) ) {
                alt146=1;
            }
            switch (alt146) {
                case 1 :
                    // InternalAorta.g:5770:4: otherlv_6= 'given' ( (lv_given_7_0= ruleGiven ) )
                    {
                    otherlv_6=(Token)match(input,97,FOLLOW_137); 

                    				newLeafNode(otherlv_6, grammarAccess.getUnitTestDefinitionAccess().getGivenKeyword_5_0());
                    			
                    // InternalAorta.g:5774:4: ( (lv_given_7_0= ruleGiven ) )
                    // InternalAorta.g:5775:5: (lv_given_7_0= ruleGiven )
                    {
                    // InternalAorta.g:5775:5: (lv_given_7_0= ruleGiven )
                    // InternalAorta.g:5776:6: lv_given_7_0= ruleGiven
                    {

                    						newCompositeNode(grammarAccess.getUnitTestDefinitionAccess().getGivenGivenParserRuleCall_5_1_0());
                    					
                    pushFollow(FOLLOW_138);
                    lv_given_7_0=ruleGiven();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getUnitTestDefinitionRule());
                    						}
                    						set(
                    							current,
                    							"given",
                    							lv_given_7_0,
                    							"org.eclipse.efbt.aorta.Aorta.Given");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:5794:3: (otherlv_8= 'when' ( (lv_when_9_0= ruleWhen ) ) )?
            int alt147=2;
            int LA147_0 = input.LA(1);

            if ( (LA147_0==98) ) {
                alt147=1;
            }
            switch (alt147) {
                case 1 :
                    // InternalAorta.g:5795:4: otherlv_8= 'when' ( (lv_when_9_0= ruleWhen ) )
                    {
                    otherlv_8=(Token)match(input,98,FOLLOW_139); 

                    				newLeafNode(otherlv_8, grammarAccess.getUnitTestDefinitionAccess().getWhenKeyword_6_0());
                    			
                    // InternalAorta.g:5799:4: ( (lv_when_9_0= ruleWhen ) )
                    // InternalAorta.g:5800:5: (lv_when_9_0= ruleWhen )
                    {
                    // InternalAorta.g:5800:5: (lv_when_9_0= ruleWhen )
                    // InternalAorta.g:5801:6: lv_when_9_0= ruleWhen
                    {

                    						newCompositeNode(grammarAccess.getUnitTestDefinitionAccess().getWhenWhenParserRuleCall_6_1_0());
                    					
                    pushFollow(FOLLOW_140);
                    lv_when_9_0=ruleWhen();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getUnitTestDefinitionRule());
                    						}
                    						set(
                    							current,
                    							"when",
                    							lv_when_9_0,
                    							"org.eclipse.efbt.aorta.Aorta.When");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:5819:3: (otherlv_10= 'then' ( (lv_then_11_0= ruleThen ) ) )?
            int alt148=2;
            int LA148_0 = input.LA(1);

            if ( (LA148_0==99) ) {
                alt148=1;
            }
            switch (alt148) {
                case 1 :
                    // InternalAorta.g:5820:4: otherlv_10= 'then' ( (lv_then_11_0= ruleThen ) )
                    {
                    otherlv_10=(Token)match(input,99,FOLLOW_141); 

                    				newLeafNode(otherlv_10, grammarAccess.getUnitTestDefinitionAccess().getThenKeyword_7_0());
                    			
                    // InternalAorta.g:5824:4: ( (lv_then_11_0= ruleThen ) )
                    // InternalAorta.g:5825:5: (lv_then_11_0= ruleThen )
                    {
                    // InternalAorta.g:5825:5: (lv_then_11_0= ruleThen )
                    // InternalAorta.g:5826:6: lv_then_11_0= ruleThen
                    {

                    						newCompositeNode(grammarAccess.getUnitTestDefinitionAccess().getThenThenParserRuleCall_7_1_0());
                    					
                    pushFollow(FOLLOW_24);
                    lv_then_11_0=ruleThen();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getUnitTestDefinitionRule());
                    						}
                    						set(
                    							current,
                    							"then",
                    							lv_then_11_0,
                    							"org.eclipse.efbt.aorta.Aorta.Then");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            otherlv_12=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_12, grammarAccess.getUnitTestDefinitionAccess().getRightCurlyBracketKeyword_8());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleUnitTestDefinition"


    // $ANTLR start "entryRuleClauseText"
    // InternalAorta.g:5852:1: entryRuleClauseText returns [EObject current=null] : iv_ruleClauseText= ruleClauseText EOF ;
    public final EObject entryRuleClauseText() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleClauseText = null;


        try {
            // InternalAorta.g:5852:51: (iv_ruleClauseText= ruleClauseText EOF )
            // InternalAorta.g:5853:2: iv_ruleClauseText= ruleClauseText EOF
            {
             newCompositeNode(grammarAccess.getClauseTextRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleClauseText=ruleClauseText();

            state._fsp--;

             current =iv_ruleClauseText; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleClauseText"


    // $ANTLR start "ruleClauseText"
    // InternalAorta.g:5859:1: ruleClauseText returns [EObject current=null] : ( () otherlv_1= 'ClauseText' ( (lv_name_2_0= ruleEString ) ) ) ;
    public final EObject ruleClauseText() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:5865:2: ( ( () otherlv_1= 'ClauseText' ( (lv_name_2_0= ruleEString ) ) ) )
            // InternalAorta.g:5866:2: ( () otherlv_1= 'ClauseText' ( (lv_name_2_0= ruleEString ) ) )
            {
            // InternalAorta.g:5866:2: ( () otherlv_1= 'ClauseText' ( (lv_name_2_0= ruleEString ) ) )
            // InternalAorta.g:5867:3: () otherlv_1= 'ClauseText' ( (lv_name_2_0= ruleEString ) )
            {
            // InternalAorta.g:5867:3: ()
            // InternalAorta.g:5868:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getClauseTextAccess().getClauseTextAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,109,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getClauseTextAccess().getClauseTextKeyword_1());
            		
            // InternalAorta.g:5878:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:5879:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:5879:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:5880:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getClauseTextAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_2);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getClauseTextRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }


            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleClauseText"


    // $ANTLR start "entryRuleParam"
    // InternalAorta.g:5901:1: entryRuleParam returns [EObject current=null] : iv_ruleParam= ruleParam EOF ;
    public final EObject entryRuleParam() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleParam = null;


        try {
            // InternalAorta.g:5901:46: (iv_ruleParam= ruleParam EOF )
            // InternalAorta.g:5902:2: iv_ruleParam= ruleParam EOF
            {
             newCompositeNode(grammarAccess.getParamRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleParam=ruleParam();

            state._fsp--;

             current =iv_ruleParam; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleParam"


    // $ANTLR start "ruleParam"
    // InternalAorta.g:5908:1: ruleParam returns [EObject current=null] : ( () otherlv_1= 'Param' otherlv_2= '{' (otherlv_3= 'param' ( ( ruleQualifiedName ) ) )? otherlv_5= '}' ) ;
    public final EObject ruleParam() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_2=null;
        Token otherlv_3=null;
        Token otherlv_5=null;


        	enterRule();

        try {
            // InternalAorta.g:5914:2: ( ( () otherlv_1= 'Param' otherlv_2= '{' (otherlv_3= 'param' ( ( ruleQualifiedName ) ) )? otherlv_5= '}' ) )
            // InternalAorta.g:5915:2: ( () otherlv_1= 'Param' otherlv_2= '{' (otherlv_3= 'param' ( ( ruleQualifiedName ) ) )? otherlv_5= '}' )
            {
            // InternalAorta.g:5915:2: ( () otherlv_1= 'Param' otherlv_2= '{' (otherlv_3= 'param' ( ( ruleQualifiedName ) ) )? otherlv_5= '}' )
            // InternalAorta.g:5916:3: () otherlv_1= 'Param' otherlv_2= '{' (otherlv_3= 'param' ( ( ruleQualifiedName ) ) )? otherlv_5= '}'
            {
            // InternalAorta.g:5916:3: ()
            // InternalAorta.g:5917:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getParamAccess().getParamAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,110,FOLLOW_3); 

            			newLeafNode(otherlv_1, grammarAccess.getParamAccess().getParamKeyword_1());
            		
            otherlv_2=(Token)match(input,12,FOLLOW_147); 

            			newLeafNode(otherlv_2, grammarAccess.getParamAccess().getLeftCurlyBracketKeyword_2());
            		
            // InternalAorta.g:5931:3: (otherlv_3= 'param' ( ( ruleQualifiedName ) ) )?
            int alt149=2;
            int LA149_0 = input.LA(1);

            if ( (LA149_0==111) ) {
                alt149=1;
            }
            switch (alt149) {
                case 1 :
                    // InternalAorta.g:5932:4: otherlv_3= 'param' ( ( ruleQualifiedName ) )
                    {
                    otherlv_3=(Token)match(input,111,FOLLOW_70); 

                    				newLeafNode(otherlv_3, grammarAccess.getParamAccess().getParamKeyword_3_0());
                    			
                    // InternalAorta.g:5936:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:5937:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:5937:5: ( ruleQualifiedName )
                    // InternalAorta.g:5938:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getParamRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getParamAccess().getParamEObjectCrossReference_3_1_0());
                    					
                    pushFollow(FOLLOW_24);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            otherlv_5=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_5, grammarAccess.getParamAccess().getRightCurlyBracketKeyword_4());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleParam"


    // $ANTLR start "entryRuleE2ETest"
    // InternalAorta.g:5961:1: entryRuleE2ETest returns [EObject current=null] : iv_ruleE2ETest= ruleE2ETest EOF ;
    public final EObject entryRuleE2ETest() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleE2ETest = null;


        try {
            // InternalAorta.g:5961:48: (iv_ruleE2ETest= ruleE2ETest EOF )
            // InternalAorta.g:5962:2: iv_ruleE2ETest= ruleE2ETest EOF
            {
             newCompositeNode(grammarAccess.getE2ETestRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleE2ETest=ruleE2ETest();

            state._fsp--;

             current =iv_ruleE2ETest; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleE2ETest"


    // $ANTLR start "ruleE2ETest"
    // InternalAorta.g:5968:1: ruleE2ETest returns [EObject current=null] : ( () otherlv_1= 'E2ETest' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'testDefinition' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'inputData' ( (lv_inputData_7_0= ruleTestInputData ) ) )? (otherlv_8= 'expectedResults' ( (lv_expectedResults_9_0= ruleResult ) ) )? otherlv_10= '}' ) ;
    public final EObject ruleE2ETest() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_6=null;
        Token otherlv_8=null;
        Token otherlv_10=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;

        EObject lv_inputData_7_0 = null;

        EObject lv_expectedResults_9_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:5974:2: ( ( () otherlv_1= 'E2ETest' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'testDefinition' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'inputData' ( (lv_inputData_7_0= ruleTestInputData ) ) )? (otherlv_8= 'expectedResults' ( (lv_expectedResults_9_0= ruleResult ) ) )? otherlv_10= '}' ) )
            // InternalAorta.g:5975:2: ( () otherlv_1= 'E2ETest' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'testDefinition' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'inputData' ( (lv_inputData_7_0= ruleTestInputData ) ) )? (otherlv_8= 'expectedResults' ( (lv_expectedResults_9_0= ruleResult ) ) )? otherlv_10= '}' )
            {
            // InternalAorta.g:5975:2: ( () otherlv_1= 'E2ETest' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'testDefinition' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'inputData' ( (lv_inputData_7_0= ruleTestInputData ) ) )? (otherlv_8= 'expectedResults' ( (lv_expectedResults_9_0= ruleResult ) ) )? otherlv_10= '}' )
            // InternalAorta.g:5976:3: () otherlv_1= 'E2ETest' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'testDefinition' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'inputData' ( (lv_inputData_7_0= ruleTestInputData ) ) )? (otherlv_8= 'expectedResults' ( (lv_expectedResults_9_0= ruleResult ) ) )? otherlv_10= '}'
            {
            // InternalAorta.g:5976:3: ()
            // InternalAorta.g:5977:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getE2ETestAccess().getE2ETestAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,112,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getE2ETestAccess().getE2ETestKeyword_1());
            		
            // InternalAorta.g:5987:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:5988:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:5988:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:5989:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getE2ETestAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getE2ETestRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_148); 

            			newLeafNode(otherlv_3, grammarAccess.getE2ETestAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:6010:3: (otherlv_4= 'testDefinition' ( ( ruleQualifiedName ) ) )?
            int alt150=2;
            int LA150_0 = input.LA(1);

            if ( (LA150_0==113) ) {
                alt150=1;
            }
            switch (alt150) {
                case 1 :
                    // InternalAorta.g:6011:4: otherlv_4= 'testDefinition' ( ( ruleQualifiedName ) )
                    {
                    otherlv_4=(Token)match(input,113,FOLLOW_70); 

                    				newLeafNode(otherlv_4, grammarAccess.getE2ETestAccess().getTestDefinitionKeyword_4_0());
                    			
                    // InternalAorta.g:6015:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:6016:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:6016:5: ( ruleQualifiedName )
                    // InternalAorta.g:6017:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getE2ETestRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getE2ETestAccess().getTestDefinitionE2ETestDefinitionCrossReference_4_1_0());
                    					
                    pushFollow(FOLLOW_149);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:6032:3: (otherlv_6= 'inputData' ( (lv_inputData_7_0= ruleTestInputData ) ) )?
            int alt151=2;
            int LA151_0 = input.LA(1);

            if ( (LA151_0==114) ) {
                alt151=1;
            }
            switch (alt151) {
                case 1 :
                    // InternalAorta.g:6033:4: otherlv_6= 'inputData' ( (lv_inputData_7_0= ruleTestInputData ) )
                    {
                    otherlv_6=(Token)match(input,114,FOLLOW_150); 

                    				newLeafNode(otherlv_6, grammarAccess.getE2ETestAccess().getInputDataKeyword_5_0());
                    			
                    // InternalAorta.g:6037:4: ( (lv_inputData_7_0= ruleTestInputData ) )
                    // InternalAorta.g:6038:5: (lv_inputData_7_0= ruleTestInputData )
                    {
                    // InternalAorta.g:6038:5: (lv_inputData_7_0= ruleTestInputData )
                    // InternalAorta.g:6039:6: lv_inputData_7_0= ruleTestInputData
                    {

                    						newCompositeNode(grammarAccess.getE2ETestAccess().getInputDataTestInputDataParserRuleCall_5_1_0());
                    					
                    pushFollow(FOLLOW_151);
                    lv_inputData_7_0=ruleTestInputData();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getE2ETestRule());
                    						}
                    						set(
                    							current,
                    							"inputData",
                    							lv_inputData_7_0,
                    							"org.eclipse.efbt.aorta.Aorta.TestInputData");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:6057:3: (otherlv_8= 'expectedResults' ( (lv_expectedResults_9_0= ruleResult ) ) )?
            int alt152=2;
            int LA152_0 = input.LA(1);

            if ( (LA152_0==115) ) {
                alt152=1;
            }
            switch (alt152) {
                case 1 :
                    // InternalAorta.g:6058:4: otherlv_8= 'expectedResults' ( (lv_expectedResults_9_0= ruleResult ) )
                    {
                    otherlv_8=(Token)match(input,115,FOLLOW_152); 

                    				newLeafNode(otherlv_8, grammarAccess.getE2ETestAccess().getExpectedResultsKeyword_6_0());
                    			
                    // InternalAorta.g:6062:4: ( (lv_expectedResults_9_0= ruleResult ) )
                    // InternalAorta.g:6063:5: (lv_expectedResults_9_0= ruleResult )
                    {
                    // InternalAorta.g:6063:5: (lv_expectedResults_9_0= ruleResult )
                    // InternalAorta.g:6064:6: lv_expectedResults_9_0= ruleResult
                    {

                    						newCompositeNode(grammarAccess.getE2ETestAccess().getExpectedResultsResultParserRuleCall_6_1_0());
                    					
                    pushFollow(FOLLOW_24);
                    lv_expectedResults_9_0=ruleResult();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getE2ETestRule());
                    						}
                    						set(
                    							current,
                    							"expectedResults",
                    							lv_expectedResults_9_0,
                    							"org.eclipse.efbt.aorta.Aorta.Result");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            otherlv_10=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_10, grammarAccess.getE2ETestAccess().getRightCurlyBracketKeyword_7());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleE2ETest"


    // $ANTLR start "entryRuleTestInputData_Impl"
    // InternalAorta.g:6090:1: entryRuleTestInputData_Impl returns [EObject current=null] : iv_ruleTestInputData_Impl= ruleTestInputData_Impl EOF ;
    public final EObject entryRuleTestInputData_Impl() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleTestInputData_Impl = null;


        try {
            // InternalAorta.g:6090:59: (iv_ruleTestInputData_Impl= ruleTestInputData_Impl EOF )
            // InternalAorta.g:6091:2: iv_ruleTestInputData_Impl= ruleTestInputData_Impl EOF
            {
             newCompositeNode(grammarAccess.getTestInputData_ImplRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleTestInputData_Impl=ruleTestInputData_Impl();

            state._fsp--;

             current =iv_ruleTestInputData_Impl; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleTestInputData_Impl"


    // $ANTLR start "ruleTestInputData_Impl"
    // InternalAorta.g:6097:1: ruleTestInputData_Impl returns [EObject current=null] : ( () otherlv_1= 'TestInputData' ( (lv_name_2_0= ruleEString ) ) ) ;
    public final EObject ruleTestInputData_Impl() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:6103:2: ( ( () otherlv_1= 'TestInputData' ( (lv_name_2_0= ruleEString ) ) ) )
            // InternalAorta.g:6104:2: ( () otherlv_1= 'TestInputData' ( (lv_name_2_0= ruleEString ) ) )
            {
            // InternalAorta.g:6104:2: ( () otherlv_1= 'TestInputData' ( (lv_name_2_0= ruleEString ) ) )
            // InternalAorta.g:6105:3: () otherlv_1= 'TestInputData' ( (lv_name_2_0= ruleEString ) )
            {
            // InternalAorta.g:6105:3: ()
            // InternalAorta.g:6106:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getTestInputData_ImplAccess().getTestInputDataAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,116,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getTestInputData_ImplAccess().getTestInputDataKeyword_1());
            		
            // InternalAorta.g:6116:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:6117:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:6117:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:6118:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getTestInputData_ImplAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_2);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getTestInputData_ImplRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }


            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleTestInputData_Impl"


    // $ANTLR start "entryRuleResult_Impl"
    // InternalAorta.g:6139:1: entryRuleResult_Impl returns [EObject current=null] : iv_ruleResult_Impl= ruleResult_Impl EOF ;
    public final EObject entryRuleResult_Impl() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleResult_Impl = null;


        try {
            // InternalAorta.g:6139:52: (iv_ruleResult_Impl= ruleResult_Impl EOF )
            // InternalAorta.g:6140:2: iv_ruleResult_Impl= ruleResult_Impl EOF
            {
             newCompositeNode(grammarAccess.getResult_ImplRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleResult_Impl=ruleResult_Impl();

            state._fsp--;

             current =iv_ruleResult_Impl; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleResult_Impl"


    // $ANTLR start "ruleResult_Impl"
    // InternalAorta.g:6146:1: ruleResult_Impl returns [EObject current=null] : ( () otherlv_1= 'Result' ) ;
    public final EObject ruleResult_Impl() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;


        	enterRule();

        try {
            // InternalAorta.g:6152:2: ( ( () otherlv_1= 'Result' ) )
            // InternalAorta.g:6153:2: ( () otherlv_1= 'Result' )
            {
            // InternalAorta.g:6153:2: ( () otherlv_1= 'Result' )
            // InternalAorta.g:6154:3: () otherlv_1= 'Result'
            {
            // InternalAorta.g:6154:3: ()
            // InternalAorta.g:6155:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getResult_ImplAccess().getResultAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,117,FOLLOW_2); 

            			newLeafNode(otherlv_1, grammarAccess.getResult_ImplAccess().getResultKeyword_1());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleResult_Impl"


    // $ANTLR start "entryRuleTestColumnStructuredData"
    // InternalAorta.g:6169:1: entryRuleTestColumnStructuredData returns [EObject current=null] : iv_ruleTestColumnStructuredData= ruleTestColumnStructuredData EOF ;
    public final EObject entryRuleTestColumnStructuredData() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleTestColumnStructuredData = null;


        try {
            // InternalAorta.g:6169:65: (iv_ruleTestColumnStructuredData= ruleTestColumnStructuredData EOF )
            // InternalAorta.g:6170:2: iv_ruleTestColumnStructuredData= ruleTestColumnStructuredData EOF
            {
             newCompositeNode(grammarAccess.getTestColumnStructuredDataRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleTestColumnStructuredData=ruleTestColumnStructuredData();

            state._fsp--;

             current =iv_ruleTestColumnStructuredData; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleTestColumnStructuredData"


    // $ANTLR start "ruleTestColumnStructuredData"
    // InternalAorta.g:6176:1: ruleTestColumnStructuredData returns [EObject current=null] : ( () otherlv_1= 'TestColumnStructuredData' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'sourceTableData' otherlv_5= '{' ( (lv_sourceTableData_6_0= ruleBaseColumnStructuredData ) ) (otherlv_7= ',' ( (lv_sourceTableData_8_0= ruleBaseColumnStructuredData ) ) )* otherlv_9= '}' )? otherlv_10= '}' ) ;
    public final EObject ruleTestColumnStructuredData() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_5=null;
        Token otherlv_7=null;
        Token otherlv_9=null;
        Token otherlv_10=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;

        EObject lv_sourceTableData_6_0 = null;

        EObject lv_sourceTableData_8_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:6182:2: ( ( () otherlv_1= 'TestColumnStructuredData' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'sourceTableData' otherlv_5= '{' ( (lv_sourceTableData_6_0= ruleBaseColumnStructuredData ) ) (otherlv_7= ',' ( (lv_sourceTableData_8_0= ruleBaseColumnStructuredData ) ) )* otherlv_9= '}' )? otherlv_10= '}' ) )
            // InternalAorta.g:6183:2: ( () otherlv_1= 'TestColumnStructuredData' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'sourceTableData' otherlv_5= '{' ( (lv_sourceTableData_6_0= ruleBaseColumnStructuredData ) ) (otherlv_7= ',' ( (lv_sourceTableData_8_0= ruleBaseColumnStructuredData ) ) )* otherlv_9= '}' )? otherlv_10= '}' )
            {
            // InternalAorta.g:6183:2: ( () otherlv_1= 'TestColumnStructuredData' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'sourceTableData' otherlv_5= '{' ( (lv_sourceTableData_6_0= ruleBaseColumnStructuredData ) ) (otherlv_7= ',' ( (lv_sourceTableData_8_0= ruleBaseColumnStructuredData ) ) )* otherlv_9= '}' )? otherlv_10= '}' )
            // InternalAorta.g:6184:3: () otherlv_1= 'TestColumnStructuredData' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'sourceTableData' otherlv_5= '{' ( (lv_sourceTableData_6_0= ruleBaseColumnStructuredData ) ) (otherlv_7= ',' ( (lv_sourceTableData_8_0= ruleBaseColumnStructuredData ) ) )* otherlv_9= '}' )? otherlv_10= '}'
            {
            // InternalAorta.g:6184:3: ()
            // InternalAorta.g:6185:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getTestColumnStructuredDataAccess().getTestColumnStructuredDataAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,118,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getTestColumnStructuredDataAccess().getTestColumnStructuredDataKeyword_1());
            		
            // InternalAorta.g:6195:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:6196:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:6196:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:6197:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getTestColumnStructuredDataAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getTestColumnStructuredDataRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_153); 

            			newLeafNode(otherlv_3, grammarAccess.getTestColumnStructuredDataAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:6218:3: (otherlv_4= 'sourceTableData' otherlv_5= '{' ( (lv_sourceTableData_6_0= ruleBaseColumnStructuredData ) ) (otherlv_7= ',' ( (lv_sourceTableData_8_0= ruleBaseColumnStructuredData ) ) )* otherlv_9= '}' )?
            int alt154=2;
            int LA154_0 = input.LA(1);

            if ( (LA154_0==119) ) {
                alt154=1;
            }
            switch (alt154) {
                case 1 :
                    // InternalAorta.g:6219:4: otherlv_4= 'sourceTableData' otherlv_5= '{' ( (lv_sourceTableData_6_0= ruleBaseColumnStructuredData ) ) (otherlv_7= ',' ( (lv_sourceTableData_8_0= ruleBaseColumnStructuredData ) ) )* otherlv_9= '}'
                    {
                    otherlv_4=(Token)match(input,119,FOLLOW_3); 

                    				newLeafNode(otherlv_4, grammarAccess.getTestColumnStructuredDataAccess().getSourceTableDataKeyword_4_0());
                    			
                    otherlv_5=(Token)match(input,12,FOLLOW_154); 

                    				newLeafNode(otherlv_5, grammarAccess.getTestColumnStructuredDataAccess().getLeftCurlyBracketKeyword_4_1());
                    			
                    // InternalAorta.g:6227:4: ( (lv_sourceTableData_6_0= ruleBaseColumnStructuredData ) )
                    // InternalAorta.g:6228:5: (lv_sourceTableData_6_0= ruleBaseColumnStructuredData )
                    {
                    // InternalAorta.g:6228:5: (lv_sourceTableData_6_0= ruleBaseColumnStructuredData )
                    // InternalAorta.g:6229:6: lv_sourceTableData_6_0= ruleBaseColumnStructuredData
                    {

                    						newCompositeNode(grammarAccess.getTestColumnStructuredDataAccess().getSourceTableDataBaseColumnStructuredDataParserRuleCall_4_2_0());
                    					
                    pushFollow(FOLLOW_35);
                    lv_sourceTableData_6_0=ruleBaseColumnStructuredData();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getTestColumnStructuredDataRule());
                    						}
                    						add(
                    							current,
                    							"sourceTableData",
                    							lv_sourceTableData_6_0,
                    							"org.eclipse.efbt.aorta.Aorta.BaseColumnStructuredData");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:6246:4: (otherlv_7= ',' ( (lv_sourceTableData_8_0= ruleBaseColumnStructuredData ) ) )*
                    loop153:
                    do {
                        int alt153=2;
                        int LA153_0 = input.LA(1);

                        if ( (LA153_0==31) ) {
                            alt153=1;
                        }


                        switch (alt153) {
                    	case 1 :
                    	    // InternalAorta.g:6247:5: otherlv_7= ',' ( (lv_sourceTableData_8_0= ruleBaseColumnStructuredData ) )
                    	    {
                    	    otherlv_7=(Token)match(input,31,FOLLOW_154); 

                    	    					newLeafNode(otherlv_7, grammarAccess.getTestColumnStructuredDataAccess().getCommaKeyword_4_3_0());
                    	    				
                    	    // InternalAorta.g:6251:5: ( (lv_sourceTableData_8_0= ruleBaseColumnStructuredData ) )
                    	    // InternalAorta.g:6252:6: (lv_sourceTableData_8_0= ruleBaseColumnStructuredData )
                    	    {
                    	    // InternalAorta.g:6252:6: (lv_sourceTableData_8_0= ruleBaseColumnStructuredData )
                    	    // InternalAorta.g:6253:7: lv_sourceTableData_8_0= ruleBaseColumnStructuredData
                    	    {

                    	    							newCompositeNode(grammarAccess.getTestColumnStructuredDataAccess().getSourceTableDataBaseColumnStructuredDataParserRuleCall_4_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_35);
                    	    lv_sourceTableData_8_0=ruleBaseColumnStructuredData();

                    	    state._fsp--;


                    	    							if (current==null) {
                    	    								current = createModelElementForParent(grammarAccess.getTestColumnStructuredDataRule());
                    	    							}
                    	    							add(
                    	    								current,
                    	    								"sourceTableData",
                    	    								lv_sourceTableData_8_0,
                    	    								"org.eclipse.efbt.aorta.Aorta.BaseColumnStructuredData");
                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop153;
                        }
                    } while (true);

                    otherlv_9=(Token)match(input,23,FOLLOW_24); 

                    				newLeafNode(otherlv_9, grammarAccess.getTestColumnStructuredDataAccess().getRightCurlyBracketKeyword_4_4());
                    			

                    }
                    break;

            }

            otherlv_10=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_10, grammarAccess.getTestColumnStructuredDataAccess().getRightCurlyBracketKeyword_5());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleTestColumnStructuredData"


    // $ANTLR start "entryRuleTestStrucuredFileData"
    // InternalAorta.g:6284:1: entryRuleTestStrucuredFileData returns [EObject current=null] : iv_ruleTestStrucuredFileData= ruleTestStrucuredFileData EOF ;
    public final EObject entryRuleTestStrucuredFileData() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleTestStrucuredFileData = null;


        try {
            // InternalAorta.g:6284:62: (iv_ruleTestStrucuredFileData= ruleTestStrucuredFileData EOF )
            // InternalAorta.g:6285:2: iv_ruleTestStrucuredFileData= ruleTestStrucuredFileData EOF
            {
             newCompositeNode(grammarAccess.getTestStrucuredFileDataRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleTestStrucuredFileData=ruleTestStrucuredFileData();

            state._fsp--;

             current =iv_ruleTestStrucuredFileData; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleTestStrucuredFileData"


    // $ANTLR start "ruleTestStrucuredFileData"
    // InternalAorta.g:6291:1: ruleTestStrucuredFileData returns [EObject current=null] : ( () otherlv_1= 'TestStrucuredFileData' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'testFileData' otherlv_5= '{' ( (lv_testFileData_6_0= ruleStructuredFile ) ) (otherlv_7= ',' ( (lv_testFileData_8_0= ruleStructuredFile ) ) )* otherlv_9= '}' )? otherlv_10= '}' ) ;
    public final EObject ruleTestStrucuredFileData() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_5=null;
        Token otherlv_7=null;
        Token otherlv_9=null;
        Token otherlv_10=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;

        EObject lv_testFileData_6_0 = null;

        EObject lv_testFileData_8_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:6297:2: ( ( () otherlv_1= 'TestStrucuredFileData' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'testFileData' otherlv_5= '{' ( (lv_testFileData_6_0= ruleStructuredFile ) ) (otherlv_7= ',' ( (lv_testFileData_8_0= ruleStructuredFile ) ) )* otherlv_9= '}' )? otherlv_10= '}' ) )
            // InternalAorta.g:6298:2: ( () otherlv_1= 'TestStrucuredFileData' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'testFileData' otherlv_5= '{' ( (lv_testFileData_6_0= ruleStructuredFile ) ) (otherlv_7= ',' ( (lv_testFileData_8_0= ruleStructuredFile ) ) )* otherlv_9= '}' )? otherlv_10= '}' )
            {
            // InternalAorta.g:6298:2: ( () otherlv_1= 'TestStrucuredFileData' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'testFileData' otherlv_5= '{' ( (lv_testFileData_6_0= ruleStructuredFile ) ) (otherlv_7= ',' ( (lv_testFileData_8_0= ruleStructuredFile ) ) )* otherlv_9= '}' )? otherlv_10= '}' )
            // InternalAorta.g:6299:3: () otherlv_1= 'TestStrucuredFileData' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'testFileData' otherlv_5= '{' ( (lv_testFileData_6_0= ruleStructuredFile ) ) (otherlv_7= ',' ( (lv_testFileData_8_0= ruleStructuredFile ) ) )* otherlv_9= '}' )? otherlv_10= '}'
            {
            // InternalAorta.g:6299:3: ()
            // InternalAorta.g:6300:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getTestStrucuredFileDataAccess().getTestStrucuredFileDataAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,120,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getTestStrucuredFileDataAccess().getTestStrucuredFileDataKeyword_1());
            		
            // InternalAorta.g:6310:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:6311:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:6311:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:6312:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getTestStrucuredFileDataAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getTestStrucuredFileDataRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_155); 

            			newLeafNode(otherlv_3, grammarAccess.getTestStrucuredFileDataAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:6333:3: (otherlv_4= 'testFileData' otherlv_5= '{' ( (lv_testFileData_6_0= ruleStructuredFile ) ) (otherlv_7= ',' ( (lv_testFileData_8_0= ruleStructuredFile ) ) )* otherlv_9= '}' )?
            int alt156=2;
            int LA156_0 = input.LA(1);

            if ( (LA156_0==121) ) {
                alt156=1;
            }
            switch (alt156) {
                case 1 :
                    // InternalAorta.g:6334:4: otherlv_4= 'testFileData' otherlv_5= '{' ( (lv_testFileData_6_0= ruleStructuredFile ) ) (otherlv_7= ',' ( (lv_testFileData_8_0= ruleStructuredFile ) ) )* otherlv_9= '}'
                    {
                    otherlv_4=(Token)match(input,121,FOLLOW_3); 

                    				newLeafNode(otherlv_4, grammarAccess.getTestStrucuredFileDataAccess().getTestFileDataKeyword_4_0());
                    			
                    otherlv_5=(Token)match(input,12,FOLLOW_156); 

                    				newLeafNode(otherlv_5, grammarAccess.getTestStrucuredFileDataAccess().getLeftCurlyBracketKeyword_4_1());
                    			
                    // InternalAorta.g:6342:4: ( (lv_testFileData_6_0= ruleStructuredFile ) )
                    // InternalAorta.g:6343:5: (lv_testFileData_6_0= ruleStructuredFile )
                    {
                    // InternalAorta.g:6343:5: (lv_testFileData_6_0= ruleStructuredFile )
                    // InternalAorta.g:6344:6: lv_testFileData_6_0= ruleStructuredFile
                    {

                    						newCompositeNode(grammarAccess.getTestStrucuredFileDataAccess().getTestFileDataStructuredFileParserRuleCall_4_2_0());
                    					
                    pushFollow(FOLLOW_35);
                    lv_testFileData_6_0=ruleStructuredFile();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getTestStrucuredFileDataRule());
                    						}
                    						add(
                    							current,
                    							"testFileData",
                    							lv_testFileData_6_0,
                    							"org.eclipse.efbt.aorta.Aorta.StructuredFile");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:6361:4: (otherlv_7= ',' ( (lv_testFileData_8_0= ruleStructuredFile ) ) )*
                    loop155:
                    do {
                        int alt155=2;
                        int LA155_0 = input.LA(1);

                        if ( (LA155_0==31) ) {
                            alt155=1;
                        }


                        switch (alt155) {
                    	case 1 :
                    	    // InternalAorta.g:6362:5: otherlv_7= ',' ( (lv_testFileData_8_0= ruleStructuredFile ) )
                    	    {
                    	    otherlv_7=(Token)match(input,31,FOLLOW_156); 

                    	    					newLeafNode(otherlv_7, grammarAccess.getTestStrucuredFileDataAccess().getCommaKeyword_4_3_0());
                    	    				
                    	    // InternalAorta.g:6366:5: ( (lv_testFileData_8_0= ruleStructuredFile ) )
                    	    // InternalAorta.g:6367:6: (lv_testFileData_8_0= ruleStructuredFile )
                    	    {
                    	    // InternalAorta.g:6367:6: (lv_testFileData_8_0= ruleStructuredFile )
                    	    // InternalAorta.g:6368:7: lv_testFileData_8_0= ruleStructuredFile
                    	    {

                    	    							newCompositeNode(grammarAccess.getTestStrucuredFileDataAccess().getTestFileDataStructuredFileParserRuleCall_4_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_35);
                    	    lv_testFileData_8_0=ruleStructuredFile();

                    	    state._fsp--;


                    	    							if (current==null) {
                    	    								current = createModelElementForParent(grammarAccess.getTestStrucuredFileDataRule());
                    	    							}
                    	    							add(
                    	    								current,
                    	    								"testFileData",
                    	    								lv_testFileData_8_0,
                    	    								"org.eclipse.efbt.aorta.Aorta.StructuredFile");
                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop155;
                        }
                    } while (true);

                    otherlv_9=(Token)match(input,23,FOLLOW_24); 

                    				newLeafNode(otherlv_9, grammarAccess.getTestStrucuredFileDataAccess().getRightCurlyBracketKeyword_4_4());
                    			

                    }
                    break;

            }

            otherlv_10=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_10, grammarAccess.getTestStrucuredFileDataAccess().getRightCurlyBracketKeyword_5());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleTestStrucuredFileData"


    // $ANTLR start "entryRuleBaseColumnStructuredData"
    // InternalAorta.g:6399:1: entryRuleBaseColumnStructuredData returns [EObject current=null] : iv_ruleBaseColumnStructuredData= ruleBaseColumnStructuredData EOF ;
    public final EObject entryRuleBaseColumnStructuredData() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleBaseColumnStructuredData = null;


        try {
            // InternalAorta.g:6399:65: (iv_ruleBaseColumnStructuredData= ruleBaseColumnStructuredData EOF )
            // InternalAorta.g:6400:2: iv_ruleBaseColumnStructuredData= ruleBaseColumnStructuredData EOF
            {
             newCompositeNode(grammarAccess.getBaseColumnStructuredDataRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleBaseColumnStructuredData=ruleBaseColumnStructuredData();

            state._fsp--;

             current =iv_ruleBaseColumnStructuredData; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleBaseColumnStructuredData"


    // $ANTLR start "ruleBaseColumnStructuredData"
    // InternalAorta.g:6406:1: ruleBaseColumnStructuredData returns [EObject current=null] : ( () otherlv_1= 'BaseColumnStructuredData' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'cube' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'rows' otherlv_7= '{' ( (lv_rows_8_0= ruleBaseRowData ) ) (otherlv_9= ',' ( (lv_rows_10_0= ruleBaseRowData ) ) )* otherlv_11= '}' )? otherlv_12= '}' ) ;
    public final EObject ruleBaseColumnStructuredData() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_6=null;
        Token otherlv_7=null;
        Token otherlv_9=null;
        Token otherlv_11=null;
        Token otherlv_12=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;

        EObject lv_rows_8_0 = null;

        EObject lv_rows_10_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:6412:2: ( ( () otherlv_1= 'BaseColumnStructuredData' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'cube' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'rows' otherlv_7= '{' ( (lv_rows_8_0= ruleBaseRowData ) ) (otherlv_9= ',' ( (lv_rows_10_0= ruleBaseRowData ) ) )* otherlv_11= '}' )? otherlv_12= '}' ) )
            // InternalAorta.g:6413:2: ( () otherlv_1= 'BaseColumnStructuredData' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'cube' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'rows' otherlv_7= '{' ( (lv_rows_8_0= ruleBaseRowData ) ) (otherlv_9= ',' ( (lv_rows_10_0= ruleBaseRowData ) ) )* otherlv_11= '}' )? otherlv_12= '}' )
            {
            // InternalAorta.g:6413:2: ( () otherlv_1= 'BaseColumnStructuredData' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'cube' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'rows' otherlv_7= '{' ( (lv_rows_8_0= ruleBaseRowData ) ) (otherlv_9= ',' ( (lv_rows_10_0= ruleBaseRowData ) ) )* otherlv_11= '}' )? otherlv_12= '}' )
            // InternalAorta.g:6414:3: () otherlv_1= 'BaseColumnStructuredData' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'cube' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'rows' otherlv_7= '{' ( (lv_rows_8_0= ruleBaseRowData ) ) (otherlv_9= ',' ( (lv_rows_10_0= ruleBaseRowData ) ) )* otherlv_11= '}' )? otherlv_12= '}'
            {
            // InternalAorta.g:6414:3: ()
            // InternalAorta.g:6415:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getBaseColumnStructuredDataAccess().getBaseColumnStructuredDataAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,122,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getBaseColumnStructuredDataAccess().getBaseColumnStructuredDataKeyword_1());
            		
            // InternalAorta.g:6425:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:6426:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:6426:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:6427:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getBaseColumnStructuredDataAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getBaseColumnStructuredDataRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_157); 

            			newLeafNode(otherlv_3, grammarAccess.getBaseColumnStructuredDataAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:6448:3: (otherlv_4= 'cube' ( ( ruleQualifiedName ) ) )?
            int alt157=2;
            int LA157_0 = input.LA(1);

            if ( (LA157_0==123) ) {
                alt157=1;
            }
            switch (alt157) {
                case 1 :
                    // InternalAorta.g:6449:4: otherlv_4= 'cube' ( ( ruleQualifiedName ) )
                    {
                    otherlv_4=(Token)match(input,123,FOLLOW_70); 

                    				newLeafNode(otherlv_4, grammarAccess.getBaseColumnStructuredDataAccess().getCubeKeyword_4_0());
                    			
                    // InternalAorta.g:6453:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:6454:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:6454:5: ( ruleQualifiedName )
                    // InternalAorta.g:6455:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getBaseColumnStructuredDataRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getBaseColumnStructuredDataAccess().getCubeColumnStructuredEntityCrossReference_4_1_0());
                    					
                    pushFollow(FOLLOW_158);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:6470:3: (otherlv_6= 'rows' otherlv_7= '{' ( (lv_rows_8_0= ruleBaseRowData ) ) (otherlv_9= ',' ( (lv_rows_10_0= ruleBaseRowData ) ) )* otherlv_11= '}' )?
            int alt159=2;
            int LA159_0 = input.LA(1);

            if ( (LA159_0==124) ) {
                alt159=1;
            }
            switch (alt159) {
                case 1 :
                    // InternalAorta.g:6471:4: otherlv_6= 'rows' otherlv_7= '{' ( (lv_rows_8_0= ruleBaseRowData ) ) (otherlv_9= ',' ( (lv_rows_10_0= ruleBaseRowData ) ) )* otherlv_11= '}'
                    {
                    otherlv_6=(Token)match(input,124,FOLLOW_3); 

                    				newLeafNode(otherlv_6, grammarAccess.getBaseColumnStructuredDataAccess().getRowsKeyword_5_0());
                    			
                    otherlv_7=(Token)match(input,12,FOLLOW_159); 

                    				newLeafNode(otherlv_7, grammarAccess.getBaseColumnStructuredDataAccess().getLeftCurlyBracketKeyword_5_1());
                    			
                    // InternalAorta.g:6479:4: ( (lv_rows_8_0= ruleBaseRowData ) )
                    // InternalAorta.g:6480:5: (lv_rows_8_0= ruleBaseRowData )
                    {
                    // InternalAorta.g:6480:5: (lv_rows_8_0= ruleBaseRowData )
                    // InternalAorta.g:6481:6: lv_rows_8_0= ruleBaseRowData
                    {

                    						newCompositeNode(grammarAccess.getBaseColumnStructuredDataAccess().getRowsBaseRowDataParserRuleCall_5_2_0());
                    					
                    pushFollow(FOLLOW_35);
                    lv_rows_8_0=ruleBaseRowData();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getBaseColumnStructuredDataRule());
                    						}
                    						add(
                    							current,
                    							"rows",
                    							lv_rows_8_0,
                    							"org.eclipse.efbt.aorta.Aorta.BaseRowData");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:6498:4: (otherlv_9= ',' ( (lv_rows_10_0= ruleBaseRowData ) ) )*
                    loop158:
                    do {
                        int alt158=2;
                        int LA158_0 = input.LA(1);

                        if ( (LA158_0==31) ) {
                            alt158=1;
                        }


                        switch (alt158) {
                    	case 1 :
                    	    // InternalAorta.g:6499:5: otherlv_9= ',' ( (lv_rows_10_0= ruleBaseRowData ) )
                    	    {
                    	    otherlv_9=(Token)match(input,31,FOLLOW_159); 

                    	    					newLeafNode(otherlv_9, grammarAccess.getBaseColumnStructuredDataAccess().getCommaKeyword_5_3_0());
                    	    				
                    	    // InternalAorta.g:6503:5: ( (lv_rows_10_0= ruleBaseRowData ) )
                    	    // InternalAorta.g:6504:6: (lv_rows_10_0= ruleBaseRowData )
                    	    {
                    	    // InternalAorta.g:6504:6: (lv_rows_10_0= ruleBaseRowData )
                    	    // InternalAorta.g:6505:7: lv_rows_10_0= ruleBaseRowData
                    	    {

                    	    							newCompositeNode(grammarAccess.getBaseColumnStructuredDataAccess().getRowsBaseRowDataParserRuleCall_5_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_35);
                    	    lv_rows_10_0=ruleBaseRowData();

                    	    state._fsp--;


                    	    							if (current==null) {
                    	    								current = createModelElementForParent(grammarAccess.getBaseColumnStructuredDataRule());
                    	    							}
                    	    							add(
                    	    								current,
                    	    								"rows",
                    	    								lv_rows_10_0,
                    	    								"org.eclipse.efbt.aorta.Aorta.BaseRowData");
                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop158;
                        }
                    } while (true);

                    otherlv_11=(Token)match(input,23,FOLLOW_24); 

                    				newLeafNode(otherlv_11, grammarAccess.getBaseColumnStructuredDataAccess().getRightCurlyBracketKeyword_5_4());
                    			

                    }
                    break;

            }

            otherlv_12=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_12, grammarAccess.getBaseColumnStructuredDataAccess().getRightCurlyBracketKeyword_6());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleBaseColumnStructuredData"


    // $ANTLR start "entryRuleBaseRowData"
    // InternalAorta.g:6536:1: entryRuleBaseRowData returns [EObject current=null] : iv_ruleBaseRowData= ruleBaseRowData EOF ;
    public final EObject entryRuleBaseRowData() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleBaseRowData = null;


        try {
            // InternalAorta.g:6536:52: (iv_ruleBaseRowData= ruleBaseRowData EOF )
            // InternalAorta.g:6537:2: iv_ruleBaseRowData= ruleBaseRowData EOF
            {
             newCompositeNode(grammarAccess.getBaseRowDataRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleBaseRowData=ruleBaseRowData();

            state._fsp--;

             current =iv_ruleBaseRowData; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleBaseRowData"


    // $ANTLR start "ruleBaseRowData"
    // InternalAorta.g:6543:1: ruleBaseRowData returns [EObject current=null] : ( () otherlv_1= 'BaseRowData' ( (lv_rowID_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'cells' otherlv_5= '{' ( (lv_cells_6_0= ruleBaseCell ) ) (otherlv_7= ',' ( (lv_cells_8_0= ruleBaseCell ) ) )* otherlv_9= '}' )? otherlv_10= '}' ) ;
    public final EObject ruleBaseRowData() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_5=null;
        Token otherlv_7=null;
        Token otherlv_9=null;
        Token otherlv_10=null;
        AntlrDatatypeRuleToken lv_rowID_2_0 = null;

        EObject lv_cells_6_0 = null;

        EObject lv_cells_8_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:6549:2: ( ( () otherlv_1= 'BaseRowData' ( (lv_rowID_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'cells' otherlv_5= '{' ( (lv_cells_6_0= ruleBaseCell ) ) (otherlv_7= ',' ( (lv_cells_8_0= ruleBaseCell ) ) )* otherlv_9= '}' )? otherlv_10= '}' ) )
            // InternalAorta.g:6550:2: ( () otherlv_1= 'BaseRowData' ( (lv_rowID_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'cells' otherlv_5= '{' ( (lv_cells_6_0= ruleBaseCell ) ) (otherlv_7= ',' ( (lv_cells_8_0= ruleBaseCell ) ) )* otherlv_9= '}' )? otherlv_10= '}' )
            {
            // InternalAorta.g:6550:2: ( () otherlv_1= 'BaseRowData' ( (lv_rowID_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'cells' otherlv_5= '{' ( (lv_cells_6_0= ruleBaseCell ) ) (otherlv_7= ',' ( (lv_cells_8_0= ruleBaseCell ) ) )* otherlv_9= '}' )? otherlv_10= '}' )
            // InternalAorta.g:6551:3: () otherlv_1= 'BaseRowData' ( (lv_rowID_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'cells' otherlv_5= '{' ( (lv_cells_6_0= ruleBaseCell ) ) (otherlv_7= ',' ( (lv_cells_8_0= ruleBaseCell ) ) )* otherlv_9= '}' )? otherlv_10= '}'
            {
            // InternalAorta.g:6551:3: ()
            // InternalAorta.g:6552:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getBaseRowDataAccess().getBaseRowDataAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,125,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getBaseRowDataAccess().getBaseRowDataKeyword_1());
            		
            // InternalAorta.g:6562:3: ( (lv_rowID_2_0= ruleEString ) )
            // InternalAorta.g:6563:4: (lv_rowID_2_0= ruleEString )
            {
            // InternalAorta.g:6563:4: (lv_rowID_2_0= ruleEString )
            // InternalAorta.g:6564:5: lv_rowID_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getBaseRowDataAccess().getRowIDEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_rowID_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getBaseRowDataRule());
            					}
            					set(
            						current,
            						"rowID",
            						lv_rowID_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_133); 

            			newLeafNode(otherlv_3, grammarAccess.getBaseRowDataAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:6585:3: (otherlv_4= 'cells' otherlv_5= '{' ( (lv_cells_6_0= ruleBaseCell ) ) (otherlv_7= ',' ( (lv_cells_8_0= ruleBaseCell ) ) )* otherlv_9= '}' )?
            int alt161=2;
            int LA161_0 = input.LA(1);

            if ( (LA161_0==93) ) {
                alt161=1;
            }
            switch (alt161) {
                case 1 :
                    // InternalAorta.g:6586:4: otherlv_4= 'cells' otherlv_5= '{' ( (lv_cells_6_0= ruleBaseCell ) ) (otherlv_7= ',' ( (lv_cells_8_0= ruleBaseCell ) ) )* otherlv_9= '}'
                    {
                    otherlv_4=(Token)match(input,93,FOLLOW_3); 

                    				newLeafNode(otherlv_4, grammarAccess.getBaseRowDataAccess().getCellsKeyword_4_0());
                    			
                    otherlv_5=(Token)match(input,12,FOLLOW_160); 

                    				newLeafNode(otherlv_5, grammarAccess.getBaseRowDataAccess().getLeftCurlyBracketKeyword_4_1());
                    			
                    // InternalAorta.g:6594:4: ( (lv_cells_6_0= ruleBaseCell ) )
                    // InternalAorta.g:6595:5: (lv_cells_6_0= ruleBaseCell )
                    {
                    // InternalAorta.g:6595:5: (lv_cells_6_0= ruleBaseCell )
                    // InternalAorta.g:6596:6: lv_cells_6_0= ruleBaseCell
                    {

                    						newCompositeNode(grammarAccess.getBaseRowDataAccess().getCellsBaseCellParserRuleCall_4_2_0());
                    					
                    pushFollow(FOLLOW_35);
                    lv_cells_6_0=ruleBaseCell();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getBaseRowDataRule());
                    						}
                    						add(
                    							current,
                    							"cells",
                    							lv_cells_6_0,
                    							"org.eclipse.efbt.aorta.Aorta.BaseCell");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:6613:4: (otherlv_7= ',' ( (lv_cells_8_0= ruleBaseCell ) ) )*
                    loop160:
                    do {
                        int alt160=2;
                        int LA160_0 = input.LA(1);

                        if ( (LA160_0==31) ) {
                            alt160=1;
                        }


                        switch (alt160) {
                    	case 1 :
                    	    // InternalAorta.g:6614:5: otherlv_7= ',' ( (lv_cells_8_0= ruleBaseCell ) )
                    	    {
                    	    otherlv_7=(Token)match(input,31,FOLLOW_160); 

                    	    					newLeafNode(otherlv_7, grammarAccess.getBaseRowDataAccess().getCommaKeyword_4_3_0());
                    	    				
                    	    // InternalAorta.g:6618:5: ( (lv_cells_8_0= ruleBaseCell ) )
                    	    // InternalAorta.g:6619:6: (lv_cells_8_0= ruleBaseCell )
                    	    {
                    	    // InternalAorta.g:6619:6: (lv_cells_8_0= ruleBaseCell )
                    	    // InternalAorta.g:6620:7: lv_cells_8_0= ruleBaseCell
                    	    {

                    	    							newCompositeNode(grammarAccess.getBaseRowDataAccess().getCellsBaseCellParserRuleCall_4_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_35);
                    	    lv_cells_8_0=ruleBaseCell();

                    	    state._fsp--;


                    	    							if (current==null) {
                    	    								current = createModelElementForParent(grammarAccess.getBaseRowDataRule());
                    	    							}
                    	    							add(
                    	    								current,
                    	    								"cells",
                    	    								lv_cells_8_0,
                    	    								"org.eclipse.efbt.aorta.Aorta.BaseCell");
                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop160;
                        }
                    } while (true);

                    otherlv_9=(Token)match(input,23,FOLLOW_24); 

                    				newLeafNode(otherlv_9, grammarAccess.getBaseRowDataAccess().getRightCurlyBracketKeyword_4_4());
                    			

                    }
                    break;

            }

            otherlv_10=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_10, grammarAccess.getBaseRowDataAccess().getRightCurlyBracketKeyword_5());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleBaseRowData"


    // $ANTLR start "entryRuleBaseCellWithEnumeratedValue"
    // InternalAorta.g:6651:1: entryRuleBaseCellWithEnumeratedValue returns [EObject current=null] : iv_ruleBaseCellWithEnumeratedValue= ruleBaseCellWithEnumeratedValue EOF ;
    public final EObject entryRuleBaseCellWithEnumeratedValue() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleBaseCellWithEnumeratedValue = null;


        try {
            // InternalAorta.g:6651:68: (iv_ruleBaseCellWithEnumeratedValue= ruleBaseCellWithEnumeratedValue EOF )
            // InternalAorta.g:6652:2: iv_ruleBaseCellWithEnumeratedValue= ruleBaseCellWithEnumeratedValue EOF
            {
             newCompositeNode(grammarAccess.getBaseCellWithEnumeratedValueRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleBaseCellWithEnumeratedValue=ruleBaseCellWithEnumeratedValue();

            state._fsp--;

             current =iv_ruleBaseCellWithEnumeratedValue; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleBaseCellWithEnumeratedValue"


    // $ANTLR start "ruleBaseCellWithEnumeratedValue"
    // InternalAorta.g:6658:1: ruleBaseCellWithEnumeratedValue returns [EObject current=null] : ( () otherlv_1= 'BaseCellWithEnumeratedValue' ( (lv_cellID_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'column' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'value' ( ( ruleQualifiedName ) ) )? otherlv_8= '}' ) ;
    public final EObject ruleBaseCellWithEnumeratedValue() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_6=null;
        Token otherlv_8=null;
        AntlrDatatypeRuleToken lv_cellID_2_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:6664:2: ( ( () otherlv_1= 'BaseCellWithEnumeratedValue' ( (lv_cellID_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'column' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'value' ( ( ruleQualifiedName ) ) )? otherlv_8= '}' ) )
            // InternalAorta.g:6665:2: ( () otherlv_1= 'BaseCellWithEnumeratedValue' ( (lv_cellID_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'column' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'value' ( ( ruleQualifiedName ) ) )? otherlv_8= '}' )
            {
            // InternalAorta.g:6665:2: ( () otherlv_1= 'BaseCellWithEnumeratedValue' ( (lv_cellID_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'column' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'value' ( ( ruleQualifiedName ) ) )? otherlv_8= '}' )
            // InternalAorta.g:6666:3: () otherlv_1= 'BaseCellWithEnumeratedValue' ( (lv_cellID_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'column' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'value' ( ( ruleQualifiedName ) ) )? otherlv_8= '}'
            {
            // InternalAorta.g:6666:3: ()
            // InternalAorta.g:6667:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getBaseCellWithEnumeratedValueAccess().getBaseCellWithEnumeratedValueAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,126,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getBaseCellWithEnumeratedValueAccess().getBaseCellWithEnumeratedValueKeyword_1());
            		
            // InternalAorta.g:6677:3: ( (lv_cellID_2_0= ruleEString ) )
            // InternalAorta.g:6678:4: (lv_cellID_2_0= ruleEString )
            {
            // InternalAorta.g:6678:4: (lv_cellID_2_0= ruleEString )
            // InternalAorta.g:6679:5: lv_cellID_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getBaseCellWithEnumeratedValueAccess().getCellIDEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_cellID_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getBaseCellWithEnumeratedValueRule());
            					}
            					set(
            						current,
            						"cellID",
            						lv_cellID_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_161); 

            			newLeafNode(otherlv_3, grammarAccess.getBaseCellWithEnumeratedValueAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:6700:3: (otherlv_4= 'column' ( ( ruleQualifiedName ) ) )?
            int alt162=2;
            int LA162_0 = input.LA(1);

            if ( (LA162_0==85) ) {
                alt162=1;
            }
            switch (alt162) {
                case 1 :
                    // InternalAorta.g:6701:4: otherlv_4= 'column' ( ( ruleQualifiedName ) )
                    {
                    otherlv_4=(Token)match(input,85,FOLLOW_70); 

                    				newLeafNode(otherlv_4, grammarAccess.getBaseCellWithEnumeratedValueAccess().getColumnKeyword_4_0());
                    			
                    // InternalAorta.g:6705:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:6706:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:6706:5: ( ruleQualifiedName )
                    // InternalAorta.g:6707:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getBaseCellWithEnumeratedValueRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getBaseCellWithEnumeratedValueAccess().getColumnColumnCrossReference_4_1_0());
                    					
                    pushFollow(FOLLOW_162);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:6722:3: (otherlv_6= 'value' ( ( ruleQualifiedName ) ) )?
            int alt163=2;
            int LA163_0 = input.LA(1);

            if ( (LA163_0==127) ) {
                alt163=1;
            }
            switch (alt163) {
                case 1 :
                    // InternalAorta.g:6723:4: otherlv_6= 'value' ( ( ruleQualifiedName ) )
                    {
                    otherlv_6=(Token)match(input,127,FOLLOW_70); 

                    				newLeafNode(otherlv_6, grammarAccess.getBaseCellWithEnumeratedValueAccess().getValueKeyword_5_0());
                    			
                    // InternalAorta.g:6727:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:6728:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:6728:5: ( ruleQualifiedName )
                    // InternalAorta.g:6729:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getBaseCellWithEnumeratedValueRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getBaseCellWithEnumeratedValueAccess().getValueEnumMemberCrossReference_5_1_0());
                    					
                    pushFollow(FOLLOW_24);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            otherlv_8=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_8, grammarAccess.getBaseCellWithEnumeratedValueAccess().getRightCurlyBracketKeyword_6());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleBaseCellWithEnumeratedValue"


    // $ANTLR start "entryRuleBaseCellWithValue"
    // InternalAorta.g:6752:1: entryRuleBaseCellWithValue returns [EObject current=null] : iv_ruleBaseCellWithValue= ruleBaseCellWithValue EOF ;
    public final EObject entryRuleBaseCellWithValue() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleBaseCellWithValue = null;


        try {
            // InternalAorta.g:6752:58: (iv_ruleBaseCellWithValue= ruleBaseCellWithValue EOF )
            // InternalAorta.g:6753:2: iv_ruleBaseCellWithValue= ruleBaseCellWithValue EOF
            {
             newCompositeNode(grammarAccess.getBaseCellWithValueRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleBaseCellWithValue=ruleBaseCellWithValue();

            state._fsp--;

             current =iv_ruleBaseCellWithValue; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleBaseCellWithValue"


    // $ANTLR start "ruleBaseCellWithValue"
    // InternalAorta.g:6759:1: ruleBaseCellWithValue returns [EObject current=null] : ( () otherlv_1= 'BaseCellWithValue' ( (lv_cellID_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'value' ( (lv_value_5_0= ruleEString ) ) )? (otherlv_6= 'column' ( ( ruleQualifiedName ) ) )? otherlv_8= '}' ) ;
    public final EObject ruleBaseCellWithValue() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_6=null;
        Token otherlv_8=null;
        AntlrDatatypeRuleToken lv_cellID_2_0 = null;

        AntlrDatatypeRuleToken lv_value_5_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:6765:2: ( ( () otherlv_1= 'BaseCellWithValue' ( (lv_cellID_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'value' ( (lv_value_5_0= ruleEString ) ) )? (otherlv_6= 'column' ( ( ruleQualifiedName ) ) )? otherlv_8= '}' ) )
            // InternalAorta.g:6766:2: ( () otherlv_1= 'BaseCellWithValue' ( (lv_cellID_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'value' ( (lv_value_5_0= ruleEString ) ) )? (otherlv_6= 'column' ( ( ruleQualifiedName ) ) )? otherlv_8= '}' )
            {
            // InternalAorta.g:6766:2: ( () otherlv_1= 'BaseCellWithValue' ( (lv_cellID_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'value' ( (lv_value_5_0= ruleEString ) ) )? (otherlv_6= 'column' ( ( ruleQualifiedName ) ) )? otherlv_8= '}' )
            // InternalAorta.g:6767:3: () otherlv_1= 'BaseCellWithValue' ( (lv_cellID_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'value' ( (lv_value_5_0= ruleEString ) ) )? (otherlv_6= 'column' ( ( ruleQualifiedName ) ) )? otherlv_8= '}'
            {
            // InternalAorta.g:6767:3: ()
            // InternalAorta.g:6768:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getBaseCellWithValueAccess().getBaseCellWithValueAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,128,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getBaseCellWithValueAccess().getBaseCellWithValueKeyword_1());
            		
            // InternalAorta.g:6778:3: ( (lv_cellID_2_0= ruleEString ) )
            // InternalAorta.g:6779:4: (lv_cellID_2_0= ruleEString )
            {
            // InternalAorta.g:6779:4: (lv_cellID_2_0= ruleEString )
            // InternalAorta.g:6780:5: lv_cellID_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getBaseCellWithValueAccess().getCellIDEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_cellID_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getBaseCellWithValueRule());
            					}
            					set(
            						current,
            						"cellID",
            						lv_cellID_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_161); 

            			newLeafNode(otherlv_3, grammarAccess.getBaseCellWithValueAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:6801:3: (otherlv_4= 'value' ( (lv_value_5_0= ruleEString ) ) )?
            int alt164=2;
            int LA164_0 = input.LA(1);

            if ( (LA164_0==127) ) {
                alt164=1;
            }
            switch (alt164) {
                case 1 :
                    // InternalAorta.g:6802:4: otherlv_4= 'value' ( (lv_value_5_0= ruleEString ) )
                    {
                    otherlv_4=(Token)match(input,127,FOLLOW_25); 

                    				newLeafNode(otherlv_4, grammarAccess.getBaseCellWithValueAccess().getValueKeyword_4_0());
                    			
                    // InternalAorta.g:6806:4: ( (lv_value_5_0= ruleEString ) )
                    // InternalAorta.g:6807:5: (lv_value_5_0= ruleEString )
                    {
                    // InternalAorta.g:6807:5: (lv_value_5_0= ruleEString )
                    // InternalAorta.g:6808:6: lv_value_5_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getBaseCellWithValueAccess().getValueEStringParserRuleCall_4_1_0());
                    					
                    pushFollow(FOLLOW_126);
                    lv_value_5_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getBaseCellWithValueRule());
                    						}
                    						set(
                    							current,
                    							"value",
                    							lv_value_5_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:6826:3: (otherlv_6= 'column' ( ( ruleQualifiedName ) ) )?
            int alt165=2;
            int LA165_0 = input.LA(1);

            if ( (LA165_0==85) ) {
                alt165=1;
            }
            switch (alt165) {
                case 1 :
                    // InternalAorta.g:6827:4: otherlv_6= 'column' ( ( ruleQualifiedName ) )
                    {
                    otherlv_6=(Token)match(input,85,FOLLOW_70); 

                    				newLeafNode(otherlv_6, grammarAccess.getBaseCellWithValueAccess().getColumnKeyword_5_0());
                    			
                    // InternalAorta.g:6831:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:6832:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:6832:5: ( ruleQualifiedName )
                    // InternalAorta.g:6833:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getBaseCellWithValueRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getBaseCellWithValueAccess().getColumnColumnCrossReference_5_1_0());
                    					
                    pushFollow(FOLLOW_24);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            otherlv_8=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_8, grammarAccess.getBaseCellWithValueAccess().getRightCurlyBracketKeyword_6());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleBaseCellWithValue"


    // $ANTLR start "entryRuleStructuredFile"
    // InternalAorta.g:6856:1: entryRuleStructuredFile returns [EObject current=null] : iv_ruleStructuredFile= ruleStructuredFile EOF ;
    public final EObject entryRuleStructuredFile() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleStructuredFile = null;


        try {
            // InternalAorta.g:6856:55: (iv_ruleStructuredFile= ruleStructuredFile EOF )
            // InternalAorta.g:6857:2: iv_ruleStructuredFile= ruleStructuredFile EOF
            {
             newCompositeNode(grammarAccess.getStructuredFileRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleStructuredFile=ruleStructuredFile();

            state._fsp--;

             current =iv_ruleStructuredFile; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleStructuredFile"


    // $ANTLR start "ruleStructuredFile"
    // InternalAorta.g:6863:1: ruleStructuredFile returns [EObject current=null] : ( () otherlv_1= 'StructuredFile' otherlv_2= '{' (otherlv_3= 'fileLocation' ( (lv_fileLocation_4_0= ruleEString ) ) )? otherlv_5= '}' ) ;
    public final EObject ruleStructuredFile() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_2=null;
        Token otherlv_3=null;
        Token otherlv_5=null;
        AntlrDatatypeRuleToken lv_fileLocation_4_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:6869:2: ( ( () otherlv_1= 'StructuredFile' otherlv_2= '{' (otherlv_3= 'fileLocation' ( (lv_fileLocation_4_0= ruleEString ) ) )? otherlv_5= '}' ) )
            // InternalAorta.g:6870:2: ( () otherlv_1= 'StructuredFile' otherlv_2= '{' (otherlv_3= 'fileLocation' ( (lv_fileLocation_4_0= ruleEString ) ) )? otherlv_5= '}' )
            {
            // InternalAorta.g:6870:2: ( () otherlv_1= 'StructuredFile' otherlv_2= '{' (otherlv_3= 'fileLocation' ( (lv_fileLocation_4_0= ruleEString ) ) )? otherlv_5= '}' )
            // InternalAorta.g:6871:3: () otherlv_1= 'StructuredFile' otherlv_2= '{' (otherlv_3= 'fileLocation' ( (lv_fileLocation_4_0= ruleEString ) ) )? otherlv_5= '}'
            {
            // InternalAorta.g:6871:3: ()
            // InternalAorta.g:6872:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getStructuredFileAccess().getStructuredFileAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,129,FOLLOW_3); 

            			newLeafNode(otherlv_1, grammarAccess.getStructuredFileAccess().getStructuredFileKeyword_1());
            		
            otherlv_2=(Token)match(input,12,FOLLOW_163); 

            			newLeafNode(otherlv_2, grammarAccess.getStructuredFileAccess().getLeftCurlyBracketKeyword_2());
            		
            // InternalAorta.g:6886:3: (otherlv_3= 'fileLocation' ( (lv_fileLocation_4_0= ruleEString ) ) )?
            int alt166=2;
            int LA166_0 = input.LA(1);

            if ( (LA166_0==130) ) {
                alt166=1;
            }
            switch (alt166) {
                case 1 :
                    // InternalAorta.g:6887:4: otherlv_3= 'fileLocation' ( (lv_fileLocation_4_0= ruleEString ) )
                    {
                    otherlv_3=(Token)match(input,130,FOLLOW_25); 

                    				newLeafNode(otherlv_3, grammarAccess.getStructuredFileAccess().getFileLocationKeyword_3_0());
                    			
                    // InternalAorta.g:6891:4: ( (lv_fileLocation_4_0= ruleEString ) )
                    // InternalAorta.g:6892:5: (lv_fileLocation_4_0= ruleEString )
                    {
                    // InternalAorta.g:6892:5: (lv_fileLocation_4_0= ruleEString )
                    // InternalAorta.g:6893:6: lv_fileLocation_4_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getStructuredFileAccess().getFileLocationEStringParserRuleCall_3_1_0());
                    					
                    pushFollow(FOLLOW_24);
                    lv_fileLocation_4_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getStructuredFileRule());
                    						}
                    						set(
                    							current,
                    							"fileLocation",
                    							lv_fileLocation_4_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            otherlv_5=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_5, grammarAccess.getStructuredFileAccess().getRightCurlyBracketKeyword_4());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleStructuredFile"


    // $ANTLR start "entryRuleReportCellResults"
    // InternalAorta.g:6919:1: entryRuleReportCellResults returns [EObject current=null] : iv_ruleReportCellResults= ruleReportCellResults EOF ;
    public final EObject entryRuleReportCellResults() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleReportCellResults = null;


        try {
            // InternalAorta.g:6919:58: (iv_ruleReportCellResults= ruleReportCellResults EOF )
            // InternalAorta.g:6920:2: iv_ruleReportCellResults= ruleReportCellResults EOF
            {
             newCompositeNode(grammarAccess.getReportCellResultsRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleReportCellResults=ruleReportCellResults();

            state._fsp--;

             current =iv_ruleReportCellResults; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleReportCellResults"


    // $ANTLR start "ruleReportCellResults"
    // InternalAorta.g:6926:1: ruleReportCellResults returns [EObject current=null] : ( () otherlv_1= 'ReportCellResults' otherlv_2= '{' (otherlv_3= 'reportCellValues' otherlv_4= '{' ( (lv_reportCellValues_5_0= ruleRegReportCellValue ) ) (otherlv_6= ',' ( (lv_reportCellValues_7_0= ruleRegReportCellValue ) ) )* otherlv_8= '}' )? otherlv_9= '}' ) ;
    public final EObject ruleReportCellResults() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_2=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_6=null;
        Token otherlv_8=null;
        Token otherlv_9=null;
        EObject lv_reportCellValues_5_0 = null;

        EObject lv_reportCellValues_7_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:6932:2: ( ( () otherlv_1= 'ReportCellResults' otherlv_2= '{' (otherlv_3= 'reportCellValues' otherlv_4= '{' ( (lv_reportCellValues_5_0= ruleRegReportCellValue ) ) (otherlv_6= ',' ( (lv_reportCellValues_7_0= ruleRegReportCellValue ) ) )* otherlv_8= '}' )? otherlv_9= '}' ) )
            // InternalAorta.g:6933:2: ( () otherlv_1= 'ReportCellResults' otherlv_2= '{' (otherlv_3= 'reportCellValues' otherlv_4= '{' ( (lv_reportCellValues_5_0= ruleRegReportCellValue ) ) (otherlv_6= ',' ( (lv_reportCellValues_7_0= ruleRegReportCellValue ) ) )* otherlv_8= '}' )? otherlv_9= '}' )
            {
            // InternalAorta.g:6933:2: ( () otherlv_1= 'ReportCellResults' otherlv_2= '{' (otherlv_3= 'reportCellValues' otherlv_4= '{' ( (lv_reportCellValues_5_0= ruleRegReportCellValue ) ) (otherlv_6= ',' ( (lv_reportCellValues_7_0= ruleRegReportCellValue ) ) )* otherlv_8= '}' )? otherlv_9= '}' )
            // InternalAorta.g:6934:3: () otherlv_1= 'ReportCellResults' otherlv_2= '{' (otherlv_3= 'reportCellValues' otherlv_4= '{' ( (lv_reportCellValues_5_0= ruleRegReportCellValue ) ) (otherlv_6= ',' ( (lv_reportCellValues_7_0= ruleRegReportCellValue ) ) )* otherlv_8= '}' )? otherlv_9= '}'
            {
            // InternalAorta.g:6934:3: ()
            // InternalAorta.g:6935:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getReportCellResultsAccess().getReportCellResultsAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,131,FOLLOW_3); 

            			newLeafNode(otherlv_1, grammarAccess.getReportCellResultsAccess().getReportCellResultsKeyword_1());
            		
            otherlv_2=(Token)match(input,12,FOLLOW_164); 

            			newLeafNode(otherlv_2, grammarAccess.getReportCellResultsAccess().getLeftCurlyBracketKeyword_2());
            		
            // InternalAorta.g:6949:3: (otherlv_3= 'reportCellValues' otherlv_4= '{' ( (lv_reportCellValues_5_0= ruleRegReportCellValue ) ) (otherlv_6= ',' ( (lv_reportCellValues_7_0= ruleRegReportCellValue ) ) )* otherlv_8= '}' )?
            int alt168=2;
            int LA168_0 = input.LA(1);

            if ( (LA168_0==132) ) {
                alt168=1;
            }
            switch (alt168) {
                case 1 :
                    // InternalAorta.g:6950:4: otherlv_3= 'reportCellValues' otherlv_4= '{' ( (lv_reportCellValues_5_0= ruleRegReportCellValue ) ) (otherlv_6= ',' ( (lv_reportCellValues_7_0= ruleRegReportCellValue ) ) )* otherlv_8= '}'
                    {
                    otherlv_3=(Token)match(input,132,FOLLOW_3); 

                    				newLeafNode(otherlv_3, grammarAccess.getReportCellResultsAccess().getReportCellValuesKeyword_3_0());
                    			
                    otherlv_4=(Token)match(input,12,FOLLOW_165); 

                    				newLeafNode(otherlv_4, grammarAccess.getReportCellResultsAccess().getLeftCurlyBracketKeyword_3_1());
                    			
                    // InternalAorta.g:6958:4: ( (lv_reportCellValues_5_0= ruleRegReportCellValue ) )
                    // InternalAorta.g:6959:5: (lv_reportCellValues_5_0= ruleRegReportCellValue )
                    {
                    // InternalAorta.g:6959:5: (lv_reportCellValues_5_0= ruleRegReportCellValue )
                    // InternalAorta.g:6960:6: lv_reportCellValues_5_0= ruleRegReportCellValue
                    {

                    						newCompositeNode(grammarAccess.getReportCellResultsAccess().getReportCellValuesRegReportCellValueParserRuleCall_3_2_0());
                    					
                    pushFollow(FOLLOW_35);
                    lv_reportCellValues_5_0=ruleRegReportCellValue();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getReportCellResultsRule());
                    						}
                    						add(
                    							current,
                    							"reportCellValues",
                    							lv_reportCellValues_5_0,
                    							"org.eclipse.efbt.aorta.Aorta.RegReportCellValue");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:6977:4: (otherlv_6= ',' ( (lv_reportCellValues_7_0= ruleRegReportCellValue ) ) )*
                    loop167:
                    do {
                        int alt167=2;
                        int LA167_0 = input.LA(1);

                        if ( (LA167_0==31) ) {
                            alt167=1;
                        }


                        switch (alt167) {
                    	case 1 :
                    	    // InternalAorta.g:6978:5: otherlv_6= ',' ( (lv_reportCellValues_7_0= ruleRegReportCellValue ) )
                    	    {
                    	    otherlv_6=(Token)match(input,31,FOLLOW_165); 

                    	    					newLeafNode(otherlv_6, grammarAccess.getReportCellResultsAccess().getCommaKeyword_3_3_0());
                    	    				
                    	    // InternalAorta.g:6982:5: ( (lv_reportCellValues_7_0= ruleRegReportCellValue ) )
                    	    // InternalAorta.g:6983:6: (lv_reportCellValues_7_0= ruleRegReportCellValue )
                    	    {
                    	    // InternalAorta.g:6983:6: (lv_reportCellValues_7_0= ruleRegReportCellValue )
                    	    // InternalAorta.g:6984:7: lv_reportCellValues_7_0= ruleRegReportCellValue
                    	    {

                    	    							newCompositeNode(grammarAccess.getReportCellResultsAccess().getReportCellValuesRegReportCellValueParserRuleCall_3_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_35);
                    	    lv_reportCellValues_7_0=ruleRegReportCellValue();

                    	    state._fsp--;


                    	    							if (current==null) {
                    	    								current = createModelElementForParent(grammarAccess.getReportCellResultsRule());
                    	    							}
                    	    							add(
                    	    								current,
                    	    								"reportCellValues",
                    	    								lv_reportCellValues_7_0,
                    	    								"org.eclipse.efbt.aorta.Aorta.RegReportCellValue");
                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop167;
                        }
                    } while (true);

                    otherlv_8=(Token)match(input,23,FOLLOW_24); 

                    				newLeafNode(otherlv_8, grammarAccess.getReportCellResultsAccess().getRightCurlyBracketKeyword_3_4());
                    			

                    }
                    break;

            }

            otherlv_9=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_9, grammarAccess.getReportCellResultsAccess().getRightCurlyBracketKeyword_4());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleReportCellResults"


    // $ANTLR start "entryRuleDataSetResults"
    // InternalAorta.g:7015:1: entryRuleDataSetResults returns [EObject current=null] : iv_ruleDataSetResults= ruleDataSetResults EOF ;
    public final EObject entryRuleDataSetResults() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleDataSetResults = null;


        try {
            // InternalAorta.g:7015:55: (iv_ruleDataSetResults= ruleDataSetResults EOF )
            // InternalAorta.g:7016:2: iv_ruleDataSetResults= ruleDataSetResults EOF
            {
             newCompositeNode(grammarAccess.getDataSetResultsRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleDataSetResults=ruleDataSetResults();

            state._fsp--;

             current =iv_ruleDataSetResults; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleDataSetResults"


    // $ANTLR start "ruleDataSetResults"
    // InternalAorta.g:7022:1: ruleDataSetResults returns [EObject current=null] : ( () otherlv_1= 'DataSetResults' otherlv_2= '{' (otherlv_3= 'expectedResults' otherlv_4= '{' ( (lv_expectedResults_5_0= ruleColumnStructuredData ) ) (otherlv_6= ',' ( (lv_expectedResults_7_0= ruleColumnStructuredData ) ) )* otherlv_8= '}' )? otherlv_9= '}' ) ;
    public final EObject ruleDataSetResults() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_2=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_6=null;
        Token otherlv_8=null;
        Token otherlv_9=null;
        EObject lv_expectedResults_5_0 = null;

        EObject lv_expectedResults_7_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:7028:2: ( ( () otherlv_1= 'DataSetResults' otherlv_2= '{' (otherlv_3= 'expectedResults' otherlv_4= '{' ( (lv_expectedResults_5_0= ruleColumnStructuredData ) ) (otherlv_6= ',' ( (lv_expectedResults_7_0= ruleColumnStructuredData ) ) )* otherlv_8= '}' )? otherlv_9= '}' ) )
            // InternalAorta.g:7029:2: ( () otherlv_1= 'DataSetResults' otherlv_2= '{' (otherlv_3= 'expectedResults' otherlv_4= '{' ( (lv_expectedResults_5_0= ruleColumnStructuredData ) ) (otherlv_6= ',' ( (lv_expectedResults_7_0= ruleColumnStructuredData ) ) )* otherlv_8= '}' )? otherlv_9= '}' )
            {
            // InternalAorta.g:7029:2: ( () otherlv_1= 'DataSetResults' otherlv_2= '{' (otherlv_3= 'expectedResults' otherlv_4= '{' ( (lv_expectedResults_5_0= ruleColumnStructuredData ) ) (otherlv_6= ',' ( (lv_expectedResults_7_0= ruleColumnStructuredData ) ) )* otherlv_8= '}' )? otherlv_9= '}' )
            // InternalAorta.g:7030:3: () otherlv_1= 'DataSetResults' otherlv_2= '{' (otherlv_3= 'expectedResults' otherlv_4= '{' ( (lv_expectedResults_5_0= ruleColumnStructuredData ) ) (otherlv_6= ',' ( (lv_expectedResults_7_0= ruleColumnStructuredData ) ) )* otherlv_8= '}' )? otherlv_9= '}'
            {
            // InternalAorta.g:7030:3: ()
            // InternalAorta.g:7031:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getDataSetResultsAccess().getDataSetResultsAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,133,FOLLOW_3); 

            			newLeafNode(otherlv_1, grammarAccess.getDataSetResultsAccess().getDataSetResultsKeyword_1());
            		
            otherlv_2=(Token)match(input,12,FOLLOW_151); 

            			newLeafNode(otherlv_2, grammarAccess.getDataSetResultsAccess().getLeftCurlyBracketKeyword_2());
            		
            // InternalAorta.g:7045:3: (otherlv_3= 'expectedResults' otherlv_4= '{' ( (lv_expectedResults_5_0= ruleColumnStructuredData ) ) (otherlv_6= ',' ( (lv_expectedResults_7_0= ruleColumnStructuredData ) ) )* otherlv_8= '}' )?
            int alt170=2;
            int LA170_0 = input.LA(1);

            if ( (LA170_0==115) ) {
                alt170=1;
            }
            switch (alt170) {
                case 1 :
                    // InternalAorta.g:7046:4: otherlv_3= 'expectedResults' otherlv_4= '{' ( (lv_expectedResults_5_0= ruleColumnStructuredData ) ) (otherlv_6= ',' ( (lv_expectedResults_7_0= ruleColumnStructuredData ) ) )* otherlv_8= '}'
                    {
                    otherlv_3=(Token)match(input,115,FOLLOW_3); 

                    				newLeafNode(otherlv_3, grammarAccess.getDataSetResultsAccess().getExpectedResultsKeyword_3_0());
                    			
                    otherlv_4=(Token)match(input,12,FOLLOW_166); 

                    				newLeafNode(otherlv_4, grammarAccess.getDataSetResultsAccess().getLeftCurlyBracketKeyword_3_1());
                    			
                    // InternalAorta.g:7054:4: ( (lv_expectedResults_5_0= ruleColumnStructuredData ) )
                    // InternalAorta.g:7055:5: (lv_expectedResults_5_0= ruleColumnStructuredData )
                    {
                    // InternalAorta.g:7055:5: (lv_expectedResults_5_0= ruleColumnStructuredData )
                    // InternalAorta.g:7056:6: lv_expectedResults_5_0= ruleColumnStructuredData
                    {

                    						newCompositeNode(grammarAccess.getDataSetResultsAccess().getExpectedResultsColumnStructuredDataParserRuleCall_3_2_0());
                    					
                    pushFollow(FOLLOW_35);
                    lv_expectedResults_5_0=ruleColumnStructuredData();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getDataSetResultsRule());
                    						}
                    						add(
                    							current,
                    							"expectedResults",
                    							lv_expectedResults_5_0,
                    							"org.eclipse.efbt.aorta.Aorta.ColumnStructuredData");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:7073:4: (otherlv_6= ',' ( (lv_expectedResults_7_0= ruleColumnStructuredData ) ) )*
                    loop169:
                    do {
                        int alt169=2;
                        int LA169_0 = input.LA(1);

                        if ( (LA169_0==31) ) {
                            alt169=1;
                        }


                        switch (alt169) {
                    	case 1 :
                    	    // InternalAorta.g:7074:5: otherlv_6= ',' ( (lv_expectedResults_7_0= ruleColumnStructuredData ) )
                    	    {
                    	    otherlv_6=(Token)match(input,31,FOLLOW_166); 

                    	    					newLeafNode(otherlv_6, grammarAccess.getDataSetResultsAccess().getCommaKeyword_3_3_0());
                    	    				
                    	    // InternalAorta.g:7078:5: ( (lv_expectedResults_7_0= ruleColumnStructuredData ) )
                    	    // InternalAorta.g:7079:6: (lv_expectedResults_7_0= ruleColumnStructuredData )
                    	    {
                    	    // InternalAorta.g:7079:6: (lv_expectedResults_7_0= ruleColumnStructuredData )
                    	    // InternalAorta.g:7080:7: lv_expectedResults_7_0= ruleColumnStructuredData
                    	    {

                    	    							newCompositeNode(grammarAccess.getDataSetResultsAccess().getExpectedResultsColumnStructuredDataParserRuleCall_3_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_35);
                    	    lv_expectedResults_7_0=ruleColumnStructuredData();

                    	    state._fsp--;


                    	    							if (current==null) {
                    	    								current = createModelElementForParent(grammarAccess.getDataSetResultsRule());
                    	    							}
                    	    							add(
                    	    								current,
                    	    								"expectedResults",
                    	    								lv_expectedResults_7_0,
                    	    								"org.eclipse.efbt.aorta.Aorta.ColumnStructuredData");
                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop169;
                        }
                    } while (true);

                    otherlv_8=(Token)match(input,23,FOLLOW_24); 

                    				newLeafNode(otherlv_8, grammarAccess.getDataSetResultsAccess().getRightCurlyBracketKeyword_3_4());
                    			

                    }
                    break;

            }

            otherlv_9=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_9, grammarAccess.getDataSetResultsAccess().getRightCurlyBracketKeyword_4());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleDataSetResults"


    // $ANTLR start "entryRuleRegReportCellValue"
    // InternalAorta.g:7111:1: entryRuleRegReportCellValue returns [EObject current=null] : iv_ruleRegReportCellValue= ruleRegReportCellValue EOF ;
    public final EObject entryRuleRegReportCellValue() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleRegReportCellValue = null;


        try {
            // InternalAorta.g:7111:59: (iv_ruleRegReportCellValue= ruleRegReportCellValue EOF )
            // InternalAorta.g:7112:2: iv_ruleRegReportCellValue= ruleRegReportCellValue EOF
            {
             newCompositeNode(grammarAccess.getRegReportCellValueRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleRegReportCellValue=ruleRegReportCellValue();

            state._fsp--;

             current =iv_ruleRegReportCellValue; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleRegReportCellValue"


    // $ANTLR start "ruleRegReportCellValue"
    // InternalAorta.g:7118:1: ruleRegReportCellValue returns [EObject current=null] : ( () otherlv_1= 'RegReportCellValue' otherlv_2= '{' (otherlv_3= 'value' ( (lv_value_4_0= ruleEString ) ) )? (otherlv_5= 'cell' ( ( ruleQualifiedName ) ) )? otherlv_7= '}' ) ;
    public final EObject ruleRegReportCellValue() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_2=null;
        Token otherlv_3=null;
        Token otherlv_5=null;
        Token otherlv_7=null;
        AntlrDatatypeRuleToken lv_value_4_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:7124:2: ( ( () otherlv_1= 'RegReportCellValue' otherlv_2= '{' (otherlv_3= 'value' ( (lv_value_4_0= ruleEString ) ) )? (otherlv_5= 'cell' ( ( ruleQualifiedName ) ) )? otherlv_7= '}' ) )
            // InternalAorta.g:7125:2: ( () otherlv_1= 'RegReportCellValue' otherlv_2= '{' (otherlv_3= 'value' ( (lv_value_4_0= ruleEString ) ) )? (otherlv_5= 'cell' ( ( ruleQualifiedName ) ) )? otherlv_7= '}' )
            {
            // InternalAorta.g:7125:2: ( () otherlv_1= 'RegReportCellValue' otherlv_2= '{' (otherlv_3= 'value' ( (lv_value_4_0= ruleEString ) ) )? (otherlv_5= 'cell' ( ( ruleQualifiedName ) ) )? otherlv_7= '}' )
            // InternalAorta.g:7126:3: () otherlv_1= 'RegReportCellValue' otherlv_2= '{' (otherlv_3= 'value' ( (lv_value_4_0= ruleEString ) ) )? (otherlv_5= 'cell' ( ( ruleQualifiedName ) ) )? otherlv_7= '}'
            {
            // InternalAorta.g:7126:3: ()
            // InternalAorta.g:7127:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getRegReportCellValueAccess().getRegReportCellValueAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,134,FOLLOW_3); 

            			newLeafNode(otherlv_1, grammarAccess.getRegReportCellValueAccess().getRegReportCellValueKeyword_1());
            		
            otherlv_2=(Token)match(input,12,FOLLOW_167); 

            			newLeafNode(otherlv_2, grammarAccess.getRegReportCellValueAccess().getLeftCurlyBracketKeyword_2());
            		
            // InternalAorta.g:7141:3: (otherlv_3= 'value' ( (lv_value_4_0= ruleEString ) ) )?
            int alt171=2;
            int LA171_0 = input.LA(1);

            if ( (LA171_0==127) ) {
                alt171=1;
            }
            switch (alt171) {
                case 1 :
                    // InternalAorta.g:7142:4: otherlv_3= 'value' ( (lv_value_4_0= ruleEString ) )
                    {
                    otherlv_3=(Token)match(input,127,FOLLOW_25); 

                    				newLeafNode(otherlv_3, grammarAccess.getRegReportCellValueAccess().getValueKeyword_3_0());
                    			
                    // InternalAorta.g:7146:4: ( (lv_value_4_0= ruleEString ) )
                    // InternalAorta.g:7147:5: (lv_value_4_0= ruleEString )
                    {
                    // InternalAorta.g:7147:5: (lv_value_4_0= ruleEString )
                    // InternalAorta.g:7148:6: lv_value_4_0= ruleEString
                    {

                    						newCompositeNode(grammarAccess.getRegReportCellValueAccess().getValueEStringParserRuleCall_3_1_0());
                    					
                    pushFollow(FOLLOW_168);
                    lv_value_4_0=ruleEString();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getRegReportCellValueRule());
                    						}
                    						set(
                    							current,
                    							"value",
                    							lv_value_4_0,
                    							"org.eclipse.efbt.aorta.Aorta.EString");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:7166:3: (otherlv_5= 'cell' ( ( ruleQualifiedName ) ) )?
            int alt172=2;
            int LA172_0 = input.LA(1);

            if ( (LA172_0==135) ) {
                alt172=1;
            }
            switch (alt172) {
                case 1 :
                    // InternalAorta.g:7167:4: otherlv_5= 'cell' ( ( ruleQualifiedName ) )
                    {
                    otherlv_5=(Token)match(input,135,FOLLOW_70); 

                    				newLeafNode(otherlv_5, grammarAccess.getRegReportCellValueAccess().getCellKeyword_4_0());
                    			
                    // InternalAorta.g:7171:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:7172:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:7172:5: ( ruleQualifiedName )
                    // InternalAorta.g:7173:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getRegReportCellValueRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getRegReportCellValueAccess().getCellRegReportCellCrossReference_4_1_0());
                    					
                    pushFollow(FOLLOW_24);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            otherlv_7=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_7, grammarAccess.getRegReportCellValueAccess().getRightCurlyBracketKeyword_5());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleRegReportCellValue"


    // $ANTLR start "entryRuleColumnStructuredData"
    // InternalAorta.g:7196:1: entryRuleColumnStructuredData returns [EObject current=null] : iv_ruleColumnStructuredData= ruleColumnStructuredData EOF ;
    public final EObject entryRuleColumnStructuredData() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleColumnStructuredData = null;


        try {
            // InternalAorta.g:7196:61: (iv_ruleColumnStructuredData= ruleColumnStructuredData EOF )
            // InternalAorta.g:7197:2: iv_ruleColumnStructuredData= ruleColumnStructuredData EOF
            {
             newCompositeNode(grammarAccess.getColumnStructuredDataRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleColumnStructuredData=ruleColumnStructuredData();

            state._fsp--;

             current =iv_ruleColumnStructuredData; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleColumnStructuredData"


    // $ANTLR start "ruleColumnStructuredData"
    // InternalAorta.g:7203:1: ruleColumnStructuredData returns [EObject current=null] : ( () otherlv_1= 'ColumnStructuredData' ( (lv_name_2_0= ruleEString ) ) ) ;
    public final EObject ruleColumnStructuredData() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:7209:2: ( ( () otherlv_1= 'ColumnStructuredData' ( (lv_name_2_0= ruleEString ) ) ) )
            // InternalAorta.g:7210:2: ( () otherlv_1= 'ColumnStructuredData' ( (lv_name_2_0= ruleEString ) ) )
            {
            // InternalAorta.g:7210:2: ( () otherlv_1= 'ColumnStructuredData' ( (lv_name_2_0= ruleEString ) ) )
            // InternalAorta.g:7211:3: () otherlv_1= 'ColumnStructuredData' ( (lv_name_2_0= ruleEString ) )
            {
            // InternalAorta.g:7211:3: ()
            // InternalAorta.g:7212:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getColumnStructuredDataAccess().getColumnStructuredDataAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,136,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getColumnStructuredDataAccess().getColumnStructuredDataKeyword_1());
            		
            // InternalAorta.g:7222:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:7223:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:7223:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:7224:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getColumnStructuredDataAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_2);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getColumnStructuredDataRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }


            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleColumnStructuredData"


    // $ANTLR start "entryRuleInputType"
    // InternalAorta.g:7245:1: entryRuleInputType returns [EObject current=null] : iv_ruleInputType= ruleInputType EOF ;
    public final EObject entryRuleInputType() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleInputType = null;


        try {
            // InternalAorta.g:7245:50: (iv_ruleInputType= ruleInputType EOF )
            // InternalAorta.g:7246:2: iv_ruleInputType= ruleInputType EOF
            {
             newCompositeNode(grammarAccess.getInputTypeRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleInputType=ruleInputType();

            state._fsp--;

             current =iv_ruleInputType; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleInputType"


    // $ANTLR start "ruleInputType"
    // InternalAorta.g:7252:1: ruleInputType returns [EObject current=null] : ( () otherlv_1= 'InputType' ( (lv_name_2_0= ruleEString ) ) ) ;
    public final EObject ruleInputType() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:7258:2: ( ( () otherlv_1= 'InputType' ( (lv_name_2_0= ruleEString ) ) ) )
            // InternalAorta.g:7259:2: ( () otherlv_1= 'InputType' ( (lv_name_2_0= ruleEString ) ) )
            {
            // InternalAorta.g:7259:2: ( () otherlv_1= 'InputType' ( (lv_name_2_0= ruleEString ) ) )
            // InternalAorta.g:7260:3: () otherlv_1= 'InputType' ( (lv_name_2_0= ruleEString ) )
            {
            // InternalAorta.g:7260:3: ()
            // InternalAorta.g:7261:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getInputTypeAccess().getInputTypeAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,137,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getInputTypeAccess().getInputTypeKeyword_1());
            		
            // InternalAorta.g:7271:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:7272:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:7272:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:7273:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getInputTypeAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_2);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getInputTypeRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }


            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleInputType"


    // $ANTLR start "entryRuleTestTemplate"
    // InternalAorta.g:7294:1: entryRuleTestTemplate returns [EObject current=null] : iv_ruleTestTemplate= ruleTestTemplate EOF ;
    public final EObject entryRuleTestTemplate() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleTestTemplate = null;


        try {
            // InternalAorta.g:7294:53: (iv_ruleTestTemplate= ruleTestTemplate EOF )
            // InternalAorta.g:7295:2: iv_ruleTestTemplate= ruleTestTemplate EOF
            {
             newCompositeNode(grammarAccess.getTestTemplateRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleTestTemplate=ruleTestTemplate();

            state._fsp--;

             current =iv_ruleTestTemplate; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleTestTemplate"


    // $ANTLR start "ruleTestTemplate"
    // InternalAorta.g:7301:1: ruleTestTemplate returns [EObject current=null] : ( () otherlv_1= 'TestTemplate' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'givenText' ( (lv_givenText_5_0= ruleClauseText ) ) )? (otherlv_6= 'whenText' ( (lv_whenText_7_0= ruleClauseText ) ) )? (otherlv_8= 'thenText' ( (lv_thenText_9_0= ruleClauseText ) ) )? (otherlv_10= 'givenParams' otherlv_11= '{' ( (lv_givenParams_12_0= ruleParam ) ) (otherlv_13= ',' ( (lv_givenParams_14_0= ruleParam ) ) )* otherlv_15= '}' )? (otherlv_16= 'whenParams' otherlv_17= '{' ( (lv_whenParams_18_0= ruleParam ) ) (otherlv_19= ',' ( (lv_whenParams_20_0= ruleParam ) ) )* otherlv_21= '}' )? (otherlv_22= 'thenParams' otherlv_23= '{' ( (lv_thenParams_24_0= ruleParam ) ) (otherlv_25= ',' ( (lv_thenParams_26_0= ruleParam ) ) )* otherlv_27= '}' )? otherlv_28= '}' ) ;
    public final EObject ruleTestTemplate() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_6=null;
        Token otherlv_8=null;
        Token otherlv_10=null;
        Token otherlv_11=null;
        Token otherlv_13=null;
        Token otherlv_15=null;
        Token otherlv_16=null;
        Token otherlv_17=null;
        Token otherlv_19=null;
        Token otherlv_21=null;
        Token otherlv_22=null;
        Token otherlv_23=null;
        Token otherlv_25=null;
        Token otherlv_27=null;
        Token otherlv_28=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;

        EObject lv_givenText_5_0 = null;

        EObject lv_whenText_7_0 = null;

        EObject lv_thenText_9_0 = null;

        EObject lv_givenParams_12_0 = null;

        EObject lv_givenParams_14_0 = null;

        EObject lv_whenParams_18_0 = null;

        EObject lv_whenParams_20_0 = null;

        EObject lv_thenParams_24_0 = null;

        EObject lv_thenParams_26_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:7307:2: ( ( () otherlv_1= 'TestTemplate' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'givenText' ( (lv_givenText_5_0= ruleClauseText ) ) )? (otherlv_6= 'whenText' ( (lv_whenText_7_0= ruleClauseText ) ) )? (otherlv_8= 'thenText' ( (lv_thenText_9_0= ruleClauseText ) ) )? (otherlv_10= 'givenParams' otherlv_11= '{' ( (lv_givenParams_12_0= ruleParam ) ) (otherlv_13= ',' ( (lv_givenParams_14_0= ruleParam ) ) )* otherlv_15= '}' )? (otherlv_16= 'whenParams' otherlv_17= '{' ( (lv_whenParams_18_0= ruleParam ) ) (otherlv_19= ',' ( (lv_whenParams_20_0= ruleParam ) ) )* otherlv_21= '}' )? (otherlv_22= 'thenParams' otherlv_23= '{' ( (lv_thenParams_24_0= ruleParam ) ) (otherlv_25= ',' ( (lv_thenParams_26_0= ruleParam ) ) )* otherlv_27= '}' )? otherlv_28= '}' ) )
            // InternalAorta.g:7308:2: ( () otherlv_1= 'TestTemplate' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'givenText' ( (lv_givenText_5_0= ruleClauseText ) ) )? (otherlv_6= 'whenText' ( (lv_whenText_7_0= ruleClauseText ) ) )? (otherlv_8= 'thenText' ( (lv_thenText_9_0= ruleClauseText ) ) )? (otherlv_10= 'givenParams' otherlv_11= '{' ( (lv_givenParams_12_0= ruleParam ) ) (otherlv_13= ',' ( (lv_givenParams_14_0= ruleParam ) ) )* otherlv_15= '}' )? (otherlv_16= 'whenParams' otherlv_17= '{' ( (lv_whenParams_18_0= ruleParam ) ) (otherlv_19= ',' ( (lv_whenParams_20_0= ruleParam ) ) )* otherlv_21= '}' )? (otherlv_22= 'thenParams' otherlv_23= '{' ( (lv_thenParams_24_0= ruleParam ) ) (otherlv_25= ',' ( (lv_thenParams_26_0= ruleParam ) ) )* otherlv_27= '}' )? otherlv_28= '}' )
            {
            // InternalAorta.g:7308:2: ( () otherlv_1= 'TestTemplate' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'givenText' ( (lv_givenText_5_0= ruleClauseText ) ) )? (otherlv_6= 'whenText' ( (lv_whenText_7_0= ruleClauseText ) ) )? (otherlv_8= 'thenText' ( (lv_thenText_9_0= ruleClauseText ) ) )? (otherlv_10= 'givenParams' otherlv_11= '{' ( (lv_givenParams_12_0= ruleParam ) ) (otherlv_13= ',' ( (lv_givenParams_14_0= ruleParam ) ) )* otherlv_15= '}' )? (otherlv_16= 'whenParams' otherlv_17= '{' ( (lv_whenParams_18_0= ruleParam ) ) (otherlv_19= ',' ( (lv_whenParams_20_0= ruleParam ) ) )* otherlv_21= '}' )? (otherlv_22= 'thenParams' otherlv_23= '{' ( (lv_thenParams_24_0= ruleParam ) ) (otherlv_25= ',' ( (lv_thenParams_26_0= ruleParam ) ) )* otherlv_27= '}' )? otherlv_28= '}' )
            // InternalAorta.g:7309:3: () otherlv_1= 'TestTemplate' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'givenText' ( (lv_givenText_5_0= ruleClauseText ) ) )? (otherlv_6= 'whenText' ( (lv_whenText_7_0= ruleClauseText ) ) )? (otherlv_8= 'thenText' ( (lv_thenText_9_0= ruleClauseText ) ) )? (otherlv_10= 'givenParams' otherlv_11= '{' ( (lv_givenParams_12_0= ruleParam ) ) (otherlv_13= ',' ( (lv_givenParams_14_0= ruleParam ) ) )* otherlv_15= '}' )? (otherlv_16= 'whenParams' otherlv_17= '{' ( (lv_whenParams_18_0= ruleParam ) ) (otherlv_19= ',' ( (lv_whenParams_20_0= ruleParam ) ) )* otherlv_21= '}' )? (otherlv_22= 'thenParams' otherlv_23= '{' ( (lv_thenParams_24_0= ruleParam ) ) (otherlv_25= ',' ( (lv_thenParams_26_0= ruleParam ) ) )* otherlv_27= '}' )? otherlv_28= '}'
            {
            // InternalAorta.g:7309:3: ()
            // InternalAorta.g:7310:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getTestTemplateAccess().getTestTemplateAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,138,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getTestTemplateAccess().getTestTemplateKeyword_1());
            		
            // InternalAorta.g:7320:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:7321:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:7321:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:7322:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getTestTemplateAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getTestTemplateRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_169); 

            			newLeafNode(otherlv_3, grammarAccess.getTestTemplateAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:7343:3: (otherlv_4= 'givenText' ( (lv_givenText_5_0= ruleClauseText ) ) )?
            int alt173=2;
            int LA173_0 = input.LA(1);

            if ( (LA173_0==139) ) {
                alt173=1;
            }
            switch (alt173) {
                case 1 :
                    // InternalAorta.g:7344:4: otherlv_4= 'givenText' ( (lv_givenText_5_0= ruleClauseText ) )
                    {
                    otherlv_4=(Token)match(input,139,FOLLOW_170); 

                    				newLeafNode(otherlv_4, grammarAccess.getTestTemplateAccess().getGivenTextKeyword_4_0());
                    			
                    // InternalAorta.g:7348:4: ( (lv_givenText_5_0= ruleClauseText ) )
                    // InternalAorta.g:7349:5: (lv_givenText_5_0= ruleClauseText )
                    {
                    // InternalAorta.g:7349:5: (lv_givenText_5_0= ruleClauseText )
                    // InternalAorta.g:7350:6: lv_givenText_5_0= ruleClauseText
                    {

                    						newCompositeNode(grammarAccess.getTestTemplateAccess().getGivenTextClauseTextParserRuleCall_4_1_0());
                    					
                    pushFollow(FOLLOW_171);
                    lv_givenText_5_0=ruleClauseText();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getTestTemplateRule());
                    						}
                    						set(
                    							current,
                    							"givenText",
                    							lv_givenText_5_0,
                    							"org.eclipse.efbt.aorta.Aorta.ClauseText");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:7368:3: (otherlv_6= 'whenText' ( (lv_whenText_7_0= ruleClauseText ) ) )?
            int alt174=2;
            int LA174_0 = input.LA(1);

            if ( (LA174_0==140) ) {
                alt174=1;
            }
            switch (alt174) {
                case 1 :
                    // InternalAorta.g:7369:4: otherlv_6= 'whenText' ( (lv_whenText_7_0= ruleClauseText ) )
                    {
                    otherlv_6=(Token)match(input,140,FOLLOW_170); 

                    				newLeafNode(otherlv_6, grammarAccess.getTestTemplateAccess().getWhenTextKeyword_5_0());
                    			
                    // InternalAorta.g:7373:4: ( (lv_whenText_7_0= ruleClauseText ) )
                    // InternalAorta.g:7374:5: (lv_whenText_7_0= ruleClauseText )
                    {
                    // InternalAorta.g:7374:5: (lv_whenText_7_0= ruleClauseText )
                    // InternalAorta.g:7375:6: lv_whenText_7_0= ruleClauseText
                    {

                    						newCompositeNode(grammarAccess.getTestTemplateAccess().getWhenTextClauseTextParserRuleCall_5_1_0());
                    					
                    pushFollow(FOLLOW_172);
                    lv_whenText_7_0=ruleClauseText();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getTestTemplateRule());
                    						}
                    						set(
                    							current,
                    							"whenText",
                    							lv_whenText_7_0,
                    							"org.eclipse.efbt.aorta.Aorta.ClauseText");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:7393:3: (otherlv_8= 'thenText' ( (lv_thenText_9_0= ruleClauseText ) ) )?
            int alt175=2;
            int LA175_0 = input.LA(1);

            if ( (LA175_0==141) ) {
                alt175=1;
            }
            switch (alt175) {
                case 1 :
                    // InternalAorta.g:7394:4: otherlv_8= 'thenText' ( (lv_thenText_9_0= ruleClauseText ) )
                    {
                    otherlv_8=(Token)match(input,141,FOLLOW_170); 

                    				newLeafNode(otherlv_8, grammarAccess.getTestTemplateAccess().getThenTextKeyword_6_0());
                    			
                    // InternalAorta.g:7398:4: ( (lv_thenText_9_0= ruleClauseText ) )
                    // InternalAorta.g:7399:5: (lv_thenText_9_0= ruleClauseText )
                    {
                    // InternalAorta.g:7399:5: (lv_thenText_9_0= ruleClauseText )
                    // InternalAorta.g:7400:6: lv_thenText_9_0= ruleClauseText
                    {

                    						newCompositeNode(grammarAccess.getTestTemplateAccess().getThenTextClauseTextParserRuleCall_6_1_0());
                    					
                    pushFollow(FOLLOW_90);
                    lv_thenText_9_0=ruleClauseText();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getTestTemplateRule());
                    						}
                    						set(
                    							current,
                    							"thenText",
                    							lv_thenText_9_0,
                    							"org.eclipse.efbt.aorta.Aorta.ClauseText");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:7418:3: (otherlv_10= 'givenParams' otherlv_11= '{' ( (lv_givenParams_12_0= ruleParam ) ) (otherlv_13= ',' ( (lv_givenParams_14_0= ruleParam ) ) )* otherlv_15= '}' )?
            int alt177=2;
            int LA177_0 = input.LA(1);

            if ( (LA177_0==48) ) {
                alt177=1;
            }
            switch (alt177) {
                case 1 :
                    // InternalAorta.g:7419:4: otherlv_10= 'givenParams' otherlv_11= '{' ( (lv_givenParams_12_0= ruleParam ) ) (otherlv_13= ',' ( (lv_givenParams_14_0= ruleParam ) ) )* otherlv_15= '}'
                    {
                    otherlv_10=(Token)match(input,48,FOLLOW_3); 

                    				newLeafNode(otherlv_10, grammarAccess.getTestTemplateAccess().getGivenParamsKeyword_7_0());
                    			
                    otherlv_11=(Token)match(input,12,FOLLOW_91); 

                    				newLeafNode(otherlv_11, grammarAccess.getTestTemplateAccess().getLeftCurlyBracketKeyword_7_1());
                    			
                    // InternalAorta.g:7427:4: ( (lv_givenParams_12_0= ruleParam ) )
                    // InternalAorta.g:7428:5: (lv_givenParams_12_0= ruleParam )
                    {
                    // InternalAorta.g:7428:5: (lv_givenParams_12_0= ruleParam )
                    // InternalAorta.g:7429:6: lv_givenParams_12_0= ruleParam
                    {

                    						newCompositeNode(grammarAccess.getTestTemplateAccess().getGivenParamsParamParserRuleCall_7_2_0());
                    					
                    pushFollow(FOLLOW_35);
                    lv_givenParams_12_0=ruleParam();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getTestTemplateRule());
                    						}
                    						add(
                    							current,
                    							"givenParams",
                    							lv_givenParams_12_0,
                    							"org.eclipse.efbt.aorta.Aorta.Param");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:7446:4: (otherlv_13= ',' ( (lv_givenParams_14_0= ruleParam ) ) )*
                    loop176:
                    do {
                        int alt176=2;
                        int LA176_0 = input.LA(1);

                        if ( (LA176_0==31) ) {
                            alt176=1;
                        }


                        switch (alt176) {
                    	case 1 :
                    	    // InternalAorta.g:7447:5: otherlv_13= ',' ( (lv_givenParams_14_0= ruleParam ) )
                    	    {
                    	    otherlv_13=(Token)match(input,31,FOLLOW_91); 

                    	    					newLeafNode(otherlv_13, grammarAccess.getTestTemplateAccess().getCommaKeyword_7_3_0());
                    	    				
                    	    // InternalAorta.g:7451:5: ( (lv_givenParams_14_0= ruleParam ) )
                    	    // InternalAorta.g:7452:6: (lv_givenParams_14_0= ruleParam )
                    	    {
                    	    // InternalAorta.g:7452:6: (lv_givenParams_14_0= ruleParam )
                    	    // InternalAorta.g:7453:7: lv_givenParams_14_0= ruleParam
                    	    {

                    	    							newCompositeNode(grammarAccess.getTestTemplateAccess().getGivenParamsParamParserRuleCall_7_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_35);
                    	    lv_givenParams_14_0=ruleParam();

                    	    state._fsp--;


                    	    							if (current==null) {
                    	    								current = createModelElementForParent(grammarAccess.getTestTemplateRule());
                    	    							}
                    	    							add(
                    	    								current,
                    	    								"givenParams",
                    	    								lv_givenParams_14_0,
                    	    								"org.eclipse.efbt.aorta.Aorta.Param");
                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop176;
                        }
                    } while (true);

                    otherlv_15=(Token)match(input,23,FOLLOW_92); 

                    				newLeafNode(otherlv_15, grammarAccess.getTestTemplateAccess().getRightCurlyBracketKeyword_7_4());
                    			

                    }
                    break;

            }

            // InternalAorta.g:7476:3: (otherlv_16= 'whenParams' otherlv_17= '{' ( (lv_whenParams_18_0= ruleParam ) ) (otherlv_19= ',' ( (lv_whenParams_20_0= ruleParam ) ) )* otherlv_21= '}' )?
            int alt179=2;
            int LA179_0 = input.LA(1);

            if ( (LA179_0==49) ) {
                alt179=1;
            }
            switch (alt179) {
                case 1 :
                    // InternalAorta.g:7477:4: otherlv_16= 'whenParams' otherlv_17= '{' ( (lv_whenParams_18_0= ruleParam ) ) (otherlv_19= ',' ( (lv_whenParams_20_0= ruleParam ) ) )* otherlv_21= '}'
                    {
                    otherlv_16=(Token)match(input,49,FOLLOW_3); 

                    				newLeafNode(otherlv_16, grammarAccess.getTestTemplateAccess().getWhenParamsKeyword_8_0());
                    			
                    otherlv_17=(Token)match(input,12,FOLLOW_91); 

                    				newLeafNode(otherlv_17, grammarAccess.getTestTemplateAccess().getLeftCurlyBracketKeyword_8_1());
                    			
                    // InternalAorta.g:7485:4: ( (lv_whenParams_18_0= ruleParam ) )
                    // InternalAorta.g:7486:5: (lv_whenParams_18_0= ruleParam )
                    {
                    // InternalAorta.g:7486:5: (lv_whenParams_18_0= ruleParam )
                    // InternalAorta.g:7487:6: lv_whenParams_18_0= ruleParam
                    {

                    						newCompositeNode(grammarAccess.getTestTemplateAccess().getWhenParamsParamParserRuleCall_8_2_0());
                    					
                    pushFollow(FOLLOW_35);
                    lv_whenParams_18_0=ruleParam();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getTestTemplateRule());
                    						}
                    						add(
                    							current,
                    							"whenParams",
                    							lv_whenParams_18_0,
                    							"org.eclipse.efbt.aorta.Aorta.Param");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:7504:4: (otherlv_19= ',' ( (lv_whenParams_20_0= ruleParam ) ) )*
                    loop178:
                    do {
                        int alt178=2;
                        int LA178_0 = input.LA(1);

                        if ( (LA178_0==31) ) {
                            alt178=1;
                        }


                        switch (alt178) {
                    	case 1 :
                    	    // InternalAorta.g:7505:5: otherlv_19= ',' ( (lv_whenParams_20_0= ruleParam ) )
                    	    {
                    	    otherlv_19=(Token)match(input,31,FOLLOW_91); 

                    	    					newLeafNode(otherlv_19, grammarAccess.getTestTemplateAccess().getCommaKeyword_8_3_0());
                    	    				
                    	    // InternalAorta.g:7509:5: ( (lv_whenParams_20_0= ruleParam ) )
                    	    // InternalAorta.g:7510:6: (lv_whenParams_20_0= ruleParam )
                    	    {
                    	    // InternalAorta.g:7510:6: (lv_whenParams_20_0= ruleParam )
                    	    // InternalAorta.g:7511:7: lv_whenParams_20_0= ruleParam
                    	    {

                    	    							newCompositeNode(grammarAccess.getTestTemplateAccess().getWhenParamsParamParserRuleCall_8_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_35);
                    	    lv_whenParams_20_0=ruleParam();

                    	    state._fsp--;


                    	    							if (current==null) {
                    	    								current = createModelElementForParent(grammarAccess.getTestTemplateRule());
                    	    							}
                    	    							add(
                    	    								current,
                    	    								"whenParams",
                    	    								lv_whenParams_20_0,
                    	    								"org.eclipse.efbt.aorta.Aorta.Param");
                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop178;
                        }
                    } while (true);

                    otherlv_21=(Token)match(input,23,FOLLOW_93); 

                    				newLeafNode(otherlv_21, grammarAccess.getTestTemplateAccess().getRightCurlyBracketKeyword_8_4());
                    			

                    }
                    break;

            }

            // InternalAorta.g:7534:3: (otherlv_22= 'thenParams' otherlv_23= '{' ( (lv_thenParams_24_0= ruleParam ) ) (otherlv_25= ',' ( (lv_thenParams_26_0= ruleParam ) ) )* otherlv_27= '}' )?
            int alt181=2;
            int LA181_0 = input.LA(1);

            if ( (LA181_0==50) ) {
                alt181=1;
            }
            switch (alt181) {
                case 1 :
                    // InternalAorta.g:7535:4: otherlv_22= 'thenParams' otherlv_23= '{' ( (lv_thenParams_24_0= ruleParam ) ) (otherlv_25= ',' ( (lv_thenParams_26_0= ruleParam ) ) )* otherlv_27= '}'
                    {
                    otherlv_22=(Token)match(input,50,FOLLOW_3); 

                    				newLeafNode(otherlv_22, grammarAccess.getTestTemplateAccess().getThenParamsKeyword_9_0());
                    			
                    otherlv_23=(Token)match(input,12,FOLLOW_91); 

                    				newLeafNode(otherlv_23, grammarAccess.getTestTemplateAccess().getLeftCurlyBracketKeyword_9_1());
                    			
                    // InternalAorta.g:7543:4: ( (lv_thenParams_24_0= ruleParam ) )
                    // InternalAorta.g:7544:5: (lv_thenParams_24_0= ruleParam )
                    {
                    // InternalAorta.g:7544:5: (lv_thenParams_24_0= ruleParam )
                    // InternalAorta.g:7545:6: lv_thenParams_24_0= ruleParam
                    {

                    						newCompositeNode(grammarAccess.getTestTemplateAccess().getThenParamsParamParserRuleCall_9_2_0());
                    					
                    pushFollow(FOLLOW_35);
                    lv_thenParams_24_0=ruleParam();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getTestTemplateRule());
                    						}
                    						add(
                    							current,
                    							"thenParams",
                    							lv_thenParams_24_0,
                    							"org.eclipse.efbt.aorta.Aorta.Param");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:7562:4: (otherlv_25= ',' ( (lv_thenParams_26_0= ruleParam ) ) )*
                    loop180:
                    do {
                        int alt180=2;
                        int LA180_0 = input.LA(1);

                        if ( (LA180_0==31) ) {
                            alt180=1;
                        }


                        switch (alt180) {
                    	case 1 :
                    	    // InternalAorta.g:7563:5: otherlv_25= ',' ( (lv_thenParams_26_0= ruleParam ) )
                    	    {
                    	    otherlv_25=(Token)match(input,31,FOLLOW_91); 

                    	    					newLeafNode(otherlv_25, grammarAccess.getTestTemplateAccess().getCommaKeyword_9_3_0());
                    	    				
                    	    // InternalAorta.g:7567:5: ( (lv_thenParams_26_0= ruleParam ) )
                    	    // InternalAorta.g:7568:6: (lv_thenParams_26_0= ruleParam )
                    	    {
                    	    // InternalAorta.g:7568:6: (lv_thenParams_26_0= ruleParam )
                    	    // InternalAorta.g:7569:7: lv_thenParams_26_0= ruleParam
                    	    {

                    	    							newCompositeNode(grammarAccess.getTestTemplateAccess().getThenParamsParamParserRuleCall_9_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_35);
                    	    lv_thenParams_26_0=ruleParam();

                    	    state._fsp--;


                    	    							if (current==null) {
                    	    								current = createModelElementForParent(grammarAccess.getTestTemplateRule());
                    	    							}
                    	    							add(
                    	    								current,
                    	    								"thenParams",
                    	    								lv_thenParams_26_0,
                    	    								"org.eclipse.efbt.aorta.Aorta.Param");
                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop180;
                        }
                    } while (true);

                    otherlv_27=(Token)match(input,23,FOLLOW_24); 

                    				newLeafNode(otherlv_27, grammarAccess.getTestTemplateAccess().getRightCurlyBracketKeyword_9_4());
                    			

                    }
                    break;

            }

            otherlv_28=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_28, grammarAccess.getTestTemplateAccess().getRightCurlyBracketKeyword_10());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleTestTemplate"


    // $ANTLR start "entryRuleFunctionalityModule_Impl"
    // InternalAorta.g:7600:1: entryRuleFunctionalityModule_Impl returns [EObject current=null] : iv_ruleFunctionalityModule_Impl= ruleFunctionalityModule_Impl EOF ;
    public final EObject entryRuleFunctionalityModule_Impl() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleFunctionalityModule_Impl = null;


        try {
            // InternalAorta.g:7600:65: (iv_ruleFunctionalityModule_Impl= ruleFunctionalityModule_Impl EOF )
            // InternalAorta.g:7601:2: iv_ruleFunctionalityModule_Impl= ruleFunctionalityModule_Impl EOF
            {
             newCompositeNode(grammarAccess.getFunctionalityModule_ImplRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleFunctionalityModule_Impl=ruleFunctionalityModule_Impl();

            state._fsp--;

             current =iv_ruleFunctionalityModule_Impl; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleFunctionalityModule_Impl"


    // $ANTLR start "ruleFunctionalityModule_Impl"
    // InternalAorta.g:7607:1: ruleFunctionalityModule_Impl returns [EObject current=null] : ( () otherlv_1= 'FunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? otherlv_10= '}' ) ;
    public final EObject ruleFunctionalityModule_Impl() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_6=null;
        Token otherlv_8=null;
        Token otherlv_10=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;

        EObject lv_scenarioSet_9_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:7613:2: ( ( () otherlv_1= 'FunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? otherlv_10= '}' ) )
            // InternalAorta.g:7614:2: ( () otherlv_1= 'FunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? otherlv_10= '}' )
            {
            // InternalAorta.g:7614:2: ( () otherlv_1= 'FunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? otherlv_10= '}' )
            // InternalAorta.g:7615:3: () otherlv_1= 'FunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? otherlv_10= '}'
            {
            // InternalAorta.g:7615:3: ()
            // InternalAorta.g:7616:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getFunctionalityModule_ImplAccess().getFunctionalityModuleAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,142,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getFunctionalityModule_ImplAccess().getFunctionalityModuleKeyword_1());
            		
            // InternalAorta.g:7626:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:7627:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:7627:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:7628:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getFunctionalityModule_ImplAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getFunctionalityModule_ImplRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_173); 

            			newLeafNode(otherlv_3, grammarAccess.getFunctionalityModule_ImplAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:7649:3: (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )?
            int alt182=2;
            int LA182_0 = input.LA(1);

            if ( (LA182_0==143) ) {
                alt182=1;
            }
            switch (alt182) {
                case 1 :
                    // InternalAorta.g:7650:4: otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) )
                    {
                    otherlv_4=(Token)match(input,143,FOLLOW_70); 

                    				newLeafNode(otherlv_4, grammarAccess.getFunctionalityModule_ImplAccess().getUsedTransformationOrSchemeKeyword_4_0());
                    			
                    // InternalAorta.g:7654:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:7655:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:7655:5: ( ruleQualifiedName )
                    // InternalAorta.g:7656:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getFunctionalityModule_ImplRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getFunctionalityModule_ImplAccess().getUsedTransformationOrSchemeFunctionalityModuleCrossReference_4_1_0());
                    					
                    pushFollow(FOLLOW_174);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:7671:3: (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )?
            int alt183=2;
            int LA183_0 = input.LA(1);

            if ( (LA183_0==144) ) {
                alt183=1;
            }
            switch (alt183) {
                case 1 :
                    // InternalAorta.g:7672:4: otherlv_6= 'scenario' ( ( ruleQualifiedName ) )
                    {
                    otherlv_6=(Token)match(input,144,FOLLOW_70); 

                    				newLeafNode(otherlv_6, grammarAccess.getFunctionalityModule_ImplAccess().getScenarioKeyword_5_0());
                    			
                    // InternalAorta.g:7676:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:7677:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:7677:5: ( ruleQualifiedName )
                    // InternalAorta.g:7678:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getFunctionalityModule_ImplRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getFunctionalityModule_ImplAccess().getScenarioScenarioCrossReference_5_1_0());
                    					
                    pushFollow(FOLLOW_175);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:7693:3: (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )?
            int alt184=2;
            int LA184_0 = input.LA(1);

            if ( (LA184_0==145) ) {
                alt184=1;
            }
            switch (alt184) {
                case 1 :
                    // InternalAorta.g:7694:4: otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) )
                    {
                    otherlv_8=(Token)match(input,145,FOLLOW_176); 

                    				newLeafNode(otherlv_8, grammarAccess.getFunctionalityModule_ImplAccess().getScenarioSetKeyword_6_0());
                    			
                    // InternalAorta.g:7698:4: ( (lv_scenarioSet_9_0= ruleScenarioSet ) )
                    // InternalAorta.g:7699:5: (lv_scenarioSet_9_0= ruleScenarioSet )
                    {
                    // InternalAorta.g:7699:5: (lv_scenarioSet_9_0= ruleScenarioSet )
                    // InternalAorta.g:7700:6: lv_scenarioSet_9_0= ruleScenarioSet
                    {

                    						newCompositeNode(grammarAccess.getFunctionalityModule_ImplAccess().getScenarioSetScenarioSetParserRuleCall_6_1_0());
                    					
                    pushFollow(FOLLOW_24);
                    lv_scenarioSet_9_0=ruleScenarioSet();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getFunctionalityModule_ImplRule());
                    						}
                    						set(
                    							current,
                    							"scenarioSet",
                    							lv_scenarioSet_9_0,
                    							"org.eclipse.efbt.aorta.Aorta.ScenarioSet");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            otherlv_10=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_10, grammarAccess.getFunctionalityModule_ImplAccess().getRightCurlyBracketKeyword_7());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleFunctionalityModule_Impl"


    // $ANTLR start "entryRuleScenario"
    // InternalAorta.g:7726:1: entryRuleScenario returns [EObject current=null] : iv_ruleScenario= ruleScenario EOF ;
    public final EObject entryRuleScenario() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleScenario = null;


        try {
            // InternalAorta.g:7726:49: (iv_ruleScenario= ruleScenario EOF )
            // InternalAorta.g:7727:2: iv_ruleScenario= ruleScenario EOF
            {
             newCompositeNode(grammarAccess.getScenarioRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleScenario=ruleScenario();

            state._fsp--;

             current =iv_ruleScenario; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleScenario"


    // $ANTLR start "ruleScenario"
    // InternalAorta.g:7733:1: ruleScenario returns [EObject current=null] : ( () otherlv_1= 'Scenario' ( (lv_name_2_0= ruleEString ) ) ) ;
    public final EObject ruleScenario() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:7739:2: ( ( () otherlv_1= 'Scenario' ( (lv_name_2_0= ruleEString ) ) ) )
            // InternalAorta.g:7740:2: ( () otherlv_1= 'Scenario' ( (lv_name_2_0= ruleEString ) ) )
            {
            // InternalAorta.g:7740:2: ( () otherlv_1= 'Scenario' ( (lv_name_2_0= ruleEString ) ) )
            // InternalAorta.g:7741:3: () otherlv_1= 'Scenario' ( (lv_name_2_0= ruleEString ) )
            {
            // InternalAorta.g:7741:3: ()
            // InternalAorta.g:7742:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getScenarioAccess().getScenarioAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,146,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getScenarioAccess().getScenarioKeyword_1());
            		
            // InternalAorta.g:7752:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:7753:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:7753:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:7754:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getScenarioAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_2);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getScenarioRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }


            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleScenario"


    // $ANTLR start "entryRuleScenarioSet"
    // InternalAorta.g:7775:1: entryRuleScenarioSet returns [EObject current=null] : iv_ruleScenarioSet= ruleScenarioSet EOF ;
    public final EObject entryRuleScenarioSet() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleScenarioSet = null;


        try {
            // InternalAorta.g:7775:52: (iv_ruleScenarioSet= ruleScenarioSet EOF )
            // InternalAorta.g:7776:2: iv_ruleScenarioSet= ruleScenarioSet EOF
            {
             newCompositeNode(grammarAccess.getScenarioSetRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleScenarioSet=ruleScenarioSet();

            state._fsp--;

             current =iv_ruleScenarioSet; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleScenarioSet"


    // $ANTLR start "ruleScenarioSet"
    // InternalAorta.g:7782:1: ruleScenarioSet returns [EObject current=null] : ( () otherlv_1= 'ScenarioSet' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'scenarios' otherlv_5= '{' ( (lv_scenarios_6_0= ruleScenario ) ) (otherlv_7= ',' ( (lv_scenarios_8_0= ruleScenario ) ) )* otherlv_9= '}' )? otherlv_10= '}' ) ;
    public final EObject ruleScenarioSet() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_5=null;
        Token otherlv_7=null;
        Token otherlv_9=null;
        Token otherlv_10=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;

        EObject lv_scenarios_6_0 = null;

        EObject lv_scenarios_8_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:7788:2: ( ( () otherlv_1= 'ScenarioSet' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'scenarios' otherlv_5= '{' ( (lv_scenarios_6_0= ruleScenario ) ) (otherlv_7= ',' ( (lv_scenarios_8_0= ruleScenario ) ) )* otherlv_9= '}' )? otherlv_10= '}' ) )
            // InternalAorta.g:7789:2: ( () otherlv_1= 'ScenarioSet' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'scenarios' otherlv_5= '{' ( (lv_scenarios_6_0= ruleScenario ) ) (otherlv_7= ',' ( (lv_scenarios_8_0= ruleScenario ) ) )* otherlv_9= '}' )? otherlv_10= '}' )
            {
            // InternalAorta.g:7789:2: ( () otherlv_1= 'ScenarioSet' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'scenarios' otherlv_5= '{' ( (lv_scenarios_6_0= ruleScenario ) ) (otherlv_7= ',' ( (lv_scenarios_8_0= ruleScenario ) ) )* otherlv_9= '}' )? otherlv_10= '}' )
            // InternalAorta.g:7790:3: () otherlv_1= 'ScenarioSet' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'scenarios' otherlv_5= '{' ( (lv_scenarios_6_0= ruleScenario ) ) (otherlv_7= ',' ( (lv_scenarios_8_0= ruleScenario ) ) )* otherlv_9= '}' )? otherlv_10= '}'
            {
            // InternalAorta.g:7790:3: ()
            // InternalAorta.g:7791:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getScenarioSetAccess().getScenarioSetAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,147,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getScenarioSetAccess().getScenarioSetKeyword_1());
            		
            // InternalAorta.g:7801:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:7802:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:7802:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:7803:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getScenarioSetAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getScenarioSetRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_177); 

            			newLeafNode(otherlv_3, grammarAccess.getScenarioSetAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:7824:3: (otherlv_4= 'scenarios' otherlv_5= '{' ( (lv_scenarios_6_0= ruleScenario ) ) (otherlv_7= ',' ( (lv_scenarios_8_0= ruleScenario ) ) )* otherlv_9= '}' )?
            int alt186=2;
            int LA186_0 = input.LA(1);

            if ( (LA186_0==148) ) {
                alt186=1;
            }
            switch (alt186) {
                case 1 :
                    // InternalAorta.g:7825:4: otherlv_4= 'scenarios' otherlv_5= '{' ( (lv_scenarios_6_0= ruleScenario ) ) (otherlv_7= ',' ( (lv_scenarios_8_0= ruleScenario ) ) )* otherlv_9= '}'
                    {
                    otherlv_4=(Token)match(input,148,FOLLOW_3); 

                    				newLeafNode(otherlv_4, grammarAccess.getScenarioSetAccess().getScenariosKeyword_4_0());
                    			
                    otherlv_5=(Token)match(input,12,FOLLOW_178); 

                    				newLeafNode(otherlv_5, grammarAccess.getScenarioSetAccess().getLeftCurlyBracketKeyword_4_1());
                    			
                    // InternalAorta.g:7833:4: ( (lv_scenarios_6_0= ruleScenario ) )
                    // InternalAorta.g:7834:5: (lv_scenarios_6_0= ruleScenario )
                    {
                    // InternalAorta.g:7834:5: (lv_scenarios_6_0= ruleScenario )
                    // InternalAorta.g:7835:6: lv_scenarios_6_0= ruleScenario
                    {

                    						newCompositeNode(grammarAccess.getScenarioSetAccess().getScenariosScenarioParserRuleCall_4_2_0());
                    					
                    pushFollow(FOLLOW_35);
                    lv_scenarios_6_0=ruleScenario();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getScenarioSetRule());
                    						}
                    						add(
                    							current,
                    							"scenarios",
                    							lv_scenarios_6_0,
                    							"org.eclipse.efbt.aorta.Aorta.Scenario");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:7852:4: (otherlv_7= ',' ( (lv_scenarios_8_0= ruleScenario ) ) )*
                    loop185:
                    do {
                        int alt185=2;
                        int LA185_0 = input.LA(1);

                        if ( (LA185_0==31) ) {
                            alt185=1;
                        }


                        switch (alt185) {
                    	case 1 :
                    	    // InternalAorta.g:7853:5: otherlv_7= ',' ( (lv_scenarios_8_0= ruleScenario ) )
                    	    {
                    	    otherlv_7=(Token)match(input,31,FOLLOW_178); 

                    	    					newLeafNode(otherlv_7, grammarAccess.getScenarioSetAccess().getCommaKeyword_4_3_0());
                    	    				
                    	    // InternalAorta.g:7857:5: ( (lv_scenarios_8_0= ruleScenario ) )
                    	    // InternalAorta.g:7858:6: (lv_scenarios_8_0= ruleScenario )
                    	    {
                    	    // InternalAorta.g:7858:6: (lv_scenarios_8_0= ruleScenario )
                    	    // InternalAorta.g:7859:7: lv_scenarios_8_0= ruleScenario
                    	    {

                    	    							newCompositeNode(grammarAccess.getScenarioSetAccess().getScenariosScenarioParserRuleCall_4_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_35);
                    	    lv_scenarios_8_0=ruleScenario();

                    	    state._fsp--;


                    	    							if (current==null) {
                    	    								current = createModelElementForParent(grammarAccess.getScenarioSetRule());
                    	    							}
                    	    							add(
                    	    								current,
                    	    								"scenarios",
                    	    								lv_scenarios_8_0,
                    	    								"org.eclipse.efbt.aorta.Aorta.Scenario");
                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop185;
                        }
                    } while (true);

                    otherlv_9=(Token)match(input,23,FOLLOW_24); 

                    				newLeafNode(otherlv_9, grammarAccess.getScenarioSetAccess().getRightCurlyBracketKeyword_4_4());
                    			

                    }
                    break;

            }

            otherlv_10=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_10, grammarAccess.getScenarioSetAccess().getRightCurlyBracketKeyword_5());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleScenarioSet"


    // $ANTLR start "entryRuleDataProcessingFunctionalityModule"
    // InternalAorta.g:7890:1: entryRuleDataProcessingFunctionalityModule returns [EObject current=null] : iv_ruleDataProcessingFunctionalityModule= ruleDataProcessingFunctionalityModule EOF ;
    public final EObject entryRuleDataProcessingFunctionalityModule() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleDataProcessingFunctionalityModule = null;


        try {
            // InternalAorta.g:7890:74: (iv_ruleDataProcessingFunctionalityModule= ruleDataProcessingFunctionalityModule EOF )
            // InternalAorta.g:7891:2: iv_ruleDataProcessingFunctionalityModule= ruleDataProcessingFunctionalityModule EOF
            {
             newCompositeNode(grammarAccess.getDataProcessingFunctionalityModuleRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleDataProcessingFunctionalityModule=ruleDataProcessingFunctionalityModule();

            state._fsp--;

             current =iv_ruleDataProcessingFunctionalityModule; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleDataProcessingFunctionalityModule"


    // $ANTLR start "ruleDataProcessingFunctionalityModule"
    // InternalAorta.g:7897:1: ruleDataProcessingFunctionalityModule returns [EObject current=null] : ( () otherlv_1= 'DataProcessingFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? otherlv_10= '}' ) ;
    public final EObject ruleDataProcessingFunctionalityModule() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_6=null;
        Token otherlv_8=null;
        Token otherlv_10=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;

        EObject lv_scenarioSet_9_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:7903:2: ( ( () otherlv_1= 'DataProcessingFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? otherlv_10= '}' ) )
            // InternalAorta.g:7904:2: ( () otherlv_1= 'DataProcessingFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? otherlv_10= '}' )
            {
            // InternalAorta.g:7904:2: ( () otherlv_1= 'DataProcessingFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? otherlv_10= '}' )
            // InternalAorta.g:7905:3: () otherlv_1= 'DataProcessingFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? otherlv_10= '}'
            {
            // InternalAorta.g:7905:3: ()
            // InternalAorta.g:7906:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getDataProcessingFunctionalityModuleAccess().getDataProcessingFunctionalityModuleAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,149,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getDataProcessingFunctionalityModuleAccess().getDataProcessingFunctionalityModuleKeyword_1());
            		
            // InternalAorta.g:7916:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:7917:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:7917:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:7918:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getDataProcessingFunctionalityModuleAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getDataProcessingFunctionalityModuleRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_173); 

            			newLeafNode(otherlv_3, grammarAccess.getDataProcessingFunctionalityModuleAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:7939:3: (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )?
            int alt187=2;
            int LA187_0 = input.LA(1);

            if ( (LA187_0==143) ) {
                alt187=1;
            }
            switch (alt187) {
                case 1 :
                    // InternalAorta.g:7940:4: otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) )
                    {
                    otherlv_4=(Token)match(input,143,FOLLOW_70); 

                    				newLeafNode(otherlv_4, grammarAccess.getDataProcessingFunctionalityModuleAccess().getUsedTransformationOrSchemeKeyword_4_0());
                    			
                    // InternalAorta.g:7944:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:7945:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:7945:5: ( ruleQualifiedName )
                    // InternalAorta.g:7946:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getDataProcessingFunctionalityModuleRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getDataProcessingFunctionalityModuleAccess().getUsedTransformationOrSchemeFunctionalityModuleCrossReference_4_1_0());
                    					
                    pushFollow(FOLLOW_174);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:7961:3: (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )?
            int alt188=2;
            int LA188_0 = input.LA(1);

            if ( (LA188_0==144) ) {
                alt188=1;
            }
            switch (alt188) {
                case 1 :
                    // InternalAorta.g:7962:4: otherlv_6= 'scenario' ( ( ruleQualifiedName ) )
                    {
                    otherlv_6=(Token)match(input,144,FOLLOW_70); 

                    				newLeafNode(otherlv_6, grammarAccess.getDataProcessingFunctionalityModuleAccess().getScenarioKeyword_5_0());
                    			
                    // InternalAorta.g:7966:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:7967:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:7967:5: ( ruleQualifiedName )
                    // InternalAorta.g:7968:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getDataProcessingFunctionalityModuleRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getDataProcessingFunctionalityModuleAccess().getScenarioScenarioCrossReference_5_1_0());
                    					
                    pushFollow(FOLLOW_175);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:7983:3: (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )?
            int alt189=2;
            int LA189_0 = input.LA(1);

            if ( (LA189_0==145) ) {
                alt189=1;
            }
            switch (alt189) {
                case 1 :
                    // InternalAorta.g:7984:4: otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) )
                    {
                    otherlv_8=(Token)match(input,145,FOLLOW_176); 

                    				newLeafNode(otherlv_8, grammarAccess.getDataProcessingFunctionalityModuleAccess().getScenarioSetKeyword_6_0());
                    			
                    // InternalAorta.g:7988:4: ( (lv_scenarioSet_9_0= ruleScenarioSet ) )
                    // InternalAorta.g:7989:5: (lv_scenarioSet_9_0= ruleScenarioSet )
                    {
                    // InternalAorta.g:7989:5: (lv_scenarioSet_9_0= ruleScenarioSet )
                    // InternalAorta.g:7990:6: lv_scenarioSet_9_0= ruleScenarioSet
                    {

                    						newCompositeNode(grammarAccess.getDataProcessingFunctionalityModuleAccess().getScenarioSetScenarioSetParserRuleCall_6_1_0());
                    					
                    pushFollow(FOLLOW_24);
                    lv_scenarioSet_9_0=ruleScenarioSet();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getDataProcessingFunctionalityModuleRule());
                    						}
                    						set(
                    							current,
                    							"scenarioSet",
                    							lv_scenarioSet_9_0,
                    							"org.eclipse.efbt.aorta.Aorta.ScenarioSet");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            otherlv_10=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_10, grammarAccess.getDataProcessingFunctionalityModuleAccess().getRightCurlyBracketKeyword_7());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleDataProcessingFunctionalityModule"


    // $ANTLR start "entryRuleLeafFunctionalityModule_Impl"
    // InternalAorta.g:8016:1: entryRuleLeafFunctionalityModule_Impl returns [EObject current=null] : iv_ruleLeafFunctionalityModule_Impl= ruleLeafFunctionalityModule_Impl EOF ;
    public final EObject entryRuleLeafFunctionalityModule_Impl() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleLeafFunctionalityModule_Impl = null;


        try {
            // InternalAorta.g:8016:69: (iv_ruleLeafFunctionalityModule_Impl= ruleLeafFunctionalityModule_Impl EOF )
            // InternalAorta.g:8017:2: iv_ruleLeafFunctionalityModule_Impl= ruleLeafFunctionalityModule_Impl EOF
            {
             newCompositeNode(grammarAccess.getLeafFunctionalityModule_ImplRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleLeafFunctionalityModule_Impl=ruleLeafFunctionalityModule_Impl();

            state._fsp--;

             current =iv_ruleLeafFunctionalityModule_Impl; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleLeafFunctionalityModule_Impl"


    // $ANTLR start "ruleLeafFunctionalityModule_Impl"
    // InternalAorta.g:8023:1: ruleLeafFunctionalityModule_Impl returns [EObject current=null] : ( () otherlv_1= 'LeafFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? otherlv_10= '}' ) ;
    public final EObject ruleLeafFunctionalityModule_Impl() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_6=null;
        Token otherlv_8=null;
        Token otherlv_10=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;

        EObject lv_scenarioSet_9_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:8029:2: ( ( () otherlv_1= 'LeafFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? otherlv_10= '}' ) )
            // InternalAorta.g:8030:2: ( () otherlv_1= 'LeafFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? otherlv_10= '}' )
            {
            // InternalAorta.g:8030:2: ( () otherlv_1= 'LeafFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? otherlv_10= '}' )
            // InternalAorta.g:8031:3: () otherlv_1= 'LeafFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? otherlv_10= '}'
            {
            // InternalAorta.g:8031:3: ()
            // InternalAorta.g:8032:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getLeafFunctionalityModule_ImplAccess().getLeafFunctionalityModuleAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,150,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getLeafFunctionalityModule_ImplAccess().getLeafFunctionalityModuleKeyword_1());
            		
            // InternalAorta.g:8042:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:8043:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:8043:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:8044:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getLeafFunctionalityModule_ImplAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getLeafFunctionalityModule_ImplRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_173); 

            			newLeafNode(otherlv_3, grammarAccess.getLeafFunctionalityModule_ImplAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:8065:3: (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )?
            int alt190=2;
            int LA190_0 = input.LA(1);

            if ( (LA190_0==143) ) {
                alt190=1;
            }
            switch (alt190) {
                case 1 :
                    // InternalAorta.g:8066:4: otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) )
                    {
                    otherlv_4=(Token)match(input,143,FOLLOW_70); 

                    				newLeafNode(otherlv_4, grammarAccess.getLeafFunctionalityModule_ImplAccess().getUsedTransformationOrSchemeKeyword_4_0());
                    			
                    // InternalAorta.g:8070:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:8071:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:8071:5: ( ruleQualifiedName )
                    // InternalAorta.g:8072:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getLeafFunctionalityModule_ImplRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getLeafFunctionalityModule_ImplAccess().getUsedTransformationOrSchemeFunctionalityModuleCrossReference_4_1_0());
                    					
                    pushFollow(FOLLOW_174);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:8087:3: (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )?
            int alt191=2;
            int LA191_0 = input.LA(1);

            if ( (LA191_0==144) ) {
                alt191=1;
            }
            switch (alt191) {
                case 1 :
                    // InternalAorta.g:8088:4: otherlv_6= 'scenario' ( ( ruleQualifiedName ) )
                    {
                    otherlv_6=(Token)match(input,144,FOLLOW_70); 

                    				newLeafNode(otherlv_6, grammarAccess.getLeafFunctionalityModule_ImplAccess().getScenarioKeyword_5_0());
                    			
                    // InternalAorta.g:8092:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:8093:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:8093:5: ( ruleQualifiedName )
                    // InternalAorta.g:8094:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getLeafFunctionalityModule_ImplRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getLeafFunctionalityModule_ImplAccess().getScenarioScenarioCrossReference_5_1_0());
                    					
                    pushFollow(FOLLOW_175);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:8109:3: (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )?
            int alt192=2;
            int LA192_0 = input.LA(1);

            if ( (LA192_0==145) ) {
                alt192=1;
            }
            switch (alt192) {
                case 1 :
                    // InternalAorta.g:8110:4: otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) )
                    {
                    otherlv_8=(Token)match(input,145,FOLLOW_176); 

                    				newLeafNode(otherlv_8, grammarAccess.getLeafFunctionalityModule_ImplAccess().getScenarioSetKeyword_6_0());
                    			
                    // InternalAorta.g:8114:4: ( (lv_scenarioSet_9_0= ruleScenarioSet ) )
                    // InternalAorta.g:8115:5: (lv_scenarioSet_9_0= ruleScenarioSet )
                    {
                    // InternalAorta.g:8115:5: (lv_scenarioSet_9_0= ruleScenarioSet )
                    // InternalAorta.g:8116:6: lv_scenarioSet_9_0= ruleScenarioSet
                    {

                    						newCompositeNode(grammarAccess.getLeafFunctionalityModule_ImplAccess().getScenarioSetScenarioSetParserRuleCall_6_1_0());
                    					
                    pushFollow(FOLLOW_24);
                    lv_scenarioSet_9_0=ruleScenarioSet();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getLeafFunctionalityModule_ImplRule());
                    						}
                    						set(
                    							current,
                    							"scenarioSet",
                    							lv_scenarioSet_9_0,
                    							"org.eclipse.efbt.aorta.Aorta.ScenarioSet");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            otherlv_10=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_10, grammarAccess.getLeafFunctionalityModule_ImplAccess().getRightCurlyBracketKeyword_7());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleLeafFunctionalityModule_Impl"


    // $ANTLR start "entryRuleParallelFunctionalityModule"
    // InternalAorta.g:8142:1: entryRuleParallelFunctionalityModule returns [EObject current=null] : iv_ruleParallelFunctionalityModule= ruleParallelFunctionalityModule EOF ;
    public final EObject entryRuleParallelFunctionalityModule() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleParallelFunctionalityModule = null;


        try {
            // InternalAorta.g:8142:68: (iv_ruleParallelFunctionalityModule= ruleParallelFunctionalityModule EOF )
            // InternalAorta.g:8143:2: iv_ruleParallelFunctionalityModule= ruleParallelFunctionalityModule EOF
            {
             newCompositeNode(grammarAccess.getParallelFunctionalityModuleRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleParallelFunctionalityModule=ruleParallelFunctionalityModule();

            state._fsp--;

             current =iv_ruleParallelFunctionalityModule; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleParallelFunctionalityModule"


    // $ANTLR start "ruleParallelFunctionalityModule"
    // InternalAorta.g:8149:1: ruleParallelFunctionalityModule returns [EObject current=null] : ( () otherlv_1= 'ParallelFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? (otherlv_10= 'subTransformationsAndSchemes' otherlv_11= '{' ( (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule ) ) (otherlv_13= ',' ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) ) )* otherlv_15= '}' )? otherlv_16= '}' ) ;
    public final EObject ruleParallelFunctionalityModule() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_6=null;
        Token otherlv_8=null;
        Token otherlv_10=null;
        Token otherlv_11=null;
        Token otherlv_13=null;
        Token otherlv_15=null;
        Token otherlv_16=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;

        EObject lv_scenarioSet_9_0 = null;

        EObject lv_subTransformationsAndSchemes_12_0 = null;

        EObject lv_subTransformationsAndSchemes_14_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:8155:2: ( ( () otherlv_1= 'ParallelFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? (otherlv_10= 'subTransformationsAndSchemes' otherlv_11= '{' ( (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule ) ) (otherlv_13= ',' ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) ) )* otherlv_15= '}' )? otherlv_16= '}' ) )
            // InternalAorta.g:8156:2: ( () otherlv_1= 'ParallelFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? (otherlv_10= 'subTransformationsAndSchemes' otherlv_11= '{' ( (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule ) ) (otherlv_13= ',' ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) ) )* otherlv_15= '}' )? otherlv_16= '}' )
            {
            // InternalAorta.g:8156:2: ( () otherlv_1= 'ParallelFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? (otherlv_10= 'subTransformationsAndSchemes' otherlv_11= '{' ( (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule ) ) (otherlv_13= ',' ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) ) )* otherlv_15= '}' )? otherlv_16= '}' )
            // InternalAorta.g:8157:3: () otherlv_1= 'ParallelFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? (otherlv_10= 'subTransformationsAndSchemes' otherlv_11= '{' ( (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule ) ) (otherlv_13= ',' ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) ) )* otherlv_15= '}' )? otherlv_16= '}'
            {
            // InternalAorta.g:8157:3: ()
            // InternalAorta.g:8158:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getParallelFunctionalityModuleAccess().getParallelFunctionalityModuleAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,151,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getParallelFunctionalityModuleAccess().getParallelFunctionalityModuleKeyword_1());
            		
            // InternalAorta.g:8168:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:8169:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:8169:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:8170:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getParallelFunctionalityModuleAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getParallelFunctionalityModuleRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_179); 

            			newLeafNode(otherlv_3, grammarAccess.getParallelFunctionalityModuleAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:8191:3: (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )?
            int alt193=2;
            int LA193_0 = input.LA(1);

            if ( (LA193_0==143) ) {
                alt193=1;
            }
            switch (alt193) {
                case 1 :
                    // InternalAorta.g:8192:4: otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) )
                    {
                    otherlv_4=(Token)match(input,143,FOLLOW_70); 

                    				newLeafNode(otherlv_4, grammarAccess.getParallelFunctionalityModuleAccess().getUsedTransformationOrSchemeKeyword_4_0());
                    			
                    // InternalAorta.g:8196:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:8197:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:8197:5: ( ruleQualifiedName )
                    // InternalAorta.g:8198:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getParallelFunctionalityModuleRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getParallelFunctionalityModuleAccess().getUsedTransformationOrSchemeFunctionalityModuleCrossReference_4_1_0());
                    					
                    pushFollow(FOLLOW_180);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:8213:3: (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )?
            int alt194=2;
            int LA194_0 = input.LA(1);

            if ( (LA194_0==144) ) {
                alt194=1;
            }
            switch (alt194) {
                case 1 :
                    // InternalAorta.g:8214:4: otherlv_6= 'scenario' ( ( ruleQualifiedName ) )
                    {
                    otherlv_6=(Token)match(input,144,FOLLOW_70); 

                    				newLeafNode(otherlv_6, grammarAccess.getParallelFunctionalityModuleAccess().getScenarioKeyword_5_0());
                    			
                    // InternalAorta.g:8218:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:8219:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:8219:5: ( ruleQualifiedName )
                    // InternalAorta.g:8220:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getParallelFunctionalityModuleRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getParallelFunctionalityModuleAccess().getScenarioScenarioCrossReference_5_1_0());
                    					
                    pushFollow(FOLLOW_181);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:8235:3: (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )?
            int alt195=2;
            int LA195_0 = input.LA(1);

            if ( (LA195_0==145) ) {
                alt195=1;
            }
            switch (alt195) {
                case 1 :
                    // InternalAorta.g:8236:4: otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) )
                    {
                    otherlv_8=(Token)match(input,145,FOLLOW_176); 

                    				newLeafNode(otherlv_8, grammarAccess.getParallelFunctionalityModuleAccess().getScenarioSetKeyword_6_0());
                    			
                    // InternalAorta.g:8240:4: ( (lv_scenarioSet_9_0= ruleScenarioSet ) )
                    // InternalAorta.g:8241:5: (lv_scenarioSet_9_0= ruleScenarioSet )
                    {
                    // InternalAorta.g:8241:5: (lv_scenarioSet_9_0= ruleScenarioSet )
                    // InternalAorta.g:8242:6: lv_scenarioSet_9_0= ruleScenarioSet
                    {

                    						newCompositeNode(grammarAccess.getParallelFunctionalityModuleAccess().getScenarioSetScenarioSetParserRuleCall_6_1_0());
                    					
                    pushFollow(FOLLOW_182);
                    lv_scenarioSet_9_0=ruleScenarioSet();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getParallelFunctionalityModuleRule());
                    						}
                    						set(
                    							current,
                    							"scenarioSet",
                    							lv_scenarioSet_9_0,
                    							"org.eclipse.efbt.aorta.Aorta.ScenarioSet");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:8260:3: (otherlv_10= 'subTransformationsAndSchemes' otherlv_11= '{' ( (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule ) ) (otherlv_13= ',' ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) ) )* otherlv_15= '}' )?
            int alt197=2;
            int LA197_0 = input.LA(1);

            if ( (LA197_0==152) ) {
                alt197=1;
            }
            switch (alt197) {
                case 1 :
                    // InternalAorta.g:8261:4: otherlv_10= 'subTransformationsAndSchemes' otherlv_11= '{' ( (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule ) ) (otherlv_13= ',' ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) ) )* otherlv_15= '}'
                    {
                    otherlv_10=(Token)match(input,152,FOLLOW_3); 

                    				newLeafNode(otherlv_10, grammarAccess.getParallelFunctionalityModuleAccess().getSubTransformationsAndSchemesKeyword_7_0());
                    			
                    otherlv_11=(Token)match(input,12,FOLLOW_100); 

                    				newLeafNode(otherlv_11, grammarAccess.getParallelFunctionalityModuleAccess().getLeftCurlyBracketKeyword_7_1());
                    			
                    // InternalAorta.g:8269:4: ( (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule ) )
                    // InternalAorta.g:8270:5: (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule )
                    {
                    // InternalAorta.g:8270:5: (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule )
                    // InternalAorta.g:8271:6: lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule
                    {

                    						newCompositeNode(grammarAccess.getParallelFunctionalityModuleAccess().getSubTransformationsAndSchemesFunctionalityModuleParserRuleCall_7_2_0());
                    					
                    pushFollow(FOLLOW_35);
                    lv_subTransformationsAndSchemes_12_0=ruleFunctionalityModule();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getParallelFunctionalityModuleRule());
                    						}
                    						add(
                    							current,
                    							"subTransformationsAndSchemes",
                    							lv_subTransformationsAndSchemes_12_0,
                    							"org.eclipse.efbt.aorta.Aorta.FunctionalityModule");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:8288:4: (otherlv_13= ',' ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) ) )*
                    loop196:
                    do {
                        int alt196=2;
                        int LA196_0 = input.LA(1);

                        if ( (LA196_0==31) ) {
                            alt196=1;
                        }


                        switch (alt196) {
                    	case 1 :
                    	    // InternalAorta.g:8289:5: otherlv_13= ',' ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) )
                    	    {
                    	    otherlv_13=(Token)match(input,31,FOLLOW_100); 

                    	    					newLeafNode(otherlv_13, grammarAccess.getParallelFunctionalityModuleAccess().getCommaKeyword_7_3_0());
                    	    				
                    	    // InternalAorta.g:8293:5: ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) )
                    	    // InternalAorta.g:8294:6: (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule )
                    	    {
                    	    // InternalAorta.g:8294:6: (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule )
                    	    // InternalAorta.g:8295:7: lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule
                    	    {

                    	    							newCompositeNode(grammarAccess.getParallelFunctionalityModuleAccess().getSubTransformationsAndSchemesFunctionalityModuleParserRuleCall_7_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_35);
                    	    lv_subTransformationsAndSchemes_14_0=ruleFunctionalityModule();

                    	    state._fsp--;


                    	    							if (current==null) {
                    	    								current = createModelElementForParent(grammarAccess.getParallelFunctionalityModuleRule());
                    	    							}
                    	    							add(
                    	    								current,
                    	    								"subTransformationsAndSchemes",
                    	    								lv_subTransformationsAndSchemes_14_0,
                    	    								"org.eclipse.efbt.aorta.Aorta.FunctionalityModule");
                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop196;
                        }
                    } while (true);

                    otherlv_15=(Token)match(input,23,FOLLOW_24); 

                    				newLeafNode(otherlv_15, grammarAccess.getParallelFunctionalityModuleAccess().getRightCurlyBracketKeyword_7_4());
                    			

                    }
                    break;

            }

            otherlv_16=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_16, grammarAccess.getParallelFunctionalityModuleAccess().getRightCurlyBracketKeyword_8());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleParallelFunctionalityModule"


    // $ANTLR start "entryRuleScenarioSetFunctionalityModule"
    // InternalAorta.g:8326:1: entryRuleScenarioSetFunctionalityModule returns [EObject current=null] : iv_ruleScenarioSetFunctionalityModule= ruleScenarioSetFunctionalityModule EOF ;
    public final EObject entryRuleScenarioSetFunctionalityModule() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleScenarioSetFunctionalityModule = null;


        try {
            // InternalAorta.g:8326:71: (iv_ruleScenarioSetFunctionalityModule= ruleScenarioSetFunctionalityModule EOF )
            // InternalAorta.g:8327:2: iv_ruleScenarioSetFunctionalityModule= ruleScenarioSetFunctionalityModule EOF
            {
             newCompositeNode(grammarAccess.getScenarioSetFunctionalityModuleRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleScenarioSetFunctionalityModule=ruleScenarioSetFunctionalityModule();

            state._fsp--;

             current =iv_ruleScenarioSetFunctionalityModule; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleScenarioSetFunctionalityModule"


    // $ANTLR start "ruleScenarioSetFunctionalityModule"
    // InternalAorta.g:8333:1: ruleScenarioSetFunctionalityModule returns [EObject current=null] : ( () otherlv_1= 'ScenarioSetFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? (otherlv_10= 'subTransformationsAndSchemes' otherlv_11= '{' ( (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule ) ) (otherlv_13= ',' ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) ) )* otherlv_15= '}' )? (otherlv_16= 'scenarioSetChoice' ( (lv_scenarioSetChoice_17_0= ruleScenarioSet ) ) )? otherlv_18= '}' ) ;
    public final EObject ruleScenarioSetFunctionalityModule() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_6=null;
        Token otherlv_8=null;
        Token otherlv_10=null;
        Token otherlv_11=null;
        Token otherlv_13=null;
        Token otherlv_15=null;
        Token otherlv_16=null;
        Token otherlv_18=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;

        EObject lv_scenarioSet_9_0 = null;

        EObject lv_subTransformationsAndSchemes_12_0 = null;

        EObject lv_subTransformationsAndSchemes_14_0 = null;

        EObject lv_scenarioSetChoice_17_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:8339:2: ( ( () otherlv_1= 'ScenarioSetFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? (otherlv_10= 'subTransformationsAndSchemes' otherlv_11= '{' ( (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule ) ) (otherlv_13= ',' ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) ) )* otherlv_15= '}' )? (otherlv_16= 'scenarioSetChoice' ( (lv_scenarioSetChoice_17_0= ruleScenarioSet ) ) )? otherlv_18= '}' ) )
            // InternalAorta.g:8340:2: ( () otherlv_1= 'ScenarioSetFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? (otherlv_10= 'subTransformationsAndSchemes' otherlv_11= '{' ( (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule ) ) (otherlv_13= ',' ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) ) )* otherlv_15= '}' )? (otherlv_16= 'scenarioSetChoice' ( (lv_scenarioSetChoice_17_0= ruleScenarioSet ) ) )? otherlv_18= '}' )
            {
            // InternalAorta.g:8340:2: ( () otherlv_1= 'ScenarioSetFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? (otherlv_10= 'subTransformationsAndSchemes' otherlv_11= '{' ( (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule ) ) (otherlv_13= ',' ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) ) )* otherlv_15= '}' )? (otherlv_16= 'scenarioSetChoice' ( (lv_scenarioSetChoice_17_0= ruleScenarioSet ) ) )? otherlv_18= '}' )
            // InternalAorta.g:8341:3: () otherlv_1= 'ScenarioSetFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? (otherlv_10= 'subTransformationsAndSchemes' otherlv_11= '{' ( (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule ) ) (otherlv_13= ',' ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) ) )* otherlv_15= '}' )? (otherlv_16= 'scenarioSetChoice' ( (lv_scenarioSetChoice_17_0= ruleScenarioSet ) ) )? otherlv_18= '}'
            {
            // InternalAorta.g:8341:3: ()
            // InternalAorta.g:8342:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getScenarioSetFunctionalityModuleAccess().getScenarioSetFunctionalityModuleAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,153,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getScenarioSetFunctionalityModuleAccess().getScenarioSetFunctionalityModuleKeyword_1());
            		
            // InternalAorta.g:8352:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:8353:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:8353:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:8354:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getScenarioSetFunctionalityModuleAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getScenarioSetFunctionalityModuleRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_183); 

            			newLeafNode(otherlv_3, grammarAccess.getScenarioSetFunctionalityModuleAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:8375:3: (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )?
            int alt198=2;
            int LA198_0 = input.LA(1);

            if ( (LA198_0==143) ) {
                alt198=1;
            }
            switch (alt198) {
                case 1 :
                    // InternalAorta.g:8376:4: otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) )
                    {
                    otherlv_4=(Token)match(input,143,FOLLOW_70); 

                    				newLeafNode(otherlv_4, grammarAccess.getScenarioSetFunctionalityModuleAccess().getUsedTransformationOrSchemeKeyword_4_0());
                    			
                    // InternalAorta.g:8380:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:8381:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:8381:5: ( ruleQualifiedName )
                    // InternalAorta.g:8382:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getScenarioSetFunctionalityModuleRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getScenarioSetFunctionalityModuleAccess().getUsedTransformationOrSchemeFunctionalityModuleCrossReference_4_1_0());
                    					
                    pushFollow(FOLLOW_184);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:8397:3: (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )?
            int alt199=2;
            int LA199_0 = input.LA(1);

            if ( (LA199_0==144) ) {
                alt199=1;
            }
            switch (alt199) {
                case 1 :
                    // InternalAorta.g:8398:4: otherlv_6= 'scenario' ( ( ruleQualifiedName ) )
                    {
                    otherlv_6=(Token)match(input,144,FOLLOW_70); 

                    				newLeafNode(otherlv_6, grammarAccess.getScenarioSetFunctionalityModuleAccess().getScenarioKeyword_5_0());
                    			
                    // InternalAorta.g:8402:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:8403:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:8403:5: ( ruleQualifiedName )
                    // InternalAorta.g:8404:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getScenarioSetFunctionalityModuleRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getScenarioSetFunctionalityModuleAccess().getScenarioScenarioCrossReference_5_1_0());
                    					
                    pushFollow(FOLLOW_185);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:8419:3: (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )?
            int alt200=2;
            int LA200_0 = input.LA(1);

            if ( (LA200_0==145) ) {
                alt200=1;
            }
            switch (alt200) {
                case 1 :
                    // InternalAorta.g:8420:4: otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) )
                    {
                    otherlv_8=(Token)match(input,145,FOLLOW_176); 

                    				newLeafNode(otherlv_8, grammarAccess.getScenarioSetFunctionalityModuleAccess().getScenarioSetKeyword_6_0());
                    			
                    // InternalAorta.g:8424:4: ( (lv_scenarioSet_9_0= ruleScenarioSet ) )
                    // InternalAorta.g:8425:5: (lv_scenarioSet_9_0= ruleScenarioSet )
                    {
                    // InternalAorta.g:8425:5: (lv_scenarioSet_9_0= ruleScenarioSet )
                    // InternalAorta.g:8426:6: lv_scenarioSet_9_0= ruleScenarioSet
                    {

                    						newCompositeNode(grammarAccess.getScenarioSetFunctionalityModuleAccess().getScenarioSetScenarioSetParserRuleCall_6_1_0());
                    					
                    pushFollow(FOLLOW_186);
                    lv_scenarioSet_9_0=ruleScenarioSet();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getScenarioSetFunctionalityModuleRule());
                    						}
                    						set(
                    							current,
                    							"scenarioSet",
                    							lv_scenarioSet_9_0,
                    							"org.eclipse.efbt.aorta.Aorta.ScenarioSet");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:8444:3: (otherlv_10= 'subTransformationsAndSchemes' otherlv_11= '{' ( (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule ) ) (otherlv_13= ',' ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) ) )* otherlv_15= '}' )?
            int alt202=2;
            int LA202_0 = input.LA(1);

            if ( (LA202_0==152) ) {
                alt202=1;
            }
            switch (alt202) {
                case 1 :
                    // InternalAorta.g:8445:4: otherlv_10= 'subTransformationsAndSchemes' otherlv_11= '{' ( (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule ) ) (otherlv_13= ',' ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) ) )* otherlv_15= '}'
                    {
                    otherlv_10=(Token)match(input,152,FOLLOW_3); 

                    				newLeafNode(otherlv_10, grammarAccess.getScenarioSetFunctionalityModuleAccess().getSubTransformationsAndSchemesKeyword_7_0());
                    			
                    otherlv_11=(Token)match(input,12,FOLLOW_100); 

                    				newLeafNode(otherlv_11, grammarAccess.getScenarioSetFunctionalityModuleAccess().getLeftCurlyBracketKeyword_7_1());
                    			
                    // InternalAorta.g:8453:4: ( (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule ) )
                    // InternalAorta.g:8454:5: (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule )
                    {
                    // InternalAorta.g:8454:5: (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule )
                    // InternalAorta.g:8455:6: lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule
                    {

                    						newCompositeNode(grammarAccess.getScenarioSetFunctionalityModuleAccess().getSubTransformationsAndSchemesFunctionalityModuleParserRuleCall_7_2_0());
                    					
                    pushFollow(FOLLOW_35);
                    lv_subTransformationsAndSchemes_12_0=ruleFunctionalityModule();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getScenarioSetFunctionalityModuleRule());
                    						}
                    						add(
                    							current,
                    							"subTransformationsAndSchemes",
                    							lv_subTransformationsAndSchemes_12_0,
                    							"org.eclipse.efbt.aorta.Aorta.FunctionalityModule");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:8472:4: (otherlv_13= ',' ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) ) )*
                    loop201:
                    do {
                        int alt201=2;
                        int LA201_0 = input.LA(1);

                        if ( (LA201_0==31) ) {
                            alt201=1;
                        }


                        switch (alt201) {
                    	case 1 :
                    	    // InternalAorta.g:8473:5: otherlv_13= ',' ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) )
                    	    {
                    	    otherlv_13=(Token)match(input,31,FOLLOW_100); 

                    	    					newLeafNode(otherlv_13, grammarAccess.getScenarioSetFunctionalityModuleAccess().getCommaKeyword_7_3_0());
                    	    				
                    	    // InternalAorta.g:8477:5: ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) )
                    	    // InternalAorta.g:8478:6: (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule )
                    	    {
                    	    // InternalAorta.g:8478:6: (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule )
                    	    // InternalAorta.g:8479:7: lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule
                    	    {

                    	    							newCompositeNode(grammarAccess.getScenarioSetFunctionalityModuleAccess().getSubTransformationsAndSchemesFunctionalityModuleParserRuleCall_7_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_35);
                    	    lv_subTransformationsAndSchemes_14_0=ruleFunctionalityModule();

                    	    state._fsp--;


                    	    							if (current==null) {
                    	    								current = createModelElementForParent(grammarAccess.getScenarioSetFunctionalityModuleRule());
                    	    							}
                    	    							add(
                    	    								current,
                    	    								"subTransformationsAndSchemes",
                    	    								lv_subTransformationsAndSchemes_14_0,
                    	    								"org.eclipse.efbt.aorta.Aorta.FunctionalityModule");
                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop201;
                        }
                    } while (true);

                    otherlv_15=(Token)match(input,23,FOLLOW_187); 

                    				newLeafNode(otherlv_15, grammarAccess.getScenarioSetFunctionalityModuleAccess().getRightCurlyBracketKeyword_7_4());
                    			

                    }
                    break;

            }

            // InternalAorta.g:8502:3: (otherlv_16= 'scenarioSetChoice' ( (lv_scenarioSetChoice_17_0= ruleScenarioSet ) ) )?
            int alt203=2;
            int LA203_0 = input.LA(1);

            if ( (LA203_0==154) ) {
                alt203=1;
            }
            switch (alt203) {
                case 1 :
                    // InternalAorta.g:8503:4: otherlv_16= 'scenarioSetChoice' ( (lv_scenarioSetChoice_17_0= ruleScenarioSet ) )
                    {
                    otherlv_16=(Token)match(input,154,FOLLOW_176); 

                    				newLeafNode(otherlv_16, grammarAccess.getScenarioSetFunctionalityModuleAccess().getScenarioSetChoiceKeyword_8_0());
                    			
                    // InternalAorta.g:8507:4: ( (lv_scenarioSetChoice_17_0= ruleScenarioSet ) )
                    // InternalAorta.g:8508:5: (lv_scenarioSetChoice_17_0= ruleScenarioSet )
                    {
                    // InternalAorta.g:8508:5: (lv_scenarioSetChoice_17_0= ruleScenarioSet )
                    // InternalAorta.g:8509:6: lv_scenarioSetChoice_17_0= ruleScenarioSet
                    {

                    						newCompositeNode(grammarAccess.getScenarioSetFunctionalityModuleAccess().getScenarioSetChoiceScenarioSetParserRuleCall_8_1_0());
                    					
                    pushFollow(FOLLOW_24);
                    lv_scenarioSetChoice_17_0=ruleScenarioSet();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getScenarioSetFunctionalityModuleRule());
                    						}
                    						set(
                    							current,
                    							"scenarioSetChoice",
                    							lv_scenarioSetChoice_17_0,
                    							"org.eclipse.efbt.aorta.Aorta.ScenarioSet");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            otherlv_18=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_18, grammarAccess.getScenarioSetFunctionalityModuleAccess().getRightCurlyBracketKeyword_9());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleScenarioSetFunctionalityModule"


    // $ANTLR start "entryRuleSerialFunctionalityModule"
    // InternalAorta.g:8535:1: entryRuleSerialFunctionalityModule returns [EObject current=null] : iv_ruleSerialFunctionalityModule= ruleSerialFunctionalityModule EOF ;
    public final EObject entryRuleSerialFunctionalityModule() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleSerialFunctionalityModule = null;


        try {
            // InternalAorta.g:8535:66: (iv_ruleSerialFunctionalityModule= ruleSerialFunctionalityModule EOF )
            // InternalAorta.g:8536:2: iv_ruleSerialFunctionalityModule= ruleSerialFunctionalityModule EOF
            {
             newCompositeNode(grammarAccess.getSerialFunctionalityModuleRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleSerialFunctionalityModule=ruleSerialFunctionalityModule();

            state._fsp--;

             current =iv_ruleSerialFunctionalityModule; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleSerialFunctionalityModule"


    // $ANTLR start "ruleSerialFunctionalityModule"
    // InternalAorta.g:8542:1: ruleSerialFunctionalityModule returns [EObject current=null] : ( () otherlv_1= 'SerialFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? (otherlv_10= 'subTransformationsAndSchemes' otherlv_11= '{' ( (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule ) ) (otherlv_13= ',' ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) ) )* otherlv_15= '}' )? otherlv_16= '}' ) ;
    public final EObject ruleSerialFunctionalityModule() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_6=null;
        Token otherlv_8=null;
        Token otherlv_10=null;
        Token otherlv_11=null;
        Token otherlv_13=null;
        Token otherlv_15=null;
        Token otherlv_16=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;

        EObject lv_scenarioSet_9_0 = null;

        EObject lv_subTransformationsAndSchemes_12_0 = null;

        EObject lv_subTransformationsAndSchemes_14_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:8548:2: ( ( () otherlv_1= 'SerialFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? (otherlv_10= 'subTransformationsAndSchemes' otherlv_11= '{' ( (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule ) ) (otherlv_13= ',' ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) ) )* otherlv_15= '}' )? otherlv_16= '}' ) )
            // InternalAorta.g:8549:2: ( () otherlv_1= 'SerialFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? (otherlv_10= 'subTransformationsAndSchemes' otherlv_11= '{' ( (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule ) ) (otherlv_13= ',' ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) ) )* otherlv_15= '}' )? otherlv_16= '}' )
            {
            // InternalAorta.g:8549:2: ( () otherlv_1= 'SerialFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? (otherlv_10= 'subTransformationsAndSchemes' otherlv_11= '{' ( (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule ) ) (otherlv_13= ',' ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) ) )* otherlv_15= '}' )? otherlv_16= '}' )
            // InternalAorta.g:8550:3: () otherlv_1= 'SerialFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? (otherlv_10= 'subTransformationsAndSchemes' otherlv_11= '{' ( (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule ) ) (otherlv_13= ',' ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) ) )* otherlv_15= '}' )? otherlv_16= '}'
            {
            // InternalAorta.g:8550:3: ()
            // InternalAorta.g:8551:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getSerialFunctionalityModuleAccess().getSerialFunctionalityModuleAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,155,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getSerialFunctionalityModuleAccess().getSerialFunctionalityModuleKeyword_1());
            		
            // InternalAorta.g:8561:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:8562:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:8562:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:8563:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getSerialFunctionalityModuleAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getSerialFunctionalityModuleRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_179); 

            			newLeafNode(otherlv_3, grammarAccess.getSerialFunctionalityModuleAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:8584:3: (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )?
            int alt204=2;
            int LA204_0 = input.LA(1);

            if ( (LA204_0==143) ) {
                alt204=1;
            }
            switch (alt204) {
                case 1 :
                    // InternalAorta.g:8585:4: otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) )
                    {
                    otherlv_4=(Token)match(input,143,FOLLOW_70); 

                    				newLeafNode(otherlv_4, grammarAccess.getSerialFunctionalityModuleAccess().getUsedTransformationOrSchemeKeyword_4_0());
                    			
                    // InternalAorta.g:8589:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:8590:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:8590:5: ( ruleQualifiedName )
                    // InternalAorta.g:8591:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getSerialFunctionalityModuleRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getSerialFunctionalityModuleAccess().getUsedTransformationOrSchemeFunctionalityModuleCrossReference_4_1_0());
                    					
                    pushFollow(FOLLOW_180);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:8606:3: (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )?
            int alt205=2;
            int LA205_0 = input.LA(1);

            if ( (LA205_0==144) ) {
                alt205=1;
            }
            switch (alt205) {
                case 1 :
                    // InternalAorta.g:8607:4: otherlv_6= 'scenario' ( ( ruleQualifiedName ) )
                    {
                    otherlv_6=(Token)match(input,144,FOLLOW_70); 

                    				newLeafNode(otherlv_6, grammarAccess.getSerialFunctionalityModuleAccess().getScenarioKeyword_5_0());
                    			
                    // InternalAorta.g:8611:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:8612:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:8612:5: ( ruleQualifiedName )
                    // InternalAorta.g:8613:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getSerialFunctionalityModuleRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getSerialFunctionalityModuleAccess().getScenarioScenarioCrossReference_5_1_0());
                    					
                    pushFollow(FOLLOW_181);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:8628:3: (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )?
            int alt206=2;
            int LA206_0 = input.LA(1);

            if ( (LA206_0==145) ) {
                alt206=1;
            }
            switch (alt206) {
                case 1 :
                    // InternalAorta.g:8629:4: otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) )
                    {
                    otherlv_8=(Token)match(input,145,FOLLOW_176); 

                    				newLeafNode(otherlv_8, grammarAccess.getSerialFunctionalityModuleAccess().getScenarioSetKeyword_6_0());
                    			
                    // InternalAorta.g:8633:4: ( (lv_scenarioSet_9_0= ruleScenarioSet ) )
                    // InternalAorta.g:8634:5: (lv_scenarioSet_9_0= ruleScenarioSet )
                    {
                    // InternalAorta.g:8634:5: (lv_scenarioSet_9_0= ruleScenarioSet )
                    // InternalAorta.g:8635:6: lv_scenarioSet_9_0= ruleScenarioSet
                    {

                    						newCompositeNode(grammarAccess.getSerialFunctionalityModuleAccess().getScenarioSetScenarioSetParserRuleCall_6_1_0());
                    					
                    pushFollow(FOLLOW_182);
                    lv_scenarioSet_9_0=ruleScenarioSet();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getSerialFunctionalityModuleRule());
                    						}
                    						set(
                    							current,
                    							"scenarioSet",
                    							lv_scenarioSet_9_0,
                    							"org.eclipse.efbt.aorta.Aorta.ScenarioSet");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:8653:3: (otherlv_10= 'subTransformationsAndSchemes' otherlv_11= '{' ( (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule ) ) (otherlv_13= ',' ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) ) )* otherlv_15= '}' )?
            int alt208=2;
            int LA208_0 = input.LA(1);

            if ( (LA208_0==152) ) {
                alt208=1;
            }
            switch (alt208) {
                case 1 :
                    // InternalAorta.g:8654:4: otherlv_10= 'subTransformationsAndSchemes' otherlv_11= '{' ( (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule ) ) (otherlv_13= ',' ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) ) )* otherlv_15= '}'
                    {
                    otherlv_10=(Token)match(input,152,FOLLOW_3); 

                    				newLeafNode(otherlv_10, grammarAccess.getSerialFunctionalityModuleAccess().getSubTransformationsAndSchemesKeyword_7_0());
                    			
                    otherlv_11=(Token)match(input,12,FOLLOW_100); 

                    				newLeafNode(otherlv_11, grammarAccess.getSerialFunctionalityModuleAccess().getLeftCurlyBracketKeyword_7_1());
                    			
                    // InternalAorta.g:8662:4: ( (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule ) )
                    // InternalAorta.g:8663:5: (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule )
                    {
                    // InternalAorta.g:8663:5: (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule )
                    // InternalAorta.g:8664:6: lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule
                    {

                    						newCompositeNode(grammarAccess.getSerialFunctionalityModuleAccess().getSubTransformationsAndSchemesFunctionalityModuleParserRuleCall_7_2_0());
                    					
                    pushFollow(FOLLOW_35);
                    lv_subTransformationsAndSchemes_12_0=ruleFunctionalityModule();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getSerialFunctionalityModuleRule());
                    						}
                    						add(
                    							current,
                    							"subTransformationsAndSchemes",
                    							lv_subTransformationsAndSchemes_12_0,
                    							"org.eclipse.efbt.aorta.Aorta.FunctionalityModule");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:8681:4: (otherlv_13= ',' ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) ) )*
                    loop207:
                    do {
                        int alt207=2;
                        int LA207_0 = input.LA(1);

                        if ( (LA207_0==31) ) {
                            alt207=1;
                        }


                        switch (alt207) {
                    	case 1 :
                    	    // InternalAorta.g:8682:5: otherlv_13= ',' ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) )
                    	    {
                    	    otherlv_13=(Token)match(input,31,FOLLOW_100); 

                    	    					newLeafNode(otherlv_13, grammarAccess.getSerialFunctionalityModuleAccess().getCommaKeyword_7_3_0());
                    	    				
                    	    // InternalAorta.g:8686:5: ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) )
                    	    // InternalAorta.g:8687:6: (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule )
                    	    {
                    	    // InternalAorta.g:8687:6: (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule )
                    	    // InternalAorta.g:8688:7: lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule
                    	    {

                    	    							newCompositeNode(grammarAccess.getSerialFunctionalityModuleAccess().getSubTransformationsAndSchemesFunctionalityModuleParserRuleCall_7_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_35);
                    	    lv_subTransformationsAndSchemes_14_0=ruleFunctionalityModule();

                    	    state._fsp--;


                    	    							if (current==null) {
                    	    								current = createModelElementForParent(grammarAccess.getSerialFunctionalityModuleRule());
                    	    							}
                    	    							add(
                    	    								current,
                    	    								"subTransformationsAndSchemes",
                    	    								lv_subTransformationsAndSchemes_14_0,
                    	    								"org.eclipse.efbt.aorta.Aorta.FunctionalityModule");
                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop207;
                        }
                    } while (true);

                    otherlv_15=(Token)match(input,23,FOLLOW_24); 

                    				newLeafNode(otherlv_15, grammarAccess.getSerialFunctionalityModuleAccess().getRightCurlyBracketKeyword_7_4());
                    			

                    }
                    break;

            }

            otherlv_16=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_16, grammarAccess.getSerialFunctionalityModuleAccess().getRightCurlyBracketKeyword_8());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleSerialFunctionalityModule"


    // $ANTLR start "entryRuleFunctionalityModuleSet_Impl"
    // InternalAorta.g:8719:1: entryRuleFunctionalityModuleSet_Impl returns [EObject current=null] : iv_ruleFunctionalityModuleSet_Impl= ruleFunctionalityModuleSet_Impl EOF ;
    public final EObject entryRuleFunctionalityModuleSet_Impl() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleFunctionalityModuleSet_Impl = null;


        try {
            // InternalAorta.g:8719:68: (iv_ruleFunctionalityModuleSet_Impl= ruleFunctionalityModuleSet_Impl EOF )
            // InternalAorta.g:8720:2: iv_ruleFunctionalityModuleSet_Impl= ruleFunctionalityModuleSet_Impl EOF
            {
             newCompositeNode(grammarAccess.getFunctionalityModuleSet_ImplRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleFunctionalityModuleSet_Impl=ruleFunctionalityModuleSet_Impl();

            state._fsp--;

             current =iv_ruleFunctionalityModuleSet_Impl; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleFunctionalityModuleSet_Impl"


    // $ANTLR start "ruleFunctionalityModuleSet_Impl"
    // InternalAorta.g:8726:1: ruleFunctionalityModuleSet_Impl returns [EObject current=null] : ( () otherlv_1= 'FunctionalityModuleSet' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? (otherlv_10= 'subTransformationsAndSchemes' otherlv_11= '{' ( (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule ) ) (otherlv_13= ',' ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) ) )* otherlv_15= '}' )? otherlv_16= '}' ) ;
    public final EObject ruleFunctionalityModuleSet_Impl() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_6=null;
        Token otherlv_8=null;
        Token otherlv_10=null;
        Token otherlv_11=null;
        Token otherlv_13=null;
        Token otherlv_15=null;
        Token otherlv_16=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;

        EObject lv_scenarioSet_9_0 = null;

        EObject lv_subTransformationsAndSchemes_12_0 = null;

        EObject lv_subTransformationsAndSchemes_14_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:8732:2: ( ( () otherlv_1= 'FunctionalityModuleSet' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? (otherlv_10= 'subTransformationsAndSchemes' otherlv_11= '{' ( (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule ) ) (otherlv_13= ',' ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) ) )* otherlv_15= '}' )? otherlv_16= '}' ) )
            // InternalAorta.g:8733:2: ( () otherlv_1= 'FunctionalityModuleSet' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? (otherlv_10= 'subTransformationsAndSchemes' otherlv_11= '{' ( (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule ) ) (otherlv_13= ',' ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) ) )* otherlv_15= '}' )? otherlv_16= '}' )
            {
            // InternalAorta.g:8733:2: ( () otherlv_1= 'FunctionalityModuleSet' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? (otherlv_10= 'subTransformationsAndSchemes' otherlv_11= '{' ( (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule ) ) (otherlv_13= ',' ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) ) )* otherlv_15= '}' )? otherlv_16= '}' )
            // InternalAorta.g:8734:3: () otherlv_1= 'FunctionalityModuleSet' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? (otherlv_10= 'subTransformationsAndSchemes' otherlv_11= '{' ( (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule ) ) (otherlv_13= ',' ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) ) )* otherlv_15= '}' )? otherlv_16= '}'
            {
            // InternalAorta.g:8734:3: ()
            // InternalAorta.g:8735:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getFunctionalityModuleSet_ImplAccess().getFunctionalityModuleSetAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,156,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getFunctionalityModuleSet_ImplAccess().getFunctionalityModuleSetKeyword_1());
            		
            // InternalAorta.g:8745:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:8746:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:8746:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:8747:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getFunctionalityModuleSet_ImplAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getFunctionalityModuleSet_ImplRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_179); 

            			newLeafNode(otherlv_3, grammarAccess.getFunctionalityModuleSet_ImplAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:8768:3: (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )?
            int alt209=2;
            int LA209_0 = input.LA(1);

            if ( (LA209_0==143) ) {
                alt209=1;
            }
            switch (alt209) {
                case 1 :
                    // InternalAorta.g:8769:4: otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) )
                    {
                    otherlv_4=(Token)match(input,143,FOLLOW_70); 

                    				newLeafNode(otherlv_4, grammarAccess.getFunctionalityModuleSet_ImplAccess().getUsedTransformationOrSchemeKeyword_4_0());
                    			
                    // InternalAorta.g:8773:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:8774:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:8774:5: ( ruleQualifiedName )
                    // InternalAorta.g:8775:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getFunctionalityModuleSet_ImplRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getFunctionalityModuleSet_ImplAccess().getUsedTransformationOrSchemeFunctionalityModuleCrossReference_4_1_0());
                    					
                    pushFollow(FOLLOW_180);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:8790:3: (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )?
            int alt210=2;
            int LA210_0 = input.LA(1);

            if ( (LA210_0==144) ) {
                alt210=1;
            }
            switch (alt210) {
                case 1 :
                    // InternalAorta.g:8791:4: otherlv_6= 'scenario' ( ( ruleQualifiedName ) )
                    {
                    otherlv_6=(Token)match(input,144,FOLLOW_70); 

                    				newLeafNode(otherlv_6, grammarAccess.getFunctionalityModuleSet_ImplAccess().getScenarioKeyword_5_0());
                    			
                    // InternalAorta.g:8795:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:8796:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:8796:5: ( ruleQualifiedName )
                    // InternalAorta.g:8797:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getFunctionalityModuleSet_ImplRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getFunctionalityModuleSet_ImplAccess().getScenarioScenarioCrossReference_5_1_0());
                    					
                    pushFollow(FOLLOW_181);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:8812:3: (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )?
            int alt211=2;
            int LA211_0 = input.LA(1);

            if ( (LA211_0==145) ) {
                alt211=1;
            }
            switch (alt211) {
                case 1 :
                    // InternalAorta.g:8813:4: otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) )
                    {
                    otherlv_8=(Token)match(input,145,FOLLOW_176); 

                    				newLeafNode(otherlv_8, grammarAccess.getFunctionalityModuleSet_ImplAccess().getScenarioSetKeyword_6_0());
                    			
                    // InternalAorta.g:8817:4: ( (lv_scenarioSet_9_0= ruleScenarioSet ) )
                    // InternalAorta.g:8818:5: (lv_scenarioSet_9_0= ruleScenarioSet )
                    {
                    // InternalAorta.g:8818:5: (lv_scenarioSet_9_0= ruleScenarioSet )
                    // InternalAorta.g:8819:6: lv_scenarioSet_9_0= ruleScenarioSet
                    {

                    						newCompositeNode(grammarAccess.getFunctionalityModuleSet_ImplAccess().getScenarioSetScenarioSetParserRuleCall_6_1_0());
                    					
                    pushFollow(FOLLOW_182);
                    lv_scenarioSet_9_0=ruleScenarioSet();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getFunctionalityModuleSet_ImplRule());
                    						}
                    						set(
                    							current,
                    							"scenarioSet",
                    							lv_scenarioSet_9_0,
                    							"org.eclipse.efbt.aorta.Aorta.ScenarioSet");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:8837:3: (otherlv_10= 'subTransformationsAndSchemes' otherlv_11= '{' ( (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule ) ) (otherlv_13= ',' ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) ) )* otherlv_15= '}' )?
            int alt213=2;
            int LA213_0 = input.LA(1);

            if ( (LA213_0==152) ) {
                alt213=1;
            }
            switch (alt213) {
                case 1 :
                    // InternalAorta.g:8838:4: otherlv_10= 'subTransformationsAndSchemes' otherlv_11= '{' ( (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule ) ) (otherlv_13= ',' ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) ) )* otherlv_15= '}'
                    {
                    otherlv_10=(Token)match(input,152,FOLLOW_3); 

                    				newLeafNode(otherlv_10, grammarAccess.getFunctionalityModuleSet_ImplAccess().getSubTransformationsAndSchemesKeyword_7_0());
                    			
                    otherlv_11=(Token)match(input,12,FOLLOW_100); 

                    				newLeafNode(otherlv_11, grammarAccess.getFunctionalityModuleSet_ImplAccess().getLeftCurlyBracketKeyword_7_1());
                    			
                    // InternalAorta.g:8846:4: ( (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule ) )
                    // InternalAorta.g:8847:5: (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule )
                    {
                    // InternalAorta.g:8847:5: (lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule )
                    // InternalAorta.g:8848:6: lv_subTransformationsAndSchemes_12_0= ruleFunctionalityModule
                    {

                    						newCompositeNode(grammarAccess.getFunctionalityModuleSet_ImplAccess().getSubTransformationsAndSchemesFunctionalityModuleParserRuleCall_7_2_0());
                    					
                    pushFollow(FOLLOW_35);
                    lv_subTransformationsAndSchemes_12_0=ruleFunctionalityModule();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getFunctionalityModuleSet_ImplRule());
                    						}
                    						add(
                    							current,
                    							"subTransformationsAndSchemes",
                    							lv_subTransformationsAndSchemes_12_0,
                    							"org.eclipse.efbt.aorta.Aorta.FunctionalityModule");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }

                    // InternalAorta.g:8865:4: (otherlv_13= ',' ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) ) )*
                    loop212:
                    do {
                        int alt212=2;
                        int LA212_0 = input.LA(1);

                        if ( (LA212_0==31) ) {
                            alt212=1;
                        }


                        switch (alt212) {
                    	case 1 :
                    	    // InternalAorta.g:8866:5: otherlv_13= ',' ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) )
                    	    {
                    	    otherlv_13=(Token)match(input,31,FOLLOW_100); 

                    	    					newLeafNode(otherlv_13, grammarAccess.getFunctionalityModuleSet_ImplAccess().getCommaKeyword_7_3_0());
                    	    				
                    	    // InternalAorta.g:8870:5: ( (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule ) )
                    	    // InternalAorta.g:8871:6: (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule )
                    	    {
                    	    // InternalAorta.g:8871:6: (lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule )
                    	    // InternalAorta.g:8872:7: lv_subTransformationsAndSchemes_14_0= ruleFunctionalityModule
                    	    {

                    	    							newCompositeNode(grammarAccess.getFunctionalityModuleSet_ImplAccess().getSubTransformationsAndSchemesFunctionalityModuleParserRuleCall_7_3_1_0());
                    	    						
                    	    pushFollow(FOLLOW_35);
                    	    lv_subTransformationsAndSchemes_14_0=ruleFunctionalityModule();

                    	    state._fsp--;


                    	    							if (current==null) {
                    	    								current = createModelElementForParent(grammarAccess.getFunctionalityModuleSet_ImplRule());
                    	    							}
                    	    							add(
                    	    								current,
                    	    								"subTransformationsAndSchemes",
                    	    								lv_subTransformationsAndSchemes_14_0,
                    	    								"org.eclipse.efbt.aorta.Aorta.FunctionalityModule");
                    	    							afterParserOrEnumRuleCall();
                    	    						

                    	    }


                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop212;
                        }
                    } while (true);

                    otherlv_15=(Token)match(input,23,FOLLOW_24); 

                    				newLeafNode(otherlv_15, grammarAccess.getFunctionalityModuleSet_ImplAccess().getRightCurlyBracketKeyword_7_4());
                    			

                    }
                    break;

            }

            otherlv_16=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_16, grammarAccess.getFunctionalityModuleSet_ImplAccess().getRightCurlyBracketKeyword_8());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleFunctionalityModuleSet_Impl"


    // $ANTLR start "entryRuleReadDataSourceFunctionalityModule"
    // InternalAorta.g:8903:1: entryRuleReadDataSourceFunctionalityModule returns [EObject current=null] : iv_ruleReadDataSourceFunctionalityModule= ruleReadDataSourceFunctionalityModule EOF ;
    public final EObject entryRuleReadDataSourceFunctionalityModule() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleReadDataSourceFunctionalityModule = null;


        try {
            // InternalAorta.g:8903:74: (iv_ruleReadDataSourceFunctionalityModule= ruleReadDataSourceFunctionalityModule EOF )
            // InternalAorta.g:8904:2: iv_ruleReadDataSourceFunctionalityModule= ruleReadDataSourceFunctionalityModule EOF
            {
             newCompositeNode(grammarAccess.getReadDataSourceFunctionalityModuleRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleReadDataSourceFunctionalityModule=ruleReadDataSourceFunctionalityModule();

            state._fsp--;

             current =iv_ruleReadDataSourceFunctionalityModule; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleReadDataSourceFunctionalityModule"


    // $ANTLR start "ruleReadDataSourceFunctionalityModule"
    // InternalAorta.g:8910:1: ruleReadDataSourceFunctionalityModule returns [EObject current=null] : ( () otherlv_1= 'ReadDataSourceFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? otherlv_10= '}' ) ;
    public final EObject ruleReadDataSourceFunctionalityModule() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_6=null;
        Token otherlv_8=null;
        Token otherlv_10=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;

        EObject lv_scenarioSet_9_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:8916:2: ( ( () otherlv_1= 'ReadDataSourceFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? otherlv_10= '}' ) )
            // InternalAorta.g:8917:2: ( () otherlv_1= 'ReadDataSourceFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? otherlv_10= '}' )
            {
            // InternalAorta.g:8917:2: ( () otherlv_1= 'ReadDataSourceFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? otherlv_10= '}' )
            // InternalAorta.g:8918:3: () otherlv_1= 'ReadDataSourceFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? otherlv_10= '}'
            {
            // InternalAorta.g:8918:3: ()
            // InternalAorta.g:8919:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getReadDataSourceFunctionalityModuleAccess().getReadDataSourceFunctionalityModuleAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,157,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getReadDataSourceFunctionalityModuleAccess().getReadDataSourceFunctionalityModuleKeyword_1());
            		
            // InternalAorta.g:8929:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:8930:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:8930:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:8931:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getReadDataSourceFunctionalityModuleAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getReadDataSourceFunctionalityModuleRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_173); 

            			newLeafNode(otherlv_3, grammarAccess.getReadDataSourceFunctionalityModuleAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:8952:3: (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )?
            int alt214=2;
            int LA214_0 = input.LA(1);

            if ( (LA214_0==143) ) {
                alt214=1;
            }
            switch (alt214) {
                case 1 :
                    // InternalAorta.g:8953:4: otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) )
                    {
                    otherlv_4=(Token)match(input,143,FOLLOW_70); 

                    				newLeafNode(otherlv_4, grammarAccess.getReadDataSourceFunctionalityModuleAccess().getUsedTransformationOrSchemeKeyword_4_0());
                    			
                    // InternalAorta.g:8957:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:8958:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:8958:5: ( ruleQualifiedName )
                    // InternalAorta.g:8959:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getReadDataSourceFunctionalityModuleRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getReadDataSourceFunctionalityModuleAccess().getUsedTransformationOrSchemeFunctionalityModuleCrossReference_4_1_0());
                    					
                    pushFollow(FOLLOW_174);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:8974:3: (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )?
            int alt215=2;
            int LA215_0 = input.LA(1);

            if ( (LA215_0==144) ) {
                alt215=1;
            }
            switch (alt215) {
                case 1 :
                    // InternalAorta.g:8975:4: otherlv_6= 'scenario' ( ( ruleQualifiedName ) )
                    {
                    otherlv_6=(Token)match(input,144,FOLLOW_70); 

                    				newLeafNode(otherlv_6, grammarAccess.getReadDataSourceFunctionalityModuleAccess().getScenarioKeyword_5_0());
                    			
                    // InternalAorta.g:8979:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:8980:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:8980:5: ( ruleQualifiedName )
                    // InternalAorta.g:8981:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getReadDataSourceFunctionalityModuleRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getReadDataSourceFunctionalityModuleAccess().getScenarioScenarioCrossReference_5_1_0());
                    					
                    pushFollow(FOLLOW_175);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:8996:3: (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )?
            int alt216=2;
            int LA216_0 = input.LA(1);

            if ( (LA216_0==145) ) {
                alt216=1;
            }
            switch (alt216) {
                case 1 :
                    // InternalAorta.g:8997:4: otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) )
                    {
                    otherlv_8=(Token)match(input,145,FOLLOW_176); 

                    				newLeafNode(otherlv_8, grammarAccess.getReadDataSourceFunctionalityModuleAccess().getScenarioSetKeyword_6_0());
                    			
                    // InternalAorta.g:9001:4: ( (lv_scenarioSet_9_0= ruleScenarioSet ) )
                    // InternalAorta.g:9002:5: (lv_scenarioSet_9_0= ruleScenarioSet )
                    {
                    // InternalAorta.g:9002:5: (lv_scenarioSet_9_0= ruleScenarioSet )
                    // InternalAorta.g:9003:6: lv_scenarioSet_9_0= ruleScenarioSet
                    {

                    						newCompositeNode(grammarAccess.getReadDataSourceFunctionalityModuleAccess().getScenarioSetScenarioSetParserRuleCall_6_1_0());
                    					
                    pushFollow(FOLLOW_24);
                    lv_scenarioSet_9_0=ruleScenarioSet();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getReadDataSourceFunctionalityModuleRule());
                    						}
                    						set(
                    							current,
                    							"scenarioSet",
                    							lv_scenarioSet_9_0,
                    							"org.eclipse.efbt.aorta.Aorta.ScenarioSet");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            otherlv_10=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_10, grammarAccess.getReadDataSourceFunctionalityModuleAccess().getRightCurlyBracketKeyword_7());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleReadDataSourceFunctionalityModule"


    // $ANTLR start "entryRuleOutputCreationFunctionalityModule"
    // InternalAorta.g:9029:1: entryRuleOutputCreationFunctionalityModule returns [EObject current=null] : iv_ruleOutputCreationFunctionalityModule= ruleOutputCreationFunctionalityModule EOF ;
    public final EObject entryRuleOutputCreationFunctionalityModule() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleOutputCreationFunctionalityModule = null;


        try {
            // InternalAorta.g:9029:74: (iv_ruleOutputCreationFunctionalityModule= ruleOutputCreationFunctionalityModule EOF )
            // InternalAorta.g:9030:2: iv_ruleOutputCreationFunctionalityModule= ruleOutputCreationFunctionalityModule EOF
            {
             newCompositeNode(grammarAccess.getOutputCreationFunctionalityModuleRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleOutputCreationFunctionalityModule=ruleOutputCreationFunctionalityModule();

            state._fsp--;

             current =iv_ruleOutputCreationFunctionalityModule; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleOutputCreationFunctionalityModule"


    // $ANTLR start "ruleOutputCreationFunctionalityModule"
    // InternalAorta.g:9036:1: ruleOutputCreationFunctionalityModule returns [EObject current=null] : ( () otherlv_1= 'OutputCreationFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? otherlv_10= '}' ) ;
    public final EObject ruleOutputCreationFunctionalityModule() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_6=null;
        Token otherlv_8=null;
        Token otherlv_10=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;

        EObject lv_scenarioSet_9_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:9042:2: ( ( () otherlv_1= 'OutputCreationFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? otherlv_10= '}' ) )
            // InternalAorta.g:9043:2: ( () otherlv_1= 'OutputCreationFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? otherlv_10= '}' )
            {
            // InternalAorta.g:9043:2: ( () otherlv_1= 'OutputCreationFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? otherlv_10= '}' )
            // InternalAorta.g:9044:3: () otherlv_1= 'OutputCreationFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? otherlv_10= '}'
            {
            // InternalAorta.g:9044:3: ()
            // InternalAorta.g:9045:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getOutputCreationFunctionalityModuleAccess().getOutputCreationFunctionalityModuleAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,158,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getOutputCreationFunctionalityModuleAccess().getOutputCreationFunctionalityModuleKeyword_1());
            		
            // InternalAorta.g:9055:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:9056:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:9056:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:9057:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getOutputCreationFunctionalityModuleAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getOutputCreationFunctionalityModuleRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_173); 

            			newLeafNode(otherlv_3, grammarAccess.getOutputCreationFunctionalityModuleAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:9078:3: (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )?
            int alt217=2;
            int LA217_0 = input.LA(1);

            if ( (LA217_0==143) ) {
                alt217=1;
            }
            switch (alt217) {
                case 1 :
                    // InternalAorta.g:9079:4: otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) )
                    {
                    otherlv_4=(Token)match(input,143,FOLLOW_70); 

                    				newLeafNode(otherlv_4, grammarAccess.getOutputCreationFunctionalityModuleAccess().getUsedTransformationOrSchemeKeyword_4_0());
                    			
                    // InternalAorta.g:9083:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:9084:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:9084:5: ( ruleQualifiedName )
                    // InternalAorta.g:9085:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getOutputCreationFunctionalityModuleRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getOutputCreationFunctionalityModuleAccess().getUsedTransformationOrSchemeFunctionalityModuleCrossReference_4_1_0());
                    					
                    pushFollow(FOLLOW_174);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:9100:3: (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )?
            int alt218=2;
            int LA218_0 = input.LA(1);

            if ( (LA218_0==144) ) {
                alt218=1;
            }
            switch (alt218) {
                case 1 :
                    // InternalAorta.g:9101:4: otherlv_6= 'scenario' ( ( ruleQualifiedName ) )
                    {
                    otherlv_6=(Token)match(input,144,FOLLOW_70); 

                    				newLeafNode(otherlv_6, grammarAccess.getOutputCreationFunctionalityModuleAccess().getScenarioKeyword_5_0());
                    			
                    // InternalAorta.g:9105:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:9106:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:9106:5: ( ruleQualifiedName )
                    // InternalAorta.g:9107:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getOutputCreationFunctionalityModuleRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getOutputCreationFunctionalityModuleAccess().getScenarioScenarioCrossReference_5_1_0());
                    					
                    pushFollow(FOLLOW_175);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:9122:3: (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )?
            int alt219=2;
            int LA219_0 = input.LA(1);

            if ( (LA219_0==145) ) {
                alt219=1;
            }
            switch (alt219) {
                case 1 :
                    // InternalAorta.g:9123:4: otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) )
                    {
                    otherlv_8=(Token)match(input,145,FOLLOW_176); 

                    				newLeafNode(otherlv_8, grammarAccess.getOutputCreationFunctionalityModuleAccess().getScenarioSetKeyword_6_0());
                    			
                    // InternalAorta.g:9127:4: ( (lv_scenarioSet_9_0= ruleScenarioSet ) )
                    // InternalAorta.g:9128:5: (lv_scenarioSet_9_0= ruleScenarioSet )
                    {
                    // InternalAorta.g:9128:5: (lv_scenarioSet_9_0= ruleScenarioSet )
                    // InternalAorta.g:9129:6: lv_scenarioSet_9_0= ruleScenarioSet
                    {

                    						newCompositeNode(grammarAccess.getOutputCreationFunctionalityModuleAccess().getScenarioSetScenarioSetParserRuleCall_6_1_0());
                    					
                    pushFollow(FOLLOW_24);
                    lv_scenarioSet_9_0=ruleScenarioSet();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getOutputCreationFunctionalityModuleRule());
                    						}
                    						set(
                    							current,
                    							"scenarioSet",
                    							lv_scenarioSet_9_0,
                    							"org.eclipse.efbt.aorta.Aorta.ScenarioSet");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            otherlv_10=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_10, grammarAccess.getOutputCreationFunctionalityModuleAccess().getRightCurlyBracketKeyword_7());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleOutputCreationFunctionalityModule"


    // $ANTLR start "entryRuleValidationFunctionalityModule"
    // InternalAorta.g:9155:1: entryRuleValidationFunctionalityModule returns [EObject current=null] : iv_ruleValidationFunctionalityModule= ruleValidationFunctionalityModule EOF ;
    public final EObject entryRuleValidationFunctionalityModule() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleValidationFunctionalityModule = null;


        try {
            // InternalAorta.g:9155:70: (iv_ruleValidationFunctionalityModule= ruleValidationFunctionalityModule EOF )
            // InternalAorta.g:9156:2: iv_ruleValidationFunctionalityModule= ruleValidationFunctionalityModule EOF
            {
             newCompositeNode(grammarAccess.getValidationFunctionalityModuleRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleValidationFunctionalityModule=ruleValidationFunctionalityModule();

            state._fsp--;

             current =iv_ruleValidationFunctionalityModule; 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleValidationFunctionalityModule"


    // $ANTLR start "ruleValidationFunctionalityModule"
    // InternalAorta.g:9162:1: ruleValidationFunctionalityModule returns [EObject current=null] : ( () otherlv_1= 'ValidationFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? otherlv_10= '}' ) ;
    public final EObject ruleValidationFunctionalityModule() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_4=null;
        Token otherlv_6=null;
        Token otherlv_8=null;
        Token otherlv_10=null;
        AntlrDatatypeRuleToken lv_name_2_0 = null;

        EObject lv_scenarioSet_9_0 = null;



        	enterRule();

        try {
            // InternalAorta.g:9168:2: ( ( () otherlv_1= 'ValidationFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? otherlv_10= '}' ) )
            // InternalAorta.g:9169:2: ( () otherlv_1= 'ValidationFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? otherlv_10= '}' )
            {
            // InternalAorta.g:9169:2: ( () otherlv_1= 'ValidationFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? otherlv_10= '}' )
            // InternalAorta.g:9170:3: () otherlv_1= 'ValidationFunctionalityModule' ( (lv_name_2_0= ruleEString ) ) otherlv_3= '{' (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )? (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )? (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )? otherlv_10= '}'
            {
            // InternalAorta.g:9170:3: ()
            // InternalAorta.g:9171:4: 
            {

            				current = forceCreateModelElement(
            					grammarAccess.getValidationFunctionalityModuleAccess().getValidationFunctionalityModuleAction_0(),
            					current);
            			

            }

            otherlv_1=(Token)match(input,159,FOLLOW_25); 

            			newLeafNode(otherlv_1, grammarAccess.getValidationFunctionalityModuleAccess().getValidationFunctionalityModuleKeyword_1());
            		
            // InternalAorta.g:9181:3: ( (lv_name_2_0= ruleEString ) )
            // InternalAorta.g:9182:4: (lv_name_2_0= ruleEString )
            {
            // InternalAorta.g:9182:4: (lv_name_2_0= ruleEString )
            // InternalAorta.g:9183:5: lv_name_2_0= ruleEString
            {

            					newCompositeNode(grammarAccess.getValidationFunctionalityModuleAccess().getNameEStringParserRuleCall_2_0());
            				
            pushFollow(FOLLOW_3);
            lv_name_2_0=ruleEString();

            state._fsp--;


            					if (current==null) {
            						current = createModelElementForParent(grammarAccess.getValidationFunctionalityModuleRule());
            					}
            					set(
            						current,
            						"name",
            						lv_name_2_0,
            						"org.eclipse.efbt.aorta.Aorta.EString");
            					afterParserOrEnumRuleCall();
            				

            }


            }

            otherlv_3=(Token)match(input,12,FOLLOW_173); 

            			newLeafNode(otherlv_3, grammarAccess.getValidationFunctionalityModuleAccess().getLeftCurlyBracketKeyword_3());
            		
            // InternalAorta.g:9204:3: (otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) ) )?
            int alt220=2;
            int LA220_0 = input.LA(1);

            if ( (LA220_0==143) ) {
                alt220=1;
            }
            switch (alt220) {
                case 1 :
                    // InternalAorta.g:9205:4: otherlv_4= 'usedTransformationOrScheme' ( ( ruleQualifiedName ) )
                    {
                    otherlv_4=(Token)match(input,143,FOLLOW_70); 

                    				newLeafNode(otherlv_4, grammarAccess.getValidationFunctionalityModuleAccess().getUsedTransformationOrSchemeKeyword_4_0());
                    			
                    // InternalAorta.g:9209:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:9210:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:9210:5: ( ruleQualifiedName )
                    // InternalAorta.g:9211:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getValidationFunctionalityModuleRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getValidationFunctionalityModuleAccess().getUsedTransformationOrSchemeFunctionalityModuleCrossReference_4_1_0());
                    					
                    pushFollow(FOLLOW_174);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:9226:3: (otherlv_6= 'scenario' ( ( ruleQualifiedName ) ) )?
            int alt221=2;
            int LA221_0 = input.LA(1);

            if ( (LA221_0==144) ) {
                alt221=1;
            }
            switch (alt221) {
                case 1 :
                    // InternalAorta.g:9227:4: otherlv_6= 'scenario' ( ( ruleQualifiedName ) )
                    {
                    otherlv_6=(Token)match(input,144,FOLLOW_70); 

                    				newLeafNode(otherlv_6, grammarAccess.getValidationFunctionalityModuleAccess().getScenarioKeyword_5_0());
                    			
                    // InternalAorta.g:9231:4: ( ( ruleQualifiedName ) )
                    // InternalAorta.g:9232:5: ( ruleQualifiedName )
                    {
                    // InternalAorta.g:9232:5: ( ruleQualifiedName )
                    // InternalAorta.g:9233:6: ruleQualifiedName
                    {

                    						if (current==null) {
                    							current = createModelElement(grammarAccess.getValidationFunctionalityModuleRule());
                    						}
                    					

                    						newCompositeNode(grammarAccess.getValidationFunctionalityModuleAccess().getScenarioScenarioCrossReference_5_1_0());
                    					
                    pushFollow(FOLLOW_175);
                    ruleQualifiedName();

                    state._fsp--;


                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            // InternalAorta.g:9248:3: (otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) ) )?
            int alt222=2;
            int LA222_0 = input.LA(1);

            if ( (LA222_0==145) ) {
                alt222=1;
            }
            switch (alt222) {
                case 1 :
                    // InternalAorta.g:9249:4: otherlv_8= 'scenarioSet' ( (lv_scenarioSet_9_0= ruleScenarioSet ) )
                    {
                    otherlv_8=(Token)match(input,145,FOLLOW_176); 

                    				newLeafNode(otherlv_8, grammarAccess.getValidationFunctionalityModuleAccess().getScenarioSetKeyword_6_0());
                    			
                    // InternalAorta.g:9253:4: ( (lv_scenarioSet_9_0= ruleScenarioSet ) )
                    // InternalAorta.g:9254:5: (lv_scenarioSet_9_0= ruleScenarioSet )
                    {
                    // InternalAorta.g:9254:5: (lv_scenarioSet_9_0= ruleScenarioSet )
                    // InternalAorta.g:9255:6: lv_scenarioSet_9_0= ruleScenarioSet
                    {

                    						newCompositeNode(grammarAccess.getValidationFunctionalityModuleAccess().getScenarioSetScenarioSetParserRuleCall_6_1_0());
                    					
                    pushFollow(FOLLOW_24);
                    lv_scenarioSet_9_0=ruleScenarioSet();

                    state._fsp--;


                    						if (current==null) {
                    							current = createModelElementForParent(grammarAccess.getValidationFunctionalityModuleRule());
                    						}
                    						set(
                    							current,
                    							"scenarioSet",
                    							lv_scenarioSet_9_0,
                    							"org.eclipse.efbt.aorta.Aorta.ScenarioSet");
                    						afterParserOrEnumRuleCall();
                    					

                    }


                    }


                    }
                    break;

            }

            otherlv_10=(Token)match(input,23,FOLLOW_2); 

            			newLeafNode(otherlv_10, grammarAccess.getValidationFunctionalityModuleAccess().getRightCurlyBracketKeyword_7());
            		

            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleValidationFunctionalityModule"


    // $ANTLR start "entryRuleQualifiedName"
    // InternalAorta.g:9281:1: entryRuleQualifiedName returns [String current=null] : iv_ruleQualifiedName= ruleQualifiedName EOF ;
    public final String entryRuleQualifiedName() throws RecognitionException {
        String current = null;

        AntlrDatatypeRuleToken iv_ruleQualifiedName = null;


        try {
            // InternalAorta.g:9281:53: (iv_ruleQualifiedName= ruleQualifiedName EOF )
            // InternalAorta.g:9282:2: iv_ruleQualifiedName= ruleQualifiedName EOF
            {
             newCompositeNode(grammarAccess.getQualifiedNameRule()); 
            pushFollow(FOLLOW_1);
            iv_ruleQualifiedName=ruleQualifiedName();

            state._fsp--;

             current =iv_ruleQualifiedName.getText(); 
            match(input,EOF,FOLLOW_2); 

            }

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "entryRuleQualifiedName"


    // $ANTLR start "ruleQualifiedName"
    // InternalAorta.g:9288:1: ruleQualifiedName returns [AntlrDatatypeRuleToken current=new AntlrDatatypeRuleToken()] : (this_ID_0= RULE_ID (kw= '.' this_ID_2= RULE_ID )* ) ;
    public final AntlrDatatypeRuleToken ruleQualifiedName() throws RecognitionException {
        AntlrDatatypeRuleToken current = new AntlrDatatypeRuleToken();

        Token this_ID_0=null;
        Token kw=null;
        Token this_ID_2=null;


        	enterRule();

        try {
            // InternalAorta.g:9294:2: ( (this_ID_0= RULE_ID (kw= '.' this_ID_2= RULE_ID )* ) )
            // InternalAorta.g:9295:2: (this_ID_0= RULE_ID (kw= '.' this_ID_2= RULE_ID )* )
            {
            // InternalAorta.g:9295:2: (this_ID_0= RULE_ID (kw= '.' this_ID_2= RULE_ID )* )
            // InternalAorta.g:9296:3: this_ID_0= RULE_ID (kw= '.' this_ID_2= RULE_ID )*
            {
            this_ID_0=(Token)match(input,RULE_ID,FOLLOW_188); 

            			current.merge(this_ID_0);
            		

            			newLeafNode(this_ID_0, grammarAccess.getQualifiedNameAccess().getIDTerminalRuleCall_0());
            		
            // InternalAorta.g:9303:3: (kw= '.' this_ID_2= RULE_ID )*
            loop223:
            do {
                int alt223=2;
                int LA223_0 = input.LA(1);

                if ( (LA223_0==160) ) {
                    alt223=1;
                }


                switch (alt223) {
            	case 1 :
            	    // InternalAorta.g:9304:4: kw= '.' this_ID_2= RULE_ID
            	    {
            	    kw=(Token)match(input,160,FOLLOW_70); 

            	    				current.merge(kw);
            	    				newLeafNode(kw, grammarAccess.getQualifiedNameAccess().getFullStopKeyword_1_0());
            	    			
            	    this_ID_2=(Token)match(input,RULE_ID,FOLLOW_188); 

            	    				current.merge(this_ID_2);
            	    			

            	    				newLeafNode(this_ID_2, grammarAccess.getQualifiedNameAccess().getIDTerminalRuleCall_1_1());
            	    			

            	    }
            	    break;

            	default :
            	    break loop223;
                }
            } while (true);


            }


            }


            	leaveRule();

        }

            catch (RecognitionException re) {
                recover(input,re);
                appendSkippedTokens();
            }
        finally {
        }
        return current;
    }
    // $ANTLR end "ruleQualifiedName"

    // Delegated rules


 

    public static final BitSet FOLLOW_1 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_2 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_3 = new BitSet(new long[]{0x0000000000001000L});
    public static final BitSet FOLLOW_4 = new BitSet(new long[]{0x0000000000FFE000L});
    public static final BitSet FOLLOW_5 = new BitSet(new long[]{0x0000000001000000L});
    public static final BitSet FOLLOW_6 = new BitSet(new long[]{0x0000000000FFC000L});
    public static final BitSet FOLLOW_7 = new BitSet(new long[]{0x0000000100000000L});
    public static final BitSet FOLLOW_8 = new BitSet(new long[]{0x0000000000FF8000L});
    public static final BitSet FOLLOW_9 = new BitSet(new long[]{0x0000000800000000L});
    public static final BitSet FOLLOW_10 = new BitSet(new long[]{0x0000000000FF0000L});
    public static final BitSet FOLLOW_11 = new BitSet(new long[]{0x0000002000000000L});
    public static final BitSet FOLLOW_12 = new BitSet(new long[]{0x0000000000FE0000L});
    public static final BitSet FOLLOW_13 = new BitSet(new long[]{0x0000008000000000L});
    public static final BitSet FOLLOW_14 = new BitSet(new long[]{0x0000000000FC0000L});
    public static final BitSet FOLLOW_15 = new BitSet(new long[]{0x0000010000000000L});
    public static final BitSet FOLLOW_16 = new BitSet(new long[]{0x0000000000F80000L});
    public static final BitSet FOLLOW_17 = new BitSet(new long[]{0x0000040000000000L});
    public static final BitSet FOLLOW_18 = new BitSet(new long[]{0x0000000000F00000L});
    public static final BitSet FOLLOW_19 = new BitSet(new long[]{0x0000100000000000L});
    public static final BitSet FOLLOW_20 = new BitSet(new long[]{0x0000000000E00000L});
    public static final BitSet FOLLOW_21 = new BitSet(new long[]{0x0000400000000000L});
    public static final BitSet FOLLOW_22 = new BitSet(new long[]{0x0000000000C00000L});
    public static final BitSet FOLLOW_23 = new BitSet(new long[]{0x0008000000000000L});
    public static final BitSet FOLLOW_24 = new BitSet(new long[]{0x0000000000800000L});
    public static final BitSet FOLLOW_25 = new BitSet(new long[]{0x0000000000000030L});
    public static final BitSet FOLLOW_26 = new BitSet(new long[]{0x000000007E800000L});
    public static final BitSet FOLLOW_27 = new BitSet(new long[]{0x000000007C800000L});
    public static final BitSet FOLLOW_28 = new BitSet(new long[]{0x0000000078800000L});
    public static final BitSet FOLLOW_29 = new BitSet(new long[]{0x0000000070800000L});
    public static final BitSet FOLLOW_30 = new BitSet(new long[]{0x0020000000000000L});
    public static final BitSet FOLLOW_31 = new BitSet(new long[]{0x0000000060800000L});
    public static final BitSet FOLLOW_32 = new BitSet(new long[]{0x0080000000000000L});
    public static final BitSet FOLLOW_33 = new BitSet(new long[]{0x0000000040800000L});
    public static final BitSet FOLLOW_34 = new BitSet(new long[]{0x0300000000000000L});
    public static final BitSet FOLLOW_35 = new BitSet(new long[]{0x0000000080800000L});
    public static final BitSet FOLLOW_36 = new BitSet(new long[]{0x000000063E800000L});
    public static final BitSet FOLLOW_37 = new BitSet(new long[]{0x000000063C800000L});
    public static final BitSet FOLLOW_38 = new BitSet(new long[]{0x0000000638800000L});
    public static final BitSet FOLLOW_39 = new BitSet(new long[]{0x0000000630800000L});
    public static final BitSet FOLLOW_40 = new BitSet(new long[]{0x0000000620800000L});
    public static final BitSet FOLLOW_41 = new BitSet(new long[]{0x0000000600800000L});
    public static final BitSet FOLLOW_42 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000008L});
    public static final BitSet FOLLOW_43 = new BitSet(new long[]{0x0000000400800000L});
    public static final BitSet FOLLOW_44 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000100L});
    public static final BitSet FOLLOW_45 = new BitSet(new long[]{0x000000103E800000L});
    public static final BitSet FOLLOW_46 = new BitSet(new long[]{0x000000103C800000L});
    public static final BitSet FOLLOW_47 = new BitSet(new long[]{0x0000001038800000L});
    public static final BitSet FOLLOW_48 = new BitSet(new long[]{0x0000001030800000L});
    public static final BitSet FOLLOW_49 = new BitSet(new long[]{0x0000001020800000L});
    public static final BitSet FOLLOW_50 = new BitSet(new long[]{0x0000001000800000L});
    public static final BitSet FOLLOW_51 = new BitSet(new long[]{0x0000000000000000L,0x0000000000100000L});
    public static final BitSet FOLLOW_52 = new BitSet(new long[]{0x000000403E800000L});
    public static final BitSet FOLLOW_53 = new BitSet(new long[]{0x000000403C800000L});
    public static final BitSet FOLLOW_54 = new BitSet(new long[]{0x0000004038800000L});
    public static final BitSet FOLLOW_55 = new BitSet(new long[]{0x0000004030800000L});
    public static final BitSet FOLLOW_56 = new BitSet(new long[]{0x0000004020800000L});
    public static final BitSet FOLLOW_57 = new BitSet(new long[]{0x0000004000800000L});
    public static final BitSet FOLLOW_58 = new BitSet(new long[]{0x0000000000000000L,0x0000000004000000L});
    public static final BitSet FOLLOW_59 = new BitSet(new long[]{0x000000003E820000L});
    public static final BitSet FOLLOW_60 = new BitSet(new long[]{0x000000003C820000L});
    public static final BitSet FOLLOW_61 = new BitSet(new long[]{0x0000000038820000L});
    public static final BitSet FOLLOW_62 = new BitSet(new long[]{0x0000000030820000L});
    public static final BitSet FOLLOW_63 = new BitSet(new long[]{0x0000000020820000L});
    public static final BitSet FOLLOW_64 = new BitSet(new long[]{0x0000000000820000L});
    public static final BitSet FOLLOW_65 = new BitSet(new long[]{0x0000000000000000L,0x0000180080000000L});
    public static final BitSet FOLLOW_66 = new BitSet(new long[]{0x000002003E840000L});
    public static final BitSet FOLLOW_67 = new BitSet(new long[]{0x000002003C840000L});
    public static final BitSet FOLLOW_68 = new BitSet(new long[]{0x0000020038840000L});
    public static final BitSet FOLLOW_69 = new BitSet(new long[]{0x0000020030840000L});
    public static final BitSet FOLLOW_70 = new BitSet(new long[]{0x0000000000000020L});
    public static final BitSet FOLLOW_71 = new BitSet(new long[]{0x0000000030840000L});
    public static final BitSet FOLLOW_72 = new BitSet(new long[]{0x0000000020840000L});
    public static final BitSet FOLLOW_73 = new BitSet(new long[]{0x0000000000840000L});
    public static final BitSet FOLLOW_74 = new BitSet(new long[]{0x0000000000000000L,0x0001000000000000L});
    public static final BitSet FOLLOW_75 = new BitSet(new long[]{0x000008003E800000L});
    public static final BitSet FOLLOW_76 = new BitSet(new long[]{0x000008003C800000L});
    public static final BitSet FOLLOW_77 = new BitSet(new long[]{0x0000080038800000L});
    public static final BitSet FOLLOW_78 = new BitSet(new long[]{0x0000080030800000L});
    public static final BitSet FOLLOW_79 = new BitSet(new long[]{0x0000080020800000L});
    public static final BitSet FOLLOW_80 = new BitSet(new long[]{0x0000080000800000L});
    public static final BitSet FOLLOW_81 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000200L});
    public static final BitSet FOLLOW_82 = new BitSet(new long[]{0x000020003E800000L});
    public static final BitSet FOLLOW_83 = new BitSet(new long[]{0x000020003C800000L});
    public static final BitSet FOLLOW_84 = new BitSet(new long[]{0x0000200038800000L});
    public static final BitSet FOLLOW_85 = new BitSet(new long[]{0x0000200030800000L});
    public static final BitSet FOLLOW_86 = new BitSet(new long[]{0x0000200020800000L});
    public static final BitSet FOLLOW_87 = new BitSet(new long[]{0x0000200000800000L});
    public static final BitSet FOLLOW_88 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000400L});
    public static final BitSet FOLLOW_89 = new BitSet(new long[]{0x0007800000800000L});
    public static final BitSet FOLLOW_90 = new BitSet(new long[]{0x0007000000800000L});
    public static final BitSet FOLLOW_91 = new BitSet(new long[]{0x0000000000000000L,0x0000400000000000L});
    public static final BitSet FOLLOW_92 = new BitSet(new long[]{0x0006000000800000L});
    public static final BitSet FOLLOW_93 = new BitSet(new long[]{0x0004000000800000L});
    public static final BitSet FOLLOW_94 = new BitSet(new long[]{0x001000003E800000L});
    public static final BitSet FOLLOW_95 = new BitSet(new long[]{0x001000003C800000L});
    public static final BitSet FOLLOW_96 = new BitSet(new long[]{0x0010000038800000L});
    public static final BitSet FOLLOW_97 = new BitSet(new long[]{0x0010000030800000L});
    public static final BitSet FOLLOW_98 = new BitSet(new long[]{0x0010000020800000L});
    public static final BitSet FOLLOW_99 = new BitSet(new long[]{0x0010000000800000L});
    public static final BitSet FOLLOW_100 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x00000000FAE04000L});
    public static final BitSet FOLLOW_101 = new BitSet(new long[]{0x0040000000800000L});
    public static final BitSet FOLLOW_102 = new BitSet(new long[]{0x1000000000000000L});
    public static final BitSet FOLLOW_103 = new BitSet(new long[]{0x0200000000000000L});
    public static final BitSet FOLLOW_104 = new BitSet(new long[]{0x0C00000000800000L});
    public static final BitSet FOLLOW_105 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_106 = new BitSet(new long[]{0x0800000000800000L});
    public static final BitSet FOLLOW_107 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000004L});
    public static final BitSet FOLLOW_108 = new BitSet(new long[]{0xE000000020800000L});
    public static final BitSet FOLLOW_109 = new BitSet(new long[]{0xC000000020800000L});
    public static final BitSet FOLLOW_110 = new BitSet(new long[]{0x8000000020800000L});
    public static final BitSet FOLLOW_111 = new BitSet(new long[]{0x0000000020800000L});
    public static final BitSet FOLLOW_112 = new BitSet(new long[]{0x0000000000800000L,0x0000000000000002L});
    public static final BitSet FOLLOW_113 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000070L});
    public static final BitSet FOLLOW_114 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000060L});
    public static final BitSet FOLLOW_115 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000040L});
    public static final BitSet FOLLOW_116 = new BitSet(new long[]{0x0000000000800000L,0x0000000000000080L});
    public static final BitSet FOLLOW_117 = new BitSet(new long[]{0x0000000000000000L,0x0000000000048408L});
    public static final BitSet FOLLOW_118 = new BitSet(new long[]{0x0000000000800000L,0x0000000000000200L});
    public static final BitSet FOLLOW_119 = new BitSet(new long[]{0x0000000000000000L,0x0000000000004000L});
    public static final BitSet FOLLOW_120 = new BitSet(new long[]{0x0000000000800000L,0x0000000000003800L});
    public static final BitSet FOLLOW_121 = new BitSet(new long[]{0x0000000000800000L,0x0000000000003000L});
    public static final BitSet FOLLOW_122 = new BitSet(new long[]{0x0000000000800000L,0x0000000000002000L});
    public static final BitSet FOLLOW_123 = new BitSet(new long[]{0x0000000000800000L,0x0000000000030000L});
    public static final BitSet FOLLOW_124 = new BitSet(new long[]{0x0000000000800000L,0x0000000000020000L});
    public static final BitSet FOLLOW_125 = new BitSet(new long[]{0x0000000000800000L,0x0000000000080000L});
    public static final BitSet FOLLOW_126 = new BitSet(new long[]{0x0000000000800000L,0x0000000000200000L});
    public static final BitSet FOLLOW_127 = new BitSet(new long[]{0x0000000000000000L,0x0000000001C00000L});
    public static final BitSet FOLLOW_128 = new BitSet(new long[]{0x0000000000000000L,0x0000000001800000L});
    public static final BitSet FOLLOW_129 = new BitSet(new long[]{0x0000000000000000L,0x0000000001000000L});
    public static final BitSet FOLLOW_130 = new BitSet(new long[]{0x0000000000800000L,0x0000000002000000L});
    public static final BitSet FOLLOW_131 = new BitSet(new long[]{0x0000000000800000L,0x0000000008000000L});
    public static final BitSet FOLLOW_132 = new BitSet(new long[]{0x0000000000000000L,0x0000000010000000L});
    public static final BitSet FOLLOW_133 = new BitSet(new long[]{0x0000000000800000L,0x0000000020000000L});
    public static final BitSet FOLLOW_134 = new BitSet(new long[]{0x0000000000000000L,0x0000000040000000L});
    public static final BitSet FOLLOW_135 = new BitSet(new long[]{0x0000000000800000L,0x0000000F00000000L});
    public static final BitSet FOLLOW_136 = new BitSet(new long[]{0x0000000000800000L,0x0000000E00000000L});
    public static final BitSet FOLLOW_137 = new BitSet(new long[]{0x0000000000000000L,0x0000001000000000L});
    public static final BitSet FOLLOW_138 = new BitSet(new long[]{0x0000000000800000L,0x0000000C00000000L});
    public static final BitSet FOLLOW_139 = new BitSet(new long[]{0x0000000000000000L,0x0000020000000000L});
    public static final BitSet FOLLOW_140 = new BitSet(new long[]{0x0000000000800000L,0x0000000800000000L});
    public static final BitSet FOLLOW_141 = new BitSet(new long[]{0x0000000000000000L,0x0000040000000000L});
    public static final BitSet FOLLOW_142 = new BitSet(new long[]{0x0000000000800000L,0x0000012000080000L});
    public static final BitSet FOLLOW_143 = new BitSet(new long[]{0x0000000000800000L,0x0000012000000000L});
    public static final BitSet FOLLOW_144 = new BitSet(new long[]{0x0000000000000000L,0x0000004000000000L});
    public static final BitSet FOLLOW_145 = new BitSet(new long[]{0x0000000080000000L,0x0000008000000000L});
    public static final BitSet FOLLOW_146 = new BitSet(new long[]{0x0000000000800000L,0x0000010000000000L});
    public static final BitSet FOLLOW_147 = new BitSet(new long[]{0x0000000000800000L,0x0000800000000000L});
    public static final BitSet FOLLOW_148 = new BitSet(new long[]{0x0000000000800000L,0x000E000000000000L});
    public static final BitSet FOLLOW_149 = new BitSet(new long[]{0x0000000000800000L,0x000C000000000000L});
    public static final BitSet FOLLOW_150 = new BitSet(new long[]{0x0000000000000000L,0x0150000000000000L});
    public static final BitSet FOLLOW_151 = new BitSet(new long[]{0x0000000000800000L,0x0008000000000000L});
    public static final BitSet FOLLOW_152 = new BitSet(new long[]{0x0000000000000000L,0x0020000000000000L,0x0000000000000028L});
    public static final BitSet FOLLOW_153 = new BitSet(new long[]{0x0000000000800000L,0x0080000000000000L});
    public static final BitSet FOLLOW_154 = new BitSet(new long[]{0x0000000000000000L,0x0400000000000000L});
    public static final BitSet FOLLOW_155 = new BitSet(new long[]{0x0000000000800000L,0x0200000000000000L});
    public static final BitSet FOLLOW_156 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000002L});
    public static final BitSet FOLLOW_157 = new BitSet(new long[]{0x0000000000800000L,0x1800000000000000L});
    public static final BitSet FOLLOW_158 = new BitSet(new long[]{0x0000000000800000L,0x1000000000000000L});
    public static final BitSet FOLLOW_159 = new BitSet(new long[]{0x0000000000000000L,0x2000000000000000L});
    public static final BitSet FOLLOW_160 = new BitSet(new long[]{0x0000000000000000L,0x4000000000000000L,0x0000000000000001L});
    public static final BitSet FOLLOW_161 = new BitSet(new long[]{0x0000000000800000L,0x8000000000200000L});
    public static final BitSet FOLLOW_162 = new BitSet(new long[]{0x0000000000800000L,0x8000000000000000L});
    public static final BitSet FOLLOW_163 = new BitSet(new long[]{0x0000000000800000L,0x0000000000000000L,0x0000000000000004L});
    public static final BitSet FOLLOW_164 = new BitSet(new long[]{0x0000000000800000L,0x0000000000000000L,0x0000000000000010L});
    public static final BitSet FOLLOW_165 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000040L});
    public static final BitSet FOLLOW_166 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000000100L});
    public static final BitSet FOLLOW_167 = new BitSet(new long[]{0x0000000000800000L,0x8000000000000000L,0x0000000000000080L});
    public static final BitSet FOLLOW_168 = new BitSet(new long[]{0x0000000000800000L,0x0000000000000000L,0x0000000000000080L});
    public static final BitSet FOLLOW_169 = new BitSet(new long[]{0x0007000000800000L,0x0000000000000000L,0x0000000000003800L});
    public static final BitSet FOLLOW_170 = new BitSet(new long[]{0x0000000000000000L,0x0000200000000000L});
    public static final BitSet FOLLOW_171 = new BitSet(new long[]{0x0007000000800000L,0x0000000000000000L,0x0000000000003000L});
    public static final BitSet FOLLOW_172 = new BitSet(new long[]{0x0007000000800000L,0x0000000000000000L,0x0000000000002000L});
    public static final BitSet FOLLOW_173 = new BitSet(new long[]{0x0000000000800000L,0x0000000000000000L,0x0000000000038000L});
    public static final BitSet FOLLOW_174 = new BitSet(new long[]{0x0000000000800000L,0x0000000000000000L,0x0000000000030000L});
    public static final BitSet FOLLOW_175 = new BitSet(new long[]{0x0000000000800000L,0x0000000000000000L,0x0000000000020000L});
    public static final BitSet FOLLOW_176 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000080000L});
    public static final BitSet FOLLOW_177 = new BitSet(new long[]{0x0000000000800000L,0x0000000000000000L,0x0000000000100000L});
    public static final BitSet FOLLOW_178 = new BitSet(new long[]{0x0000000000000000L,0x0000000000000000L,0x0000000000040000L});
    public static final BitSet FOLLOW_179 = new BitSet(new long[]{0x0000000000800000L,0x0000000000000000L,0x0000000001038000L});
    public static final BitSet FOLLOW_180 = new BitSet(new long[]{0x0000000000800000L,0x0000000000000000L,0x0000000001030000L});
    public static final BitSet FOLLOW_181 = new BitSet(new long[]{0x0000000000800000L,0x0000000000000000L,0x0000000001020000L});
    public static final BitSet FOLLOW_182 = new BitSet(new long[]{0x0000000000800000L,0x0000000000000000L,0x0000000001000000L});
    public static final BitSet FOLLOW_183 = new BitSet(new long[]{0x0000000000800000L,0x0000000000000000L,0x0000000005038000L});
    public static final BitSet FOLLOW_184 = new BitSet(new long[]{0x0000000000800000L,0x0000000000000000L,0x0000000005030000L});
    public static final BitSet FOLLOW_185 = new BitSet(new long[]{0x0000000000800000L,0x0000000000000000L,0x0000000005020000L});
    public static final BitSet FOLLOW_186 = new BitSet(new long[]{0x0000000000800000L,0x0000000000000000L,0x0000000005000000L});
    public static final BitSet FOLLOW_187 = new BitSet(new long[]{0x0000000000800000L,0x0000000000000000L,0x0000000004000000L});
    public static final BitSet FOLLOW_188 = new BitSet(new long[]{0x0000000000000002L,0x0000000000000000L,0x0000000100000000L});

}