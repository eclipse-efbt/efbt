package org.eclipse.emf.eson.parser.antlr.internal; 

import org.eclipse.xtext.*;
import org.eclipse.xtext.parser.*;
import org.eclipse.xtext.parser.impl.*;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.xtext.parser.antlr.AbstractInternalAntlrParser;
import org.eclipse.xtext.parser.antlr.XtextTokenStream;
import org.eclipse.xtext.parser.antlr.XtextTokenStream.HiddenTokens;
import org.eclipse.xtext.parser.antlr.AntlrDatatypeRuleToken;
import org.eclipse.emf.eson.services.EFactoryGrammarAccess;



import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
@SuppressWarnings("all")
public class InternalEFactoryParser extends AbstractInternalAntlrParser {
    public static final String[] tokenNames = new String[] {
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "RULE_STRING", "RULE_ID", "RULE_LONG_ID", "RULE_LONG_UNSIGNED", "RULE_INT", "RULE_ML_COMMENT", "RULE_SL_COMMENT", "RULE_WS", "RULE_ANY_OTHER", "'use'", "'import'", "'@Name'", "'{'", "'='", "'}'", "':'", "'['", "']'", "'NULL'", "'.*'", "'.'", "'true'", "'false'", "'-'"
    };
    public static final int RULE_LONG_ID=6;
    public static final int RULE_STRING=4;
    public static final int RULE_SL_COMMENT=10;
    public static final int T__19=19;
    public static final int T__15=15;
    public static final int T__16=16;
    public static final int T__17=17;
    public static final int T__18=18;
    public static final int T__13=13;
    public static final int T__14=14;
    public static final int EOF=-1;
    public static final int RULE_ID=5;
    public static final int RULE_WS=11;
    public static final int RULE_LONG_UNSIGNED=7;
    public static final int RULE_ANY_OTHER=12;
    public static final int T__26=26;
    public static final int T__27=27;
    public static final int RULE_INT=8;
    public static final int T__22=22;
    public static final int RULE_ML_COMMENT=9;
    public static final int T__23=23;
    public static final int T__24=24;
    public static final int T__25=25;
    public static final int T__20=20;
    public static final int T__21=21;

    // delegates
    // delegators


        public InternalEFactoryParser(TokenStream input) {
            this(input, new RecognizerSharedState());
        }
        public InternalEFactoryParser(TokenStream input, RecognizerSharedState state) {
            super(input, state);
             
        }
        

    public String[] getTokenNames() { return InternalEFactoryParser.tokenNames; }
    public String getGrammarFileName() { return "../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g"; }



     	private EFactoryGrammarAccess grammarAccess;
     	
        public InternalEFactoryParser(TokenStream input, EFactoryGrammarAccess grammarAccess) {
            this(input);
            this.grammarAccess = grammarAccess;
            registerRules(grammarAccess.getGrammar());
        }
        
        @Override
        protected String getFirstRuleName() {
        	return "Factory";	
       	}
       	
       	@Override
       	protected EFactoryGrammarAccess getGrammarAccess() {
       		return grammarAccess;
       	}



    // $ANTLR start "entryRuleFactory"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:67:1: entryRuleFactory returns [EObject current=null] : iv_ruleFactory= ruleFactory EOF ;
    public final EObject entryRuleFactory() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleFactory = null;


        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:68:2: (iv_ruleFactory= ruleFactory EOF )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:69:2: iv_ruleFactory= ruleFactory EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getFactoryRule()); 
            }
            pushFollow(FOLLOW_ruleFactory_in_entryRuleFactory75);
            iv_ruleFactory=ruleFactory();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleFactory; 
            }
            match(input,EOF,FOLLOW_EOF_in_entryRuleFactory85); if (state.failed) return current;

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
    // $ANTLR end "entryRuleFactory"


    // $ANTLR start "ruleFactory"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:76:1: ruleFactory returns [EObject current=null] : ( ( (lv_imports_0_0= ruleNamespaceImport ) )* ( (lv_ePackages_1_0= rulePackageImport ) )* ( (lv_annotations_2_0= ruleAnnotation ) )* ( (lv_root_3_0= ruleNewObject ) ) ) ;
    public final EObject ruleFactory() throws RecognitionException {
        EObject current = null;

        EObject lv_imports_0_0 = null;

        EObject lv_ePackages_1_0 = null;

        EObject lv_annotations_2_0 = null;

        EObject lv_root_3_0 = null;


         enterRule(); 
            
        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:79:28: ( ( ( (lv_imports_0_0= ruleNamespaceImport ) )* ( (lv_ePackages_1_0= rulePackageImport ) )* ( (lv_annotations_2_0= ruleAnnotation ) )* ( (lv_root_3_0= ruleNewObject ) ) ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:80:1: ( ( (lv_imports_0_0= ruleNamespaceImport ) )* ( (lv_ePackages_1_0= rulePackageImport ) )* ( (lv_annotations_2_0= ruleAnnotation ) )* ( (lv_root_3_0= ruleNewObject ) ) )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:80:1: ( ( (lv_imports_0_0= ruleNamespaceImport ) )* ( (lv_ePackages_1_0= rulePackageImport ) )* ( (lv_annotations_2_0= ruleAnnotation ) )* ( (lv_root_3_0= ruleNewObject ) ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:80:2: ( (lv_imports_0_0= ruleNamespaceImport ) )* ( (lv_ePackages_1_0= rulePackageImport ) )* ( (lv_annotations_2_0= ruleAnnotation ) )* ( (lv_root_3_0= ruleNewObject ) )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:80:2: ( (lv_imports_0_0= ruleNamespaceImport ) )*
            loop1:
            do {
                int alt1=2;
                int LA1_0 = input.LA(1);

                if ( (LA1_0==14) ) {
                    alt1=1;
                }


                switch (alt1) {
            	case 1 :
            	    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:81:1: (lv_imports_0_0= ruleNamespaceImport )
            	    {
            	    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:81:1: (lv_imports_0_0= ruleNamespaceImport )
            	    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:82:3: lv_imports_0_0= ruleNamespaceImport
            	    {
            	    if ( state.backtracking==0 ) {
            	       
            	      	        newCompositeNode(grammarAccess.getFactoryAccess().getImportsNamespaceImportParserRuleCall_0_0()); 
            	      	    
            	    }
            	    pushFollow(FOLLOW_ruleNamespaceImport_in_ruleFactory131);
            	    lv_imports_0_0=ruleNamespaceImport();

            	    state._fsp--;
            	    if (state.failed) return current;
            	    if ( state.backtracking==0 ) {

            	      	        if (current==null) {
            	      	            current = createModelElementForParent(grammarAccess.getFactoryRule());
            	      	        }
            	             		add(
            	             			current, 
            	             			"imports",
            	              		lv_imports_0_0, 
            	              		"NamespaceImport");
            	      	        afterParserOrEnumRuleCall();
            	      	    
            	    }

            	    }


            	    }
            	    break;

            	default :
            	    break loop1;
                }
            } while (true);

            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:98:3: ( (lv_ePackages_1_0= rulePackageImport ) )*
            loop2:
            do {
                int alt2=2;
                int LA2_0 = input.LA(1);

                if ( (LA2_0==13) ) {
                    alt2=1;
                }


                switch (alt2) {
            	case 1 :
            	    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:99:1: (lv_ePackages_1_0= rulePackageImport )
            	    {
            	    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:99:1: (lv_ePackages_1_0= rulePackageImport )
            	    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:100:3: lv_ePackages_1_0= rulePackageImport
            	    {
            	    if ( state.backtracking==0 ) {
            	       
            	      	        newCompositeNode(grammarAccess.getFactoryAccess().getEPackagesPackageImportParserRuleCall_1_0()); 
            	      	    
            	    }
            	    pushFollow(FOLLOW_rulePackageImport_in_ruleFactory153);
            	    lv_ePackages_1_0=rulePackageImport();

            	    state._fsp--;
            	    if (state.failed) return current;
            	    if ( state.backtracking==0 ) {

            	      	        if (current==null) {
            	      	            current = createModelElementForParent(grammarAccess.getFactoryRule());
            	      	        }
            	             		add(
            	             			current, 
            	             			"ePackages",
            	              		lv_ePackages_1_0, 
            	              		"PackageImport");
            	      	        afterParserOrEnumRuleCall();
            	      	    
            	    }

            	    }


            	    }
            	    break;

            	default :
            	    break loop2;
                }
            } while (true);

            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:116:3: ( (lv_annotations_2_0= ruleAnnotation ) )*
            loop3:
            do {
                int alt3=2;
                int LA3_0 = input.LA(1);

                if ( (LA3_0==15) ) {
                    alt3=1;
                }


                switch (alt3) {
            	case 1 :
            	    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:117:1: (lv_annotations_2_0= ruleAnnotation )
            	    {
            	    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:117:1: (lv_annotations_2_0= ruleAnnotation )
            	    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:118:3: lv_annotations_2_0= ruleAnnotation
            	    {
            	    if ( state.backtracking==0 ) {
            	       
            	      	        newCompositeNode(grammarAccess.getFactoryAccess().getAnnotationsAnnotationParserRuleCall_2_0()); 
            	      	    
            	    }
            	    pushFollow(FOLLOW_ruleAnnotation_in_ruleFactory175);
            	    lv_annotations_2_0=ruleAnnotation();

            	    state._fsp--;
            	    if (state.failed) return current;
            	    if ( state.backtracking==0 ) {

            	      	        if (current==null) {
            	      	            current = createModelElementForParent(grammarAccess.getFactoryRule());
            	      	        }
            	             		add(
            	             			current, 
            	             			"annotations",
            	              		lv_annotations_2_0, 
            	              		"Annotation");
            	      	        afterParserOrEnumRuleCall();
            	      	    
            	    }

            	    }


            	    }
            	    break;

            	default :
            	    break loop3;
                }
            } while (true);

            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:134:3: ( (lv_root_3_0= ruleNewObject ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:135:1: (lv_root_3_0= ruleNewObject )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:135:1: (lv_root_3_0= ruleNewObject )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:136:3: lv_root_3_0= ruleNewObject
            {
            if ( state.backtracking==0 ) {
               
              	        newCompositeNode(grammarAccess.getFactoryAccess().getRootNewObjectParserRuleCall_3_0()); 
              	    
            }
            pushFollow(FOLLOW_ruleNewObject_in_ruleFactory197);
            lv_root_3_0=ruleNewObject();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {

              	        if (current==null) {
              	            current = createModelElementForParent(grammarAccess.getFactoryRule());
              	        }
                     		set(
                     			current, 
                     			"root",
                      		lv_root_3_0, 
                      		"NewObject");
              	        afterParserOrEnumRuleCall();
              	    
            }

            }


            }


            }


            }

            if ( state.backtracking==0 ) {
               leaveRule(); 
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
    // $ANTLR end "ruleFactory"


    // $ANTLR start "entryRulePackageImport"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:160:1: entryRulePackageImport returns [EObject current=null] : iv_rulePackageImport= rulePackageImport EOF ;
    public final EObject entryRulePackageImport() throws RecognitionException {
        EObject current = null;

        EObject iv_rulePackageImport = null;


        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:161:2: (iv_rulePackageImport= rulePackageImport EOF )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:162:2: iv_rulePackageImport= rulePackageImport EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getPackageImportRule()); 
            }
            pushFollow(FOLLOW_rulePackageImport_in_entryRulePackageImport233);
            iv_rulePackageImport=rulePackageImport();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_rulePackageImport; 
            }
            match(input,EOF,FOLLOW_EOF_in_entryRulePackageImport243); if (state.failed) return current;

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
    // $ANTLR end "entryRulePackageImport"


    // $ANTLR start "rulePackageImport"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:169:1: rulePackageImport returns [EObject current=null] : (otherlv_0= 'use' ( ( ruleStringOrQualifiedNameWithWildcard ) ) ) ;
    public final EObject rulePackageImport() throws RecognitionException {
        EObject current = null;

        Token otherlv_0=null;

         enterRule(); 
            
        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:172:28: ( (otherlv_0= 'use' ( ( ruleStringOrQualifiedNameWithWildcard ) ) ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:173:1: (otherlv_0= 'use' ( ( ruleStringOrQualifiedNameWithWildcard ) ) )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:173:1: (otherlv_0= 'use' ( ( ruleStringOrQualifiedNameWithWildcard ) ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:173:3: otherlv_0= 'use' ( ( ruleStringOrQualifiedNameWithWildcard ) )
            {
            otherlv_0=(Token)match(input,13,FOLLOW_13_in_rulePackageImport280); if (state.failed) return current;
            if ( state.backtracking==0 ) {

                  	newLeafNode(otherlv_0, grammarAccess.getPackageImportAccess().getUseKeyword_0());
                  
            }
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:177:1: ( ( ruleStringOrQualifiedNameWithWildcard ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:178:1: ( ruleStringOrQualifiedNameWithWildcard )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:178:1: ( ruleStringOrQualifiedNameWithWildcard )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:179:3: ruleStringOrQualifiedNameWithWildcard
            {
            if ( state.backtracking==0 ) {

              			if (current==null) {
              	            current = createModelElement(grammarAccess.getPackageImportRule());
              	        }
                      
            }
            if ( state.backtracking==0 ) {
               
              	        newCompositeNode(grammarAccess.getPackageImportAccess().getEPackageEPackageCrossReference_1_0()); 
              	    
            }
            pushFollow(FOLLOW_ruleStringOrQualifiedNameWithWildcard_in_rulePackageImport303);
            ruleStringOrQualifiedNameWithWildcard();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               
              	        afterParserOrEnumRuleCall();
              	    
            }

            }


            }


            }


            }

            if ( state.backtracking==0 ) {
               leaveRule(); 
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
    // $ANTLR end "rulePackageImport"


    // $ANTLR start "entryRuleNamespaceImport"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:200:1: entryRuleNamespaceImport returns [EObject current=null] : iv_ruleNamespaceImport= ruleNamespaceImport EOF ;
    public final EObject entryRuleNamespaceImport() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleNamespaceImport = null;


        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:201:2: (iv_ruleNamespaceImport= ruleNamespaceImport EOF )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:202:2: iv_ruleNamespaceImport= ruleNamespaceImport EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getNamespaceImportRule()); 
            }
            pushFollow(FOLLOW_ruleNamespaceImport_in_entryRuleNamespaceImport339);
            iv_ruleNamespaceImport=ruleNamespaceImport();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleNamespaceImport; 
            }
            match(input,EOF,FOLLOW_EOF_in_entryRuleNamespaceImport349); if (state.failed) return current;

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
    // $ANTLR end "entryRuleNamespaceImport"


    // $ANTLR start "ruleNamespaceImport"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:209:1: ruleNamespaceImport returns [EObject current=null] : (otherlv_0= 'import' ( (lv_importedNamespace_1_0= ruleQualifiedNameWithWildcard ) ) ) ;
    public final EObject ruleNamespaceImport() throws RecognitionException {
        EObject current = null;

        Token otherlv_0=null;
        AntlrDatatypeRuleToken lv_importedNamespace_1_0 = null;


         enterRule(); 
            
        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:212:28: ( (otherlv_0= 'import' ( (lv_importedNamespace_1_0= ruleQualifiedNameWithWildcard ) ) ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:213:1: (otherlv_0= 'import' ( (lv_importedNamespace_1_0= ruleQualifiedNameWithWildcard ) ) )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:213:1: (otherlv_0= 'import' ( (lv_importedNamespace_1_0= ruleQualifiedNameWithWildcard ) ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:213:3: otherlv_0= 'import' ( (lv_importedNamespace_1_0= ruleQualifiedNameWithWildcard ) )
            {
            otherlv_0=(Token)match(input,14,FOLLOW_14_in_ruleNamespaceImport386); if (state.failed) return current;
            if ( state.backtracking==0 ) {

                  	newLeafNode(otherlv_0, grammarAccess.getNamespaceImportAccess().getImportKeyword_0());
                  
            }
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:217:1: ( (lv_importedNamespace_1_0= ruleQualifiedNameWithWildcard ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:218:1: (lv_importedNamespace_1_0= ruleQualifiedNameWithWildcard )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:218:1: (lv_importedNamespace_1_0= ruleQualifiedNameWithWildcard )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:219:3: lv_importedNamespace_1_0= ruleQualifiedNameWithWildcard
            {
            if ( state.backtracking==0 ) {
               
              	        newCompositeNode(grammarAccess.getNamespaceImportAccess().getImportedNamespaceQualifiedNameWithWildcardParserRuleCall_1_0()); 
              	    
            }
            pushFollow(FOLLOW_ruleQualifiedNameWithWildcard_in_ruleNamespaceImport407);
            lv_importedNamespace_1_0=ruleQualifiedNameWithWildcard();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {

              	        if (current==null) {
              	            current = createModelElementForParent(grammarAccess.getNamespaceImportRule());
              	        }
                     		set(
                     			current, 
                     			"importedNamespace",
                      		lv_importedNamespace_1_0, 
                      		"QualifiedNameWithWildcard");
              	        afterParserOrEnumRuleCall();
              	    
            }

            }


            }


            }


            }

            if ( state.backtracking==0 ) {
               leaveRule(); 
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
    // $ANTLR end "ruleNamespaceImport"


    // $ANTLR start "entryRuleAnnotation"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:243:1: entryRuleAnnotation returns [EObject current=null] : iv_ruleAnnotation= ruleAnnotation EOF ;
    public final EObject entryRuleAnnotation() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleAnnotation = null;


        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:244:2: (iv_ruleAnnotation= ruleAnnotation EOF )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:245:2: iv_ruleAnnotation= ruleAnnotation EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getAnnotationRule()); 
            }
            pushFollow(FOLLOW_ruleAnnotation_in_entryRuleAnnotation443);
            iv_ruleAnnotation=ruleAnnotation();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleAnnotation; 
            }
            match(input,EOF,FOLLOW_EOF_in_entryRuleAnnotation453); if (state.failed) return current;

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
    // $ANTLR end "entryRuleAnnotation"


    // $ANTLR start "ruleAnnotation"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:252:1: ruleAnnotation returns [EObject current=null] : this_CustomNameMapping_0= ruleCustomNameMapping ;
    public final EObject ruleAnnotation() throws RecognitionException {
        EObject current = null;

        EObject this_CustomNameMapping_0 = null;


         enterRule(); 
            
        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:255:28: (this_CustomNameMapping_0= ruleCustomNameMapping )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:257:5: this_CustomNameMapping_0= ruleCustomNameMapping
            {
            if ( state.backtracking==0 ) {
               
                      newCompositeNode(grammarAccess.getAnnotationAccess().getCustomNameMappingParserRuleCall()); 
                  
            }
            pushFollow(FOLLOW_ruleCustomNameMapping_in_ruleAnnotation499);
            this_CustomNameMapping_0=ruleCustomNameMapping();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               
                      current = this_CustomNameMapping_0; 
                      afterParserOrEnumRuleCall();
                  
            }

            }

            if ( state.backtracking==0 ) {
               leaveRule(); 
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
    // $ANTLR end "ruleAnnotation"


    // $ANTLR start "entryRuleCustomNameMapping"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:273:1: entryRuleCustomNameMapping returns [EObject current=null] : iv_ruleCustomNameMapping= ruleCustomNameMapping EOF ;
    public final EObject entryRuleCustomNameMapping() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleCustomNameMapping = null;


        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:274:2: (iv_ruleCustomNameMapping= ruleCustomNameMapping EOF )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:275:2: iv_ruleCustomNameMapping= ruleCustomNameMapping EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getCustomNameMappingRule()); 
            }
            pushFollow(FOLLOW_ruleCustomNameMapping_in_entryRuleCustomNameMapping533);
            iv_ruleCustomNameMapping=ruleCustomNameMapping();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleCustomNameMapping; 
            }
            match(input,EOF,FOLLOW_EOF_in_entryRuleCustomNameMapping543); if (state.failed) return current;

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
    // $ANTLR end "entryRuleCustomNameMapping"


    // $ANTLR start "ruleCustomNameMapping"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:282:1: ruleCustomNameMapping returns [EObject current=null] : (otherlv_0= '@Name' otherlv_1= '{' ( ( ruleQualifiedName ) ) otherlv_3= '=' ( ( ruleQualifiedName ) ) otherlv_5= '}' ) ;
    public final EObject ruleCustomNameMapping() throws RecognitionException {
        EObject current = null;

        Token otherlv_0=null;
        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_5=null;

         enterRule(); 
            
        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:285:28: ( (otherlv_0= '@Name' otherlv_1= '{' ( ( ruleQualifiedName ) ) otherlv_3= '=' ( ( ruleQualifiedName ) ) otherlv_5= '}' ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:286:1: (otherlv_0= '@Name' otherlv_1= '{' ( ( ruleQualifiedName ) ) otherlv_3= '=' ( ( ruleQualifiedName ) ) otherlv_5= '}' )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:286:1: (otherlv_0= '@Name' otherlv_1= '{' ( ( ruleQualifiedName ) ) otherlv_3= '=' ( ( ruleQualifiedName ) ) otherlv_5= '}' )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:286:3: otherlv_0= '@Name' otherlv_1= '{' ( ( ruleQualifiedName ) ) otherlv_3= '=' ( ( ruleQualifiedName ) ) otherlv_5= '}'
            {
            otherlv_0=(Token)match(input,15,FOLLOW_15_in_ruleCustomNameMapping580); if (state.failed) return current;
            if ( state.backtracking==0 ) {

                  	newLeafNode(otherlv_0, grammarAccess.getCustomNameMappingAccess().getNameKeyword_0());
                  
            }
            otherlv_1=(Token)match(input,16,FOLLOW_16_in_ruleCustomNameMapping592); if (state.failed) return current;
            if ( state.backtracking==0 ) {

                  	newLeafNode(otherlv_1, grammarAccess.getCustomNameMappingAccess().getLeftCurlyBracketKeyword_1());
                  
            }
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:294:1: ( ( ruleQualifiedName ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:295:1: ( ruleQualifiedName )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:295:1: ( ruleQualifiedName )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:296:3: ruleQualifiedName
            {
            if ( state.backtracking==0 ) {

              			if (current==null) {
              	            current = createModelElement(grammarAccess.getCustomNameMappingRule());
              	        }
                      
            }
            if ( state.backtracking==0 ) {
               
              	        newCompositeNode(grammarAccess.getCustomNameMappingAccess().getEClassEClassCrossReference_2_0()); 
              	    
            }
            pushFollow(FOLLOW_ruleQualifiedName_in_ruleCustomNameMapping615);
            ruleQualifiedName();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               
              	        afterParserOrEnumRuleCall();
              	    
            }

            }


            }

            otherlv_3=(Token)match(input,17,FOLLOW_17_in_ruleCustomNameMapping627); if (state.failed) return current;
            if ( state.backtracking==0 ) {

                  	newLeafNode(otherlv_3, grammarAccess.getCustomNameMappingAccess().getEqualsSignKeyword_3());
                  
            }
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:313:1: ( ( ruleQualifiedName ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:314:1: ( ruleQualifiedName )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:314:1: ( ruleQualifiedName )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:315:3: ruleQualifiedName
            {
            if ( state.backtracking==0 ) {

              			if (current==null) {
              	            current = createModelElement(grammarAccess.getCustomNameMappingRule());
              	        }
                      
            }
            if ( state.backtracking==0 ) {
               
              	        newCompositeNode(grammarAccess.getCustomNameMappingAccess().getNameFeatureEAttributeCrossReference_4_0()); 
              	    
            }
            pushFollow(FOLLOW_ruleQualifiedName_in_ruleCustomNameMapping650);
            ruleQualifiedName();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               
              	        afterParserOrEnumRuleCall();
              	    
            }

            }


            }

            otherlv_5=(Token)match(input,18,FOLLOW_18_in_ruleCustomNameMapping662); if (state.failed) return current;
            if ( state.backtracking==0 ) {

                  	newLeafNode(otherlv_5, grammarAccess.getCustomNameMappingAccess().getRightCurlyBracketKeyword_5());
                  
            }

            }


            }

            if ( state.backtracking==0 ) {
               leaveRule(); 
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
    // $ANTLR end "ruleCustomNameMapping"


    // $ANTLR start "entryRuleFeature"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:340:1: entryRuleFeature returns [EObject current=null] : iv_ruleFeature= ruleFeature EOF ;
    public final EObject entryRuleFeature() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleFeature = null;


        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:341:2: (iv_ruleFeature= ruleFeature EOF )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:342:2: iv_ruleFeature= ruleFeature EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getFeatureRule()); 
            }
            pushFollow(FOLLOW_ruleFeature_in_entryRuleFeature698);
            iv_ruleFeature=ruleFeature();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleFeature; 
            }
            match(input,EOF,FOLLOW_EOF_in_entryRuleFeature708); if (state.failed) return current;

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
    // $ANTLR end "entryRuleFeature"


    // $ANTLR start "ruleFeature"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:349:1: ruleFeature returns [EObject current=null] : ( ( ( ruleQualifiedName ) ) otherlv_1= ':' ( (lv_value_2_0= ruleValue ) )? ) ;
    public final EObject ruleFeature() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        EObject lv_value_2_0 = null;


         enterRule(); 
            
        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:352:28: ( ( ( ( ruleQualifiedName ) ) otherlv_1= ':' ( (lv_value_2_0= ruleValue ) )? ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:353:1: ( ( ( ruleQualifiedName ) ) otherlv_1= ':' ( (lv_value_2_0= ruleValue ) )? )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:353:1: ( ( ( ruleQualifiedName ) ) otherlv_1= ':' ( (lv_value_2_0= ruleValue ) )? )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:353:2: ( ( ruleQualifiedName ) ) otherlv_1= ':' ( (lv_value_2_0= ruleValue ) )?
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:353:2: ( ( ruleQualifiedName ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:354:1: ( ruleQualifiedName )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:354:1: ( ruleQualifiedName )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:355:3: ruleQualifiedName
            {
            if ( state.backtracking==0 ) {

              			if (current==null) {
              	            current = createModelElement(grammarAccess.getFeatureRule());
              	        }
                      
            }
            if ( state.backtracking==0 ) {
               
              	        newCompositeNode(grammarAccess.getFeatureAccess().getEFeatureEStructuralFeatureCrossReference_0_0()); 
              	    
            }
            pushFollow(FOLLOW_ruleQualifiedName_in_ruleFeature756);
            ruleQualifiedName();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               
              	        afterParserOrEnumRuleCall();
              	    
            }

            }


            }

            otherlv_1=(Token)match(input,19,FOLLOW_19_in_ruleFeature768); if (state.failed) return current;
            if ( state.backtracking==0 ) {

                  	newLeafNode(otherlv_1, grammarAccess.getFeatureAccess().getColonKeyword_1());
                  
            }
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:372:1: ( (lv_value_2_0= ruleValue ) )?
            int alt4=2;
            alt4 = dfa4.predict(input);
            switch (alt4) {
                case 1 :
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:373:1: (lv_value_2_0= ruleValue )
                    {
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:373:1: (lv_value_2_0= ruleValue )
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:374:3: lv_value_2_0= ruleValue
                    {
                    if ( state.backtracking==0 ) {
                       
                      	        newCompositeNode(grammarAccess.getFeatureAccess().getValueValueParserRuleCall_2_0()); 
                      	    
                    }
                    pushFollow(FOLLOW_ruleValue_in_ruleFeature789);
                    lv_value_2_0=ruleValue();

                    state._fsp--;
                    if (state.failed) return current;
                    if ( state.backtracking==0 ) {

                      	        if (current==null) {
                      	            current = createModelElementForParent(grammarAccess.getFeatureRule());
                      	        }
                             		set(
                             			current, 
                             			"value",
                              		lv_value_2_0, 
                              		"Value");
                      	        afterParserOrEnumRuleCall();
                      	    
                    }

                    }


                    }
                    break;

            }


            }


            }

            if ( state.backtracking==0 ) {
               leaveRule(); 
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
    // $ANTLR end "ruleFeature"


    // $ANTLR start "entryRuleNewObject"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:398:1: entryRuleNewObject returns [EObject current=null] : iv_ruleNewObject= ruleNewObject EOF ;
    public final EObject entryRuleNewObject() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleNewObject = null;


        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:399:2: (iv_ruleNewObject= ruleNewObject EOF )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:400:2: iv_ruleNewObject= ruleNewObject EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getNewObjectRule()); 
            }
            pushFollow(FOLLOW_ruleNewObject_in_entryRuleNewObject826);
            iv_ruleNewObject=ruleNewObject();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleNewObject; 
            }
            match(input,EOF,FOLLOW_EOF_in_entryRuleNewObject836); if (state.failed) return current;

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
    // $ANTLR end "entryRuleNewObject"


    // $ANTLR start "ruleNewObject"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:407:1: ruleNewObject returns [EObject current=null] : ( ( ( ruleQualifiedName ) ) ( (lv_name_1_0= ruleValidID ) )? otherlv_2= '{' ( (lv_features_3_0= ruleFeature ) )* otherlv_4= '}' ) ;
    public final EObject ruleNewObject() throws RecognitionException {
        EObject current = null;

        Token otherlv_2=null;
        Token otherlv_4=null;
        AntlrDatatypeRuleToken lv_name_1_0 = null;

        EObject lv_features_3_0 = null;


         enterRule(); 
            
        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:410:28: ( ( ( ( ruleQualifiedName ) ) ( (lv_name_1_0= ruleValidID ) )? otherlv_2= '{' ( (lv_features_3_0= ruleFeature ) )* otherlv_4= '}' ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:411:1: ( ( ( ruleQualifiedName ) ) ( (lv_name_1_0= ruleValidID ) )? otherlv_2= '{' ( (lv_features_3_0= ruleFeature ) )* otherlv_4= '}' )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:411:1: ( ( ( ruleQualifiedName ) ) ( (lv_name_1_0= ruleValidID ) )? otherlv_2= '{' ( (lv_features_3_0= ruleFeature ) )* otherlv_4= '}' )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:411:2: ( ( ruleQualifiedName ) ) ( (lv_name_1_0= ruleValidID ) )? otherlv_2= '{' ( (lv_features_3_0= ruleFeature ) )* otherlv_4= '}'
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:411:2: ( ( ruleQualifiedName ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:412:1: ( ruleQualifiedName )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:412:1: ( ruleQualifiedName )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:413:3: ruleQualifiedName
            {
            if ( state.backtracking==0 ) {

              			if (current==null) {
              	            current = createModelElement(grammarAccess.getNewObjectRule());
              	        }
                      
            }
            if ( state.backtracking==0 ) {
               
              	        newCompositeNode(grammarAccess.getNewObjectAccess().getEClassEClassCrossReference_0_0()); 
              	    
            }
            pushFollow(FOLLOW_ruleQualifiedName_in_ruleNewObject884);
            ruleQualifiedName();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               
              	        afterParserOrEnumRuleCall();
              	    
            }

            }


            }

            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:426:2: ( (lv_name_1_0= ruleValidID ) )?
            int alt5=2;
            int LA5_0 = input.LA(1);

            if ( ((LA5_0>=RULE_ID && LA5_0<=RULE_LONG_UNSIGNED)) ) {
                alt5=1;
            }
            switch (alt5) {
                case 1 :
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:427:1: (lv_name_1_0= ruleValidID )
                    {
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:427:1: (lv_name_1_0= ruleValidID )
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:428:3: lv_name_1_0= ruleValidID
                    {
                    if ( state.backtracking==0 ) {
                       
                      	        newCompositeNode(grammarAccess.getNewObjectAccess().getNameValidIDParserRuleCall_1_0()); 
                      	    
                    }
                    pushFollow(FOLLOW_ruleValidID_in_ruleNewObject905);
                    lv_name_1_0=ruleValidID();

                    state._fsp--;
                    if (state.failed) return current;
                    if ( state.backtracking==0 ) {

                      	        if (current==null) {
                      	            current = createModelElementForParent(grammarAccess.getNewObjectRule());
                      	        }
                             		set(
                             			current, 
                             			"name",
                              		lv_name_1_0, 
                              		"ValidID");
                      	        afterParserOrEnumRuleCall();
                      	    
                    }

                    }


                    }
                    break;

            }

            otherlv_2=(Token)match(input,16,FOLLOW_16_in_ruleNewObject918); if (state.failed) return current;
            if ( state.backtracking==0 ) {

                  	newLeafNode(otherlv_2, grammarAccess.getNewObjectAccess().getLeftCurlyBracketKeyword_2());
                  
            }
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:448:1: ( (lv_features_3_0= ruleFeature ) )*
            loop6:
            do {
                int alt6=2;
                int LA6_0 = input.LA(1);

                if ( ((LA6_0>=RULE_ID && LA6_0<=RULE_LONG_UNSIGNED)) ) {
                    alt6=1;
                }


                switch (alt6) {
            	case 1 :
            	    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:449:1: (lv_features_3_0= ruleFeature )
            	    {
            	    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:449:1: (lv_features_3_0= ruleFeature )
            	    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:450:3: lv_features_3_0= ruleFeature
            	    {
            	    if ( state.backtracking==0 ) {
            	       
            	      	        newCompositeNode(grammarAccess.getNewObjectAccess().getFeaturesFeatureParserRuleCall_3_0()); 
            	      	    
            	    }
            	    pushFollow(FOLLOW_ruleFeature_in_ruleNewObject939);
            	    lv_features_3_0=ruleFeature();

            	    state._fsp--;
            	    if (state.failed) return current;
            	    if ( state.backtracking==0 ) {

            	      	        if (current==null) {
            	      	            current = createModelElementForParent(grammarAccess.getNewObjectRule());
            	      	        }
            	             		add(
            	             			current, 
            	             			"features",
            	              		lv_features_3_0, 
            	              		"Feature");
            	      	        afterParserOrEnumRuleCall();
            	      	    
            	    }

            	    }


            	    }
            	    break;

            	default :
            	    break loop6;
                }
            } while (true);

            otherlv_4=(Token)match(input,18,FOLLOW_18_in_ruleNewObject952); if (state.failed) return current;
            if ( state.backtracking==0 ) {

                  	newLeafNode(otherlv_4, grammarAccess.getNewObjectAccess().getRightCurlyBracketKeyword_4());
                  
            }

            }


            }

            if ( state.backtracking==0 ) {
               leaveRule(); 
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
    // $ANTLR end "ruleNewObject"


    // $ANTLR start "entryRuleValue"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:478:1: entryRuleValue returns [EObject current=null] : iv_ruleValue= ruleValue EOF ;
    public final EObject entryRuleValue() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleValue = null;


        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:479:2: (iv_ruleValue= ruleValue EOF )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:480:2: iv_ruleValue= ruleValue EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getValueRule()); 
            }
            pushFollow(FOLLOW_ruleValue_in_entryRuleValue988);
            iv_ruleValue=ruleValue();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleValue; 
            }
            match(input,EOF,FOLLOW_EOF_in_entryRuleValue998); if (state.failed) return current;

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
    // $ANTLR end "entryRuleValue"


    // $ANTLR start "ruleValue"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:487:1: ruleValue returns [EObject current=null] : (this_MultiValue_0= ruleMultiValue | this_Attribute_1= ruleAttribute | ( ( ( ( () ( ( ruleQualifiedName ) )? ( ( ruleValidID ) )? '{' ) )=> ( () ( ( ruleQualifiedName ) )? ( (lv_name_4_0= ruleValidID ) )? otherlv_5= '{' ) ) ( (lv_features_6_0= ruleFeature ) )* otherlv_7= '}' () ) | ( ( ruleReference )=>this_Reference_9= ruleReference ) ) ;
    public final EObject ruleValue() throws RecognitionException {
        EObject current = null;

        Token otherlv_5=null;
        Token otherlv_7=null;
        EObject this_MultiValue_0 = null;

        EObject this_Attribute_1 = null;

        AntlrDatatypeRuleToken lv_name_4_0 = null;

        EObject lv_features_6_0 = null;

        EObject this_Reference_9 = null;


         enterRule(); 
            
        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:490:28: ( (this_MultiValue_0= ruleMultiValue | this_Attribute_1= ruleAttribute | ( ( ( ( () ( ( ruleQualifiedName ) )? ( ( ruleValidID ) )? '{' ) )=> ( () ( ( ruleQualifiedName ) )? ( (lv_name_4_0= ruleValidID ) )? otherlv_5= '{' ) ) ( (lv_features_6_0= ruleFeature ) )* otherlv_7= '}' () ) | ( ( ruleReference )=>this_Reference_9= ruleReference ) ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:491:1: (this_MultiValue_0= ruleMultiValue | this_Attribute_1= ruleAttribute | ( ( ( ( () ( ( ruleQualifiedName ) )? ( ( ruleValidID ) )? '{' ) )=> ( () ( ( ruleQualifiedName ) )? ( (lv_name_4_0= ruleValidID ) )? otherlv_5= '{' ) ) ( (lv_features_6_0= ruleFeature ) )* otherlv_7= '}' () ) | ( ( ruleReference )=>this_Reference_9= ruleReference ) )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:491:1: (this_MultiValue_0= ruleMultiValue | this_Attribute_1= ruleAttribute | ( ( ( ( () ( ( ruleQualifiedName ) )? ( ( ruleValidID ) )? '{' ) )=> ( () ( ( ruleQualifiedName ) )? ( (lv_name_4_0= ruleValidID ) )? otherlv_5= '{' ) ) ( (lv_features_6_0= ruleFeature ) )* otherlv_7= '}' () ) | ( ( ruleReference )=>this_Reference_9= ruleReference ) )
            int alt10=4;
            int LA10_0 = input.LA(1);

            if ( (LA10_0==20) ) {
                alt10=1;
            }
            else if ( (LA10_0==RULE_STRING||LA10_0==19||LA10_0==22||(LA10_0>=25 && LA10_0<=27)) ) {
                alt10=2;
            }
            else if ( (LA10_0==RULE_LONG_UNSIGNED) ) {
                int LA10_3 = input.LA(2);

                if ( (true) ) {
                    alt10=2;
                }
                else if ( (synpred1_InternalEFactory()) ) {
                    alt10=3;
                }
                else if ( (synpred2_InternalEFactory()) ) {
                    alt10=4;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return current;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 10, 3, input);

                    throw nvae;
                }
            }
            else if ( (LA10_0==RULE_ID) ) {
                int LA10_4 = input.LA(2);

                if ( (synpred1_InternalEFactory()) ) {
                    alt10=3;
                }
                else if ( (synpred2_InternalEFactory()) ) {
                    alt10=4;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return current;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 10, 4, input);

                    throw nvae;
                }
            }
            else if ( (LA10_0==RULE_LONG_ID) ) {
                int LA10_5 = input.LA(2);

                if ( (synpred1_InternalEFactory()) ) {
                    alt10=3;
                }
                else if ( (synpred2_InternalEFactory()) ) {
                    alt10=4;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return current;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 10, 5, input);

                    throw nvae;
                }
            }
            else if ( (LA10_0==16) && (synpred1_InternalEFactory())) {
                alt10=3;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return current;}
                NoViableAltException nvae =
                    new NoViableAltException("", 10, 0, input);

                throw nvae;
            }
            switch (alt10) {
                case 1 :
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:492:5: this_MultiValue_0= ruleMultiValue
                    {
                    if ( state.backtracking==0 ) {
                       
                              newCompositeNode(grammarAccess.getValueAccess().getMultiValueParserRuleCall_0()); 
                          
                    }
                    pushFollow(FOLLOW_ruleMultiValue_in_ruleValue1045);
                    this_MultiValue_0=ruleMultiValue();

                    state._fsp--;
                    if (state.failed) return current;
                    if ( state.backtracking==0 ) {
                       
                              current = this_MultiValue_0; 
                              afterParserOrEnumRuleCall();
                          
                    }

                    }
                    break;
                case 2 :
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:502:5: this_Attribute_1= ruleAttribute
                    {
                    if ( state.backtracking==0 ) {
                       
                              newCompositeNode(grammarAccess.getValueAccess().getAttributeParserRuleCall_1()); 
                          
                    }
                    pushFollow(FOLLOW_ruleAttribute_in_ruleValue1072);
                    this_Attribute_1=ruleAttribute();

                    state._fsp--;
                    if (state.failed) return current;
                    if ( state.backtracking==0 ) {
                       
                              current = this_Attribute_1; 
                              afterParserOrEnumRuleCall();
                          
                    }

                    }
                    break;
                case 3 :
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:511:6: ( ( ( ( () ( ( ruleQualifiedName ) )? ( ( ruleValidID ) )? '{' ) )=> ( () ( ( ruleQualifiedName ) )? ( (lv_name_4_0= ruleValidID ) )? otherlv_5= '{' ) ) ( (lv_features_6_0= ruleFeature ) )* otherlv_7= '}' () )
                    {
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:511:6: ( ( ( ( () ( ( ruleQualifiedName ) )? ( ( ruleValidID ) )? '{' ) )=> ( () ( ( ruleQualifiedName ) )? ( (lv_name_4_0= ruleValidID ) )? otherlv_5= '{' ) ) ( (lv_features_6_0= ruleFeature ) )* otherlv_7= '}' () )
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:511:7: ( ( ( () ( ( ruleQualifiedName ) )? ( ( ruleValidID ) )? '{' ) )=> ( () ( ( ruleQualifiedName ) )? ( (lv_name_4_0= ruleValidID ) )? otherlv_5= '{' ) ) ( (lv_features_6_0= ruleFeature ) )* otherlv_7= '}' ()
                    {
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:511:7: ( ( ( () ( ( ruleQualifiedName ) )? ( ( ruleValidID ) )? '{' ) )=> ( () ( ( ruleQualifiedName ) )? ( (lv_name_4_0= ruleValidID ) )? otherlv_5= '{' ) )
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:511:8: ( ( () ( ( ruleQualifiedName ) )? ( ( ruleValidID ) )? '{' ) )=> ( () ( ( ruleQualifiedName ) )? ( (lv_name_4_0= ruleValidID ) )? otherlv_5= '{' )
                    {
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:521:5: ( () ( ( ruleQualifiedName ) )? ( (lv_name_4_0= ruleValidID ) )? otherlv_5= '{' )
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:521:6: () ( ( ruleQualifiedName ) )? ( (lv_name_4_0= ruleValidID ) )? otherlv_5= '{'
                    {
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:521:6: ()
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:522:5: 
                    {
                    if ( state.backtracking==0 ) {

                              current = forceCreateModelElement(
                                  grammarAccess.getValueAccess().getNewObjectAction_2_0_0_0(),
                                  current);
                          
                    }

                    }

                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:527:2: ( ( ruleQualifiedName ) )?
                    int alt7=2;
                    switch ( input.LA(1) ) {
                        case RULE_ID:
                            {
                            alt7=1;
                            }
                            break;
                        case RULE_LONG_ID:
                            {
                            alt7=1;
                            }
                            break;
                        case RULE_LONG_UNSIGNED:
                            {
                            alt7=1;
                            }
                            break;
                    }

                    switch (alt7) {
                        case 1 :
                            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:528:1: ( ruleQualifiedName )
                            {
                            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:528:1: ( ruleQualifiedName )
                            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:529:3: ruleQualifiedName
                            {
                            if ( state.backtracking==0 ) {

                              			if (current==null) {
                              	            current = createModelElement(grammarAccess.getValueRule());
                              	        }
                                      
                            }
                            if ( state.backtracking==0 ) {
                               
                              	        newCompositeNode(grammarAccess.getValueAccess().getEClassEClassCrossReference_2_0_0_1_0()); 
                              	    
                            }
                            pushFollow(FOLLOW_ruleQualifiedName_in_ruleValue1146);
                            ruleQualifiedName();

                            state._fsp--;
                            if (state.failed) return current;
                            if ( state.backtracking==0 ) {
                               
                              	        afterParserOrEnumRuleCall();
                              	    
                            }

                            }


                            }
                            break;

                    }

                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:542:3: ( (lv_name_4_0= ruleValidID ) )?
                    int alt8=2;
                    int LA8_0 = input.LA(1);

                    if ( ((LA8_0>=RULE_ID && LA8_0<=RULE_LONG_UNSIGNED)) ) {
                        alt8=1;
                    }
                    switch (alt8) {
                        case 1 :
                            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:543:1: (lv_name_4_0= ruleValidID )
                            {
                            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:543:1: (lv_name_4_0= ruleValidID )
                            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:544:3: lv_name_4_0= ruleValidID
                            {
                            if ( state.backtracking==0 ) {
                               
                              	        newCompositeNode(grammarAccess.getValueAccess().getNameValidIDParserRuleCall_2_0_0_2_0()); 
                              	    
                            }
                            pushFollow(FOLLOW_ruleValidID_in_ruleValue1168);
                            lv_name_4_0=ruleValidID();

                            state._fsp--;
                            if (state.failed) return current;
                            if ( state.backtracking==0 ) {

                              	        if (current==null) {
                              	            current = createModelElementForParent(grammarAccess.getValueRule());
                              	        }
                                     		set(
                                     			current, 
                                     			"name",
                                      		lv_name_4_0, 
                                      		"ValidID");
                              	        afterParserOrEnumRuleCall();
                              	    
                            }

                            }


                            }
                            break;

                    }

                    otherlv_5=(Token)match(input,16,FOLLOW_16_in_ruleValue1181); if (state.failed) return current;
                    if ( state.backtracking==0 ) {

                          	newLeafNode(otherlv_5, grammarAccess.getValueAccess().getLeftCurlyBracketKeyword_2_0_0_3());
                          
                    }

                    }


                    }

                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:564:3: ( (lv_features_6_0= ruleFeature ) )*
                    loop9:
                    do {
                        int alt9=2;
                        int LA9_0 = input.LA(1);

                        if ( ((LA9_0>=RULE_ID && LA9_0<=RULE_LONG_UNSIGNED)) ) {
                            alt9=1;
                        }


                        switch (alt9) {
                    	case 1 :
                    	    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:565:1: (lv_features_6_0= ruleFeature )
                    	    {
                    	    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:565:1: (lv_features_6_0= ruleFeature )
                    	    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:566:3: lv_features_6_0= ruleFeature
                    	    {
                    	    if ( state.backtracking==0 ) {
                    	       
                    	      	        newCompositeNode(grammarAccess.getValueAccess().getFeaturesFeatureParserRuleCall_2_1_0()); 
                    	      	    
                    	    }
                    	    pushFollow(FOLLOW_ruleFeature_in_ruleValue1204);
                    	    lv_features_6_0=ruleFeature();

                    	    state._fsp--;
                    	    if (state.failed) return current;
                    	    if ( state.backtracking==0 ) {

                    	      	        if (current==null) {
                    	      	            current = createModelElementForParent(grammarAccess.getValueRule());
                    	      	        }
                    	             		add(
                    	             			current, 
                    	             			"features",
                    	              		lv_features_6_0, 
                    	              		"Feature");
                    	      	        afterParserOrEnumRuleCall();
                    	      	    
                    	    }

                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop9;
                        }
                    } while (true);

                    otherlv_7=(Token)match(input,18,FOLLOW_18_in_ruleValue1217); if (state.failed) return current;
                    if ( state.backtracking==0 ) {

                          	newLeafNode(otherlv_7, grammarAccess.getValueAccess().getRightCurlyBracketKeyword_2_2());
                          
                    }
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:586:1: ()
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:587:5: 
                    {
                    if ( state.backtracking==0 ) {

                              current = forceCreateModelElementAndSet(
                                  grammarAccess.getValueAccess().getContainmentValueAction_2_3(),
                                  current);
                          
                    }

                    }


                    }


                    }
                    break;
                case 4 :
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:593:6: ( ( ruleReference )=>this_Reference_9= ruleReference )
                    {
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:593:6: ( ( ruleReference )=>this_Reference_9= ruleReference )
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:593:7: ( ruleReference )=>this_Reference_9= ruleReference
                    {
                    if ( state.backtracking==0 ) {
                       
                              newCompositeNode(grammarAccess.getValueAccess().getReferenceParserRuleCall_3()); 
                          
                    }
                    pushFollow(FOLLOW_ruleReference_in_ruleValue1261);
                    this_Reference_9=ruleReference();

                    state._fsp--;
                    if (state.failed) return current;
                    if ( state.backtracking==0 ) {
                       
                              current = this_Reference_9; 
                              afterParserOrEnumRuleCall();
                          
                    }

                    }


                    }
                    break;

            }


            }

            if ( state.backtracking==0 ) {
               leaveRule(); 
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
    // $ANTLR end "ruleValue"


    // $ANTLR start "entryRuleMultiValue"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:610:1: entryRuleMultiValue returns [EObject current=null] : iv_ruleMultiValue= ruleMultiValue EOF ;
    public final EObject entryRuleMultiValue() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleMultiValue = null;


        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:611:2: (iv_ruleMultiValue= ruleMultiValue EOF )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:612:2: iv_ruleMultiValue= ruleMultiValue EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getMultiValueRule()); 
            }
            pushFollow(FOLLOW_ruleMultiValue_in_entryRuleMultiValue1297);
            iv_ruleMultiValue=ruleMultiValue();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleMultiValue; 
            }
            match(input,EOF,FOLLOW_EOF_in_entryRuleMultiValue1307); if (state.failed) return current;

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
    // $ANTLR end "entryRuleMultiValue"


    // $ANTLR start "ruleMultiValue"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:619:1: ruleMultiValue returns [EObject current=null] : ( () otherlv_1= '[' ( (lv_values_2_0= ruleValue ) )* otherlv_3= ']' ) ;
    public final EObject ruleMultiValue() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        EObject lv_values_2_0 = null;


         enterRule(); 
            
        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:622:28: ( ( () otherlv_1= '[' ( (lv_values_2_0= ruleValue ) )* otherlv_3= ']' ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:623:1: ( () otherlv_1= '[' ( (lv_values_2_0= ruleValue ) )* otherlv_3= ']' )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:623:1: ( () otherlv_1= '[' ( (lv_values_2_0= ruleValue ) )* otherlv_3= ']' )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:623:2: () otherlv_1= '[' ( (lv_values_2_0= ruleValue ) )* otherlv_3= ']'
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:623:2: ()
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:624:5: 
            {
            if ( state.backtracking==0 ) {

                      current = forceCreateModelElement(
                          grammarAccess.getMultiValueAccess().getMultiValueAction_0(),
                          current);
                  
            }

            }

            otherlv_1=(Token)match(input,20,FOLLOW_20_in_ruleMultiValue1353); if (state.failed) return current;
            if ( state.backtracking==0 ) {

                  	newLeafNode(otherlv_1, grammarAccess.getMultiValueAccess().getLeftSquareBracketKeyword_1());
                  
            }
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:633:1: ( (lv_values_2_0= ruleValue ) )*
            loop11:
            do {
                int alt11=2;
                int LA11_0 = input.LA(1);

                if ( ((LA11_0>=RULE_STRING && LA11_0<=RULE_LONG_UNSIGNED)||LA11_0==16||(LA11_0>=19 && LA11_0<=20)||LA11_0==22||(LA11_0>=25 && LA11_0<=27)) ) {
                    alt11=1;
                }


                switch (alt11) {
            	case 1 :
            	    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:634:1: (lv_values_2_0= ruleValue )
            	    {
            	    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:634:1: (lv_values_2_0= ruleValue )
            	    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:635:3: lv_values_2_0= ruleValue
            	    {
            	    if ( state.backtracking==0 ) {
            	       
            	      	        newCompositeNode(grammarAccess.getMultiValueAccess().getValuesValueParserRuleCall_2_0()); 
            	      	    
            	    }
            	    pushFollow(FOLLOW_ruleValue_in_ruleMultiValue1374);
            	    lv_values_2_0=ruleValue();

            	    state._fsp--;
            	    if (state.failed) return current;
            	    if ( state.backtracking==0 ) {

            	      	        if (current==null) {
            	      	            current = createModelElementForParent(grammarAccess.getMultiValueRule());
            	      	        }
            	             		add(
            	             			current, 
            	             			"values",
            	              		lv_values_2_0, 
            	              		"Value");
            	      	        afterParserOrEnumRuleCall();
            	      	    
            	    }

            	    }


            	    }
            	    break;

            	default :
            	    break loop11;
                }
            } while (true);

            otherlv_3=(Token)match(input,21,FOLLOW_21_in_ruleMultiValue1387); if (state.failed) return current;
            if ( state.backtracking==0 ) {

                  	newLeafNode(otherlv_3, grammarAccess.getMultiValueAccess().getRightSquareBracketKeyword_3());
                  
            }

            }


            }

            if ( state.backtracking==0 ) {
               leaveRule(); 
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
    // $ANTLR end "ruleMultiValue"


    // $ANTLR start "entryRuleReference"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:663:1: entryRuleReference returns [EObject current=null] : iv_ruleReference= ruleReference EOF ;
    public final EObject entryRuleReference() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleReference = null;


        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:664:2: (iv_ruleReference= ruleReference EOF )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:665:2: iv_ruleReference= ruleReference EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getReferenceRule()); 
            }
            pushFollow(FOLLOW_ruleReference_in_entryRuleReference1423);
            iv_ruleReference=ruleReference();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleReference; 
            }
            match(input,EOF,FOLLOW_EOF_in_entryRuleReference1433); if (state.failed) return current;

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
    // $ANTLR end "entryRuleReference"


    // $ANTLR start "ruleReference"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:672:1: ruleReference returns [EObject current=null] : ( ( ruleQualifiedName ) ) ;
    public final EObject ruleReference() throws RecognitionException {
        EObject current = null;

         enterRule(); 
            
        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:675:28: ( ( ( ruleQualifiedName ) ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:676:1: ( ( ruleQualifiedName ) )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:676:1: ( ( ruleQualifiedName ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:677:1: ( ruleQualifiedName )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:677:1: ( ruleQualifiedName )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:678:3: ruleQualifiedName
            {
            if ( state.backtracking==0 ) {

              			if (current==null) {
              	            current = createModelElement(grammarAccess.getReferenceRule());
              	        }
                      
            }
            if ( state.backtracking==0 ) {
               
              	        newCompositeNode(grammarAccess.getReferenceAccess().getValueEObjectCrossReference_0()); 
              	    
            }
            pushFollow(FOLLOW_ruleQualifiedName_in_ruleReference1480);
            ruleQualifiedName();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               
              	        afterParserOrEnumRuleCall();
              	    
            }

            }


            }


            }

            if ( state.backtracking==0 ) {
               leaveRule(); 
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
    // $ANTLR end "ruleReference"


    // $ANTLR start "entryRuleAttribute"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:699:1: entryRuleAttribute returns [EObject current=null] : iv_ruleAttribute= ruleAttribute EOF ;
    public final EObject entryRuleAttribute() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleAttribute = null;


        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:700:2: (iv_ruleAttribute= ruleAttribute EOF )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:701:2: iv_ruleAttribute= ruleAttribute EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getAttributeRule()); 
            }
            pushFollow(FOLLOW_ruleAttribute_in_entryRuleAttribute1515);
            iv_ruleAttribute=ruleAttribute();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleAttribute; 
            }
            match(input,EOF,FOLLOW_EOF_in_entryRuleAttribute1525); if (state.failed) return current;

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
    // $ANTLR end "entryRuleAttribute"


    // $ANTLR start "ruleAttribute"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:708:1: ruleAttribute returns [EObject current=null] : (this_EnumAttribute_0= ruleEnumAttribute | this_StringAttribute_1= ruleStringAttribute | this_IntegerAttribute_2= ruleIntegerAttribute | this_BooleanAttribute_3= ruleBooleanAttribute | this_DoubleAttribute_4= ruleDoubleAttribute | this_DateAttribute_5= ruleDateAttribute | this_NullAttribute_6= ruleNullAttribute ) ;
    public final EObject ruleAttribute() throws RecognitionException {
        EObject current = null;

        EObject this_EnumAttribute_0 = null;

        EObject this_StringAttribute_1 = null;

        EObject this_IntegerAttribute_2 = null;

        EObject this_BooleanAttribute_3 = null;

        EObject this_DoubleAttribute_4 = null;

        EObject this_DateAttribute_5 = null;

        EObject this_NullAttribute_6 = null;


         enterRule(); 
            
        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:711:28: ( (this_EnumAttribute_0= ruleEnumAttribute | this_StringAttribute_1= ruleStringAttribute | this_IntegerAttribute_2= ruleIntegerAttribute | this_BooleanAttribute_3= ruleBooleanAttribute | this_DoubleAttribute_4= ruleDoubleAttribute | this_DateAttribute_5= ruleDateAttribute | this_NullAttribute_6= ruleNullAttribute ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:712:1: (this_EnumAttribute_0= ruleEnumAttribute | this_StringAttribute_1= ruleStringAttribute | this_IntegerAttribute_2= ruleIntegerAttribute | this_BooleanAttribute_3= ruleBooleanAttribute | this_DoubleAttribute_4= ruleDoubleAttribute | this_DateAttribute_5= ruleDateAttribute | this_NullAttribute_6= ruleNullAttribute )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:712:1: (this_EnumAttribute_0= ruleEnumAttribute | this_StringAttribute_1= ruleStringAttribute | this_IntegerAttribute_2= ruleIntegerAttribute | this_BooleanAttribute_3= ruleBooleanAttribute | this_DoubleAttribute_4= ruleDoubleAttribute | this_DateAttribute_5= ruleDateAttribute | this_NullAttribute_6= ruleNullAttribute )
            int alt12=7;
            alt12 = dfa12.predict(input);
            switch (alt12) {
                case 1 :
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:713:5: this_EnumAttribute_0= ruleEnumAttribute
                    {
                    if ( state.backtracking==0 ) {
                       
                              newCompositeNode(grammarAccess.getAttributeAccess().getEnumAttributeParserRuleCall_0()); 
                          
                    }
                    pushFollow(FOLLOW_ruleEnumAttribute_in_ruleAttribute1572);
                    this_EnumAttribute_0=ruleEnumAttribute();

                    state._fsp--;
                    if (state.failed) return current;
                    if ( state.backtracking==0 ) {
                       
                              current = this_EnumAttribute_0; 
                              afterParserOrEnumRuleCall();
                          
                    }

                    }
                    break;
                case 2 :
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:723:5: this_StringAttribute_1= ruleStringAttribute
                    {
                    if ( state.backtracking==0 ) {
                       
                              newCompositeNode(grammarAccess.getAttributeAccess().getStringAttributeParserRuleCall_1()); 
                          
                    }
                    pushFollow(FOLLOW_ruleStringAttribute_in_ruleAttribute1599);
                    this_StringAttribute_1=ruleStringAttribute();

                    state._fsp--;
                    if (state.failed) return current;
                    if ( state.backtracking==0 ) {
                       
                              current = this_StringAttribute_1; 
                              afterParserOrEnumRuleCall();
                          
                    }

                    }
                    break;
                case 3 :
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:733:5: this_IntegerAttribute_2= ruleIntegerAttribute
                    {
                    if ( state.backtracking==0 ) {
                       
                              newCompositeNode(grammarAccess.getAttributeAccess().getIntegerAttributeParserRuleCall_2()); 
                          
                    }
                    pushFollow(FOLLOW_ruleIntegerAttribute_in_ruleAttribute1626);
                    this_IntegerAttribute_2=ruleIntegerAttribute();

                    state._fsp--;
                    if (state.failed) return current;
                    if ( state.backtracking==0 ) {
                       
                              current = this_IntegerAttribute_2; 
                              afterParserOrEnumRuleCall();
                          
                    }

                    }
                    break;
                case 4 :
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:743:5: this_BooleanAttribute_3= ruleBooleanAttribute
                    {
                    if ( state.backtracking==0 ) {
                       
                              newCompositeNode(grammarAccess.getAttributeAccess().getBooleanAttributeParserRuleCall_3()); 
                          
                    }
                    pushFollow(FOLLOW_ruleBooleanAttribute_in_ruleAttribute1653);
                    this_BooleanAttribute_3=ruleBooleanAttribute();

                    state._fsp--;
                    if (state.failed) return current;
                    if ( state.backtracking==0 ) {
                       
                              current = this_BooleanAttribute_3; 
                              afterParserOrEnumRuleCall();
                          
                    }

                    }
                    break;
                case 5 :
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:753:5: this_DoubleAttribute_4= ruleDoubleAttribute
                    {
                    if ( state.backtracking==0 ) {
                       
                              newCompositeNode(grammarAccess.getAttributeAccess().getDoubleAttributeParserRuleCall_4()); 
                          
                    }
                    pushFollow(FOLLOW_ruleDoubleAttribute_in_ruleAttribute1680);
                    this_DoubleAttribute_4=ruleDoubleAttribute();

                    state._fsp--;
                    if (state.failed) return current;
                    if ( state.backtracking==0 ) {
                       
                              current = this_DoubleAttribute_4; 
                              afterParserOrEnumRuleCall();
                          
                    }

                    }
                    break;
                case 6 :
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:763:5: this_DateAttribute_5= ruleDateAttribute
                    {
                    if ( state.backtracking==0 ) {
                       
                              newCompositeNode(grammarAccess.getAttributeAccess().getDateAttributeParserRuleCall_5()); 
                          
                    }
                    pushFollow(FOLLOW_ruleDateAttribute_in_ruleAttribute1707);
                    this_DateAttribute_5=ruleDateAttribute();

                    state._fsp--;
                    if (state.failed) return current;
                    if ( state.backtracking==0 ) {
                       
                              current = this_DateAttribute_5; 
                              afterParserOrEnumRuleCall();
                          
                    }

                    }
                    break;
                case 7 :
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:773:5: this_NullAttribute_6= ruleNullAttribute
                    {
                    if ( state.backtracking==0 ) {
                       
                              newCompositeNode(grammarAccess.getAttributeAccess().getNullAttributeParserRuleCall_6()); 
                          
                    }
                    pushFollow(FOLLOW_ruleNullAttribute_in_ruleAttribute1734);
                    this_NullAttribute_6=ruleNullAttribute();

                    state._fsp--;
                    if (state.failed) return current;
                    if ( state.backtracking==0 ) {
                       
                              current = this_NullAttribute_6; 
                              afterParserOrEnumRuleCall();
                          
                    }

                    }
                    break;

            }


            }

            if ( state.backtracking==0 ) {
               leaveRule(); 
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
    // $ANTLR end "ruleAttribute"


    // $ANTLR start "entryRuleEnumAttribute"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:789:1: entryRuleEnumAttribute returns [EObject current=null] : iv_ruleEnumAttribute= ruleEnumAttribute EOF ;
    public final EObject entryRuleEnumAttribute() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleEnumAttribute = null;


        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:790:2: (iv_ruleEnumAttribute= ruleEnumAttribute EOF )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:791:2: iv_ruleEnumAttribute= ruleEnumAttribute EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getEnumAttributeRule()); 
            }
            pushFollow(FOLLOW_ruleEnumAttribute_in_entryRuleEnumAttribute1769);
            iv_ruleEnumAttribute=ruleEnumAttribute();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleEnumAttribute; 
            }
            match(input,EOF,FOLLOW_EOF_in_entryRuleEnumAttribute1779); if (state.failed) return current;

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
    // $ANTLR end "entryRuleEnumAttribute"


    // $ANTLR start "ruleEnumAttribute"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:798:1: ruleEnumAttribute returns [EObject current=null] : (otherlv_0= ':' ( ( ruleQualifiedName ) ) ) ;
    public final EObject ruleEnumAttribute() throws RecognitionException {
        EObject current = null;

        Token otherlv_0=null;

         enterRule(); 
            
        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:801:28: ( (otherlv_0= ':' ( ( ruleQualifiedName ) ) ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:802:1: (otherlv_0= ':' ( ( ruleQualifiedName ) ) )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:802:1: (otherlv_0= ':' ( ( ruleQualifiedName ) ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:802:3: otherlv_0= ':' ( ( ruleQualifiedName ) )
            {
            otherlv_0=(Token)match(input,19,FOLLOW_19_in_ruleEnumAttribute1816); if (state.failed) return current;
            if ( state.backtracking==0 ) {

                  	newLeafNode(otherlv_0, grammarAccess.getEnumAttributeAccess().getColonKeyword_0());
                  
            }
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:806:1: ( ( ruleQualifiedName ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:807:1: ( ruleQualifiedName )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:807:1: ( ruleQualifiedName )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:808:3: ruleQualifiedName
            {
            if ( state.backtracking==0 ) {

              			if (current==null) {
              	            current = createModelElement(grammarAccess.getEnumAttributeRule());
              	        }
                      
            }
            if ( state.backtracking==0 ) {
               
              	        newCompositeNode(grammarAccess.getEnumAttributeAccess().getValueEEnumLiteralCrossReference_1_0()); 
              	    
            }
            pushFollow(FOLLOW_ruleQualifiedName_in_ruleEnumAttribute1839);
            ruleQualifiedName();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               
              	        afterParserOrEnumRuleCall();
              	    
            }

            }


            }


            }


            }

            if ( state.backtracking==0 ) {
               leaveRule(); 
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
    // $ANTLR end "ruleEnumAttribute"


    // $ANTLR start "entryRuleStringAttribute"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:829:1: entryRuleStringAttribute returns [EObject current=null] : iv_ruleStringAttribute= ruleStringAttribute EOF ;
    public final EObject entryRuleStringAttribute() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleStringAttribute = null;


        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:830:2: (iv_ruleStringAttribute= ruleStringAttribute EOF )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:831:2: iv_ruleStringAttribute= ruleStringAttribute EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getStringAttributeRule()); 
            }
            pushFollow(FOLLOW_ruleStringAttribute_in_entryRuleStringAttribute1875);
            iv_ruleStringAttribute=ruleStringAttribute();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleStringAttribute; 
            }
            match(input,EOF,FOLLOW_EOF_in_entryRuleStringAttribute1885); if (state.failed) return current;

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
    // $ANTLR end "entryRuleStringAttribute"


    // $ANTLR start "ruleStringAttribute"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:838:1: ruleStringAttribute returns [EObject current=null] : ( (lv_value_0_0= RULE_STRING ) ) ;
    public final EObject ruleStringAttribute() throws RecognitionException {
        EObject current = null;

        Token lv_value_0_0=null;

         enterRule(); 
            
        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:841:28: ( ( (lv_value_0_0= RULE_STRING ) ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:842:1: ( (lv_value_0_0= RULE_STRING ) )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:842:1: ( (lv_value_0_0= RULE_STRING ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:843:1: (lv_value_0_0= RULE_STRING )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:843:1: (lv_value_0_0= RULE_STRING )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:844:3: lv_value_0_0= RULE_STRING
            {
            lv_value_0_0=(Token)match(input,RULE_STRING,FOLLOW_RULE_STRING_in_ruleStringAttribute1926); if (state.failed) return current;
            if ( state.backtracking==0 ) {

              			newLeafNode(lv_value_0_0, grammarAccess.getStringAttributeAccess().getValueSTRINGTerminalRuleCall_0()); 
              		
            }
            if ( state.backtracking==0 ) {

              	        if (current==null) {
              	            current = createModelElement(grammarAccess.getStringAttributeRule());
              	        }
                     		setWithLastConsumed(
                     			current, 
                     			"value",
                      		lv_value_0_0, 
                      		"STRING");
              	    
            }

            }


            }


            }

            if ( state.backtracking==0 ) {
               leaveRule(); 
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
    // $ANTLR end "ruleStringAttribute"


    // $ANTLR start "entryRuleIntegerAttribute"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:868:1: entryRuleIntegerAttribute returns [EObject current=null] : iv_ruleIntegerAttribute= ruleIntegerAttribute EOF ;
    public final EObject entryRuleIntegerAttribute() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleIntegerAttribute = null;


        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:869:2: (iv_ruleIntegerAttribute= ruleIntegerAttribute EOF )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:870:2: iv_ruleIntegerAttribute= ruleIntegerAttribute EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getIntegerAttributeRule()); 
            }
            pushFollow(FOLLOW_ruleIntegerAttribute_in_entryRuleIntegerAttribute1966);
            iv_ruleIntegerAttribute=ruleIntegerAttribute();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleIntegerAttribute; 
            }
            match(input,EOF,FOLLOW_EOF_in_entryRuleIntegerAttribute1976); if (state.failed) return current;

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
    // $ANTLR end "entryRuleIntegerAttribute"


    // $ANTLR start "ruleIntegerAttribute"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:877:1: ruleIntegerAttribute returns [EObject current=null] : ( (lv_value_0_0= ruleLong ) ) ;
    public final EObject ruleIntegerAttribute() throws RecognitionException {
        EObject current = null;

        AntlrDatatypeRuleToken lv_value_0_0 = null;


         enterRule(); 
            
        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:880:28: ( ( (lv_value_0_0= ruleLong ) ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:881:1: ( (lv_value_0_0= ruleLong ) )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:881:1: ( (lv_value_0_0= ruleLong ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:882:1: (lv_value_0_0= ruleLong )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:882:1: (lv_value_0_0= ruleLong )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:883:3: lv_value_0_0= ruleLong
            {
            if ( state.backtracking==0 ) {
               
              	        newCompositeNode(grammarAccess.getIntegerAttributeAccess().getValueLongParserRuleCall_0()); 
              	    
            }
            pushFollow(FOLLOW_ruleLong_in_ruleIntegerAttribute2021);
            lv_value_0_0=ruleLong();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {

              	        if (current==null) {
              	            current = createModelElementForParent(grammarAccess.getIntegerAttributeRule());
              	        }
                     		set(
                     			current, 
                     			"value",
                      		lv_value_0_0, 
                      		"Long");
              	        afterParserOrEnumRuleCall();
              	    
            }

            }


            }


            }

            if ( state.backtracking==0 ) {
               leaveRule(); 
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
    // $ANTLR end "ruleIntegerAttribute"


    // $ANTLR start "entryRuleDoubleAttribute"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:907:1: entryRuleDoubleAttribute returns [EObject current=null] : iv_ruleDoubleAttribute= ruleDoubleAttribute EOF ;
    public final EObject entryRuleDoubleAttribute() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleDoubleAttribute = null;


        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:908:2: (iv_ruleDoubleAttribute= ruleDoubleAttribute EOF )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:909:2: iv_ruleDoubleAttribute= ruleDoubleAttribute EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getDoubleAttributeRule()); 
            }
            pushFollow(FOLLOW_ruleDoubleAttribute_in_entryRuleDoubleAttribute2056);
            iv_ruleDoubleAttribute=ruleDoubleAttribute();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleDoubleAttribute; 
            }
            match(input,EOF,FOLLOW_EOF_in_entryRuleDoubleAttribute2066); if (state.failed) return current;

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
    // $ANTLR end "entryRuleDoubleAttribute"


    // $ANTLR start "ruleDoubleAttribute"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:916:1: ruleDoubleAttribute returns [EObject current=null] : ( (lv_value_0_0= ruleDouble ) ) ;
    public final EObject ruleDoubleAttribute() throws RecognitionException {
        EObject current = null;

        AntlrDatatypeRuleToken lv_value_0_0 = null;


         enterRule(); 
            
        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:919:28: ( ( (lv_value_0_0= ruleDouble ) ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:920:1: ( (lv_value_0_0= ruleDouble ) )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:920:1: ( (lv_value_0_0= ruleDouble ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:921:1: (lv_value_0_0= ruleDouble )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:921:1: (lv_value_0_0= ruleDouble )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:922:3: lv_value_0_0= ruleDouble
            {
            if ( state.backtracking==0 ) {
               
              	        newCompositeNode(grammarAccess.getDoubleAttributeAccess().getValueDoubleParserRuleCall_0()); 
              	    
            }
            pushFollow(FOLLOW_ruleDouble_in_ruleDoubleAttribute2111);
            lv_value_0_0=ruleDouble();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {

              	        if (current==null) {
              	            current = createModelElementForParent(grammarAccess.getDoubleAttributeRule());
              	        }
                     		set(
                     			current, 
                     			"value",
                      		lv_value_0_0, 
                      		"Double");
              	        afterParserOrEnumRuleCall();
              	    
            }

            }


            }


            }

            if ( state.backtracking==0 ) {
               leaveRule(); 
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
    // $ANTLR end "ruleDoubleAttribute"


    // $ANTLR start "entryRuleDateAttribute"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:946:1: entryRuleDateAttribute returns [EObject current=null] : iv_ruleDateAttribute= ruleDateAttribute EOF ;
    public final EObject entryRuleDateAttribute() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleDateAttribute = null;


        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:947:2: (iv_ruleDateAttribute= ruleDateAttribute EOF )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:948:2: iv_ruleDateAttribute= ruleDateAttribute EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getDateAttributeRule()); 
            }
            pushFollow(FOLLOW_ruleDateAttribute_in_entryRuleDateAttribute2146);
            iv_ruleDateAttribute=ruleDateAttribute();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleDateAttribute; 
            }
            match(input,EOF,FOLLOW_EOF_in_entryRuleDateAttribute2156); if (state.failed) return current;

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
    // $ANTLR end "entryRuleDateAttribute"


    // $ANTLR start "ruleDateAttribute"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:955:1: ruleDateAttribute returns [EObject current=null] : ( (lv_value_0_0= ruleDate ) ) ;
    public final EObject ruleDateAttribute() throws RecognitionException {
        EObject current = null;

        AntlrDatatypeRuleToken lv_value_0_0 = null;


         enterRule(); 
            
        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:958:28: ( ( (lv_value_0_0= ruleDate ) ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:959:1: ( (lv_value_0_0= ruleDate ) )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:959:1: ( (lv_value_0_0= ruleDate ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:960:1: (lv_value_0_0= ruleDate )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:960:1: (lv_value_0_0= ruleDate )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:961:3: lv_value_0_0= ruleDate
            {
            if ( state.backtracking==0 ) {
               
              	        newCompositeNode(grammarAccess.getDateAttributeAccess().getValueDateParserRuleCall_0()); 
              	    
            }
            pushFollow(FOLLOW_ruleDate_in_ruleDateAttribute2201);
            lv_value_0_0=ruleDate();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {

              	        if (current==null) {
              	            current = createModelElementForParent(grammarAccess.getDateAttributeRule());
              	        }
                     		set(
                     			current, 
                     			"value",
                      		lv_value_0_0, 
                      		"Date");
              	        afterParserOrEnumRuleCall();
              	    
            }

            }


            }


            }

            if ( state.backtracking==0 ) {
               leaveRule(); 
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
    // $ANTLR end "ruleDateAttribute"


    // $ANTLR start "entryRuleNullAttribute"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:985:1: entryRuleNullAttribute returns [EObject current=null] : iv_ruleNullAttribute= ruleNullAttribute EOF ;
    public final EObject entryRuleNullAttribute() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleNullAttribute = null;


        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:986:2: (iv_ruleNullAttribute= ruleNullAttribute EOF )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:987:2: iv_ruleNullAttribute= ruleNullAttribute EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getNullAttributeRule()); 
            }
            pushFollow(FOLLOW_ruleNullAttribute_in_entryRuleNullAttribute2236);
            iv_ruleNullAttribute=ruleNullAttribute();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleNullAttribute; 
            }
            match(input,EOF,FOLLOW_EOF_in_entryRuleNullAttribute2246); if (state.failed) return current;

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
    // $ANTLR end "entryRuleNullAttribute"


    // $ANTLR start "ruleNullAttribute"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:994:1: ruleNullAttribute returns [EObject current=null] : ( (lv_value_0_0= 'NULL' ) ) ;
    public final EObject ruleNullAttribute() throws RecognitionException {
        EObject current = null;

        Token lv_value_0_0=null;

         enterRule(); 
            
        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:997:28: ( ( (lv_value_0_0= 'NULL' ) ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:998:1: ( (lv_value_0_0= 'NULL' ) )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:998:1: ( (lv_value_0_0= 'NULL' ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:999:1: (lv_value_0_0= 'NULL' )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:999:1: (lv_value_0_0= 'NULL' )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1000:3: lv_value_0_0= 'NULL'
            {
            lv_value_0_0=(Token)match(input,22,FOLLOW_22_in_ruleNullAttribute2288); if (state.failed) return current;
            if ( state.backtracking==0 ) {

                      newLeafNode(lv_value_0_0, grammarAccess.getNullAttributeAccess().getValueNULLKeyword_0());
                  
            }
            if ( state.backtracking==0 ) {

              	        if (current==null) {
              	            current = createModelElement(grammarAccess.getNullAttributeRule());
              	        }
                     		setWithLastConsumed(current, "value", lv_value_0_0, "NULL");
              	    
            }

            }


            }


            }

            if ( state.backtracking==0 ) {
               leaveRule(); 
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
    // $ANTLR end "ruleNullAttribute"


    // $ANTLR start "entryRuleBooleanAttribute"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1021:1: entryRuleBooleanAttribute returns [EObject current=null] : iv_ruleBooleanAttribute= ruleBooleanAttribute EOF ;
    public final EObject entryRuleBooleanAttribute() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleBooleanAttribute = null;


        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1022:2: (iv_ruleBooleanAttribute= ruleBooleanAttribute EOF )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1023:2: iv_ruleBooleanAttribute= ruleBooleanAttribute EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getBooleanAttributeRule()); 
            }
            pushFollow(FOLLOW_ruleBooleanAttribute_in_entryRuleBooleanAttribute2336);
            iv_ruleBooleanAttribute=ruleBooleanAttribute();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleBooleanAttribute; 
            }
            match(input,EOF,FOLLOW_EOF_in_entryRuleBooleanAttribute2346); if (state.failed) return current;

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
    // $ANTLR end "entryRuleBooleanAttribute"


    // $ANTLR start "ruleBooleanAttribute"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1030:1: ruleBooleanAttribute returns [EObject current=null] : ( (lv_value_0_0= ruleBoolean ) ) ;
    public final EObject ruleBooleanAttribute() throws RecognitionException {
        EObject current = null;

        AntlrDatatypeRuleToken lv_value_0_0 = null;


         enterRule(); 
            
        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1033:28: ( ( (lv_value_0_0= ruleBoolean ) ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1034:1: ( (lv_value_0_0= ruleBoolean ) )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1034:1: ( (lv_value_0_0= ruleBoolean ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1035:1: (lv_value_0_0= ruleBoolean )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1035:1: (lv_value_0_0= ruleBoolean )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1036:3: lv_value_0_0= ruleBoolean
            {
            if ( state.backtracking==0 ) {
               
              	        newCompositeNode(grammarAccess.getBooleanAttributeAccess().getValueBooleanParserRuleCall_0()); 
              	    
            }
            pushFollow(FOLLOW_ruleBoolean_in_ruleBooleanAttribute2391);
            lv_value_0_0=ruleBoolean();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {

              	        if (current==null) {
              	            current = createModelElementForParent(grammarAccess.getBooleanAttributeRule());
              	        }
                     		set(
                     			current, 
                     			"value",
                      		lv_value_0_0, 
                      		"Boolean");
              	        afterParserOrEnumRuleCall();
              	    
            }

            }


            }


            }

            if ( state.backtracking==0 ) {
               leaveRule(); 
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
    // $ANTLR end "ruleBooleanAttribute"


    // $ANTLR start "entryRuleStringOrQualifiedNameWithWildcard"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1060:1: entryRuleStringOrQualifiedNameWithWildcard returns [String current=null] : iv_ruleStringOrQualifiedNameWithWildcard= ruleStringOrQualifiedNameWithWildcard EOF ;
    public final String entryRuleStringOrQualifiedNameWithWildcard() throws RecognitionException {
        String current = null;

        AntlrDatatypeRuleToken iv_ruleStringOrQualifiedNameWithWildcard = null;


        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1061:2: (iv_ruleStringOrQualifiedNameWithWildcard= ruleStringOrQualifiedNameWithWildcard EOF )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1062:2: iv_ruleStringOrQualifiedNameWithWildcard= ruleStringOrQualifiedNameWithWildcard EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getStringOrQualifiedNameWithWildcardRule()); 
            }
            pushFollow(FOLLOW_ruleStringOrQualifiedNameWithWildcard_in_entryRuleStringOrQualifiedNameWithWildcard2427);
            iv_ruleStringOrQualifiedNameWithWildcard=ruleStringOrQualifiedNameWithWildcard();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleStringOrQualifiedNameWithWildcard.getText(); 
            }
            match(input,EOF,FOLLOW_EOF_in_entryRuleStringOrQualifiedNameWithWildcard2438); if (state.failed) return current;

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
    // $ANTLR end "entryRuleStringOrQualifiedNameWithWildcard"


    // $ANTLR start "ruleStringOrQualifiedNameWithWildcard"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1069:1: ruleStringOrQualifiedNameWithWildcard returns [AntlrDatatypeRuleToken current=new AntlrDatatypeRuleToken()] : (this_STRING_0= RULE_STRING | this_QualifiedNameWithWildcard_1= ruleQualifiedNameWithWildcard ) ;
    public final AntlrDatatypeRuleToken ruleStringOrQualifiedNameWithWildcard() throws RecognitionException {
        AntlrDatatypeRuleToken current = new AntlrDatatypeRuleToken();

        Token this_STRING_0=null;
        AntlrDatatypeRuleToken this_QualifiedNameWithWildcard_1 = null;


         enterRule(); 
            
        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1072:28: ( (this_STRING_0= RULE_STRING | this_QualifiedNameWithWildcard_1= ruleQualifiedNameWithWildcard ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1073:1: (this_STRING_0= RULE_STRING | this_QualifiedNameWithWildcard_1= ruleQualifiedNameWithWildcard )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1073:1: (this_STRING_0= RULE_STRING | this_QualifiedNameWithWildcard_1= ruleQualifiedNameWithWildcard )
            int alt13=2;
            int LA13_0 = input.LA(1);

            if ( (LA13_0==RULE_STRING) ) {
                alt13=1;
            }
            else if ( ((LA13_0>=RULE_ID && LA13_0<=RULE_LONG_UNSIGNED)) ) {
                alt13=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return current;}
                NoViableAltException nvae =
                    new NoViableAltException("", 13, 0, input);

                throw nvae;
            }
            switch (alt13) {
                case 1 :
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1073:6: this_STRING_0= RULE_STRING
                    {
                    this_STRING_0=(Token)match(input,RULE_STRING,FOLLOW_RULE_STRING_in_ruleStringOrQualifiedNameWithWildcard2478); if (state.failed) return current;
                    if ( state.backtracking==0 ) {

                      		current.merge(this_STRING_0);
                          
                    }
                    if ( state.backtracking==0 ) {
                       
                          newLeafNode(this_STRING_0, grammarAccess.getStringOrQualifiedNameWithWildcardAccess().getSTRINGTerminalRuleCall_0()); 
                          
                    }

                    }
                    break;
                case 2 :
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1082:5: this_QualifiedNameWithWildcard_1= ruleQualifiedNameWithWildcard
                    {
                    if ( state.backtracking==0 ) {
                       
                              newCompositeNode(grammarAccess.getStringOrQualifiedNameWithWildcardAccess().getQualifiedNameWithWildcardParserRuleCall_1()); 
                          
                    }
                    pushFollow(FOLLOW_ruleQualifiedNameWithWildcard_in_ruleStringOrQualifiedNameWithWildcard2511);
                    this_QualifiedNameWithWildcard_1=ruleQualifiedNameWithWildcard();

                    state._fsp--;
                    if (state.failed) return current;
                    if ( state.backtracking==0 ) {

                      		current.merge(this_QualifiedNameWithWildcard_1);
                          
                    }
                    if ( state.backtracking==0 ) {
                       
                              afterParserOrEnumRuleCall();
                          
                    }

                    }
                    break;

            }


            }

            if ( state.backtracking==0 ) {
               leaveRule(); 
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
    // $ANTLR end "ruleStringOrQualifiedNameWithWildcard"


    // $ANTLR start "entryRuleQualifiedNameWithWildcard"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1100:1: entryRuleQualifiedNameWithWildcard returns [String current=null] : iv_ruleQualifiedNameWithWildcard= ruleQualifiedNameWithWildcard EOF ;
    public final String entryRuleQualifiedNameWithWildcard() throws RecognitionException {
        String current = null;

        AntlrDatatypeRuleToken iv_ruleQualifiedNameWithWildcard = null;


        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1101:2: (iv_ruleQualifiedNameWithWildcard= ruleQualifiedNameWithWildcard EOF )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1102:2: iv_ruleQualifiedNameWithWildcard= ruleQualifiedNameWithWildcard EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getQualifiedNameWithWildcardRule()); 
            }
            pushFollow(FOLLOW_ruleQualifiedNameWithWildcard_in_entryRuleQualifiedNameWithWildcard2557);
            iv_ruleQualifiedNameWithWildcard=ruleQualifiedNameWithWildcard();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleQualifiedNameWithWildcard.getText(); 
            }
            match(input,EOF,FOLLOW_EOF_in_entryRuleQualifiedNameWithWildcard2568); if (state.failed) return current;

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
    // $ANTLR end "entryRuleQualifiedNameWithWildcard"


    // $ANTLR start "ruleQualifiedNameWithWildcard"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1109:1: ruleQualifiedNameWithWildcard returns [AntlrDatatypeRuleToken current=new AntlrDatatypeRuleToken()] : (this_QualifiedName_0= ruleQualifiedName (kw= '.*' )? ) ;
    public final AntlrDatatypeRuleToken ruleQualifiedNameWithWildcard() throws RecognitionException {
        AntlrDatatypeRuleToken current = new AntlrDatatypeRuleToken();

        Token kw=null;
        AntlrDatatypeRuleToken this_QualifiedName_0 = null;


         enterRule(); 
            
        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1112:28: ( (this_QualifiedName_0= ruleQualifiedName (kw= '.*' )? ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1113:1: (this_QualifiedName_0= ruleQualifiedName (kw= '.*' )? )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1113:1: (this_QualifiedName_0= ruleQualifiedName (kw= '.*' )? )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1114:5: this_QualifiedName_0= ruleQualifiedName (kw= '.*' )?
            {
            if ( state.backtracking==0 ) {
               
                      newCompositeNode(grammarAccess.getQualifiedNameWithWildcardAccess().getQualifiedNameParserRuleCall_0()); 
                  
            }
            pushFollow(FOLLOW_ruleQualifiedName_in_ruleQualifiedNameWithWildcard2615);
            this_QualifiedName_0=ruleQualifiedName();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {

              		current.merge(this_QualifiedName_0);
                  
            }
            if ( state.backtracking==0 ) {
               
                      afterParserOrEnumRuleCall();
                  
            }
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1124:1: (kw= '.*' )?
            int alt14=2;
            int LA14_0 = input.LA(1);

            if ( (LA14_0==23) ) {
                alt14=1;
            }
            switch (alt14) {
                case 1 :
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1125:2: kw= '.*'
                    {
                    kw=(Token)match(input,23,FOLLOW_23_in_ruleQualifiedNameWithWildcard2634); if (state.failed) return current;
                    if ( state.backtracking==0 ) {

                              current.merge(kw);
                              newLeafNode(kw, grammarAccess.getQualifiedNameWithWildcardAccess().getFullStopAsteriskKeyword_1()); 
                          
                    }

                    }
                    break;

            }


            }


            }

            if ( state.backtracking==0 ) {
               leaveRule(); 
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
    // $ANTLR end "ruleQualifiedNameWithWildcard"


    // $ANTLR start "entryRuleQualifiedName"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1138:1: entryRuleQualifiedName returns [String current=null] : iv_ruleQualifiedName= ruleQualifiedName EOF ;
    public final String entryRuleQualifiedName() throws RecognitionException {
        String current = null;

        AntlrDatatypeRuleToken iv_ruleQualifiedName = null;


        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1139:2: (iv_ruleQualifiedName= ruleQualifiedName EOF )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1140:2: iv_ruleQualifiedName= ruleQualifiedName EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getQualifiedNameRule()); 
            }
            pushFollow(FOLLOW_ruleQualifiedName_in_entryRuleQualifiedName2677);
            iv_ruleQualifiedName=ruleQualifiedName();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleQualifiedName.getText(); 
            }
            match(input,EOF,FOLLOW_EOF_in_entryRuleQualifiedName2688); if (state.failed) return current;

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
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1147:1: ruleQualifiedName returns [AntlrDatatypeRuleToken current=new AntlrDatatypeRuleToken()] : (this_ValidID_0= ruleValidID ( ( ( '.' )=>kw= '.' ) this_ValidID_2= ruleValidID )* ) ;
    public final AntlrDatatypeRuleToken ruleQualifiedName() throws RecognitionException {
        AntlrDatatypeRuleToken current = new AntlrDatatypeRuleToken();

        Token kw=null;
        AntlrDatatypeRuleToken this_ValidID_0 = null;

        AntlrDatatypeRuleToken this_ValidID_2 = null;


         enterRule(); 
            
        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1150:28: ( (this_ValidID_0= ruleValidID ( ( ( '.' )=>kw= '.' ) this_ValidID_2= ruleValidID )* ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1151:1: (this_ValidID_0= ruleValidID ( ( ( '.' )=>kw= '.' ) this_ValidID_2= ruleValidID )* )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1151:1: (this_ValidID_0= ruleValidID ( ( ( '.' )=>kw= '.' ) this_ValidID_2= ruleValidID )* )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1152:5: this_ValidID_0= ruleValidID ( ( ( '.' )=>kw= '.' ) this_ValidID_2= ruleValidID )*
            {
            if ( state.backtracking==0 ) {
               
                      newCompositeNode(grammarAccess.getQualifiedNameAccess().getValidIDParserRuleCall_0()); 
                  
            }
            pushFollow(FOLLOW_ruleValidID_in_ruleQualifiedName2735);
            this_ValidID_0=ruleValidID();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {

              		current.merge(this_ValidID_0);
                  
            }
            if ( state.backtracking==0 ) {
               
                      afterParserOrEnumRuleCall();
                  
            }
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1162:1: ( ( ( '.' )=>kw= '.' ) this_ValidID_2= ruleValidID )*
            loop15:
            do {
                int alt15=2;
                int LA15_0 = input.LA(1);

                if ( (LA15_0==24) && (synpred3_InternalEFactory())) {
                    alt15=1;
                }


                switch (alt15) {
            	case 1 :
            	    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1162:2: ( ( '.' )=>kw= '.' ) this_ValidID_2= ruleValidID
            	    {
            	    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1162:2: ( ( '.' )=>kw= '.' )
            	    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1162:3: ( '.' )=>kw= '.'
            	    {
            	    kw=(Token)match(input,24,FOLLOW_24_in_ruleQualifiedName2763); if (state.failed) return current;
            	    if ( state.backtracking==0 ) {

            	              current.merge(kw);
            	              newLeafNode(kw, grammarAccess.getQualifiedNameAccess().getFullStopKeyword_1_0()); 
            	          
            	    }

            	    }

            	    if ( state.backtracking==0 ) {
            	       
            	              newCompositeNode(grammarAccess.getQualifiedNameAccess().getValidIDParserRuleCall_1_1()); 
            	          
            	    }
            	    pushFollow(FOLLOW_ruleValidID_in_ruleQualifiedName2786);
            	    this_ValidID_2=ruleValidID();

            	    state._fsp--;
            	    if (state.failed) return current;
            	    if ( state.backtracking==0 ) {

            	      		current.merge(this_ValidID_2);
            	          
            	    }
            	    if ( state.backtracking==0 ) {
            	       
            	              afterParserOrEnumRuleCall();
            	          
            	    }

            	    }
            	    break;

            	default :
            	    break loop15;
                }
            } while (true);


            }


            }

            if ( state.backtracking==0 ) {
               leaveRule(); 
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
    // $ANTLR end "ruleQualifiedName"


    // $ANTLR start "entryRuleValidID"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1189:1: entryRuleValidID returns [String current=null] : iv_ruleValidID= ruleValidID EOF ;
    public final String entryRuleValidID() throws RecognitionException {
        String current = null;

        AntlrDatatypeRuleToken iv_ruleValidID = null;


        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1190:2: (iv_ruleValidID= ruleValidID EOF )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1191:2: iv_ruleValidID= ruleValidID EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getValidIDRule()); 
            }
            pushFollow(FOLLOW_ruleValidID_in_entryRuleValidID2834);
            iv_ruleValidID=ruleValidID();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleValidID.getText(); 
            }
            match(input,EOF,FOLLOW_EOF_in_entryRuleValidID2845); if (state.failed) return current;

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
    // $ANTLR end "entryRuleValidID"


    // $ANTLR start "ruleValidID"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1198:1: ruleValidID returns [AntlrDatatypeRuleToken current=new AntlrDatatypeRuleToken()] : (this_ID_0= RULE_ID | this_LONG_ID_1= RULE_LONG_ID | this_LONG_UNSIGNED_2= RULE_LONG_UNSIGNED ) ;
    public final AntlrDatatypeRuleToken ruleValidID() throws RecognitionException {
        AntlrDatatypeRuleToken current = new AntlrDatatypeRuleToken();

        Token this_ID_0=null;
        Token this_LONG_ID_1=null;
        Token this_LONG_UNSIGNED_2=null;

         enterRule(); 
            
        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1201:28: ( (this_ID_0= RULE_ID | this_LONG_ID_1= RULE_LONG_ID | this_LONG_UNSIGNED_2= RULE_LONG_UNSIGNED ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1202:1: (this_ID_0= RULE_ID | this_LONG_ID_1= RULE_LONG_ID | this_LONG_UNSIGNED_2= RULE_LONG_UNSIGNED )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1202:1: (this_ID_0= RULE_ID | this_LONG_ID_1= RULE_LONG_ID | this_LONG_UNSIGNED_2= RULE_LONG_UNSIGNED )
            int alt16=3;
            switch ( input.LA(1) ) {
            case RULE_ID:
                {
                alt16=1;
                }
                break;
            case RULE_LONG_ID:
                {
                alt16=2;
                }
                break;
            case RULE_LONG_UNSIGNED:
                {
                alt16=3;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return current;}
                NoViableAltException nvae =
                    new NoViableAltException("", 16, 0, input);

                throw nvae;
            }

            switch (alt16) {
                case 1 :
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1202:6: this_ID_0= RULE_ID
                    {
                    this_ID_0=(Token)match(input,RULE_ID,FOLLOW_RULE_ID_in_ruleValidID2885); if (state.failed) return current;
                    if ( state.backtracking==0 ) {

                      		current.merge(this_ID_0);
                          
                    }
                    if ( state.backtracking==0 ) {
                       
                          newLeafNode(this_ID_0, grammarAccess.getValidIDAccess().getIDTerminalRuleCall_0()); 
                          
                    }

                    }
                    break;
                case 2 :
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1210:10: this_LONG_ID_1= RULE_LONG_ID
                    {
                    this_LONG_ID_1=(Token)match(input,RULE_LONG_ID,FOLLOW_RULE_LONG_ID_in_ruleValidID2911); if (state.failed) return current;
                    if ( state.backtracking==0 ) {

                      		current.merge(this_LONG_ID_1);
                          
                    }
                    if ( state.backtracking==0 ) {
                       
                          newLeafNode(this_LONG_ID_1, grammarAccess.getValidIDAccess().getLONG_IDTerminalRuleCall_1()); 
                          
                    }

                    }
                    break;
                case 3 :
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1218:10: this_LONG_UNSIGNED_2= RULE_LONG_UNSIGNED
                    {
                    this_LONG_UNSIGNED_2=(Token)match(input,RULE_LONG_UNSIGNED,FOLLOW_RULE_LONG_UNSIGNED_in_ruleValidID2937); if (state.failed) return current;
                    if ( state.backtracking==0 ) {

                      		current.merge(this_LONG_UNSIGNED_2);
                          
                    }
                    if ( state.backtracking==0 ) {
                       
                          newLeafNode(this_LONG_UNSIGNED_2, grammarAccess.getValidIDAccess().getLONG_UNSIGNEDTerminalRuleCall_2()); 
                          
                    }

                    }
                    break;

            }


            }

            if ( state.backtracking==0 ) {
               leaveRule(); 
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
    // $ANTLR end "ruleValidID"


    // $ANTLR start "entryRuleBoolean"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1233:1: entryRuleBoolean returns [String current=null] : iv_ruleBoolean= ruleBoolean EOF ;
    public final String entryRuleBoolean() throws RecognitionException {
        String current = null;

        AntlrDatatypeRuleToken iv_ruleBoolean = null;


        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1234:2: (iv_ruleBoolean= ruleBoolean EOF )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1235:2: iv_ruleBoolean= ruleBoolean EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getBooleanRule()); 
            }
            pushFollow(FOLLOW_ruleBoolean_in_entryRuleBoolean2983);
            iv_ruleBoolean=ruleBoolean();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleBoolean.getText(); 
            }
            match(input,EOF,FOLLOW_EOF_in_entryRuleBoolean2994); if (state.failed) return current;

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
    // $ANTLR end "entryRuleBoolean"


    // $ANTLR start "ruleBoolean"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1242:1: ruleBoolean returns [AntlrDatatypeRuleToken current=new AntlrDatatypeRuleToken()] : (kw= 'true' | kw= 'false' ) ;
    public final AntlrDatatypeRuleToken ruleBoolean() throws RecognitionException {
        AntlrDatatypeRuleToken current = new AntlrDatatypeRuleToken();

        Token kw=null;

         enterRule(); 
            
        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1245:28: ( (kw= 'true' | kw= 'false' ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1246:1: (kw= 'true' | kw= 'false' )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1246:1: (kw= 'true' | kw= 'false' )
            int alt17=2;
            int LA17_0 = input.LA(1);

            if ( (LA17_0==25) ) {
                alt17=1;
            }
            else if ( (LA17_0==26) ) {
                alt17=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return current;}
                NoViableAltException nvae =
                    new NoViableAltException("", 17, 0, input);

                throw nvae;
            }
            switch (alt17) {
                case 1 :
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1247:2: kw= 'true'
                    {
                    kw=(Token)match(input,25,FOLLOW_25_in_ruleBoolean3032); if (state.failed) return current;
                    if ( state.backtracking==0 ) {

                              current.merge(kw);
                              newLeafNode(kw, grammarAccess.getBooleanAccess().getTrueKeyword_0()); 
                          
                    }

                    }
                    break;
                case 2 :
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1254:2: kw= 'false'
                    {
                    kw=(Token)match(input,26,FOLLOW_26_in_ruleBoolean3051); if (state.failed) return current;
                    if ( state.backtracking==0 ) {

                              current.merge(kw);
                              newLeafNode(kw, grammarAccess.getBooleanAccess().getFalseKeyword_1()); 
                          
                    }

                    }
                    break;

            }


            }

            if ( state.backtracking==0 ) {
               leaveRule(); 
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
    // $ANTLR end "ruleBoolean"


    // $ANTLR start "entryRuleLong"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1267:1: entryRuleLong returns [String current=null] : iv_ruleLong= ruleLong EOF ;
    public final String entryRuleLong() throws RecognitionException {
        String current = null;

        AntlrDatatypeRuleToken iv_ruleLong = null;


        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1268:2: (iv_ruleLong= ruleLong EOF )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1269:2: iv_ruleLong= ruleLong EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getLongRule()); 
            }
            pushFollow(FOLLOW_ruleLong_in_entryRuleLong3092);
            iv_ruleLong=ruleLong();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleLong.getText(); 
            }
            match(input,EOF,FOLLOW_EOF_in_entryRuleLong3103); if (state.failed) return current;

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
    // $ANTLR end "entryRuleLong"


    // $ANTLR start "ruleLong"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1276:1: ruleLong returns [AntlrDatatypeRuleToken current=new AntlrDatatypeRuleToken()] : ( (kw= '-' )? this_LONG_UNSIGNED_1= RULE_LONG_UNSIGNED ) ;
    public final AntlrDatatypeRuleToken ruleLong() throws RecognitionException {
        AntlrDatatypeRuleToken current = new AntlrDatatypeRuleToken();

        Token kw=null;
        Token this_LONG_UNSIGNED_1=null;

         enterRule(); 
            
        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1279:28: ( ( (kw= '-' )? this_LONG_UNSIGNED_1= RULE_LONG_UNSIGNED ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1280:1: ( (kw= '-' )? this_LONG_UNSIGNED_1= RULE_LONG_UNSIGNED )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1280:1: ( (kw= '-' )? this_LONG_UNSIGNED_1= RULE_LONG_UNSIGNED )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1280:2: (kw= '-' )? this_LONG_UNSIGNED_1= RULE_LONG_UNSIGNED
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1280:2: (kw= '-' )?
            int alt18=2;
            int LA18_0 = input.LA(1);

            if ( (LA18_0==27) ) {
                alt18=1;
            }
            switch (alt18) {
                case 1 :
                    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1281:2: kw= '-'
                    {
                    kw=(Token)match(input,27,FOLLOW_27_in_ruleLong3142); if (state.failed) return current;
                    if ( state.backtracking==0 ) {

                              current.merge(kw);
                              newLeafNode(kw, grammarAccess.getLongAccess().getHyphenMinusKeyword_0()); 
                          
                    }

                    }
                    break;

            }

            this_LONG_UNSIGNED_1=(Token)match(input,RULE_LONG_UNSIGNED,FOLLOW_RULE_LONG_UNSIGNED_in_ruleLong3159); if (state.failed) return current;
            if ( state.backtracking==0 ) {

              		current.merge(this_LONG_UNSIGNED_1);
                  
            }
            if ( state.backtracking==0 ) {
               
                  newLeafNode(this_LONG_UNSIGNED_1, grammarAccess.getLongAccess().getLONG_UNSIGNEDTerminalRuleCall_1()); 
                  
            }

            }


            }

            if ( state.backtracking==0 ) {
               leaveRule(); 
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
    // $ANTLR end "ruleLong"


    // $ANTLR start "entryRuleDouble"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1301:1: entryRuleDouble returns [String current=null] : iv_ruleDouble= ruleDouble EOF ;
    public final String entryRuleDouble() throws RecognitionException {
        String current = null;

        AntlrDatatypeRuleToken iv_ruleDouble = null;


        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1302:2: (iv_ruleDouble= ruleDouble EOF )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1303:2: iv_ruleDouble= ruleDouble EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getDoubleRule()); 
            }
            pushFollow(FOLLOW_ruleDouble_in_entryRuleDouble3205);
            iv_ruleDouble=ruleDouble();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleDouble.getText(); 
            }
            match(input,EOF,FOLLOW_EOF_in_entryRuleDouble3216); if (state.failed) return current;

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
    // $ANTLR end "entryRuleDouble"


    // $ANTLR start "ruleDouble"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1310:1: ruleDouble returns [AntlrDatatypeRuleToken current=new AntlrDatatypeRuleToken()] : (this_Long_0= ruleLong kw= '.' this_LONG_UNSIGNED_2= RULE_LONG_UNSIGNED ) ;
    public final AntlrDatatypeRuleToken ruleDouble() throws RecognitionException {
        AntlrDatatypeRuleToken current = new AntlrDatatypeRuleToken();

        Token kw=null;
        Token this_LONG_UNSIGNED_2=null;
        AntlrDatatypeRuleToken this_Long_0 = null;


         enterRule(); 
            
        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1313:28: ( (this_Long_0= ruleLong kw= '.' this_LONG_UNSIGNED_2= RULE_LONG_UNSIGNED ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1314:1: (this_Long_0= ruleLong kw= '.' this_LONG_UNSIGNED_2= RULE_LONG_UNSIGNED )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1314:1: (this_Long_0= ruleLong kw= '.' this_LONG_UNSIGNED_2= RULE_LONG_UNSIGNED )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1315:5: this_Long_0= ruleLong kw= '.' this_LONG_UNSIGNED_2= RULE_LONG_UNSIGNED
            {
            if ( state.backtracking==0 ) {
               
                      newCompositeNode(grammarAccess.getDoubleAccess().getLongParserRuleCall_0()); 
                  
            }
            pushFollow(FOLLOW_ruleLong_in_ruleDouble3263);
            this_Long_0=ruleLong();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {

              		current.merge(this_Long_0);
                  
            }
            if ( state.backtracking==0 ) {
               
                      afterParserOrEnumRuleCall();
                  
            }
            kw=(Token)match(input,24,FOLLOW_24_in_ruleDouble3281); if (state.failed) return current;
            if ( state.backtracking==0 ) {

                      current.merge(kw);
                      newLeafNode(kw, grammarAccess.getDoubleAccess().getFullStopKeyword_1()); 
                  
            }
            this_LONG_UNSIGNED_2=(Token)match(input,RULE_LONG_UNSIGNED,FOLLOW_RULE_LONG_UNSIGNED_in_ruleDouble3296); if (state.failed) return current;
            if ( state.backtracking==0 ) {

              		current.merge(this_LONG_UNSIGNED_2);
                  
            }
            if ( state.backtracking==0 ) {
               
                  newLeafNode(this_LONG_UNSIGNED_2, grammarAccess.getDoubleAccess().getLONG_UNSIGNEDTerminalRuleCall_2()); 
                  
            }

            }


            }

            if ( state.backtracking==0 ) {
               leaveRule(); 
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
    // $ANTLR end "ruleDouble"


    // $ANTLR start "entryRuleDate"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1346:1: entryRuleDate returns [String current=null] : iv_ruleDate= ruleDate EOF ;
    public final String entryRuleDate() throws RecognitionException {
        String current = null;

        AntlrDatatypeRuleToken iv_ruleDate = null;


        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1347:2: (iv_ruleDate= ruleDate EOF )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1348:2: iv_ruleDate= ruleDate EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getDateRule()); 
            }
            pushFollow(FOLLOW_ruleDate_in_entryRuleDate3342);
            iv_ruleDate=ruleDate();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleDate.getText(); 
            }
            match(input,EOF,FOLLOW_EOF_in_entryRuleDate3353); if (state.failed) return current;

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
    // $ANTLR end "entryRuleDate"


    // $ANTLR start "ruleDate"
    // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1355:1: ruleDate returns [AntlrDatatypeRuleToken current=new AntlrDatatypeRuleToken()] : (this_LONG_UNSIGNED_0= RULE_LONG_UNSIGNED kw= '.' this_LONG_UNSIGNED_2= RULE_LONG_UNSIGNED kw= '.' this_LONG_UNSIGNED_4= RULE_LONG_UNSIGNED ) ;
    public final AntlrDatatypeRuleToken ruleDate() throws RecognitionException {
        AntlrDatatypeRuleToken current = new AntlrDatatypeRuleToken();

        Token this_LONG_UNSIGNED_0=null;
        Token kw=null;
        Token this_LONG_UNSIGNED_2=null;
        Token this_LONG_UNSIGNED_4=null;

         enterRule(); 
            
        try {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1358:28: ( (this_LONG_UNSIGNED_0= RULE_LONG_UNSIGNED kw= '.' this_LONG_UNSIGNED_2= RULE_LONG_UNSIGNED kw= '.' this_LONG_UNSIGNED_4= RULE_LONG_UNSIGNED ) )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1359:1: (this_LONG_UNSIGNED_0= RULE_LONG_UNSIGNED kw= '.' this_LONG_UNSIGNED_2= RULE_LONG_UNSIGNED kw= '.' this_LONG_UNSIGNED_4= RULE_LONG_UNSIGNED )
            {
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1359:1: (this_LONG_UNSIGNED_0= RULE_LONG_UNSIGNED kw= '.' this_LONG_UNSIGNED_2= RULE_LONG_UNSIGNED kw= '.' this_LONG_UNSIGNED_4= RULE_LONG_UNSIGNED )
            // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1359:6: this_LONG_UNSIGNED_0= RULE_LONG_UNSIGNED kw= '.' this_LONG_UNSIGNED_2= RULE_LONG_UNSIGNED kw= '.' this_LONG_UNSIGNED_4= RULE_LONG_UNSIGNED
            {
            this_LONG_UNSIGNED_0=(Token)match(input,RULE_LONG_UNSIGNED,FOLLOW_RULE_LONG_UNSIGNED_in_ruleDate3393); if (state.failed) return current;
            if ( state.backtracking==0 ) {

              		current.merge(this_LONG_UNSIGNED_0);
                  
            }
            if ( state.backtracking==0 ) {
               
                  newLeafNode(this_LONG_UNSIGNED_0, grammarAccess.getDateAccess().getLONG_UNSIGNEDTerminalRuleCall_0()); 
                  
            }
            kw=(Token)match(input,24,FOLLOW_24_in_ruleDate3411); if (state.failed) return current;
            if ( state.backtracking==0 ) {

                      current.merge(kw);
                      newLeafNode(kw, grammarAccess.getDateAccess().getFullStopKeyword_1()); 
                  
            }
            this_LONG_UNSIGNED_2=(Token)match(input,RULE_LONG_UNSIGNED,FOLLOW_RULE_LONG_UNSIGNED_in_ruleDate3426); if (state.failed) return current;
            if ( state.backtracking==0 ) {

              		current.merge(this_LONG_UNSIGNED_2);
                  
            }
            if ( state.backtracking==0 ) {
               
                  newLeafNode(this_LONG_UNSIGNED_2, grammarAccess.getDateAccess().getLONG_UNSIGNEDTerminalRuleCall_2()); 
                  
            }
            kw=(Token)match(input,24,FOLLOW_24_in_ruleDate3444); if (state.failed) return current;
            if ( state.backtracking==0 ) {

                      current.merge(kw);
                      newLeafNode(kw, grammarAccess.getDateAccess().getFullStopKeyword_3()); 
                  
            }
            this_LONG_UNSIGNED_4=(Token)match(input,RULE_LONG_UNSIGNED,FOLLOW_RULE_LONG_UNSIGNED_in_ruleDate3459); if (state.failed) return current;
            if ( state.backtracking==0 ) {

              		current.merge(this_LONG_UNSIGNED_4);
                  
            }
            if ( state.backtracking==0 ) {
               
                  newLeafNode(this_LONG_UNSIGNED_4, grammarAccess.getDateAccess().getLONG_UNSIGNEDTerminalRuleCall_4()); 
                  
            }

            }


            }

            if ( state.backtracking==0 ) {
               leaveRule(); 
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
    // $ANTLR end "ruleDate"

    // $ANTLR start synpred1_InternalEFactory
    public final void synpred1_InternalEFactory_fragment() throws RecognitionException {   
        // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:511:8: ( ( () ( ( ruleQualifiedName ) )? ( ( ruleValidID ) )? '{' ) )
        // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:511:9: ( () ( ( ruleQualifiedName ) )? ( ( ruleValidID ) )? '{' )
        {
        // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:511:9: ( () ( ( ruleQualifiedName ) )? ( ( ruleValidID ) )? '{' )
        // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:511:10: () ( ( ruleQualifiedName ) )? ( ( ruleValidID ) )? '{'
        {
        // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:511:10: ()
        // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:512:1: 
        {
        }

        // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:512:2: ( ( ruleQualifiedName ) )?
        int alt19=2;
        switch ( input.LA(1) ) {
            case RULE_ID:
                {
                alt19=1;
                }
                break;
            case RULE_LONG_ID:
                {
                alt19=1;
                }
                break;
            case RULE_LONG_UNSIGNED:
                {
                alt19=1;
                }
                break;
        }

        switch (alt19) {
            case 1 :
                // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:513:1: ( ruleQualifiedName )
                {
                // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:513:1: ( ruleQualifiedName )
                // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:514:3: ruleQualifiedName
                {
                pushFollow(FOLLOW_ruleQualifiedName_in_synpred1_InternalEFactory1099);
                ruleQualifiedName();

                state._fsp--;
                if (state.failed) return ;

                }


                }
                break;

        }

        // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:516:3: ( ( ruleValidID ) )?
        int alt20=2;
        int LA20_0 = input.LA(1);

        if ( ((LA20_0>=RULE_ID && LA20_0<=RULE_LONG_UNSIGNED)) ) {
            alt20=1;
        }
        switch (alt20) {
            case 1 :
                // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:517:1: ( ruleValidID )
                {
                // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:517:1: ( ruleValidID )
                // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:518:1: ruleValidID
                {
                pushFollow(FOLLOW_ruleValidID_in_synpred1_InternalEFactory1109);
                ruleValidID();

                state._fsp--;
                if (state.failed) return ;

                }


                }
                break;

        }

        match(input,16,FOLLOW_16_in_synpred1_InternalEFactory1116); if (state.failed) return ;

        }


        }
    }
    // $ANTLR end synpred1_InternalEFactory

    // $ANTLR start synpred2_InternalEFactory
    public final void synpred2_InternalEFactory_fragment() throws RecognitionException {   
        // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:593:7: ( ruleReference )
        // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:593:9: ruleReference
        {
        pushFollow(FOLLOW_ruleReference_in_synpred2_InternalEFactory1245);
        ruleReference();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred2_InternalEFactory

    // $ANTLR start synpred3_InternalEFactory
    public final void synpred3_InternalEFactory_fragment() throws RecognitionException {   
        // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1162:3: ( '.' )
        // ../org.eclipse.emf.eson/src-gen/org/eclipse/emf/eson/parser/antlr/internal/InternalEFactory.g:1163:2: '.'
        {
        match(input,24,FOLLOW_24_in_synpred3_InternalEFactory2754); if (state.failed) return ;

        }
    }
    // $ANTLR end synpred3_InternalEFactory

    // Delegated rules

    public final boolean synpred1_InternalEFactory() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred1_InternalEFactory_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred2_InternalEFactory() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred2_InternalEFactory_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }
    public final boolean synpred3_InternalEFactory() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred3_InternalEFactory_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }


    protected DFA4 dfa4 = new DFA4(this);
    protected DFA12 dfa12 = new DFA12(this);
    static final String DFA4_eotS =
        "\16\uffff";
    static final String DFA4_eofS =
        "\1\5\1\uffff\3\1\3\uffff\4\1\1\uffff\1\1";
    static final String DFA4_minS =
        "\1\4\1\uffff\3\5\1\uffff\10\5";
    static final String DFA4_maxS =
        "\1\33\1\uffff\3\30\1\uffff\2\7\4\30\1\7\1\30";
    static final String DFA4_acceptS =
        "\1\uffff\1\1\3\uffff\1\2\10\uffff";
    static final String DFA4_specialS =
        "\16\uffff}>";
    static final String[] DFA4_transitionS = {
            "\1\1\1\3\1\4\1\2\10\uffff\1\1\1\uffff\1\5\2\1\1\uffff\1\1\2\uffff\3\1",
            "",
            "\3\1\10\uffff\1\1\1\uffff\1\1\1\5\4\uffff\1\6",
            "\3\1\10\uffff\1\1\1\uffff\1\1\1\5\4\uffff\1\7",
            "\3\1\10\uffff\1\1\1\uffff\1\1\1\5\4\uffff\1\7",
            "",
            "\1\10\1\11\1\12",
            "\1\10\1\11\1\13",
            "\3\1\10\uffff\1\1\1\uffff\1\1\1\5\4\uffff\1\7",
            "\3\1\10\uffff\1\1\1\uffff\1\1\1\5\4\uffff\1\7",
            "\3\1\10\uffff\1\1\1\uffff\1\1\1\5\4\uffff\1\14",
            "\3\1\10\uffff\1\1\1\uffff\1\1\1\5\4\uffff\1\7",
            "\1\10\1\11\1\15",
            "\3\1\10\uffff\1\1\1\uffff\1\1\1\5\4\uffff\1\7"
    };

    static final short[] DFA4_eot = DFA.unpackEncodedString(DFA4_eotS);
    static final short[] DFA4_eof = DFA.unpackEncodedString(DFA4_eofS);
    static final char[] DFA4_min = DFA.unpackEncodedStringToUnsignedChars(DFA4_minS);
    static final char[] DFA4_max = DFA.unpackEncodedStringToUnsignedChars(DFA4_maxS);
    static final short[] DFA4_accept = DFA.unpackEncodedString(DFA4_acceptS);
    static final short[] DFA4_special = DFA.unpackEncodedString(DFA4_specialS);
    static final short[][] DFA4_transition;

    static {
        int numStates = DFA4_transitionS.length;
        DFA4_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA4_transition[i] = DFA.unpackEncodedString(DFA4_transitionS[i]);
        }
    }

    class DFA4 extends DFA {

        public DFA4(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 4;
            this.eot = DFA4_eot;
            this.eof = DFA4_eof;
            this.min = DFA4_min;
            this.max = DFA4_max;
            this.accept = DFA4_accept;
            this.special = DFA4_special;
            this.transition = DFA4_transition;
        }
        public String getDescription() {
            return "372:1: ( (lv_value_2_0= ruleValue ) )?";
        }
    }
    static final String DFA12_eotS =
        "\15\uffff";
    static final String DFA12_eofS =
        "\4\uffff\1\10\2\uffff\1\10\3\uffff\1\12\1\uffff";
    static final String DFA12_minS =
        "\1\4\2\uffff\1\7\1\4\2\uffff\1\4\1\uffff\1\7\1\uffff\1\4\1\uffff";
    static final String DFA12_maxS =
        "\1\33\2\uffff\1\7\1\33\2\uffff\1\33\1\uffff\1\7\1\uffff\1\33\1\uffff";
    static final String DFA12_acceptS =
        "\1\uffff\1\1\1\2\2\uffff\1\4\1\7\1\uffff\1\3\1\uffff\1\5\1\uffff\1\6";
    static final String DFA12_specialS =
        "\15\uffff}>";
    static final String[] DFA12_transitionS = {
            "\1\2\2\uffff\1\4\13\uffff\1\1\2\uffff\1\6\2\uffff\2\5\1\3",
            "",
            "",
            "\1\7",
            "\4\10\10\uffff\1\10\1\uffff\5\10\1\uffff\1\11\3\10",
            "",
            "",
            "\4\10\10\uffff\1\10\1\uffff\5\10\1\uffff\1\12\3\10",
            "",
            "\1\13",
            "",
            "\4\12\10\uffff\1\12\1\uffff\5\12\1\uffff\1\14\3\12",
            ""
    };

    static final short[] DFA12_eot = DFA.unpackEncodedString(DFA12_eotS);
    static final short[] DFA12_eof = DFA.unpackEncodedString(DFA12_eofS);
    static final char[] DFA12_min = DFA.unpackEncodedStringToUnsignedChars(DFA12_minS);
    static final char[] DFA12_max = DFA.unpackEncodedStringToUnsignedChars(DFA12_maxS);
    static final short[] DFA12_accept = DFA.unpackEncodedString(DFA12_acceptS);
    static final short[] DFA12_special = DFA.unpackEncodedString(DFA12_specialS);
    static final short[][] DFA12_transition;

    static {
        int numStates = DFA12_transitionS.length;
        DFA12_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA12_transition[i] = DFA.unpackEncodedString(DFA12_transitionS[i]);
        }
    }

    class DFA12 extends DFA {

        public DFA12(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 12;
            this.eot = DFA12_eot;
            this.eof = DFA12_eof;
            this.min = DFA12_min;
            this.max = DFA12_max;
            this.accept = DFA12_accept;
            this.special = DFA12_special;
            this.transition = DFA12_transition;
        }
        public String getDescription() {
            return "712:1: (this_EnumAttribute_0= ruleEnumAttribute | this_StringAttribute_1= ruleStringAttribute | this_IntegerAttribute_2= ruleIntegerAttribute | this_BooleanAttribute_3= ruleBooleanAttribute | this_DoubleAttribute_4= ruleDoubleAttribute | this_DateAttribute_5= ruleDateAttribute | this_NullAttribute_6= ruleNullAttribute )";
        }
    }
 

    public static final BitSet FOLLOW_ruleFactory_in_entryRuleFactory75 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_entryRuleFactory85 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleNamespaceImport_in_ruleFactory131 = new BitSet(new long[]{0x000000000000E0E0L});
    public static final BitSet FOLLOW_rulePackageImport_in_ruleFactory153 = new BitSet(new long[]{0x000000000000A0E0L});
    public static final BitSet FOLLOW_ruleAnnotation_in_ruleFactory175 = new BitSet(new long[]{0x000000000000A0E0L});
    public static final BitSet FOLLOW_ruleNewObject_in_ruleFactory197 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_rulePackageImport_in_entryRulePackageImport233 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_entryRulePackageImport243 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_13_in_rulePackageImport280 = new BitSet(new long[]{0x000000000000A0F0L});
    public static final BitSet FOLLOW_ruleStringOrQualifiedNameWithWildcard_in_rulePackageImport303 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleNamespaceImport_in_entryRuleNamespaceImport339 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_entryRuleNamespaceImport349 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_14_in_ruleNamespaceImport386 = new BitSet(new long[]{0x000000000000A0F0L});
    public static final BitSet FOLLOW_ruleQualifiedNameWithWildcard_in_ruleNamespaceImport407 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleAnnotation_in_entryRuleAnnotation443 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_entryRuleAnnotation453 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleCustomNameMapping_in_ruleAnnotation499 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleCustomNameMapping_in_entryRuleCustomNameMapping533 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_entryRuleCustomNameMapping543 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_15_in_ruleCustomNameMapping580 = new BitSet(new long[]{0x0000000000010000L});
    public static final BitSet FOLLOW_16_in_ruleCustomNameMapping592 = new BitSet(new long[]{0x000000000000A0E0L});
    public static final BitSet FOLLOW_ruleQualifiedName_in_ruleCustomNameMapping615 = new BitSet(new long[]{0x0000000000020000L});
    public static final BitSet FOLLOW_17_in_ruleCustomNameMapping627 = new BitSet(new long[]{0x000000000000A0E0L});
    public static final BitSet FOLLOW_ruleQualifiedName_in_ruleCustomNameMapping650 = new BitSet(new long[]{0x0000000000040000L});
    public static final BitSet FOLLOW_18_in_ruleCustomNameMapping662 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleFeature_in_entryRuleFeature698 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_entryRuleFeature708 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleQualifiedName_in_ruleFeature756 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_19_in_ruleFeature768 = new BitSet(new long[]{0x000000000E59A0F2L});
    public static final BitSet FOLLOW_ruleValue_in_ruleFeature789 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleNewObject_in_entryRuleNewObject826 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_entryRuleNewObject836 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleQualifiedName_in_ruleNewObject884 = new BitSet(new long[]{0x000000000001A0E0L});
    public static final BitSet FOLLOW_ruleValidID_in_ruleNewObject905 = new BitSet(new long[]{0x0000000000010000L});
    public static final BitSet FOLLOW_16_in_ruleNewObject918 = new BitSet(new long[]{0x000000000004A0E0L});
    public static final BitSet FOLLOW_ruleFeature_in_ruleNewObject939 = new BitSet(new long[]{0x000000000004A0E0L});
    public static final BitSet FOLLOW_18_in_ruleNewObject952 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleValue_in_entryRuleValue988 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_entryRuleValue998 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleMultiValue_in_ruleValue1045 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleAttribute_in_ruleValue1072 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleQualifiedName_in_ruleValue1146 = new BitSet(new long[]{0x000000000001A0E0L});
    public static final BitSet FOLLOW_ruleValidID_in_ruleValue1168 = new BitSet(new long[]{0x0000000000010000L});
    public static final BitSet FOLLOW_16_in_ruleValue1181 = new BitSet(new long[]{0x000000000004A0E0L});
    public static final BitSet FOLLOW_ruleFeature_in_ruleValue1204 = new BitSet(new long[]{0x000000000004A0E0L});
    public static final BitSet FOLLOW_18_in_ruleValue1217 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleReference_in_ruleValue1261 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleMultiValue_in_entryRuleMultiValue1297 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_entryRuleMultiValue1307 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_20_in_ruleMultiValue1353 = new BitSet(new long[]{0x000000000E79A0F0L});
    public static final BitSet FOLLOW_ruleValue_in_ruleMultiValue1374 = new BitSet(new long[]{0x000000000E79A0F0L});
    public static final BitSet FOLLOW_21_in_ruleMultiValue1387 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleReference_in_entryRuleReference1423 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_entryRuleReference1433 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleQualifiedName_in_ruleReference1480 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleAttribute_in_entryRuleAttribute1515 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_entryRuleAttribute1525 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleEnumAttribute_in_ruleAttribute1572 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleStringAttribute_in_ruleAttribute1599 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleIntegerAttribute_in_ruleAttribute1626 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleBooleanAttribute_in_ruleAttribute1653 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleDoubleAttribute_in_ruleAttribute1680 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleDateAttribute_in_ruleAttribute1707 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleNullAttribute_in_ruleAttribute1734 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleEnumAttribute_in_entryRuleEnumAttribute1769 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_entryRuleEnumAttribute1779 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_19_in_ruleEnumAttribute1816 = new BitSet(new long[]{0x000000000000A0E0L});
    public static final BitSet FOLLOW_ruleQualifiedName_in_ruleEnumAttribute1839 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleStringAttribute_in_entryRuleStringAttribute1875 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_entryRuleStringAttribute1885 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_RULE_STRING_in_ruleStringAttribute1926 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleIntegerAttribute_in_entryRuleIntegerAttribute1966 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_entryRuleIntegerAttribute1976 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleLong_in_ruleIntegerAttribute2021 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleDoubleAttribute_in_entryRuleDoubleAttribute2056 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_entryRuleDoubleAttribute2066 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleDouble_in_ruleDoubleAttribute2111 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleDateAttribute_in_entryRuleDateAttribute2146 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_entryRuleDateAttribute2156 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleDate_in_ruleDateAttribute2201 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleNullAttribute_in_entryRuleNullAttribute2236 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_entryRuleNullAttribute2246 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_22_in_ruleNullAttribute2288 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleBooleanAttribute_in_entryRuleBooleanAttribute2336 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_entryRuleBooleanAttribute2346 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleBoolean_in_ruleBooleanAttribute2391 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleStringOrQualifiedNameWithWildcard_in_entryRuleStringOrQualifiedNameWithWildcard2427 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_entryRuleStringOrQualifiedNameWithWildcard2438 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_RULE_STRING_in_ruleStringOrQualifiedNameWithWildcard2478 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleQualifiedNameWithWildcard_in_ruleStringOrQualifiedNameWithWildcard2511 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleQualifiedNameWithWildcard_in_entryRuleQualifiedNameWithWildcard2557 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_entryRuleQualifiedNameWithWildcard2568 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleQualifiedName_in_ruleQualifiedNameWithWildcard2615 = new BitSet(new long[]{0x0000000000800002L});
    public static final BitSet FOLLOW_23_in_ruleQualifiedNameWithWildcard2634 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleQualifiedName_in_entryRuleQualifiedName2677 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_entryRuleQualifiedName2688 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleValidID_in_ruleQualifiedName2735 = new BitSet(new long[]{0x0000000001000002L});
    public static final BitSet FOLLOW_24_in_ruleQualifiedName2763 = new BitSet(new long[]{0x000000000000A0E0L});
    public static final BitSet FOLLOW_ruleValidID_in_ruleQualifiedName2786 = new BitSet(new long[]{0x0000000001000002L});
    public static final BitSet FOLLOW_ruleValidID_in_entryRuleValidID2834 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_entryRuleValidID2845 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_RULE_ID_in_ruleValidID2885 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_RULE_LONG_ID_in_ruleValidID2911 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_RULE_LONG_UNSIGNED_in_ruleValidID2937 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleBoolean_in_entryRuleBoolean2983 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_entryRuleBoolean2994 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_25_in_ruleBoolean3032 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_26_in_ruleBoolean3051 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleLong_in_entryRuleLong3092 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_entryRuleLong3103 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_27_in_ruleLong3142 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_RULE_LONG_UNSIGNED_in_ruleLong3159 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleDouble_in_entryRuleDouble3205 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_entryRuleDouble3216 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleLong_in_ruleDouble3263 = new BitSet(new long[]{0x0000000001000000L});
    public static final BitSet FOLLOW_24_in_ruleDouble3281 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_RULE_LONG_UNSIGNED_in_ruleDouble3296 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleDate_in_entryRuleDate3342 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_entryRuleDate3353 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_RULE_LONG_UNSIGNED_in_ruleDate3393 = new BitSet(new long[]{0x0000000001000000L});
    public static final BitSet FOLLOW_24_in_ruleDate3411 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_RULE_LONG_UNSIGNED_in_ruleDate3426 = new BitSet(new long[]{0x0000000001000000L});
    public static final BitSet FOLLOW_24_in_ruleDate3444 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_RULE_LONG_UNSIGNED_in_ruleDate3459 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleQualifiedName_in_synpred1_InternalEFactory1099 = new BitSet(new long[]{0x000000000001A0E0L});
    public static final BitSet FOLLOW_ruleValidID_in_synpred1_InternalEFactory1109 = new BitSet(new long[]{0x0000000000010000L});
    public static final BitSet FOLLOW_16_in_synpred1_InternalEFactory1116 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleReference_in_synpred2_InternalEFactory1245 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_24_in_synpred3_InternalEFactory2754 = new BitSet(new long[]{0x0000000000000002L});

}