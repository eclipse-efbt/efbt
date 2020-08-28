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
    public String getGrammarFileName() { return "InternalEFactory.g"; }



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
    // InternalEFactory.g:67:1: entryRuleFactory returns [EObject current=null] : iv_ruleFactory= ruleFactory EOF ;
    public final EObject entryRuleFactory() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleFactory = null;


        try {
            // InternalEFactory.g:68:2: (iv_ruleFactory= ruleFactory EOF )
            // InternalEFactory.g:69:2: iv_ruleFactory= ruleFactory EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getFactoryRule()); 
            }
            pushFollow(FOLLOW_1);
            iv_ruleFactory=ruleFactory();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleFactory; 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return current;

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
    // InternalEFactory.g:76:1: ruleFactory returns [EObject current=null] : ( ( (lv_imports_0_0= ruleNamespaceImport ) )* ( (lv_ePackages_1_0= rulePackageImport ) )* ( (lv_annotations_2_0= ruleAnnotation ) )* ( (lv_root_3_0= ruleNewObject ) ) ) ;
    public final EObject ruleFactory() throws RecognitionException {
        EObject current = null;

        EObject lv_imports_0_0 = null;

        EObject lv_ePackages_1_0 = null;

        EObject lv_annotations_2_0 = null;

        EObject lv_root_3_0 = null;


         enterRule(); 
            
        try {
            // InternalEFactory.g:79:28: ( ( ( (lv_imports_0_0= ruleNamespaceImport ) )* ( (lv_ePackages_1_0= rulePackageImport ) )* ( (lv_annotations_2_0= ruleAnnotation ) )* ( (lv_root_3_0= ruleNewObject ) ) ) )
            // InternalEFactory.g:80:1: ( ( (lv_imports_0_0= ruleNamespaceImport ) )* ( (lv_ePackages_1_0= rulePackageImport ) )* ( (lv_annotations_2_0= ruleAnnotation ) )* ( (lv_root_3_0= ruleNewObject ) ) )
            {
            // InternalEFactory.g:80:1: ( ( (lv_imports_0_0= ruleNamespaceImport ) )* ( (lv_ePackages_1_0= rulePackageImport ) )* ( (lv_annotations_2_0= ruleAnnotation ) )* ( (lv_root_3_0= ruleNewObject ) ) )
            // InternalEFactory.g:80:2: ( (lv_imports_0_0= ruleNamespaceImport ) )* ( (lv_ePackages_1_0= rulePackageImport ) )* ( (lv_annotations_2_0= ruleAnnotation ) )* ( (lv_root_3_0= ruleNewObject ) )
            {
            // InternalEFactory.g:80:2: ( (lv_imports_0_0= ruleNamespaceImport ) )*
            loop1:
            do {
                int alt1=2;
                int LA1_0 = input.LA(1);

                if ( (LA1_0==14) ) {
                    alt1=1;
                }


                switch (alt1) {
            	case 1 :
            	    // InternalEFactory.g:81:1: (lv_imports_0_0= ruleNamespaceImport )
            	    {
            	    // InternalEFactory.g:81:1: (lv_imports_0_0= ruleNamespaceImport )
            	    // InternalEFactory.g:82:3: lv_imports_0_0= ruleNamespaceImport
            	    {
            	    if ( state.backtracking==0 ) {
            	       
            	      	        newCompositeNode(grammarAccess.getFactoryAccess().getImportsNamespaceImportParserRuleCall_0_0()); 
            	      	    
            	    }
            	    pushFollow(FOLLOW_3);
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
            	              		"org.eclipse.emf.eson.EFactory.NamespaceImport");
            	      	        afterParserOrEnumRuleCall();
            	      	    
            	    }

            	    }


            	    }
            	    break;

            	default :
            	    break loop1;
                }
            } while (true);

            // InternalEFactory.g:98:3: ( (lv_ePackages_1_0= rulePackageImport ) )*
            loop2:
            do {
                int alt2=2;
                int LA2_0 = input.LA(1);

                if ( (LA2_0==13) ) {
                    alt2=1;
                }


                switch (alt2) {
            	case 1 :
            	    // InternalEFactory.g:99:1: (lv_ePackages_1_0= rulePackageImport )
            	    {
            	    // InternalEFactory.g:99:1: (lv_ePackages_1_0= rulePackageImport )
            	    // InternalEFactory.g:100:3: lv_ePackages_1_0= rulePackageImport
            	    {
            	    if ( state.backtracking==0 ) {
            	       
            	      	        newCompositeNode(grammarAccess.getFactoryAccess().getEPackagesPackageImportParserRuleCall_1_0()); 
            	      	    
            	    }
            	    pushFollow(FOLLOW_4);
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
            	              		"org.eclipse.emf.eson.EFactory.PackageImport");
            	      	        afterParserOrEnumRuleCall();
            	      	    
            	    }

            	    }


            	    }
            	    break;

            	default :
            	    break loop2;
                }
            } while (true);

            // InternalEFactory.g:116:3: ( (lv_annotations_2_0= ruleAnnotation ) )*
            loop3:
            do {
                int alt3=2;
                int LA3_0 = input.LA(1);

                if ( (LA3_0==15) ) {
                    alt3=1;
                }


                switch (alt3) {
            	case 1 :
            	    // InternalEFactory.g:117:1: (lv_annotations_2_0= ruleAnnotation )
            	    {
            	    // InternalEFactory.g:117:1: (lv_annotations_2_0= ruleAnnotation )
            	    // InternalEFactory.g:118:3: lv_annotations_2_0= ruleAnnotation
            	    {
            	    if ( state.backtracking==0 ) {
            	       
            	      	        newCompositeNode(grammarAccess.getFactoryAccess().getAnnotationsAnnotationParserRuleCall_2_0()); 
            	      	    
            	    }
            	    pushFollow(FOLLOW_4);
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
            	              		"org.eclipse.emf.eson.EFactory.Annotation");
            	      	        afterParserOrEnumRuleCall();
            	      	    
            	    }

            	    }


            	    }
            	    break;

            	default :
            	    break loop3;
                }
            } while (true);

            // InternalEFactory.g:134:3: ( (lv_root_3_0= ruleNewObject ) )
            // InternalEFactory.g:135:1: (lv_root_3_0= ruleNewObject )
            {
            // InternalEFactory.g:135:1: (lv_root_3_0= ruleNewObject )
            // InternalEFactory.g:136:3: lv_root_3_0= ruleNewObject
            {
            if ( state.backtracking==0 ) {
               
              	        newCompositeNode(grammarAccess.getFactoryAccess().getRootNewObjectParserRuleCall_3_0()); 
              	    
            }
            pushFollow(FOLLOW_2);
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
                      		"org.eclipse.emf.eson.EFactory.NewObject");
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
    // InternalEFactory.g:160:1: entryRulePackageImport returns [EObject current=null] : iv_rulePackageImport= rulePackageImport EOF ;
    public final EObject entryRulePackageImport() throws RecognitionException {
        EObject current = null;

        EObject iv_rulePackageImport = null;


        try {
            // InternalEFactory.g:161:2: (iv_rulePackageImport= rulePackageImport EOF )
            // InternalEFactory.g:162:2: iv_rulePackageImport= rulePackageImport EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getPackageImportRule()); 
            }
            pushFollow(FOLLOW_1);
            iv_rulePackageImport=rulePackageImport();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_rulePackageImport; 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return current;

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
    // InternalEFactory.g:169:1: rulePackageImport returns [EObject current=null] : (otherlv_0= 'use' ( ( ruleStringOrQualifiedNameWithWildcard ) ) ) ;
    public final EObject rulePackageImport() throws RecognitionException {
        EObject current = null;

        Token otherlv_0=null;

         enterRule(); 
            
        try {
            // InternalEFactory.g:172:28: ( (otherlv_0= 'use' ( ( ruleStringOrQualifiedNameWithWildcard ) ) ) )
            // InternalEFactory.g:173:1: (otherlv_0= 'use' ( ( ruleStringOrQualifiedNameWithWildcard ) ) )
            {
            // InternalEFactory.g:173:1: (otherlv_0= 'use' ( ( ruleStringOrQualifiedNameWithWildcard ) ) )
            // InternalEFactory.g:173:3: otherlv_0= 'use' ( ( ruleStringOrQualifiedNameWithWildcard ) )
            {
            otherlv_0=(Token)match(input,13,FOLLOW_5); if (state.failed) return current;
            if ( state.backtracking==0 ) {

                  	newLeafNode(otherlv_0, grammarAccess.getPackageImportAccess().getUseKeyword_0());
                  
            }
            // InternalEFactory.g:177:1: ( ( ruleStringOrQualifiedNameWithWildcard ) )
            // InternalEFactory.g:178:1: ( ruleStringOrQualifiedNameWithWildcard )
            {
            // InternalEFactory.g:178:1: ( ruleStringOrQualifiedNameWithWildcard )
            // InternalEFactory.g:179:3: ruleStringOrQualifiedNameWithWildcard
            {
            if ( state.backtracking==0 ) {

              			if (current==null) {
              	            current = createModelElement(grammarAccess.getPackageImportRule());
              	        }
                      
            }
            if ( state.backtracking==0 ) {
               
              	        newCompositeNode(grammarAccess.getPackageImportAccess().getEPackageEPackageCrossReference_1_0()); 
              	    
            }
            pushFollow(FOLLOW_2);
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
    // InternalEFactory.g:200:1: entryRuleNamespaceImport returns [EObject current=null] : iv_ruleNamespaceImport= ruleNamespaceImport EOF ;
    public final EObject entryRuleNamespaceImport() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleNamespaceImport = null;


        try {
            // InternalEFactory.g:201:2: (iv_ruleNamespaceImport= ruleNamespaceImport EOF )
            // InternalEFactory.g:202:2: iv_ruleNamespaceImport= ruleNamespaceImport EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getNamespaceImportRule()); 
            }
            pushFollow(FOLLOW_1);
            iv_ruleNamespaceImport=ruleNamespaceImport();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleNamespaceImport; 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return current;

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
    // InternalEFactory.g:209:1: ruleNamespaceImport returns [EObject current=null] : (otherlv_0= 'import' ( (lv_importedNamespace_1_0= ruleQualifiedNameWithWildcard ) ) ) ;
    public final EObject ruleNamespaceImport() throws RecognitionException {
        EObject current = null;

        Token otherlv_0=null;
        AntlrDatatypeRuleToken lv_importedNamespace_1_0 = null;


         enterRule(); 
            
        try {
            // InternalEFactory.g:212:28: ( (otherlv_0= 'import' ( (lv_importedNamespace_1_0= ruleQualifiedNameWithWildcard ) ) ) )
            // InternalEFactory.g:213:1: (otherlv_0= 'import' ( (lv_importedNamespace_1_0= ruleQualifiedNameWithWildcard ) ) )
            {
            // InternalEFactory.g:213:1: (otherlv_0= 'import' ( (lv_importedNamespace_1_0= ruleQualifiedNameWithWildcard ) ) )
            // InternalEFactory.g:213:3: otherlv_0= 'import' ( (lv_importedNamespace_1_0= ruleQualifiedNameWithWildcard ) )
            {
            otherlv_0=(Token)match(input,14,FOLLOW_5); if (state.failed) return current;
            if ( state.backtracking==0 ) {

                  	newLeafNode(otherlv_0, grammarAccess.getNamespaceImportAccess().getImportKeyword_0());
                  
            }
            // InternalEFactory.g:217:1: ( (lv_importedNamespace_1_0= ruleQualifiedNameWithWildcard ) )
            // InternalEFactory.g:218:1: (lv_importedNamespace_1_0= ruleQualifiedNameWithWildcard )
            {
            // InternalEFactory.g:218:1: (lv_importedNamespace_1_0= ruleQualifiedNameWithWildcard )
            // InternalEFactory.g:219:3: lv_importedNamespace_1_0= ruleQualifiedNameWithWildcard
            {
            if ( state.backtracking==0 ) {
               
              	        newCompositeNode(grammarAccess.getNamespaceImportAccess().getImportedNamespaceQualifiedNameWithWildcardParserRuleCall_1_0()); 
              	    
            }
            pushFollow(FOLLOW_2);
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
                      		"org.eclipse.emf.eson.EFactory.QualifiedNameWithWildcard");
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
    // InternalEFactory.g:243:1: entryRuleAnnotation returns [EObject current=null] : iv_ruleAnnotation= ruleAnnotation EOF ;
    public final EObject entryRuleAnnotation() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleAnnotation = null;


        try {
            // InternalEFactory.g:244:2: (iv_ruleAnnotation= ruleAnnotation EOF )
            // InternalEFactory.g:245:2: iv_ruleAnnotation= ruleAnnotation EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getAnnotationRule()); 
            }
            pushFollow(FOLLOW_1);
            iv_ruleAnnotation=ruleAnnotation();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleAnnotation; 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return current;

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
    // InternalEFactory.g:252:1: ruleAnnotation returns [EObject current=null] : this_CustomNameMapping_0= ruleCustomNameMapping ;
    public final EObject ruleAnnotation() throws RecognitionException {
        EObject current = null;

        EObject this_CustomNameMapping_0 = null;


         enterRule(); 
            
        try {
            // InternalEFactory.g:255:28: (this_CustomNameMapping_0= ruleCustomNameMapping )
            // InternalEFactory.g:257:5: this_CustomNameMapping_0= ruleCustomNameMapping
            {
            if ( state.backtracking==0 ) {
               
                      newCompositeNode(grammarAccess.getAnnotationAccess().getCustomNameMappingParserRuleCall()); 
                  
            }
            pushFollow(FOLLOW_2);
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
    // InternalEFactory.g:273:1: entryRuleCustomNameMapping returns [EObject current=null] : iv_ruleCustomNameMapping= ruleCustomNameMapping EOF ;
    public final EObject entryRuleCustomNameMapping() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleCustomNameMapping = null;


        try {
            // InternalEFactory.g:274:2: (iv_ruleCustomNameMapping= ruleCustomNameMapping EOF )
            // InternalEFactory.g:275:2: iv_ruleCustomNameMapping= ruleCustomNameMapping EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getCustomNameMappingRule()); 
            }
            pushFollow(FOLLOW_1);
            iv_ruleCustomNameMapping=ruleCustomNameMapping();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleCustomNameMapping; 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return current;

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
    // InternalEFactory.g:282:1: ruleCustomNameMapping returns [EObject current=null] : (otherlv_0= '@Name' otherlv_1= '{' ( ( ruleQualifiedName ) ) otherlv_3= '=' ( ( ruleQualifiedName ) ) otherlv_5= '}' ) ;
    public final EObject ruleCustomNameMapping() throws RecognitionException {
        EObject current = null;

        Token otherlv_0=null;
        Token otherlv_1=null;
        Token otherlv_3=null;
        Token otherlv_5=null;

         enterRule(); 
            
        try {
            // InternalEFactory.g:285:28: ( (otherlv_0= '@Name' otherlv_1= '{' ( ( ruleQualifiedName ) ) otherlv_3= '=' ( ( ruleQualifiedName ) ) otherlv_5= '}' ) )
            // InternalEFactory.g:286:1: (otherlv_0= '@Name' otherlv_1= '{' ( ( ruleQualifiedName ) ) otherlv_3= '=' ( ( ruleQualifiedName ) ) otherlv_5= '}' )
            {
            // InternalEFactory.g:286:1: (otherlv_0= '@Name' otherlv_1= '{' ( ( ruleQualifiedName ) ) otherlv_3= '=' ( ( ruleQualifiedName ) ) otherlv_5= '}' )
            // InternalEFactory.g:286:3: otherlv_0= '@Name' otherlv_1= '{' ( ( ruleQualifiedName ) ) otherlv_3= '=' ( ( ruleQualifiedName ) ) otherlv_5= '}'
            {
            otherlv_0=(Token)match(input,15,FOLLOW_6); if (state.failed) return current;
            if ( state.backtracking==0 ) {

                  	newLeafNode(otherlv_0, grammarAccess.getCustomNameMappingAccess().getNameKeyword_0());
                  
            }
            otherlv_1=(Token)match(input,16,FOLLOW_4); if (state.failed) return current;
            if ( state.backtracking==0 ) {

                  	newLeafNode(otherlv_1, grammarAccess.getCustomNameMappingAccess().getLeftCurlyBracketKeyword_1());
                  
            }
            // InternalEFactory.g:294:1: ( ( ruleQualifiedName ) )
            // InternalEFactory.g:295:1: ( ruleQualifiedName )
            {
            // InternalEFactory.g:295:1: ( ruleQualifiedName )
            // InternalEFactory.g:296:3: ruleQualifiedName
            {
            if ( state.backtracking==0 ) {

              			if (current==null) {
              	            current = createModelElement(grammarAccess.getCustomNameMappingRule());
              	        }
                      
            }
            if ( state.backtracking==0 ) {
               
              	        newCompositeNode(grammarAccess.getCustomNameMappingAccess().getEClassEClassCrossReference_2_0()); 
              	    
            }
            pushFollow(FOLLOW_7);
            ruleQualifiedName();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               
              	        afterParserOrEnumRuleCall();
              	    
            }

            }


            }

            otherlv_3=(Token)match(input,17,FOLLOW_4); if (state.failed) return current;
            if ( state.backtracking==0 ) {

                  	newLeafNode(otherlv_3, grammarAccess.getCustomNameMappingAccess().getEqualsSignKeyword_3());
                  
            }
            // InternalEFactory.g:313:1: ( ( ruleQualifiedName ) )
            // InternalEFactory.g:314:1: ( ruleQualifiedName )
            {
            // InternalEFactory.g:314:1: ( ruleQualifiedName )
            // InternalEFactory.g:315:3: ruleQualifiedName
            {
            if ( state.backtracking==0 ) {

              			if (current==null) {
              	            current = createModelElement(grammarAccess.getCustomNameMappingRule());
              	        }
                      
            }
            if ( state.backtracking==0 ) {
               
              	        newCompositeNode(grammarAccess.getCustomNameMappingAccess().getNameFeatureEAttributeCrossReference_4_0()); 
              	    
            }
            pushFollow(FOLLOW_8);
            ruleQualifiedName();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               
              	        afterParserOrEnumRuleCall();
              	    
            }

            }


            }

            otherlv_5=(Token)match(input,18,FOLLOW_2); if (state.failed) return current;
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
    // InternalEFactory.g:340:1: entryRuleFeature returns [EObject current=null] : iv_ruleFeature= ruleFeature EOF ;
    public final EObject entryRuleFeature() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleFeature = null;


        try {
            // InternalEFactory.g:341:2: (iv_ruleFeature= ruleFeature EOF )
            // InternalEFactory.g:342:2: iv_ruleFeature= ruleFeature EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getFeatureRule()); 
            }
            pushFollow(FOLLOW_1);
            iv_ruleFeature=ruleFeature();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleFeature; 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return current;

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
    // InternalEFactory.g:349:1: ruleFeature returns [EObject current=null] : ( ( ( ruleQualifiedName ) ) otherlv_1= ':' ( (lv_value_2_0= ruleValue ) )? ) ;
    public final EObject ruleFeature() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        EObject lv_value_2_0 = null;


         enterRule(); 
            
        try {
            // InternalEFactory.g:352:28: ( ( ( ( ruleQualifiedName ) ) otherlv_1= ':' ( (lv_value_2_0= ruleValue ) )? ) )
            // InternalEFactory.g:353:1: ( ( ( ruleQualifiedName ) ) otherlv_1= ':' ( (lv_value_2_0= ruleValue ) )? )
            {
            // InternalEFactory.g:353:1: ( ( ( ruleQualifiedName ) ) otherlv_1= ':' ( (lv_value_2_0= ruleValue ) )? )
            // InternalEFactory.g:353:2: ( ( ruleQualifiedName ) ) otherlv_1= ':' ( (lv_value_2_0= ruleValue ) )?
            {
            // InternalEFactory.g:353:2: ( ( ruleQualifiedName ) )
            // InternalEFactory.g:354:1: ( ruleQualifiedName )
            {
            // InternalEFactory.g:354:1: ( ruleQualifiedName )
            // InternalEFactory.g:355:3: ruleQualifiedName
            {
            if ( state.backtracking==0 ) {

              			if (current==null) {
              	            current = createModelElement(grammarAccess.getFeatureRule());
              	        }
                      
            }
            if ( state.backtracking==0 ) {
               
              	        newCompositeNode(grammarAccess.getFeatureAccess().getEFeatureEStructuralFeatureCrossReference_0_0()); 
              	    
            }
            pushFollow(FOLLOW_9);
            ruleQualifiedName();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               
              	        afterParserOrEnumRuleCall();
              	    
            }

            }


            }

            otherlv_1=(Token)match(input,19,FOLLOW_10); if (state.failed) return current;
            if ( state.backtracking==0 ) {

                  	newLeafNode(otherlv_1, grammarAccess.getFeatureAccess().getColonKeyword_1());
                  
            }
            // InternalEFactory.g:372:1: ( (lv_value_2_0= ruleValue ) )?
            int alt4=2;
            alt4 = dfa4.predict(input);
            switch (alt4) {
                case 1 :
                    // InternalEFactory.g:373:1: (lv_value_2_0= ruleValue )
                    {
                    // InternalEFactory.g:373:1: (lv_value_2_0= ruleValue )
                    // InternalEFactory.g:374:3: lv_value_2_0= ruleValue
                    {
                    if ( state.backtracking==0 ) {
                       
                      	        newCompositeNode(grammarAccess.getFeatureAccess().getValueValueParserRuleCall_2_0()); 
                      	    
                    }
                    pushFollow(FOLLOW_2);
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
                              		"org.eclipse.emf.eson.EFactory.Value");
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
    // InternalEFactory.g:398:1: entryRuleNewObject returns [EObject current=null] : iv_ruleNewObject= ruleNewObject EOF ;
    public final EObject entryRuleNewObject() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleNewObject = null;


        try {
            // InternalEFactory.g:399:2: (iv_ruleNewObject= ruleNewObject EOF )
            // InternalEFactory.g:400:2: iv_ruleNewObject= ruleNewObject EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getNewObjectRule()); 
            }
            pushFollow(FOLLOW_1);
            iv_ruleNewObject=ruleNewObject();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleNewObject; 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return current;

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
    // InternalEFactory.g:407:1: ruleNewObject returns [EObject current=null] : ( ( ( ruleQualifiedName ) ) ( (lv_name_1_0= ruleValidID ) )? otherlv_2= '{' ( (lv_features_3_0= ruleFeature ) )* otherlv_4= '}' ) ;
    public final EObject ruleNewObject() throws RecognitionException {
        EObject current = null;

        Token otherlv_2=null;
        Token otherlv_4=null;
        AntlrDatatypeRuleToken lv_name_1_0 = null;

        EObject lv_features_3_0 = null;


         enterRule(); 
            
        try {
            // InternalEFactory.g:410:28: ( ( ( ( ruleQualifiedName ) ) ( (lv_name_1_0= ruleValidID ) )? otherlv_2= '{' ( (lv_features_3_0= ruleFeature ) )* otherlv_4= '}' ) )
            // InternalEFactory.g:411:1: ( ( ( ruleQualifiedName ) ) ( (lv_name_1_0= ruleValidID ) )? otherlv_2= '{' ( (lv_features_3_0= ruleFeature ) )* otherlv_4= '}' )
            {
            // InternalEFactory.g:411:1: ( ( ( ruleQualifiedName ) ) ( (lv_name_1_0= ruleValidID ) )? otherlv_2= '{' ( (lv_features_3_0= ruleFeature ) )* otherlv_4= '}' )
            // InternalEFactory.g:411:2: ( ( ruleQualifiedName ) ) ( (lv_name_1_0= ruleValidID ) )? otherlv_2= '{' ( (lv_features_3_0= ruleFeature ) )* otherlv_4= '}'
            {
            // InternalEFactory.g:411:2: ( ( ruleQualifiedName ) )
            // InternalEFactory.g:412:1: ( ruleQualifiedName )
            {
            // InternalEFactory.g:412:1: ( ruleQualifiedName )
            // InternalEFactory.g:413:3: ruleQualifiedName
            {
            if ( state.backtracking==0 ) {

              			if (current==null) {
              	            current = createModelElement(grammarAccess.getNewObjectRule());
              	        }
                      
            }
            if ( state.backtracking==0 ) {
               
              	        newCompositeNode(grammarAccess.getNewObjectAccess().getEClassEClassCrossReference_0_0()); 
              	    
            }
            pushFollow(FOLLOW_11);
            ruleQualifiedName();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               
              	        afterParserOrEnumRuleCall();
              	    
            }

            }


            }

            // InternalEFactory.g:426:2: ( (lv_name_1_0= ruleValidID ) )?
            int alt5=2;
            int LA5_0 = input.LA(1);

            if ( ((LA5_0>=RULE_ID && LA5_0<=RULE_LONG_UNSIGNED)) ) {
                alt5=1;
            }
            switch (alt5) {
                case 1 :
                    // InternalEFactory.g:427:1: (lv_name_1_0= ruleValidID )
                    {
                    // InternalEFactory.g:427:1: (lv_name_1_0= ruleValidID )
                    // InternalEFactory.g:428:3: lv_name_1_0= ruleValidID
                    {
                    if ( state.backtracking==0 ) {
                       
                      	        newCompositeNode(grammarAccess.getNewObjectAccess().getNameValidIDParserRuleCall_1_0()); 
                      	    
                    }
                    pushFollow(FOLLOW_6);
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
                              		"org.eclipse.emf.eson.EFactory.ValidID");
                      	        afterParserOrEnumRuleCall();
                      	    
                    }

                    }


                    }
                    break;

            }

            otherlv_2=(Token)match(input,16,FOLLOW_12); if (state.failed) return current;
            if ( state.backtracking==0 ) {

                  	newLeafNode(otherlv_2, grammarAccess.getNewObjectAccess().getLeftCurlyBracketKeyword_2());
                  
            }
            // InternalEFactory.g:448:1: ( (lv_features_3_0= ruleFeature ) )*
            loop6:
            do {
                int alt6=2;
                int LA6_0 = input.LA(1);

                if ( ((LA6_0>=RULE_ID && LA6_0<=RULE_LONG_UNSIGNED)) ) {
                    alt6=1;
                }


                switch (alt6) {
            	case 1 :
            	    // InternalEFactory.g:449:1: (lv_features_3_0= ruleFeature )
            	    {
            	    // InternalEFactory.g:449:1: (lv_features_3_0= ruleFeature )
            	    // InternalEFactory.g:450:3: lv_features_3_0= ruleFeature
            	    {
            	    if ( state.backtracking==0 ) {
            	       
            	      	        newCompositeNode(grammarAccess.getNewObjectAccess().getFeaturesFeatureParserRuleCall_3_0()); 
            	      	    
            	    }
            	    pushFollow(FOLLOW_12);
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
            	              		"org.eclipse.emf.eson.EFactory.Feature");
            	      	        afterParserOrEnumRuleCall();
            	      	    
            	    }

            	    }


            	    }
            	    break;

            	default :
            	    break loop6;
                }
            } while (true);

            otherlv_4=(Token)match(input,18,FOLLOW_2); if (state.failed) return current;
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
    // InternalEFactory.g:478:1: entryRuleValue returns [EObject current=null] : iv_ruleValue= ruleValue EOF ;
    public final EObject entryRuleValue() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleValue = null;


        try {
            // InternalEFactory.g:479:2: (iv_ruleValue= ruleValue EOF )
            // InternalEFactory.g:480:2: iv_ruleValue= ruleValue EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getValueRule()); 
            }
            pushFollow(FOLLOW_1);
            iv_ruleValue=ruleValue();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleValue; 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return current;

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
    // InternalEFactory.g:487:1: ruleValue returns [EObject current=null] : (this_MultiValue_0= ruleMultiValue | this_Attribute_1= ruleAttribute | ( ( ( ( () ( ( ruleQualifiedName ) )? ( ( ruleValidID ) )? '{' ) )=> ( () ( ( ruleQualifiedName ) )? ( (lv_name_4_0= ruleValidID ) )? otherlv_5= '{' ) ) ( (lv_features_6_0= ruleFeature ) )* otherlv_7= '}' () ) | ( ( ruleReference )=>this_Reference_9= ruleReference ) ) ;
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
            // InternalEFactory.g:490:28: ( (this_MultiValue_0= ruleMultiValue | this_Attribute_1= ruleAttribute | ( ( ( ( () ( ( ruleQualifiedName ) )? ( ( ruleValidID ) )? '{' ) )=> ( () ( ( ruleQualifiedName ) )? ( (lv_name_4_0= ruleValidID ) )? otherlv_5= '{' ) ) ( (lv_features_6_0= ruleFeature ) )* otherlv_7= '}' () ) | ( ( ruleReference )=>this_Reference_9= ruleReference ) ) )
            // InternalEFactory.g:491:1: (this_MultiValue_0= ruleMultiValue | this_Attribute_1= ruleAttribute | ( ( ( ( () ( ( ruleQualifiedName ) )? ( ( ruleValidID ) )? '{' ) )=> ( () ( ( ruleQualifiedName ) )? ( (lv_name_4_0= ruleValidID ) )? otherlv_5= '{' ) ) ( (lv_features_6_0= ruleFeature ) )* otherlv_7= '}' () ) | ( ( ruleReference )=>this_Reference_9= ruleReference ) )
            {
            // InternalEFactory.g:491:1: (this_MultiValue_0= ruleMultiValue | this_Attribute_1= ruleAttribute | ( ( ( ( () ( ( ruleQualifiedName ) )? ( ( ruleValidID ) )? '{' ) )=> ( () ( ( ruleQualifiedName ) )? ( (lv_name_4_0= ruleValidID ) )? otherlv_5= '{' ) ) ( (lv_features_6_0= ruleFeature ) )* otherlv_7= '}' () ) | ( ( ruleReference )=>this_Reference_9= ruleReference ) )
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
                    // InternalEFactory.g:492:5: this_MultiValue_0= ruleMultiValue
                    {
                    if ( state.backtracking==0 ) {
                       
                              newCompositeNode(grammarAccess.getValueAccess().getMultiValueParserRuleCall_0()); 
                          
                    }
                    pushFollow(FOLLOW_2);
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
                    // InternalEFactory.g:502:5: this_Attribute_1= ruleAttribute
                    {
                    if ( state.backtracking==0 ) {
                       
                              newCompositeNode(grammarAccess.getValueAccess().getAttributeParserRuleCall_1()); 
                          
                    }
                    pushFollow(FOLLOW_2);
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
                    // InternalEFactory.g:511:6: ( ( ( ( () ( ( ruleQualifiedName ) )? ( ( ruleValidID ) )? '{' ) )=> ( () ( ( ruleQualifiedName ) )? ( (lv_name_4_0= ruleValidID ) )? otherlv_5= '{' ) ) ( (lv_features_6_0= ruleFeature ) )* otherlv_7= '}' () )
                    {
                    // InternalEFactory.g:511:6: ( ( ( ( () ( ( ruleQualifiedName ) )? ( ( ruleValidID ) )? '{' ) )=> ( () ( ( ruleQualifiedName ) )? ( (lv_name_4_0= ruleValidID ) )? otherlv_5= '{' ) ) ( (lv_features_6_0= ruleFeature ) )* otherlv_7= '}' () )
                    // InternalEFactory.g:511:7: ( ( ( () ( ( ruleQualifiedName ) )? ( ( ruleValidID ) )? '{' ) )=> ( () ( ( ruleQualifiedName ) )? ( (lv_name_4_0= ruleValidID ) )? otherlv_5= '{' ) ) ( (lv_features_6_0= ruleFeature ) )* otherlv_7= '}' ()
                    {
                    // InternalEFactory.g:511:7: ( ( ( () ( ( ruleQualifiedName ) )? ( ( ruleValidID ) )? '{' ) )=> ( () ( ( ruleQualifiedName ) )? ( (lv_name_4_0= ruleValidID ) )? otherlv_5= '{' ) )
                    // InternalEFactory.g:511:8: ( ( () ( ( ruleQualifiedName ) )? ( ( ruleValidID ) )? '{' ) )=> ( () ( ( ruleQualifiedName ) )? ( (lv_name_4_0= ruleValidID ) )? otherlv_5= '{' )
                    {
                    // InternalEFactory.g:521:5: ( () ( ( ruleQualifiedName ) )? ( (lv_name_4_0= ruleValidID ) )? otherlv_5= '{' )
                    // InternalEFactory.g:521:6: () ( ( ruleQualifiedName ) )? ( (lv_name_4_0= ruleValidID ) )? otherlv_5= '{'
                    {
                    // InternalEFactory.g:521:6: ()
                    // InternalEFactory.g:522:5: 
                    {
                    if ( state.backtracking==0 ) {

                              current = forceCreateModelElement(
                                  grammarAccess.getValueAccess().getNewObjectAction_2_0_0_0(),
                                  current);
                          
                    }

                    }

                    // InternalEFactory.g:527:2: ( ( ruleQualifiedName ) )?
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
                            // InternalEFactory.g:528:1: ( ruleQualifiedName )
                            {
                            // InternalEFactory.g:528:1: ( ruleQualifiedName )
                            // InternalEFactory.g:529:3: ruleQualifiedName
                            {
                            if ( state.backtracking==0 ) {

                              			if (current==null) {
                              	            current = createModelElement(grammarAccess.getValueRule());
                              	        }
                                      
                            }
                            if ( state.backtracking==0 ) {
                               
                              	        newCompositeNode(grammarAccess.getValueAccess().getEClassEClassCrossReference_2_0_0_1_0()); 
                              	    
                            }
                            pushFollow(FOLLOW_11);
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

                    // InternalEFactory.g:542:3: ( (lv_name_4_0= ruleValidID ) )?
                    int alt8=2;
                    int LA8_0 = input.LA(1);

                    if ( ((LA8_0>=RULE_ID && LA8_0<=RULE_LONG_UNSIGNED)) ) {
                        alt8=1;
                    }
                    switch (alt8) {
                        case 1 :
                            // InternalEFactory.g:543:1: (lv_name_4_0= ruleValidID )
                            {
                            // InternalEFactory.g:543:1: (lv_name_4_0= ruleValidID )
                            // InternalEFactory.g:544:3: lv_name_4_0= ruleValidID
                            {
                            if ( state.backtracking==0 ) {
                               
                              	        newCompositeNode(grammarAccess.getValueAccess().getNameValidIDParserRuleCall_2_0_0_2_0()); 
                              	    
                            }
                            pushFollow(FOLLOW_6);
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
                                      		"org.eclipse.emf.eson.EFactory.ValidID");
                              	        afterParserOrEnumRuleCall();
                              	    
                            }

                            }


                            }
                            break;

                    }

                    otherlv_5=(Token)match(input,16,FOLLOW_12); if (state.failed) return current;
                    if ( state.backtracking==0 ) {

                          	newLeafNode(otherlv_5, grammarAccess.getValueAccess().getLeftCurlyBracketKeyword_2_0_0_3());
                          
                    }

                    }


                    }

                    // InternalEFactory.g:564:3: ( (lv_features_6_0= ruleFeature ) )*
                    loop9:
                    do {
                        int alt9=2;
                        int LA9_0 = input.LA(1);

                        if ( ((LA9_0>=RULE_ID && LA9_0<=RULE_LONG_UNSIGNED)) ) {
                            alt9=1;
                        }


                        switch (alt9) {
                    	case 1 :
                    	    // InternalEFactory.g:565:1: (lv_features_6_0= ruleFeature )
                    	    {
                    	    // InternalEFactory.g:565:1: (lv_features_6_0= ruleFeature )
                    	    // InternalEFactory.g:566:3: lv_features_6_0= ruleFeature
                    	    {
                    	    if ( state.backtracking==0 ) {
                    	       
                    	      	        newCompositeNode(grammarAccess.getValueAccess().getFeaturesFeatureParserRuleCall_2_1_0()); 
                    	      	    
                    	    }
                    	    pushFollow(FOLLOW_12);
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
                    	              		"org.eclipse.emf.eson.EFactory.Feature");
                    	      	        afterParserOrEnumRuleCall();
                    	      	    
                    	    }

                    	    }


                    	    }
                    	    break;

                    	default :
                    	    break loop9;
                        }
                    } while (true);

                    otherlv_7=(Token)match(input,18,FOLLOW_2); if (state.failed) return current;
                    if ( state.backtracking==0 ) {

                          	newLeafNode(otherlv_7, grammarAccess.getValueAccess().getRightCurlyBracketKeyword_2_2());
                          
                    }
                    // InternalEFactory.g:586:1: ()
                    // InternalEFactory.g:587:5: 
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
                    // InternalEFactory.g:593:6: ( ( ruleReference )=>this_Reference_9= ruleReference )
                    {
                    // InternalEFactory.g:593:6: ( ( ruleReference )=>this_Reference_9= ruleReference )
                    // InternalEFactory.g:593:7: ( ruleReference )=>this_Reference_9= ruleReference
                    {
                    if ( state.backtracking==0 ) {
                       
                              newCompositeNode(grammarAccess.getValueAccess().getReferenceParserRuleCall_3()); 
                          
                    }
                    pushFollow(FOLLOW_2);
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
    // InternalEFactory.g:610:1: entryRuleMultiValue returns [EObject current=null] : iv_ruleMultiValue= ruleMultiValue EOF ;
    public final EObject entryRuleMultiValue() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleMultiValue = null;


        try {
            // InternalEFactory.g:611:2: (iv_ruleMultiValue= ruleMultiValue EOF )
            // InternalEFactory.g:612:2: iv_ruleMultiValue= ruleMultiValue EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getMultiValueRule()); 
            }
            pushFollow(FOLLOW_1);
            iv_ruleMultiValue=ruleMultiValue();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleMultiValue; 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return current;

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
    // InternalEFactory.g:619:1: ruleMultiValue returns [EObject current=null] : ( () otherlv_1= '[' ( (lv_values_2_0= ruleValue ) )* otherlv_3= ']' ) ;
    public final EObject ruleMultiValue() throws RecognitionException {
        EObject current = null;

        Token otherlv_1=null;
        Token otherlv_3=null;
        EObject lv_values_2_0 = null;


         enterRule(); 
            
        try {
            // InternalEFactory.g:622:28: ( ( () otherlv_1= '[' ( (lv_values_2_0= ruleValue ) )* otherlv_3= ']' ) )
            // InternalEFactory.g:623:1: ( () otherlv_1= '[' ( (lv_values_2_0= ruleValue ) )* otherlv_3= ']' )
            {
            // InternalEFactory.g:623:1: ( () otherlv_1= '[' ( (lv_values_2_0= ruleValue ) )* otherlv_3= ']' )
            // InternalEFactory.g:623:2: () otherlv_1= '[' ( (lv_values_2_0= ruleValue ) )* otherlv_3= ']'
            {
            // InternalEFactory.g:623:2: ()
            // InternalEFactory.g:624:5: 
            {
            if ( state.backtracking==0 ) {

                      current = forceCreateModelElement(
                          grammarAccess.getMultiValueAccess().getMultiValueAction_0(),
                          current);
                  
            }

            }

            otherlv_1=(Token)match(input,20,FOLLOW_13); if (state.failed) return current;
            if ( state.backtracking==0 ) {

                  	newLeafNode(otherlv_1, grammarAccess.getMultiValueAccess().getLeftSquareBracketKeyword_1());
                  
            }
            // InternalEFactory.g:633:1: ( (lv_values_2_0= ruleValue ) )*
            loop11:
            do {
                int alt11=2;
                int LA11_0 = input.LA(1);

                if ( ((LA11_0>=RULE_STRING && LA11_0<=RULE_LONG_UNSIGNED)||LA11_0==16||(LA11_0>=19 && LA11_0<=20)||LA11_0==22||(LA11_0>=25 && LA11_0<=27)) ) {
                    alt11=1;
                }


                switch (alt11) {
            	case 1 :
            	    // InternalEFactory.g:634:1: (lv_values_2_0= ruleValue )
            	    {
            	    // InternalEFactory.g:634:1: (lv_values_2_0= ruleValue )
            	    // InternalEFactory.g:635:3: lv_values_2_0= ruleValue
            	    {
            	    if ( state.backtracking==0 ) {
            	       
            	      	        newCompositeNode(grammarAccess.getMultiValueAccess().getValuesValueParserRuleCall_2_0()); 
            	      	    
            	    }
            	    pushFollow(FOLLOW_13);
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
            	              		"org.eclipse.emf.eson.EFactory.Value");
            	      	        afterParserOrEnumRuleCall();
            	      	    
            	    }

            	    }


            	    }
            	    break;

            	default :
            	    break loop11;
                }
            } while (true);

            otherlv_3=(Token)match(input,21,FOLLOW_2); if (state.failed) return current;
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
    // InternalEFactory.g:663:1: entryRuleReference returns [EObject current=null] : iv_ruleReference= ruleReference EOF ;
    public final EObject entryRuleReference() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleReference = null;


        try {
            // InternalEFactory.g:664:2: (iv_ruleReference= ruleReference EOF )
            // InternalEFactory.g:665:2: iv_ruleReference= ruleReference EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getReferenceRule()); 
            }
            pushFollow(FOLLOW_1);
            iv_ruleReference=ruleReference();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleReference; 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return current;

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
    // InternalEFactory.g:672:1: ruleReference returns [EObject current=null] : ( ( ruleQualifiedName ) ) ;
    public final EObject ruleReference() throws RecognitionException {
        EObject current = null;

         enterRule(); 
            
        try {
            // InternalEFactory.g:675:28: ( ( ( ruleQualifiedName ) ) )
            // InternalEFactory.g:676:1: ( ( ruleQualifiedName ) )
            {
            // InternalEFactory.g:676:1: ( ( ruleQualifiedName ) )
            // InternalEFactory.g:677:1: ( ruleQualifiedName )
            {
            // InternalEFactory.g:677:1: ( ruleQualifiedName )
            // InternalEFactory.g:678:3: ruleQualifiedName
            {
            if ( state.backtracking==0 ) {

              			if (current==null) {
              	            current = createModelElement(grammarAccess.getReferenceRule());
              	        }
                      
            }
            if ( state.backtracking==0 ) {
               
              	        newCompositeNode(grammarAccess.getReferenceAccess().getValueEObjectCrossReference_0()); 
              	    
            }
            pushFollow(FOLLOW_2);
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
    // InternalEFactory.g:699:1: entryRuleAttribute returns [EObject current=null] : iv_ruleAttribute= ruleAttribute EOF ;
    public final EObject entryRuleAttribute() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleAttribute = null;


        try {
            // InternalEFactory.g:700:2: (iv_ruleAttribute= ruleAttribute EOF )
            // InternalEFactory.g:701:2: iv_ruleAttribute= ruleAttribute EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getAttributeRule()); 
            }
            pushFollow(FOLLOW_1);
            iv_ruleAttribute=ruleAttribute();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleAttribute; 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return current;

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
    // InternalEFactory.g:708:1: ruleAttribute returns [EObject current=null] : (this_EnumAttribute_0= ruleEnumAttribute | this_StringAttribute_1= ruleStringAttribute | this_IntegerAttribute_2= ruleIntegerAttribute | this_BooleanAttribute_3= ruleBooleanAttribute | this_DoubleAttribute_4= ruleDoubleAttribute | this_DateAttribute_5= ruleDateAttribute | this_NullAttribute_6= ruleNullAttribute ) ;
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
            // InternalEFactory.g:711:28: ( (this_EnumAttribute_0= ruleEnumAttribute | this_StringAttribute_1= ruleStringAttribute | this_IntegerAttribute_2= ruleIntegerAttribute | this_BooleanAttribute_3= ruleBooleanAttribute | this_DoubleAttribute_4= ruleDoubleAttribute | this_DateAttribute_5= ruleDateAttribute | this_NullAttribute_6= ruleNullAttribute ) )
            // InternalEFactory.g:712:1: (this_EnumAttribute_0= ruleEnumAttribute | this_StringAttribute_1= ruleStringAttribute | this_IntegerAttribute_2= ruleIntegerAttribute | this_BooleanAttribute_3= ruleBooleanAttribute | this_DoubleAttribute_4= ruleDoubleAttribute | this_DateAttribute_5= ruleDateAttribute | this_NullAttribute_6= ruleNullAttribute )
            {
            // InternalEFactory.g:712:1: (this_EnumAttribute_0= ruleEnumAttribute | this_StringAttribute_1= ruleStringAttribute | this_IntegerAttribute_2= ruleIntegerAttribute | this_BooleanAttribute_3= ruleBooleanAttribute | this_DoubleAttribute_4= ruleDoubleAttribute | this_DateAttribute_5= ruleDateAttribute | this_NullAttribute_6= ruleNullAttribute )
            int alt12=7;
            alt12 = dfa12.predict(input);
            switch (alt12) {
                case 1 :
                    // InternalEFactory.g:713:5: this_EnumAttribute_0= ruleEnumAttribute
                    {
                    if ( state.backtracking==0 ) {
                       
                              newCompositeNode(grammarAccess.getAttributeAccess().getEnumAttributeParserRuleCall_0()); 
                          
                    }
                    pushFollow(FOLLOW_2);
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
                    // InternalEFactory.g:723:5: this_StringAttribute_1= ruleStringAttribute
                    {
                    if ( state.backtracking==0 ) {
                       
                              newCompositeNode(grammarAccess.getAttributeAccess().getStringAttributeParserRuleCall_1()); 
                          
                    }
                    pushFollow(FOLLOW_2);
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
                    // InternalEFactory.g:733:5: this_IntegerAttribute_2= ruleIntegerAttribute
                    {
                    if ( state.backtracking==0 ) {
                       
                              newCompositeNode(grammarAccess.getAttributeAccess().getIntegerAttributeParserRuleCall_2()); 
                          
                    }
                    pushFollow(FOLLOW_2);
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
                    // InternalEFactory.g:743:5: this_BooleanAttribute_3= ruleBooleanAttribute
                    {
                    if ( state.backtracking==0 ) {
                       
                              newCompositeNode(grammarAccess.getAttributeAccess().getBooleanAttributeParserRuleCall_3()); 
                          
                    }
                    pushFollow(FOLLOW_2);
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
                    // InternalEFactory.g:753:5: this_DoubleAttribute_4= ruleDoubleAttribute
                    {
                    if ( state.backtracking==0 ) {
                       
                              newCompositeNode(grammarAccess.getAttributeAccess().getDoubleAttributeParserRuleCall_4()); 
                          
                    }
                    pushFollow(FOLLOW_2);
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
                    // InternalEFactory.g:763:5: this_DateAttribute_5= ruleDateAttribute
                    {
                    if ( state.backtracking==0 ) {
                       
                              newCompositeNode(grammarAccess.getAttributeAccess().getDateAttributeParserRuleCall_5()); 
                          
                    }
                    pushFollow(FOLLOW_2);
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
                    // InternalEFactory.g:773:5: this_NullAttribute_6= ruleNullAttribute
                    {
                    if ( state.backtracking==0 ) {
                       
                              newCompositeNode(grammarAccess.getAttributeAccess().getNullAttributeParserRuleCall_6()); 
                          
                    }
                    pushFollow(FOLLOW_2);
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
    // InternalEFactory.g:789:1: entryRuleEnumAttribute returns [EObject current=null] : iv_ruleEnumAttribute= ruleEnumAttribute EOF ;
    public final EObject entryRuleEnumAttribute() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleEnumAttribute = null;


        try {
            // InternalEFactory.g:790:2: (iv_ruleEnumAttribute= ruleEnumAttribute EOF )
            // InternalEFactory.g:791:2: iv_ruleEnumAttribute= ruleEnumAttribute EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getEnumAttributeRule()); 
            }
            pushFollow(FOLLOW_1);
            iv_ruleEnumAttribute=ruleEnumAttribute();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleEnumAttribute; 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return current;

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
    // InternalEFactory.g:798:1: ruleEnumAttribute returns [EObject current=null] : (otherlv_0= ':' ( ( ruleQualifiedName ) ) ) ;
    public final EObject ruleEnumAttribute() throws RecognitionException {
        EObject current = null;

        Token otherlv_0=null;

         enterRule(); 
            
        try {
            // InternalEFactory.g:801:28: ( (otherlv_0= ':' ( ( ruleQualifiedName ) ) ) )
            // InternalEFactory.g:802:1: (otherlv_0= ':' ( ( ruleQualifiedName ) ) )
            {
            // InternalEFactory.g:802:1: (otherlv_0= ':' ( ( ruleQualifiedName ) ) )
            // InternalEFactory.g:802:3: otherlv_0= ':' ( ( ruleQualifiedName ) )
            {
            otherlv_0=(Token)match(input,19,FOLLOW_4); if (state.failed) return current;
            if ( state.backtracking==0 ) {

                  	newLeafNode(otherlv_0, grammarAccess.getEnumAttributeAccess().getColonKeyword_0());
                  
            }
            // InternalEFactory.g:806:1: ( ( ruleQualifiedName ) )
            // InternalEFactory.g:807:1: ( ruleQualifiedName )
            {
            // InternalEFactory.g:807:1: ( ruleQualifiedName )
            // InternalEFactory.g:808:3: ruleQualifiedName
            {
            if ( state.backtracking==0 ) {

              			if (current==null) {
              	            current = createModelElement(grammarAccess.getEnumAttributeRule());
              	        }
                      
            }
            if ( state.backtracking==0 ) {
               
              	        newCompositeNode(grammarAccess.getEnumAttributeAccess().getValueEEnumLiteralCrossReference_1_0()); 
              	    
            }
            pushFollow(FOLLOW_2);
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
    // InternalEFactory.g:829:1: entryRuleStringAttribute returns [EObject current=null] : iv_ruleStringAttribute= ruleStringAttribute EOF ;
    public final EObject entryRuleStringAttribute() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleStringAttribute = null;


        try {
            // InternalEFactory.g:830:2: (iv_ruleStringAttribute= ruleStringAttribute EOF )
            // InternalEFactory.g:831:2: iv_ruleStringAttribute= ruleStringAttribute EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getStringAttributeRule()); 
            }
            pushFollow(FOLLOW_1);
            iv_ruleStringAttribute=ruleStringAttribute();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleStringAttribute; 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return current;

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
    // InternalEFactory.g:838:1: ruleStringAttribute returns [EObject current=null] : ( (lv_value_0_0= RULE_STRING ) ) ;
    public final EObject ruleStringAttribute() throws RecognitionException {
        EObject current = null;

        Token lv_value_0_0=null;

         enterRule(); 
            
        try {
            // InternalEFactory.g:841:28: ( ( (lv_value_0_0= RULE_STRING ) ) )
            // InternalEFactory.g:842:1: ( (lv_value_0_0= RULE_STRING ) )
            {
            // InternalEFactory.g:842:1: ( (lv_value_0_0= RULE_STRING ) )
            // InternalEFactory.g:843:1: (lv_value_0_0= RULE_STRING )
            {
            // InternalEFactory.g:843:1: (lv_value_0_0= RULE_STRING )
            // InternalEFactory.g:844:3: lv_value_0_0= RULE_STRING
            {
            lv_value_0_0=(Token)match(input,RULE_STRING,FOLLOW_2); if (state.failed) return current;
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
                      		"org.eclipse.xtext.common.Terminals.STRING");
              	    
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
    // InternalEFactory.g:868:1: entryRuleIntegerAttribute returns [EObject current=null] : iv_ruleIntegerAttribute= ruleIntegerAttribute EOF ;
    public final EObject entryRuleIntegerAttribute() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleIntegerAttribute = null;


        try {
            // InternalEFactory.g:869:2: (iv_ruleIntegerAttribute= ruleIntegerAttribute EOF )
            // InternalEFactory.g:870:2: iv_ruleIntegerAttribute= ruleIntegerAttribute EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getIntegerAttributeRule()); 
            }
            pushFollow(FOLLOW_1);
            iv_ruleIntegerAttribute=ruleIntegerAttribute();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleIntegerAttribute; 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return current;

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
    // InternalEFactory.g:877:1: ruleIntegerAttribute returns [EObject current=null] : ( (lv_value_0_0= ruleLong ) ) ;
    public final EObject ruleIntegerAttribute() throws RecognitionException {
        EObject current = null;

        AntlrDatatypeRuleToken lv_value_0_0 = null;


         enterRule(); 
            
        try {
            // InternalEFactory.g:880:28: ( ( (lv_value_0_0= ruleLong ) ) )
            // InternalEFactory.g:881:1: ( (lv_value_0_0= ruleLong ) )
            {
            // InternalEFactory.g:881:1: ( (lv_value_0_0= ruleLong ) )
            // InternalEFactory.g:882:1: (lv_value_0_0= ruleLong )
            {
            // InternalEFactory.g:882:1: (lv_value_0_0= ruleLong )
            // InternalEFactory.g:883:3: lv_value_0_0= ruleLong
            {
            if ( state.backtracking==0 ) {
               
              	        newCompositeNode(grammarAccess.getIntegerAttributeAccess().getValueLongParserRuleCall_0()); 
              	    
            }
            pushFollow(FOLLOW_2);
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
                      		"org.eclipse.emf.eson.EFactory.Long");
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
    // InternalEFactory.g:907:1: entryRuleDoubleAttribute returns [EObject current=null] : iv_ruleDoubleAttribute= ruleDoubleAttribute EOF ;
    public final EObject entryRuleDoubleAttribute() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleDoubleAttribute = null;


        try {
            // InternalEFactory.g:908:2: (iv_ruleDoubleAttribute= ruleDoubleAttribute EOF )
            // InternalEFactory.g:909:2: iv_ruleDoubleAttribute= ruleDoubleAttribute EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getDoubleAttributeRule()); 
            }
            pushFollow(FOLLOW_1);
            iv_ruleDoubleAttribute=ruleDoubleAttribute();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleDoubleAttribute; 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return current;

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
    // InternalEFactory.g:916:1: ruleDoubleAttribute returns [EObject current=null] : ( (lv_value_0_0= ruleDouble ) ) ;
    public final EObject ruleDoubleAttribute() throws RecognitionException {
        EObject current = null;

        AntlrDatatypeRuleToken lv_value_0_0 = null;


         enterRule(); 
            
        try {
            // InternalEFactory.g:919:28: ( ( (lv_value_0_0= ruleDouble ) ) )
            // InternalEFactory.g:920:1: ( (lv_value_0_0= ruleDouble ) )
            {
            // InternalEFactory.g:920:1: ( (lv_value_0_0= ruleDouble ) )
            // InternalEFactory.g:921:1: (lv_value_0_0= ruleDouble )
            {
            // InternalEFactory.g:921:1: (lv_value_0_0= ruleDouble )
            // InternalEFactory.g:922:3: lv_value_0_0= ruleDouble
            {
            if ( state.backtracking==0 ) {
               
              	        newCompositeNode(grammarAccess.getDoubleAttributeAccess().getValueDoubleParserRuleCall_0()); 
              	    
            }
            pushFollow(FOLLOW_2);
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
                      		"org.eclipse.emf.eson.EFactory.Double");
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
    // InternalEFactory.g:946:1: entryRuleDateAttribute returns [EObject current=null] : iv_ruleDateAttribute= ruleDateAttribute EOF ;
    public final EObject entryRuleDateAttribute() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleDateAttribute = null;


        try {
            // InternalEFactory.g:947:2: (iv_ruleDateAttribute= ruleDateAttribute EOF )
            // InternalEFactory.g:948:2: iv_ruleDateAttribute= ruleDateAttribute EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getDateAttributeRule()); 
            }
            pushFollow(FOLLOW_1);
            iv_ruleDateAttribute=ruleDateAttribute();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleDateAttribute; 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return current;

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
    // InternalEFactory.g:955:1: ruleDateAttribute returns [EObject current=null] : ( (lv_value_0_0= ruleDate ) ) ;
    public final EObject ruleDateAttribute() throws RecognitionException {
        EObject current = null;

        AntlrDatatypeRuleToken lv_value_0_0 = null;


         enterRule(); 
            
        try {
            // InternalEFactory.g:958:28: ( ( (lv_value_0_0= ruleDate ) ) )
            // InternalEFactory.g:959:1: ( (lv_value_0_0= ruleDate ) )
            {
            // InternalEFactory.g:959:1: ( (lv_value_0_0= ruleDate ) )
            // InternalEFactory.g:960:1: (lv_value_0_0= ruleDate )
            {
            // InternalEFactory.g:960:1: (lv_value_0_0= ruleDate )
            // InternalEFactory.g:961:3: lv_value_0_0= ruleDate
            {
            if ( state.backtracking==0 ) {
               
              	        newCompositeNode(grammarAccess.getDateAttributeAccess().getValueDateParserRuleCall_0()); 
              	    
            }
            pushFollow(FOLLOW_2);
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
                      		"org.eclipse.emf.eson.EFactory.Date");
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
    // InternalEFactory.g:985:1: entryRuleNullAttribute returns [EObject current=null] : iv_ruleNullAttribute= ruleNullAttribute EOF ;
    public final EObject entryRuleNullAttribute() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleNullAttribute = null;


        try {
            // InternalEFactory.g:986:2: (iv_ruleNullAttribute= ruleNullAttribute EOF )
            // InternalEFactory.g:987:2: iv_ruleNullAttribute= ruleNullAttribute EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getNullAttributeRule()); 
            }
            pushFollow(FOLLOW_1);
            iv_ruleNullAttribute=ruleNullAttribute();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleNullAttribute; 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return current;

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
    // InternalEFactory.g:994:1: ruleNullAttribute returns [EObject current=null] : ( (lv_value_0_0= 'NULL' ) ) ;
    public final EObject ruleNullAttribute() throws RecognitionException {
        EObject current = null;

        Token lv_value_0_0=null;

         enterRule(); 
            
        try {
            // InternalEFactory.g:997:28: ( ( (lv_value_0_0= 'NULL' ) ) )
            // InternalEFactory.g:998:1: ( (lv_value_0_0= 'NULL' ) )
            {
            // InternalEFactory.g:998:1: ( (lv_value_0_0= 'NULL' ) )
            // InternalEFactory.g:999:1: (lv_value_0_0= 'NULL' )
            {
            // InternalEFactory.g:999:1: (lv_value_0_0= 'NULL' )
            // InternalEFactory.g:1000:3: lv_value_0_0= 'NULL'
            {
            lv_value_0_0=(Token)match(input,22,FOLLOW_2); if (state.failed) return current;
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
    // InternalEFactory.g:1021:1: entryRuleBooleanAttribute returns [EObject current=null] : iv_ruleBooleanAttribute= ruleBooleanAttribute EOF ;
    public final EObject entryRuleBooleanAttribute() throws RecognitionException {
        EObject current = null;

        EObject iv_ruleBooleanAttribute = null;


        try {
            // InternalEFactory.g:1022:2: (iv_ruleBooleanAttribute= ruleBooleanAttribute EOF )
            // InternalEFactory.g:1023:2: iv_ruleBooleanAttribute= ruleBooleanAttribute EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getBooleanAttributeRule()); 
            }
            pushFollow(FOLLOW_1);
            iv_ruleBooleanAttribute=ruleBooleanAttribute();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleBooleanAttribute; 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return current;

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
    // InternalEFactory.g:1030:1: ruleBooleanAttribute returns [EObject current=null] : ( (lv_value_0_0= ruleBoolean ) ) ;
    public final EObject ruleBooleanAttribute() throws RecognitionException {
        EObject current = null;

        AntlrDatatypeRuleToken lv_value_0_0 = null;


         enterRule(); 
            
        try {
            // InternalEFactory.g:1033:28: ( ( (lv_value_0_0= ruleBoolean ) ) )
            // InternalEFactory.g:1034:1: ( (lv_value_0_0= ruleBoolean ) )
            {
            // InternalEFactory.g:1034:1: ( (lv_value_0_0= ruleBoolean ) )
            // InternalEFactory.g:1035:1: (lv_value_0_0= ruleBoolean )
            {
            // InternalEFactory.g:1035:1: (lv_value_0_0= ruleBoolean )
            // InternalEFactory.g:1036:3: lv_value_0_0= ruleBoolean
            {
            if ( state.backtracking==0 ) {
               
              	        newCompositeNode(grammarAccess.getBooleanAttributeAccess().getValueBooleanParserRuleCall_0()); 
              	    
            }
            pushFollow(FOLLOW_2);
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
                      		"org.eclipse.emf.eson.EFactory.Boolean");
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
    // InternalEFactory.g:1060:1: entryRuleStringOrQualifiedNameWithWildcard returns [String current=null] : iv_ruleStringOrQualifiedNameWithWildcard= ruleStringOrQualifiedNameWithWildcard EOF ;
    public final String entryRuleStringOrQualifiedNameWithWildcard() throws RecognitionException {
        String current = null;

        AntlrDatatypeRuleToken iv_ruleStringOrQualifiedNameWithWildcard = null;


        try {
            // InternalEFactory.g:1061:2: (iv_ruleStringOrQualifiedNameWithWildcard= ruleStringOrQualifiedNameWithWildcard EOF )
            // InternalEFactory.g:1062:2: iv_ruleStringOrQualifiedNameWithWildcard= ruleStringOrQualifiedNameWithWildcard EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getStringOrQualifiedNameWithWildcardRule()); 
            }
            pushFollow(FOLLOW_1);
            iv_ruleStringOrQualifiedNameWithWildcard=ruleStringOrQualifiedNameWithWildcard();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleStringOrQualifiedNameWithWildcard.getText(); 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return current;

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
    // InternalEFactory.g:1069:1: ruleStringOrQualifiedNameWithWildcard returns [AntlrDatatypeRuleToken current=new AntlrDatatypeRuleToken()] : (this_STRING_0= RULE_STRING | this_QualifiedNameWithWildcard_1= ruleQualifiedNameWithWildcard ) ;
    public final AntlrDatatypeRuleToken ruleStringOrQualifiedNameWithWildcard() throws RecognitionException {
        AntlrDatatypeRuleToken current = new AntlrDatatypeRuleToken();

        Token this_STRING_0=null;
        AntlrDatatypeRuleToken this_QualifiedNameWithWildcard_1 = null;


         enterRule(); 
            
        try {
            // InternalEFactory.g:1072:28: ( (this_STRING_0= RULE_STRING | this_QualifiedNameWithWildcard_1= ruleQualifiedNameWithWildcard ) )
            // InternalEFactory.g:1073:1: (this_STRING_0= RULE_STRING | this_QualifiedNameWithWildcard_1= ruleQualifiedNameWithWildcard )
            {
            // InternalEFactory.g:1073:1: (this_STRING_0= RULE_STRING | this_QualifiedNameWithWildcard_1= ruleQualifiedNameWithWildcard )
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
                    // InternalEFactory.g:1073:6: this_STRING_0= RULE_STRING
                    {
                    this_STRING_0=(Token)match(input,RULE_STRING,FOLLOW_2); if (state.failed) return current;
                    if ( state.backtracking==0 ) {

                      		current.merge(this_STRING_0);
                          
                    }
                    if ( state.backtracking==0 ) {
                       
                          newLeafNode(this_STRING_0, grammarAccess.getStringOrQualifiedNameWithWildcardAccess().getSTRINGTerminalRuleCall_0()); 
                          
                    }

                    }
                    break;
                case 2 :
                    // InternalEFactory.g:1082:5: this_QualifiedNameWithWildcard_1= ruleQualifiedNameWithWildcard
                    {
                    if ( state.backtracking==0 ) {
                       
                              newCompositeNode(grammarAccess.getStringOrQualifiedNameWithWildcardAccess().getQualifiedNameWithWildcardParserRuleCall_1()); 
                          
                    }
                    pushFollow(FOLLOW_2);
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
    // InternalEFactory.g:1100:1: entryRuleQualifiedNameWithWildcard returns [String current=null] : iv_ruleQualifiedNameWithWildcard= ruleQualifiedNameWithWildcard EOF ;
    public final String entryRuleQualifiedNameWithWildcard() throws RecognitionException {
        String current = null;

        AntlrDatatypeRuleToken iv_ruleQualifiedNameWithWildcard = null;


        try {
            // InternalEFactory.g:1101:2: (iv_ruleQualifiedNameWithWildcard= ruleQualifiedNameWithWildcard EOF )
            // InternalEFactory.g:1102:2: iv_ruleQualifiedNameWithWildcard= ruleQualifiedNameWithWildcard EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getQualifiedNameWithWildcardRule()); 
            }
            pushFollow(FOLLOW_1);
            iv_ruleQualifiedNameWithWildcard=ruleQualifiedNameWithWildcard();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleQualifiedNameWithWildcard.getText(); 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return current;

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
    // InternalEFactory.g:1109:1: ruleQualifiedNameWithWildcard returns [AntlrDatatypeRuleToken current=new AntlrDatatypeRuleToken()] : (this_QualifiedName_0= ruleQualifiedName (kw= '.*' )? ) ;
    public final AntlrDatatypeRuleToken ruleQualifiedNameWithWildcard() throws RecognitionException {
        AntlrDatatypeRuleToken current = new AntlrDatatypeRuleToken();

        Token kw=null;
        AntlrDatatypeRuleToken this_QualifiedName_0 = null;


         enterRule(); 
            
        try {
            // InternalEFactory.g:1112:28: ( (this_QualifiedName_0= ruleQualifiedName (kw= '.*' )? ) )
            // InternalEFactory.g:1113:1: (this_QualifiedName_0= ruleQualifiedName (kw= '.*' )? )
            {
            // InternalEFactory.g:1113:1: (this_QualifiedName_0= ruleQualifiedName (kw= '.*' )? )
            // InternalEFactory.g:1114:5: this_QualifiedName_0= ruleQualifiedName (kw= '.*' )?
            {
            if ( state.backtracking==0 ) {
               
                      newCompositeNode(grammarAccess.getQualifiedNameWithWildcardAccess().getQualifiedNameParserRuleCall_0()); 
                  
            }
            pushFollow(FOLLOW_14);
            this_QualifiedName_0=ruleQualifiedName();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {

              		current.merge(this_QualifiedName_0);
                  
            }
            if ( state.backtracking==0 ) {
               
                      afterParserOrEnumRuleCall();
                  
            }
            // InternalEFactory.g:1124:1: (kw= '.*' )?
            int alt14=2;
            int LA14_0 = input.LA(1);

            if ( (LA14_0==23) ) {
                alt14=1;
            }
            switch (alt14) {
                case 1 :
                    // InternalEFactory.g:1125:2: kw= '.*'
                    {
                    kw=(Token)match(input,23,FOLLOW_2); if (state.failed) return current;
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
    // InternalEFactory.g:1138:1: entryRuleQualifiedName returns [String current=null] : iv_ruleQualifiedName= ruleQualifiedName EOF ;
    public final String entryRuleQualifiedName() throws RecognitionException {
        String current = null;

        AntlrDatatypeRuleToken iv_ruleQualifiedName = null;


        try {
            // InternalEFactory.g:1139:2: (iv_ruleQualifiedName= ruleQualifiedName EOF )
            // InternalEFactory.g:1140:2: iv_ruleQualifiedName= ruleQualifiedName EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getQualifiedNameRule()); 
            }
            pushFollow(FOLLOW_1);
            iv_ruleQualifiedName=ruleQualifiedName();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleQualifiedName.getText(); 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return current;

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
    // InternalEFactory.g:1147:1: ruleQualifiedName returns [AntlrDatatypeRuleToken current=new AntlrDatatypeRuleToken()] : (this_ValidID_0= ruleValidID ( ( ( '.' )=>kw= '.' ) this_ValidID_2= ruleValidID )* ) ;
    public final AntlrDatatypeRuleToken ruleQualifiedName() throws RecognitionException {
        AntlrDatatypeRuleToken current = new AntlrDatatypeRuleToken();

        Token kw=null;
        AntlrDatatypeRuleToken this_ValidID_0 = null;

        AntlrDatatypeRuleToken this_ValidID_2 = null;


         enterRule(); 
            
        try {
            // InternalEFactory.g:1150:28: ( (this_ValidID_0= ruleValidID ( ( ( '.' )=>kw= '.' ) this_ValidID_2= ruleValidID )* ) )
            // InternalEFactory.g:1151:1: (this_ValidID_0= ruleValidID ( ( ( '.' )=>kw= '.' ) this_ValidID_2= ruleValidID )* )
            {
            // InternalEFactory.g:1151:1: (this_ValidID_0= ruleValidID ( ( ( '.' )=>kw= '.' ) this_ValidID_2= ruleValidID )* )
            // InternalEFactory.g:1152:5: this_ValidID_0= ruleValidID ( ( ( '.' )=>kw= '.' ) this_ValidID_2= ruleValidID )*
            {
            if ( state.backtracking==0 ) {
               
                      newCompositeNode(grammarAccess.getQualifiedNameAccess().getValidIDParserRuleCall_0()); 
                  
            }
            pushFollow(FOLLOW_15);
            this_ValidID_0=ruleValidID();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {

              		current.merge(this_ValidID_0);
                  
            }
            if ( state.backtracking==0 ) {
               
                      afterParserOrEnumRuleCall();
                  
            }
            // InternalEFactory.g:1162:1: ( ( ( '.' )=>kw= '.' ) this_ValidID_2= ruleValidID )*
            loop15:
            do {
                int alt15=2;
                int LA15_0 = input.LA(1);

                if ( (LA15_0==24) && (synpred3_InternalEFactory())) {
                    alt15=1;
                }


                switch (alt15) {
            	case 1 :
            	    // InternalEFactory.g:1162:2: ( ( '.' )=>kw= '.' ) this_ValidID_2= ruleValidID
            	    {
            	    // InternalEFactory.g:1162:2: ( ( '.' )=>kw= '.' )
            	    // InternalEFactory.g:1162:3: ( '.' )=>kw= '.'
            	    {
            	    kw=(Token)match(input,24,FOLLOW_4); if (state.failed) return current;
            	    if ( state.backtracking==0 ) {

            	              current.merge(kw);
            	              newLeafNode(kw, grammarAccess.getQualifiedNameAccess().getFullStopKeyword_1_0()); 
            	          
            	    }

            	    }

            	    if ( state.backtracking==0 ) {
            	       
            	              newCompositeNode(grammarAccess.getQualifiedNameAccess().getValidIDParserRuleCall_1_1()); 
            	          
            	    }
            	    pushFollow(FOLLOW_15);
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
    // InternalEFactory.g:1189:1: entryRuleValidID returns [String current=null] : iv_ruleValidID= ruleValidID EOF ;
    public final String entryRuleValidID() throws RecognitionException {
        String current = null;

        AntlrDatatypeRuleToken iv_ruleValidID = null;


        try {
            // InternalEFactory.g:1190:2: (iv_ruleValidID= ruleValidID EOF )
            // InternalEFactory.g:1191:2: iv_ruleValidID= ruleValidID EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getValidIDRule()); 
            }
            pushFollow(FOLLOW_1);
            iv_ruleValidID=ruleValidID();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleValidID.getText(); 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return current;

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
    // InternalEFactory.g:1198:1: ruleValidID returns [AntlrDatatypeRuleToken current=new AntlrDatatypeRuleToken()] : (this_ID_0= RULE_ID | this_LONG_ID_1= RULE_LONG_ID | this_LONG_UNSIGNED_2= RULE_LONG_UNSIGNED ) ;
    public final AntlrDatatypeRuleToken ruleValidID() throws RecognitionException {
        AntlrDatatypeRuleToken current = new AntlrDatatypeRuleToken();

        Token this_ID_0=null;
        Token this_LONG_ID_1=null;
        Token this_LONG_UNSIGNED_2=null;

         enterRule(); 
            
        try {
            // InternalEFactory.g:1201:28: ( (this_ID_0= RULE_ID | this_LONG_ID_1= RULE_LONG_ID | this_LONG_UNSIGNED_2= RULE_LONG_UNSIGNED ) )
            // InternalEFactory.g:1202:1: (this_ID_0= RULE_ID | this_LONG_ID_1= RULE_LONG_ID | this_LONG_UNSIGNED_2= RULE_LONG_UNSIGNED )
            {
            // InternalEFactory.g:1202:1: (this_ID_0= RULE_ID | this_LONG_ID_1= RULE_LONG_ID | this_LONG_UNSIGNED_2= RULE_LONG_UNSIGNED )
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
                    // InternalEFactory.g:1202:6: this_ID_0= RULE_ID
                    {
                    this_ID_0=(Token)match(input,RULE_ID,FOLLOW_2); if (state.failed) return current;
                    if ( state.backtracking==0 ) {

                      		current.merge(this_ID_0);
                          
                    }
                    if ( state.backtracking==0 ) {
                       
                          newLeafNode(this_ID_0, grammarAccess.getValidIDAccess().getIDTerminalRuleCall_0()); 
                          
                    }

                    }
                    break;
                case 2 :
                    // InternalEFactory.g:1210:10: this_LONG_ID_1= RULE_LONG_ID
                    {
                    this_LONG_ID_1=(Token)match(input,RULE_LONG_ID,FOLLOW_2); if (state.failed) return current;
                    if ( state.backtracking==0 ) {

                      		current.merge(this_LONG_ID_1);
                          
                    }
                    if ( state.backtracking==0 ) {
                       
                          newLeafNode(this_LONG_ID_1, grammarAccess.getValidIDAccess().getLONG_IDTerminalRuleCall_1()); 
                          
                    }

                    }
                    break;
                case 3 :
                    // InternalEFactory.g:1218:10: this_LONG_UNSIGNED_2= RULE_LONG_UNSIGNED
                    {
                    this_LONG_UNSIGNED_2=(Token)match(input,RULE_LONG_UNSIGNED,FOLLOW_2); if (state.failed) return current;
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
    // InternalEFactory.g:1233:1: entryRuleBoolean returns [String current=null] : iv_ruleBoolean= ruleBoolean EOF ;
    public final String entryRuleBoolean() throws RecognitionException {
        String current = null;

        AntlrDatatypeRuleToken iv_ruleBoolean = null;


        try {
            // InternalEFactory.g:1234:2: (iv_ruleBoolean= ruleBoolean EOF )
            // InternalEFactory.g:1235:2: iv_ruleBoolean= ruleBoolean EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getBooleanRule()); 
            }
            pushFollow(FOLLOW_1);
            iv_ruleBoolean=ruleBoolean();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleBoolean.getText(); 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return current;

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
    // InternalEFactory.g:1242:1: ruleBoolean returns [AntlrDatatypeRuleToken current=new AntlrDatatypeRuleToken()] : (kw= 'true' | kw= 'false' ) ;
    public final AntlrDatatypeRuleToken ruleBoolean() throws RecognitionException {
        AntlrDatatypeRuleToken current = new AntlrDatatypeRuleToken();

        Token kw=null;

         enterRule(); 
            
        try {
            // InternalEFactory.g:1245:28: ( (kw= 'true' | kw= 'false' ) )
            // InternalEFactory.g:1246:1: (kw= 'true' | kw= 'false' )
            {
            // InternalEFactory.g:1246:1: (kw= 'true' | kw= 'false' )
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
                    // InternalEFactory.g:1247:2: kw= 'true'
                    {
                    kw=(Token)match(input,25,FOLLOW_2); if (state.failed) return current;
                    if ( state.backtracking==0 ) {

                              current.merge(kw);
                              newLeafNode(kw, grammarAccess.getBooleanAccess().getTrueKeyword_0()); 
                          
                    }

                    }
                    break;
                case 2 :
                    // InternalEFactory.g:1254:2: kw= 'false'
                    {
                    kw=(Token)match(input,26,FOLLOW_2); if (state.failed) return current;
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
    // InternalEFactory.g:1267:1: entryRuleLong returns [String current=null] : iv_ruleLong= ruleLong EOF ;
    public final String entryRuleLong() throws RecognitionException {
        String current = null;

        AntlrDatatypeRuleToken iv_ruleLong = null;


        try {
            // InternalEFactory.g:1268:2: (iv_ruleLong= ruleLong EOF )
            // InternalEFactory.g:1269:2: iv_ruleLong= ruleLong EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getLongRule()); 
            }
            pushFollow(FOLLOW_1);
            iv_ruleLong=ruleLong();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleLong.getText(); 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return current;

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
    // InternalEFactory.g:1276:1: ruleLong returns [AntlrDatatypeRuleToken current=new AntlrDatatypeRuleToken()] : ( (kw= '-' )? this_LONG_UNSIGNED_1= RULE_LONG_UNSIGNED ) ;
    public final AntlrDatatypeRuleToken ruleLong() throws RecognitionException {
        AntlrDatatypeRuleToken current = new AntlrDatatypeRuleToken();

        Token kw=null;
        Token this_LONG_UNSIGNED_1=null;

         enterRule(); 
            
        try {
            // InternalEFactory.g:1279:28: ( ( (kw= '-' )? this_LONG_UNSIGNED_1= RULE_LONG_UNSIGNED ) )
            // InternalEFactory.g:1280:1: ( (kw= '-' )? this_LONG_UNSIGNED_1= RULE_LONG_UNSIGNED )
            {
            // InternalEFactory.g:1280:1: ( (kw= '-' )? this_LONG_UNSIGNED_1= RULE_LONG_UNSIGNED )
            // InternalEFactory.g:1280:2: (kw= '-' )? this_LONG_UNSIGNED_1= RULE_LONG_UNSIGNED
            {
            // InternalEFactory.g:1280:2: (kw= '-' )?
            int alt18=2;
            int LA18_0 = input.LA(1);

            if ( (LA18_0==27) ) {
                alt18=1;
            }
            switch (alt18) {
                case 1 :
                    // InternalEFactory.g:1281:2: kw= '-'
                    {
                    kw=(Token)match(input,27,FOLLOW_16); if (state.failed) return current;
                    if ( state.backtracking==0 ) {

                              current.merge(kw);
                              newLeafNode(kw, grammarAccess.getLongAccess().getHyphenMinusKeyword_0()); 
                          
                    }

                    }
                    break;

            }

            this_LONG_UNSIGNED_1=(Token)match(input,RULE_LONG_UNSIGNED,FOLLOW_2); if (state.failed) return current;
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
    // InternalEFactory.g:1301:1: entryRuleDouble returns [String current=null] : iv_ruleDouble= ruleDouble EOF ;
    public final String entryRuleDouble() throws RecognitionException {
        String current = null;

        AntlrDatatypeRuleToken iv_ruleDouble = null;


        try {
            // InternalEFactory.g:1302:2: (iv_ruleDouble= ruleDouble EOF )
            // InternalEFactory.g:1303:2: iv_ruleDouble= ruleDouble EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getDoubleRule()); 
            }
            pushFollow(FOLLOW_1);
            iv_ruleDouble=ruleDouble();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleDouble.getText(); 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return current;

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
    // InternalEFactory.g:1310:1: ruleDouble returns [AntlrDatatypeRuleToken current=new AntlrDatatypeRuleToken()] : (this_Long_0= ruleLong kw= '.' this_LONG_UNSIGNED_2= RULE_LONG_UNSIGNED ) ;
    public final AntlrDatatypeRuleToken ruleDouble() throws RecognitionException {
        AntlrDatatypeRuleToken current = new AntlrDatatypeRuleToken();

        Token kw=null;
        Token this_LONG_UNSIGNED_2=null;
        AntlrDatatypeRuleToken this_Long_0 = null;


         enterRule(); 
            
        try {
            // InternalEFactory.g:1313:28: ( (this_Long_0= ruleLong kw= '.' this_LONG_UNSIGNED_2= RULE_LONG_UNSIGNED ) )
            // InternalEFactory.g:1314:1: (this_Long_0= ruleLong kw= '.' this_LONG_UNSIGNED_2= RULE_LONG_UNSIGNED )
            {
            // InternalEFactory.g:1314:1: (this_Long_0= ruleLong kw= '.' this_LONG_UNSIGNED_2= RULE_LONG_UNSIGNED )
            // InternalEFactory.g:1315:5: this_Long_0= ruleLong kw= '.' this_LONG_UNSIGNED_2= RULE_LONG_UNSIGNED
            {
            if ( state.backtracking==0 ) {
               
                      newCompositeNode(grammarAccess.getDoubleAccess().getLongParserRuleCall_0()); 
                  
            }
            pushFollow(FOLLOW_17);
            this_Long_0=ruleLong();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {

              		current.merge(this_Long_0);
                  
            }
            if ( state.backtracking==0 ) {
               
                      afterParserOrEnumRuleCall();
                  
            }
            kw=(Token)match(input,24,FOLLOW_16); if (state.failed) return current;
            if ( state.backtracking==0 ) {

                      current.merge(kw);
                      newLeafNode(kw, grammarAccess.getDoubleAccess().getFullStopKeyword_1()); 
                  
            }
            this_LONG_UNSIGNED_2=(Token)match(input,RULE_LONG_UNSIGNED,FOLLOW_2); if (state.failed) return current;
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
    // InternalEFactory.g:1346:1: entryRuleDate returns [String current=null] : iv_ruleDate= ruleDate EOF ;
    public final String entryRuleDate() throws RecognitionException {
        String current = null;

        AntlrDatatypeRuleToken iv_ruleDate = null;


        try {
            // InternalEFactory.g:1347:2: (iv_ruleDate= ruleDate EOF )
            // InternalEFactory.g:1348:2: iv_ruleDate= ruleDate EOF
            {
            if ( state.backtracking==0 ) {
               newCompositeNode(grammarAccess.getDateRule()); 
            }
            pushFollow(FOLLOW_1);
            iv_ruleDate=ruleDate();

            state._fsp--;
            if (state.failed) return current;
            if ( state.backtracking==0 ) {
               current =iv_ruleDate.getText(); 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return current;

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
    // InternalEFactory.g:1355:1: ruleDate returns [AntlrDatatypeRuleToken current=new AntlrDatatypeRuleToken()] : (this_LONG_UNSIGNED_0= RULE_LONG_UNSIGNED kw= '.' this_LONG_UNSIGNED_2= RULE_LONG_UNSIGNED kw= '.' this_LONG_UNSIGNED_4= RULE_LONG_UNSIGNED ) ;
    public final AntlrDatatypeRuleToken ruleDate() throws RecognitionException {
        AntlrDatatypeRuleToken current = new AntlrDatatypeRuleToken();

        Token this_LONG_UNSIGNED_0=null;
        Token kw=null;
        Token this_LONG_UNSIGNED_2=null;
        Token this_LONG_UNSIGNED_4=null;

         enterRule(); 
            
        try {
            // InternalEFactory.g:1358:28: ( (this_LONG_UNSIGNED_0= RULE_LONG_UNSIGNED kw= '.' this_LONG_UNSIGNED_2= RULE_LONG_UNSIGNED kw= '.' this_LONG_UNSIGNED_4= RULE_LONG_UNSIGNED ) )
            // InternalEFactory.g:1359:1: (this_LONG_UNSIGNED_0= RULE_LONG_UNSIGNED kw= '.' this_LONG_UNSIGNED_2= RULE_LONG_UNSIGNED kw= '.' this_LONG_UNSIGNED_4= RULE_LONG_UNSIGNED )
            {
            // InternalEFactory.g:1359:1: (this_LONG_UNSIGNED_0= RULE_LONG_UNSIGNED kw= '.' this_LONG_UNSIGNED_2= RULE_LONG_UNSIGNED kw= '.' this_LONG_UNSIGNED_4= RULE_LONG_UNSIGNED )
            // InternalEFactory.g:1359:6: this_LONG_UNSIGNED_0= RULE_LONG_UNSIGNED kw= '.' this_LONG_UNSIGNED_2= RULE_LONG_UNSIGNED kw= '.' this_LONG_UNSIGNED_4= RULE_LONG_UNSIGNED
            {
            this_LONG_UNSIGNED_0=(Token)match(input,RULE_LONG_UNSIGNED,FOLLOW_17); if (state.failed) return current;
            if ( state.backtracking==0 ) {

              		current.merge(this_LONG_UNSIGNED_0);
                  
            }
            if ( state.backtracking==0 ) {
               
                  newLeafNode(this_LONG_UNSIGNED_0, grammarAccess.getDateAccess().getLONG_UNSIGNEDTerminalRuleCall_0()); 
                  
            }
            kw=(Token)match(input,24,FOLLOW_16); if (state.failed) return current;
            if ( state.backtracking==0 ) {

                      current.merge(kw);
                      newLeafNode(kw, grammarAccess.getDateAccess().getFullStopKeyword_1()); 
                  
            }
            this_LONG_UNSIGNED_2=(Token)match(input,RULE_LONG_UNSIGNED,FOLLOW_17); if (state.failed) return current;
            if ( state.backtracking==0 ) {

              		current.merge(this_LONG_UNSIGNED_2);
                  
            }
            if ( state.backtracking==0 ) {
               
                  newLeafNode(this_LONG_UNSIGNED_2, grammarAccess.getDateAccess().getLONG_UNSIGNEDTerminalRuleCall_2()); 
                  
            }
            kw=(Token)match(input,24,FOLLOW_16); if (state.failed) return current;
            if ( state.backtracking==0 ) {

                      current.merge(kw);
                      newLeafNode(kw, grammarAccess.getDateAccess().getFullStopKeyword_3()); 
                  
            }
            this_LONG_UNSIGNED_4=(Token)match(input,RULE_LONG_UNSIGNED,FOLLOW_2); if (state.failed) return current;
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
        // InternalEFactory.g:511:8: ( ( () ( ( ruleQualifiedName ) )? ( ( ruleValidID ) )? '{' ) )
        // InternalEFactory.g:511:9: ( () ( ( ruleQualifiedName ) )? ( ( ruleValidID ) )? '{' )
        {
        // InternalEFactory.g:511:9: ( () ( ( ruleQualifiedName ) )? ( ( ruleValidID ) )? '{' )
        // InternalEFactory.g:511:10: () ( ( ruleQualifiedName ) )? ( ( ruleValidID ) )? '{'
        {
        // InternalEFactory.g:511:10: ()
        // InternalEFactory.g:512:1: 
        {
        }

        // InternalEFactory.g:512:2: ( ( ruleQualifiedName ) )?
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
                // InternalEFactory.g:513:1: ( ruleQualifiedName )
                {
                // InternalEFactory.g:513:1: ( ruleQualifiedName )
                // InternalEFactory.g:514:3: ruleQualifiedName
                {
                pushFollow(FOLLOW_11);
                ruleQualifiedName();

                state._fsp--;
                if (state.failed) return ;

                }


                }
                break;

        }

        // InternalEFactory.g:516:3: ( ( ruleValidID ) )?
        int alt20=2;
        int LA20_0 = input.LA(1);

        if ( ((LA20_0>=RULE_ID && LA20_0<=RULE_LONG_UNSIGNED)) ) {
            alt20=1;
        }
        switch (alt20) {
            case 1 :
                // InternalEFactory.g:517:1: ( ruleValidID )
                {
                // InternalEFactory.g:517:1: ( ruleValidID )
                // InternalEFactory.g:518:1: ruleValidID
                {
                pushFollow(FOLLOW_6);
                ruleValidID();

                state._fsp--;
                if (state.failed) return ;

                }


                }
                break;

        }

        match(input,16,FOLLOW_2); if (state.failed) return ;

        }


        }
    }
    // $ANTLR end synpred1_InternalEFactory

    // $ANTLR start synpred2_InternalEFactory
    public final void synpred2_InternalEFactory_fragment() throws RecognitionException {   
        // InternalEFactory.g:593:7: ( ruleReference )
        // InternalEFactory.g:593:9: ruleReference
        {
        pushFollow(FOLLOW_2);
        ruleReference();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred2_InternalEFactory

    // $ANTLR start synpred3_InternalEFactory
    public final void synpred3_InternalEFactory_fragment() throws RecognitionException {   
        // InternalEFactory.g:1162:3: ( '.' )
        // InternalEFactory.g:1163:2: '.'
        {
        match(input,24,FOLLOW_2); if (state.failed) return ;

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
    static final String dfa_1s = "\16\uffff";
    static final String dfa_2s = "\1\5\1\uffff\3\1\3\uffff\4\1\1\uffff\1\1";
    static final String dfa_3s = "\1\4\1\uffff\3\5\1\uffff\10\5";
    static final String dfa_4s = "\1\33\1\uffff\3\30\1\uffff\2\7\4\30\1\7\1\30";
    static final String dfa_5s = "\1\uffff\1\1\3\uffff\1\2\10\uffff";
    static final String dfa_6s = "\16\uffff}>";
    static final String[] dfa_7s = {
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

    static final short[] dfa_1 = DFA.unpackEncodedString(dfa_1s);
    static final short[] dfa_2 = DFA.unpackEncodedString(dfa_2s);
    static final char[] dfa_3 = DFA.unpackEncodedStringToUnsignedChars(dfa_3s);
    static final char[] dfa_4 = DFA.unpackEncodedStringToUnsignedChars(dfa_4s);
    static final short[] dfa_5 = DFA.unpackEncodedString(dfa_5s);
    static final short[] dfa_6 = DFA.unpackEncodedString(dfa_6s);
    static final short[][] dfa_7 = unpackEncodedStringArray(dfa_7s);

    class DFA4 extends DFA {

        public DFA4(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 4;
            this.eot = dfa_1;
            this.eof = dfa_2;
            this.min = dfa_3;
            this.max = dfa_4;
            this.accept = dfa_5;
            this.special = dfa_6;
            this.transition = dfa_7;
        }
        public String getDescription() {
            return "372:1: ( (lv_value_2_0= ruleValue ) )?";
        }
    }
    static final String dfa_8s = "\15\uffff";
    static final String dfa_9s = "\4\uffff\1\10\2\uffff\1\10\3\uffff\1\12\1\uffff";
    static final String dfa_10s = "\1\4\2\uffff\1\7\1\4\2\uffff\1\4\1\uffff\1\7\1\uffff\1\4\1\uffff";
    static final String dfa_11s = "\1\33\2\uffff\1\7\1\33\2\uffff\1\33\1\uffff\1\7\1\uffff\1\33\1\uffff";
    static final String dfa_12s = "\1\uffff\1\1\1\2\2\uffff\1\4\1\7\1\uffff\1\3\1\uffff\1\5\1\uffff\1\6";
    static final String dfa_13s = "\15\uffff}>";
    static final String[] dfa_14s = {
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

    static final short[] dfa_8 = DFA.unpackEncodedString(dfa_8s);
    static final short[] dfa_9 = DFA.unpackEncodedString(dfa_9s);
    static final char[] dfa_10 = DFA.unpackEncodedStringToUnsignedChars(dfa_10s);
    static final char[] dfa_11 = DFA.unpackEncodedStringToUnsignedChars(dfa_11s);
    static final short[] dfa_12 = DFA.unpackEncodedString(dfa_12s);
    static final short[] dfa_13 = DFA.unpackEncodedString(dfa_13s);
    static final short[][] dfa_14 = unpackEncodedStringArray(dfa_14s);

    class DFA12 extends DFA {

        public DFA12(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 12;
            this.eot = dfa_8;
            this.eof = dfa_9;
            this.min = dfa_10;
            this.max = dfa_11;
            this.accept = dfa_12;
            this.special = dfa_13;
            this.transition = dfa_14;
        }
        public String getDescription() {
            return "712:1: (this_EnumAttribute_0= ruleEnumAttribute | this_StringAttribute_1= ruleStringAttribute | this_IntegerAttribute_2= ruleIntegerAttribute | this_BooleanAttribute_3= ruleBooleanAttribute | this_DoubleAttribute_4= ruleDoubleAttribute | this_DateAttribute_5= ruleDateAttribute | this_NullAttribute_6= ruleNullAttribute )";
        }
    }
 

    public static final BitSet FOLLOW_1 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_2 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_3 = new BitSet(new long[]{0x000000000000E0E0L});
    public static final BitSet FOLLOW_4 = new BitSet(new long[]{0x000000000000A0E0L});
    public static final BitSet FOLLOW_5 = new BitSet(new long[]{0x000000000000A0F0L});
    public static final BitSet FOLLOW_6 = new BitSet(new long[]{0x0000000000010000L});
    public static final BitSet FOLLOW_7 = new BitSet(new long[]{0x0000000000020000L});
    public static final BitSet FOLLOW_8 = new BitSet(new long[]{0x0000000000040000L});
    public static final BitSet FOLLOW_9 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_10 = new BitSet(new long[]{0x000000000E59A0F2L});
    public static final BitSet FOLLOW_11 = new BitSet(new long[]{0x000000000001A0E0L});
    public static final BitSet FOLLOW_12 = new BitSet(new long[]{0x000000000004A0E0L});
    public static final BitSet FOLLOW_13 = new BitSet(new long[]{0x000000000E79A0F0L});
    public static final BitSet FOLLOW_14 = new BitSet(new long[]{0x0000000000800002L});
    public static final BitSet FOLLOW_15 = new BitSet(new long[]{0x0000000001000002L});
    public static final BitSet FOLLOW_16 = new BitSet(new long[]{0x0000000000000080L});
    public static final BitSet FOLLOW_17 = new BitSet(new long[]{0x0000000001000000L});

}