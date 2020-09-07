package org.eclipse.emf.eson.ui.contentassist.antlr.internal; 

import java.io.InputStream;
import org.eclipse.xtext.*;
import org.eclipse.xtext.parser.*;
import org.eclipse.xtext.parser.impl.*;
import org.eclipse.emf.ecore.util.EcoreUtil;
import org.eclipse.emf.ecore.EObject;
import org.eclipse.xtext.parser.antlr.XtextTokenStream;
import org.eclipse.xtext.parser.antlr.XtextTokenStream.HiddenTokens;
import org.eclipse.xtext.ui.editor.contentassist.antlr.internal.AbstractInternalContentAssistParser;
import org.eclipse.xtext.ui.editor.contentassist.antlr.internal.DFA;
import org.eclipse.emf.eson.services.EFactoryGrammarAccess;



import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
@SuppressWarnings("all")
public class InternalEFactoryParser extends AbstractInternalContentAssistParser {
    public static final String[] tokenNames = new String[] {
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "RULE_STRING", "RULE_ID", "RULE_LONG_ID", "RULE_LONG_UNSIGNED", "RULE_INT", "RULE_ML_COMMENT", "RULE_SL_COMMENT", "RULE_WS", "RULE_ANY_OTHER", "'true'", "'false'", "'use'", "'import'", "'@Name'", "'{'", "'='", "'}'", "':'", "'['", "']'", "'.*'", "'.'", "'-'", "'NULL'"
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
     	
        public void setGrammarAccess(EFactoryGrammarAccess grammarAccess) {
        	this.grammarAccess = grammarAccess;
        }
        
        @Override
        protected Grammar getGrammar() {
        	return grammarAccess.getGrammar();
        }
        
        @Override
        protected String getValueForTokenName(String tokenName) {
        	return tokenName;
        }




    // $ANTLR start "entryRuleFactory"
    // InternalEFactory.g:61:1: entryRuleFactory : ruleFactory EOF ;
    public final void entryRuleFactory() throws RecognitionException {
        try {
            // InternalEFactory.g:62:1: ( ruleFactory EOF )
            // InternalEFactory.g:63:1: ruleFactory EOF
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getFactoryRule()); 
            }
            pushFollow(FOLLOW_1);
            ruleFactory();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getFactoryRule()); 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "entryRuleFactory"


    // $ANTLR start "ruleFactory"
    // InternalEFactory.g:70:1: ruleFactory : ( ( rule__Factory__Group__0 ) ) ;
    public final void ruleFactory() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:74:2: ( ( ( rule__Factory__Group__0 ) ) )
            // InternalEFactory.g:75:1: ( ( rule__Factory__Group__0 ) )
            {
            // InternalEFactory.g:75:1: ( ( rule__Factory__Group__0 ) )
            // InternalEFactory.g:76:1: ( rule__Factory__Group__0 )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getFactoryAccess().getGroup()); 
            }
            // InternalEFactory.g:77:1: ( rule__Factory__Group__0 )
            // InternalEFactory.g:77:2: rule__Factory__Group__0
            {
            pushFollow(FOLLOW_2);
            rule__Factory__Group__0();

            state._fsp--;
            if (state.failed) return ;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getFactoryAccess().getGroup()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "ruleFactory"


    // $ANTLR start "entryRulePackageImport"
    // InternalEFactory.g:89:1: entryRulePackageImport : rulePackageImport EOF ;
    public final void entryRulePackageImport() throws RecognitionException {
        try {
            // InternalEFactory.g:90:1: ( rulePackageImport EOF )
            // InternalEFactory.g:91:1: rulePackageImport EOF
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getPackageImportRule()); 
            }
            pushFollow(FOLLOW_1);
            rulePackageImport();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getPackageImportRule()); 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "entryRulePackageImport"


    // $ANTLR start "rulePackageImport"
    // InternalEFactory.g:98:1: rulePackageImport : ( ( rule__PackageImport__Group__0 ) ) ;
    public final void rulePackageImport() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:102:2: ( ( ( rule__PackageImport__Group__0 ) ) )
            // InternalEFactory.g:103:1: ( ( rule__PackageImport__Group__0 ) )
            {
            // InternalEFactory.g:103:1: ( ( rule__PackageImport__Group__0 ) )
            // InternalEFactory.g:104:1: ( rule__PackageImport__Group__0 )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getPackageImportAccess().getGroup()); 
            }
            // InternalEFactory.g:105:1: ( rule__PackageImport__Group__0 )
            // InternalEFactory.g:105:2: rule__PackageImport__Group__0
            {
            pushFollow(FOLLOW_2);
            rule__PackageImport__Group__0();

            state._fsp--;
            if (state.failed) return ;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getPackageImportAccess().getGroup()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rulePackageImport"


    // $ANTLR start "entryRuleNamespaceImport"
    // InternalEFactory.g:117:1: entryRuleNamespaceImport : ruleNamespaceImport EOF ;
    public final void entryRuleNamespaceImport() throws RecognitionException {
        try {
            // InternalEFactory.g:118:1: ( ruleNamespaceImport EOF )
            // InternalEFactory.g:119:1: ruleNamespaceImport EOF
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getNamespaceImportRule()); 
            }
            pushFollow(FOLLOW_1);
            ruleNamespaceImport();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getNamespaceImportRule()); 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "entryRuleNamespaceImport"


    // $ANTLR start "ruleNamespaceImport"
    // InternalEFactory.g:126:1: ruleNamespaceImport : ( ( rule__NamespaceImport__Group__0 ) ) ;
    public final void ruleNamespaceImport() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:130:2: ( ( ( rule__NamespaceImport__Group__0 ) ) )
            // InternalEFactory.g:131:1: ( ( rule__NamespaceImport__Group__0 ) )
            {
            // InternalEFactory.g:131:1: ( ( rule__NamespaceImport__Group__0 ) )
            // InternalEFactory.g:132:1: ( rule__NamespaceImport__Group__0 )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getNamespaceImportAccess().getGroup()); 
            }
            // InternalEFactory.g:133:1: ( rule__NamespaceImport__Group__0 )
            // InternalEFactory.g:133:2: rule__NamespaceImport__Group__0
            {
            pushFollow(FOLLOW_2);
            rule__NamespaceImport__Group__0();

            state._fsp--;
            if (state.failed) return ;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getNamespaceImportAccess().getGroup()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "ruleNamespaceImport"


    // $ANTLR start "entryRuleAnnotation"
    // InternalEFactory.g:145:1: entryRuleAnnotation : ruleAnnotation EOF ;
    public final void entryRuleAnnotation() throws RecognitionException {
        try {
            // InternalEFactory.g:146:1: ( ruleAnnotation EOF )
            // InternalEFactory.g:147:1: ruleAnnotation EOF
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getAnnotationRule()); 
            }
            pushFollow(FOLLOW_1);
            ruleAnnotation();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getAnnotationRule()); 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "entryRuleAnnotation"


    // $ANTLR start "ruleAnnotation"
    // InternalEFactory.g:154:1: ruleAnnotation : ( ruleCustomNameMapping ) ;
    public final void ruleAnnotation() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:158:2: ( ( ruleCustomNameMapping ) )
            // InternalEFactory.g:159:1: ( ruleCustomNameMapping )
            {
            // InternalEFactory.g:159:1: ( ruleCustomNameMapping )
            // InternalEFactory.g:160:1: ruleCustomNameMapping
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getAnnotationAccess().getCustomNameMappingParserRuleCall()); 
            }
            pushFollow(FOLLOW_2);
            ruleCustomNameMapping();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getAnnotationAccess().getCustomNameMappingParserRuleCall()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "ruleAnnotation"


    // $ANTLR start "entryRuleCustomNameMapping"
    // InternalEFactory.g:173:1: entryRuleCustomNameMapping : ruleCustomNameMapping EOF ;
    public final void entryRuleCustomNameMapping() throws RecognitionException {
        try {
            // InternalEFactory.g:174:1: ( ruleCustomNameMapping EOF )
            // InternalEFactory.g:175:1: ruleCustomNameMapping EOF
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getCustomNameMappingRule()); 
            }
            pushFollow(FOLLOW_1);
            ruleCustomNameMapping();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getCustomNameMappingRule()); 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "entryRuleCustomNameMapping"


    // $ANTLR start "ruleCustomNameMapping"
    // InternalEFactory.g:182:1: ruleCustomNameMapping : ( ( rule__CustomNameMapping__Group__0 ) ) ;
    public final void ruleCustomNameMapping() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:186:2: ( ( ( rule__CustomNameMapping__Group__0 ) ) )
            // InternalEFactory.g:187:1: ( ( rule__CustomNameMapping__Group__0 ) )
            {
            // InternalEFactory.g:187:1: ( ( rule__CustomNameMapping__Group__0 ) )
            // InternalEFactory.g:188:1: ( rule__CustomNameMapping__Group__0 )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getCustomNameMappingAccess().getGroup()); 
            }
            // InternalEFactory.g:189:1: ( rule__CustomNameMapping__Group__0 )
            // InternalEFactory.g:189:2: rule__CustomNameMapping__Group__0
            {
            pushFollow(FOLLOW_2);
            rule__CustomNameMapping__Group__0();

            state._fsp--;
            if (state.failed) return ;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getCustomNameMappingAccess().getGroup()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "ruleCustomNameMapping"


    // $ANTLR start "entryRuleFeature"
    // InternalEFactory.g:201:1: entryRuleFeature : ruleFeature EOF ;
    public final void entryRuleFeature() throws RecognitionException {
        try {
            // InternalEFactory.g:202:1: ( ruleFeature EOF )
            // InternalEFactory.g:203:1: ruleFeature EOF
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getFeatureRule()); 
            }
            pushFollow(FOLLOW_1);
            ruleFeature();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getFeatureRule()); 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "entryRuleFeature"


    // $ANTLR start "ruleFeature"
    // InternalEFactory.g:210:1: ruleFeature : ( ( rule__Feature__Group__0 ) ) ;
    public final void ruleFeature() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:214:2: ( ( ( rule__Feature__Group__0 ) ) )
            // InternalEFactory.g:215:1: ( ( rule__Feature__Group__0 ) )
            {
            // InternalEFactory.g:215:1: ( ( rule__Feature__Group__0 ) )
            // InternalEFactory.g:216:1: ( rule__Feature__Group__0 )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getFeatureAccess().getGroup()); 
            }
            // InternalEFactory.g:217:1: ( rule__Feature__Group__0 )
            // InternalEFactory.g:217:2: rule__Feature__Group__0
            {
            pushFollow(FOLLOW_2);
            rule__Feature__Group__0();

            state._fsp--;
            if (state.failed) return ;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getFeatureAccess().getGroup()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "ruleFeature"


    // $ANTLR start "entryRuleNewObject"
    // InternalEFactory.g:229:1: entryRuleNewObject : ruleNewObject EOF ;
    public final void entryRuleNewObject() throws RecognitionException {
        try {
            // InternalEFactory.g:230:1: ( ruleNewObject EOF )
            // InternalEFactory.g:231:1: ruleNewObject EOF
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getNewObjectRule()); 
            }
            pushFollow(FOLLOW_1);
            ruleNewObject();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getNewObjectRule()); 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "entryRuleNewObject"


    // $ANTLR start "ruleNewObject"
    // InternalEFactory.g:238:1: ruleNewObject : ( ( rule__NewObject__Group__0 ) ) ;
    public final void ruleNewObject() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:242:2: ( ( ( rule__NewObject__Group__0 ) ) )
            // InternalEFactory.g:243:1: ( ( rule__NewObject__Group__0 ) )
            {
            // InternalEFactory.g:243:1: ( ( rule__NewObject__Group__0 ) )
            // InternalEFactory.g:244:1: ( rule__NewObject__Group__0 )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getNewObjectAccess().getGroup()); 
            }
            // InternalEFactory.g:245:1: ( rule__NewObject__Group__0 )
            // InternalEFactory.g:245:2: rule__NewObject__Group__0
            {
            pushFollow(FOLLOW_2);
            rule__NewObject__Group__0();

            state._fsp--;
            if (state.failed) return ;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getNewObjectAccess().getGroup()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "ruleNewObject"


    // $ANTLR start "entryRuleValue"
    // InternalEFactory.g:257:1: entryRuleValue : ruleValue EOF ;
    public final void entryRuleValue() throws RecognitionException {
        try {
            // InternalEFactory.g:258:1: ( ruleValue EOF )
            // InternalEFactory.g:259:1: ruleValue EOF
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getValueRule()); 
            }
            pushFollow(FOLLOW_1);
            ruleValue();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getValueRule()); 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "entryRuleValue"


    // $ANTLR start "ruleValue"
    // InternalEFactory.g:266:1: ruleValue : ( ( rule__Value__Alternatives ) ) ;
    public final void ruleValue() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:270:2: ( ( ( rule__Value__Alternatives ) ) )
            // InternalEFactory.g:271:1: ( ( rule__Value__Alternatives ) )
            {
            // InternalEFactory.g:271:1: ( ( rule__Value__Alternatives ) )
            // InternalEFactory.g:272:1: ( rule__Value__Alternatives )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getValueAccess().getAlternatives()); 
            }
            // InternalEFactory.g:273:1: ( rule__Value__Alternatives )
            // InternalEFactory.g:273:2: rule__Value__Alternatives
            {
            pushFollow(FOLLOW_2);
            rule__Value__Alternatives();

            state._fsp--;
            if (state.failed) return ;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getValueAccess().getAlternatives()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "ruleValue"


    // $ANTLR start "entryRuleMultiValue"
    // InternalEFactory.g:285:1: entryRuleMultiValue : ruleMultiValue EOF ;
    public final void entryRuleMultiValue() throws RecognitionException {
        try {
            // InternalEFactory.g:286:1: ( ruleMultiValue EOF )
            // InternalEFactory.g:287:1: ruleMultiValue EOF
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getMultiValueRule()); 
            }
            pushFollow(FOLLOW_1);
            ruleMultiValue();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getMultiValueRule()); 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "entryRuleMultiValue"


    // $ANTLR start "ruleMultiValue"
    // InternalEFactory.g:294:1: ruleMultiValue : ( ( rule__MultiValue__Group__0 ) ) ;
    public final void ruleMultiValue() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:298:2: ( ( ( rule__MultiValue__Group__0 ) ) )
            // InternalEFactory.g:299:1: ( ( rule__MultiValue__Group__0 ) )
            {
            // InternalEFactory.g:299:1: ( ( rule__MultiValue__Group__0 ) )
            // InternalEFactory.g:300:1: ( rule__MultiValue__Group__0 )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getMultiValueAccess().getGroup()); 
            }
            // InternalEFactory.g:301:1: ( rule__MultiValue__Group__0 )
            // InternalEFactory.g:301:2: rule__MultiValue__Group__0
            {
            pushFollow(FOLLOW_2);
            rule__MultiValue__Group__0();

            state._fsp--;
            if (state.failed) return ;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getMultiValueAccess().getGroup()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "ruleMultiValue"


    // $ANTLR start "entryRuleReference"
    // InternalEFactory.g:313:1: entryRuleReference : ruleReference EOF ;
    public final void entryRuleReference() throws RecognitionException {
        try {
            // InternalEFactory.g:314:1: ( ruleReference EOF )
            // InternalEFactory.g:315:1: ruleReference EOF
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getReferenceRule()); 
            }
            pushFollow(FOLLOW_1);
            ruleReference();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getReferenceRule()); 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "entryRuleReference"


    // $ANTLR start "ruleReference"
    // InternalEFactory.g:322:1: ruleReference : ( ( rule__Reference__ValueAssignment ) ) ;
    public final void ruleReference() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:326:2: ( ( ( rule__Reference__ValueAssignment ) ) )
            // InternalEFactory.g:327:1: ( ( rule__Reference__ValueAssignment ) )
            {
            // InternalEFactory.g:327:1: ( ( rule__Reference__ValueAssignment ) )
            // InternalEFactory.g:328:1: ( rule__Reference__ValueAssignment )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getReferenceAccess().getValueAssignment()); 
            }
            // InternalEFactory.g:329:1: ( rule__Reference__ValueAssignment )
            // InternalEFactory.g:329:2: rule__Reference__ValueAssignment
            {
            pushFollow(FOLLOW_2);
            rule__Reference__ValueAssignment();

            state._fsp--;
            if (state.failed) return ;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getReferenceAccess().getValueAssignment()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "ruleReference"


    // $ANTLR start "entryRuleAttribute"
    // InternalEFactory.g:341:1: entryRuleAttribute : ruleAttribute EOF ;
    public final void entryRuleAttribute() throws RecognitionException {
        try {
            // InternalEFactory.g:342:1: ( ruleAttribute EOF )
            // InternalEFactory.g:343:1: ruleAttribute EOF
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getAttributeRule()); 
            }
            pushFollow(FOLLOW_1);
            ruleAttribute();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getAttributeRule()); 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "entryRuleAttribute"


    // $ANTLR start "ruleAttribute"
    // InternalEFactory.g:350:1: ruleAttribute : ( ( rule__Attribute__Alternatives ) ) ;
    public final void ruleAttribute() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:354:2: ( ( ( rule__Attribute__Alternatives ) ) )
            // InternalEFactory.g:355:1: ( ( rule__Attribute__Alternatives ) )
            {
            // InternalEFactory.g:355:1: ( ( rule__Attribute__Alternatives ) )
            // InternalEFactory.g:356:1: ( rule__Attribute__Alternatives )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getAttributeAccess().getAlternatives()); 
            }
            // InternalEFactory.g:357:1: ( rule__Attribute__Alternatives )
            // InternalEFactory.g:357:2: rule__Attribute__Alternatives
            {
            pushFollow(FOLLOW_2);
            rule__Attribute__Alternatives();

            state._fsp--;
            if (state.failed) return ;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getAttributeAccess().getAlternatives()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "ruleAttribute"


    // $ANTLR start "entryRuleEnumAttribute"
    // InternalEFactory.g:369:1: entryRuleEnumAttribute : ruleEnumAttribute EOF ;
    public final void entryRuleEnumAttribute() throws RecognitionException {
        try {
            // InternalEFactory.g:370:1: ( ruleEnumAttribute EOF )
            // InternalEFactory.g:371:1: ruleEnumAttribute EOF
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getEnumAttributeRule()); 
            }
            pushFollow(FOLLOW_1);
            ruleEnumAttribute();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getEnumAttributeRule()); 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "entryRuleEnumAttribute"


    // $ANTLR start "ruleEnumAttribute"
    // InternalEFactory.g:378:1: ruleEnumAttribute : ( ( rule__EnumAttribute__Group__0 ) ) ;
    public final void ruleEnumAttribute() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:382:2: ( ( ( rule__EnumAttribute__Group__0 ) ) )
            // InternalEFactory.g:383:1: ( ( rule__EnumAttribute__Group__0 ) )
            {
            // InternalEFactory.g:383:1: ( ( rule__EnumAttribute__Group__0 ) )
            // InternalEFactory.g:384:1: ( rule__EnumAttribute__Group__0 )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getEnumAttributeAccess().getGroup()); 
            }
            // InternalEFactory.g:385:1: ( rule__EnumAttribute__Group__0 )
            // InternalEFactory.g:385:2: rule__EnumAttribute__Group__0
            {
            pushFollow(FOLLOW_2);
            rule__EnumAttribute__Group__0();

            state._fsp--;
            if (state.failed) return ;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getEnumAttributeAccess().getGroup()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "ruleEnumAttribute"


    // $ANTLR start "entryRuleStringAttribute"
    // InternalEFactory.g:397:1: entryRuleStringAttribute : ruleStringAttribute EOF ;
    public final void entryRuleStringAttribute() throws RecognitionException {
        try {
            // InternalEFactory.g:398:1: ( ruleStringAttribute EOF )
            // InternalEFactory.g:399:1: ruleStringAttribute EOF
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getStringAttributeRule()); 
            }
            pushFollow(FOLLOW_1);
            ruleStringAttribute();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getStringAttributeRule()); 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "entryRuleStringAttribute"


    // $ANTLR start "ruleStringAttribute"
    // InternalEFactory.g:406:1: ruleStringAttribute : ( ( rule__StringAttribute__ValueAssignment ) ) ;
    public final void ruleStringAttribute() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:410:2: ( ( ( rule__StringAttribute__ValueAssignment ) ) )
            // InternalEFactory.g:411:1: ( ( rule__StringAttribute__ValueAssignment ) )
            {
            // InternalEFactory.g:411:1: ( ( rule__StringAttribute__ValueAssignment ) )
            // InternalEFactory.g:412:1: ( rule__StringAttribute__ValueAssignment )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getStringAttributeAccess().getValueAssignment()); 
            }
            // InternalEFactory.g:413:1: ( rule__StringAttribute__ValueAssignment )
            // InternalEFactory.g:413:2: rule__StringAttribute__ValueAssignment
            {
            pushFollow(FOLLOW_2);
            rule__StringAttribute__ValueAssignment();

            state._fsp--;
            if (state.failed) return ;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getStringAttributeAccess().getValueAssignment()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "ruleStringAttribute"


    // $ANTLR start "entryRuleIntegerAttribute"
    // InternalEFactory.g:425:1: entryRuleIntegerAttribute : ruleIntegerAttribute EOF ;
    public final void entryRuleIntegerAttribute() throws RecognitionException {
        try {
            // InternalEFactory.g:426:1: ( ruleIntegerAttribute EOF )
            // InternalEFactory.g:427:1: ruleIntegerAttribute EOF
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getIntegerAttributeRule()); 
            }
            pushFollow(FOLLOW_1);
            ruleIntegerAttribute();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getIntegerAttributeRule()); 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "entryRuleIntegerAttribute"


    // $ANTLR start "ruleIntegerAttribute"
    // InternalEFactory.g:434:1: ruleIntegerAttribute : ( ( rule__IntegerAttribute__ValueAssignment ) ) ;
    public final void ruleIntegerAttribute() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:438:2: ( ( ( rule__IntegerAttribute__ValueAssignment ) ) )
            // InternalEFactory.g:439:1: ( ( rule__IntegerAttribute__ValueAssignment ) )
            {
            // InternalEFactory.g:439:1: ( ( rule__IntegerAttribute__ValueAssignment ) )
            // InternalEFactory.g:440:1: ( rule__IntegerAttribute__ValueAssignment )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getIntegerAttributeAccess().getValueAssignment()); 
            }
            // InternalEFactory.g:441:1: ( rule__IntegerAttribute__ValueAssignment )
            // InternalEFactory.g:441:2: rule__IntegerAttribute__ValueAssignment
            {
            pushFollow(FOLLOW_2);
            rule__IntegerAttribute__ValueAssignment();

            state._fsp--;
            if (state.failed) return ;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getIntegerAttributeAccess().getValueAssignment()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "ruleIntegerAttribute"


    // $ANTLR start "entryRuleDoubleAttribute"
    // InternalEFactory.g:453:1: entryRuleDoubleAttribute : ruleDoubleAttribute EOF ;
    public final void entryRuleDoubleAttribute() throws RecognitionException {
        try {
            // InternalEFactory.g:454:1: ( ruleDoubleAttribute EOF )
            // InternalEFactory.g:455:1: ruleDoubleAttribute EOF
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getDoubleAttributeRule()); 
            }
            pushFollow(FOLLOW_1);
            ruleDoubleAttribute();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getDoubleAttributeRule()); 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "entryRuleDoubleAttribute"


    // $ANTLR start "ruleDoubleAttribute"
    // InternalEFactory.g:462:1: ruleDoubleAttribute : ( ( rule__DoubleAttribute__ValueAssignment ) ) ;
    public final void ruleDoubleAttribute() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:466:2: ( ( ( rule__DoubleAttribute__ValueAssignment ) ) )
            // InternalEFactory.g:467:1: ( ( rule__DoubleAttribute__ValueAssignment ) )
            {
            // InternalEFactory.g:467:1: ( ( rule__DoubleAttribute__ValueAssignment ) )
            // InternalEFactory.g:468:1: ( rule__DoubleAttribute__ValueAssignment )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getDoubleAttributeAccess().getValueAssignment()); 
            }
            // InternalEFactory.g:469:1: ( rule__DoubleAttribute__ValueAssignment )
            // InternalEFactory.g:469:2: rule__DoubleAttribute__ValueAssignment
            {
            pushFollow(FOLLOW_2);
            rule__DoubleAttribute__ValueAssignment();

            state._fsp--;
            if (state.failed) return ;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getDoubleAttributeAccess().getValueAssignment()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "ruleDoubleAttribute"


    // $ANTLR start "entryRuleDateAttribute"
    // InternalEFactory.g:481:1: entryRuleDateAttribute : ruleDateAttribute EOF ;
    public final void entryRuleDateAttribute() throws RecognitionException {
        try {
            // InternalEFactory.g:482:1: ( ruleDateAttribute EOF )
            // InternalEFactory.g:483:1: ruleDateAttribute EOF
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getDateAttributeRule()); 
            }
            pushFollow(FOLLOW_1);
            ruleDateAttribute();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getDateAttributeRule()); 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "entryRuleDateAttribute"


    // $ANTLR start "ruleDateAttribute"
    // InternalEFactory.g:490:1: ruleDateAttribute : ( ( rule__DateAttribute__ValueAssignment ) ) ;
    public final void ruleDateAttribute() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:494:2: ( ( ( rule__DateAttribute__ValueAssignment ) ) )
            // InternalEFactory.g:495:1: ( ( rule__DateAttribute__ValueAssignment ) )
            {
            // InternalEFactory.g:495:1: ( ( rule__DateAttribute__ValueAssignment ) )
            // InternalEFactory.g:496:1: ( rule__DateAttribute__ValueAssignment )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getDateAttributeAccess().getValueAssignment()); 
            }
            // InternalEFactory.g:497:1: ( rule__DateAttribute__ValueAssignment )
            // InternalEFactory.g:497:2: rule__DateAttribute__ValueAssignment
            {
            pushFollow(FOLLOW_2);
            rule__DateAttribute__ValueAssignment();

            state._fsp--;
            if (state.failed) return ;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getDateAttributeAccess().getValueAssignment()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "ruleDateAttribute"


    // $ANTLR start "entryRuleNullAttribute"
    // InternalEFactory.g:509:1: entryRuleNullAttribute : ruleNullAttribute EOF ;
    public final void entryRuleNullAttribute() throws RecognitionException {
        try {
            // InternalEFactory.g:510:1: ( ruleNullAttribute EOF )
            // InternalEFactory.g:511:1: ruleNullAttribute EOF
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getNullAttributeRule()); 
            }
            pushFollow(FOLLOW_1);
            ruleNullAttribute();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getNullAttributeRule()); 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "entryRuleNullAttribute"


    // $ANTLR start "ruleNullAttribute"
    // InternalEFactory.g:518:1: ruleNullAttribute : ( ( rule__NullAttribute__ValueAssignment ) ) ;
    public final void ruleNullAttribute() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:522:2: ( ( ( rule__NullAttribute__ValueAssignment ) ) )
            // InternalEFactory.g:523:1: ( ( rule__NullAttribute__ValueAssignment ) )
            {
            // InternalEFactory.g:523:1: ( ( rule__NullAttribute__ValueAssignment ) )
            // InternalEFactory.g:524:1: ( rule__NullAttribute__ValueAssignment )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getNullAttributeAccess().getValueAssignment()); 
            }
            // InternalEFactory.g:525:1: ( rule__NullAttribute__ValueAssignment )
            // InternalEFactory.g:525:2: rule__NullAttribute__ValueAssignment
            {
            pushFollow(FOLLOW_2);
            rule__NullAttribute__ValueAssignment();

            state._fsp--;
            if (state.failed) return ;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getNullAttributeAccess().getValueAssignment()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "ruleNullAttribute"


    // $ANTLR start "entryRuleBooleanAttribute"
    // InternalEFactory.g:537:1: entryRuleBooleanAttribute : ruleBooleanAttribute EOF ;
    public final void entryRuleBooleanAttribute() throws RecognitionException {
        try {
            // InternalEFactory.g:538:1: ( ruleBooleanAttribute EOF )
            // InternalEFactory.g:539:1: ruleBooleanAttribute EOF
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getBooleanAttributeRule()); 
            }
            pushFollow(FOLLOW_1);
            ruleBooleanAttribute();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getBooleanAttributeRule()); 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "entryRuleBooleanAttribute"


    // $ANTLR start "ruleBooleanAttribute"
    // InternalEFactory.g:546:1: ruleBooleanAttribute : ( ( rule__BooleanAttribute__ValueAssignment ) ) ;
    public final void ruleBooleanAttribute() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:550:2: ( ( ( rule__BooleanAttribute__ValueAssignment ) ) )
            // InternalEFactory.g:551:1: ( ( rule__BooleanAttribute__ValueAssignment ) )
            {
            // InternalEFactory.g:551:1: ( ( rule__BooleanAttribute__ValueAssignment ) )
            // InternalEFactory.g:552:1: ( rule__BooleanAttribute__ValueAssignment )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getBooleanAttributeAccess().getValueAssignment()); 
            }
            // InternalEFactory.g:553:1: ( rule__BooleanAttribute__ValueAssignment )
            // InternalEFactory.g:553:2: rule__BooleanAttribute__ValueAssignment
            {
            pushFollow(FOLLOW_2);
            rule__BooleanAttribute__ValueAssignment();

            state._fsp--;
            if (state.failed) return ;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getBooleanAttributeAccess().getValueAssignment()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "ruleBooleanAttribute"


    // $ANTLR start "entryRuleStringOrQualifiedNameWithWildcard"
    // InternalEFactory.g:565:1: entryRuleStringOrQualifiedNameWithWildcard : ruleStringOrQualifiedNameWithWildcard EOF ;
    public final void entryRuleStringOrQualifiedNameWithWildcard() throws RecognitionException {
        try {
            // InternalEFactory.g:566:1: ( ruleStringOrQualifiedNameWithWildcard EOF )
            // InternalEFactory.g:567:1: ruleStringOrQualifiedNameWithWildcard EOF
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getStringOrQualifiedNameWithWildcardRule()); 
            }
            pushFollow(FOLLOW_1);
            ruleStringOrQualifiedNameWithWildcard();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getStringOrQualifiedNameWithWildcardRule()); 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "entryRuleStringOrQualifiedNameWithWildcard"


    // $ANTLR start "ruleStringOrQualifiedNameWithWildcard"
    // InternalEFactory.g:574:1: ruleStringOrQualifiedNameWithWildcard : ( ( rule__StringOrQualifiedNameWithWildcard__Alternatives ) ) ;
    public final void ruleStringOrQualifiedNameWithWildcard() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:578:2: ( ( ( rule__StringOrQualifiedNameWithWildcard__Alternatives ) ) )
            // InternalEFactory.g:579:1: ( ( rule__StringOrQualifiedNameWithWildcard__Alternatives ) )
            {
            // InternalEFactory.g:579:1: ( ( rule__StringOrQualifiedNameWithWildcard__Alternatives ) )
            // InternalEFactory.g:580:1: ( rule__StringOrQualifiedNameWithWildcard__Alternatives )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getStringOrQualifiedNameWithWildcardAccess().getAlternatives()); 
            }
            // InternalEFactory.g:581:1: ( rule__StringOrQualifiedNameWithWildcard__Alternatives )
            // InternalEFactory.g:581:2: rule__StringOrQualifiedNameWithWildcard__Alternatives
            {
            pushFollow(FOLLOW_2);
            rule__StringOrQualifiedNameWithWildcard__Alternatives();

            state._fsp--;
            if (state.failed) return ;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getStringOrQualifiedNameWithWildcardAccess().getAlternatives()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "ruleStringOrQualifiedNameWithWildcard"


    // $ANTLR start "entryRuleQualifiedNameWithWildcard"
    // InternalEFactory.g:593:1: entryRuleQualifiedNameWithWildcard : ruleQualifiedNameWithWildcard EOF ;
    public final void entryRuleQualifiedNameWithWildcard() throws RecognitionException {
        try {
            // InternalEFactory.g:594:1: ( ruleQualifiedNameWithWildcard EOF )
            // InternalEFactory.g:595:1: ruleQualifiedNameWithWildcard EOF
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getQualifiedNameWithWildcardRule()); 
            }
            pushFollow(FOLLOW_1);
            ruleQualifiedNameWithWildcard();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getQualifiedNameWithWildcardRule()); 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "entryRuleQualifiedNameWithWildcard"


    // $ANTLR start "ruleQualifiedNameWithWildcard"
    // InternalEFactory.g:602:1: ruleQualifiedNameWithWildcard : ( ( rule__QualifiedNameWithWildcard__Group__0 ) ) ;
    public final void ruleQualifiedNameWithWildcard() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:606:2: ( ( ( rule__QualifiedNameWithWildcard__Group__0 ) ) )
            // InternalEFactory.g:607:1: ( ( rule__QualifiedNameWithWildcard__Group__0 ) )
            {
            // InternalEFactory.g:607:1: ( ( rule__QualifiedNameWithWildcard__Group__0 ) )
            // InternalEFactory.g:608:1: ( rule__QualifiedNameWithWildcard__Group__0 )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getQualifiedNameWithWildcardAccess().getGroup()); 
            }
            // InternalEFactory.g:609:1: ( rule__QualifiedNameWithWildcard__Group__0 )
            // InternalEFactory.g:609:2: rule__QualifiedNameWithWildcard__Group__0
            {
            pushFollow(FOLLOW_2);
            rule__QualifiedNameWithWildcard__Group__0();

            state._fsp--;
            if (state.failed) return ;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getQualifiedNameWithWildcardAccess().getGroup()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "ruleQualifiedNameWithWildcard"


    // $ANTLR start "entryRuleQualifiedName"
    // InternalEFactory.g:621:1: entryRuleQualifiedName : ruleQualifiedName EOF ;
    public final void entryRuleQualifiedName() throws RecognitionException {
        try {
            // InternalEFactory.g:622:1: ( ruleQualifiedName EOF )
            // InternalEFactory.g:623:1: ruleQualifiedName EOF
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getQualifiedNameRule()); 
            }
            pushFollow(FOLLOW_1);
            ruleQualifiedName();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getQualifiedNameRule()); 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "entryRuleQualifiedName"


    // $ANTLR start "ruleQualifiedName"
    // InternalEFactory.g:630:1: ruleQualifiedName : ( ( rule__QualifiedName__Group__0 ) ) ;
    public final void ruleQualifiedName() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:634:2: ( ( ( rule__QualifiedName__Group__0 ) ) )
            // InternalEFactory.g:635:1: ( ( rule__QualifiedName__Group__0 ) )
            {
            // InternalEFactory.g:635:1: ( ( rule__QualifiedName__Group__0 ) )
            // InternalEFactory.g:636:1: ( rule__QualifiedName__Group__0 )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getQualifiedNameAccess().getGroup()); 
            }
            // InternalEFactory.g:637:1: ( rule__QualifiedName__Group__0 )
            // InternalEFactory.g:637:2: rule__QualifiedName__Group__0
            {
            pushFollow(FOLLOW_2);
            rule__QualifiedName__Group__0();

            state._fsp--;
            if (state.failed) return ;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getQualifiedNameAccess().getGroup()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "ruleQualifiedName"


    // $ANTLR start "entryRuleValidID"
    // InternalEFactory.g:649:1: entryRuleValidID : ruleValidID EOF ;
    public final void entryRuleValidID() throws RecognitionException {
        try {
            // InternalEFactory.g:650:1: ( ruleValidID EOF )
            // InternalEFactory.g:651:1: ruleValidID EOF
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getValidIDRule()); 
            }
            pushFollow(FOLLOW_1);
            ruleValidID();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getValidIDRule()); 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "entryRuleValidID"


    // $ANTLR start "ruleValidID"
    // InternalEFactory.g:658:1: ruleValidID : ( ( rule__ValidID__Alternatives ) ) ;
    public final void ruleValidID() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:662:2: ( ( ( rule__ValidID__Alternatives ) ) )
            // InternalEFactory.g:663:1: ( ( rule__ValidID__Alternatives ) )
            {
            // InternalEFactory.g:663:1: ( ( rule__ValidID__Alternatives ) )
            // InternalEFactory.g:664:1: ( rule__ValidID__Alternatives )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getValidIDAccess().getAlternatives()); 
            }
            // InternalEFactory.g:665:1: ( rule__ValidID__Alternatives )
            // InternalEFactory.g:665:2: rule__ValidID__Alternatives
            {
            pushFollow(FOLLOW_2);
            rule__ValidID__Alternatives();

            state._fsp--;
            if (state.failed) return ;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getValidIDAccess().getAlternatives()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "ruleValidID"


    // $ANTLR start "entryRuleBoolean"
    // InternalEFactory.g:677:1: entryRuleBoolean : ruleBoolean EOF ;
    public final void entryRuleBoolean() throws RecognitionException {
        try {
            // InternalEFactory.g:678:1: ( ruleBoolean EOF )
            // InternalEFactory.g:679:1: ruleBoolean EOF
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getBooleanRule()); 
            }
            pushFollow(FOLLOW_1);
            ruleBoolean();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getBooleanRule()); 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "entryRuleBoolean"


    // $ANTLR start "ruleBoolean"
    // InternalEFactory.g:686:1: ruleBoolean : ( ( rule__Boolean__Alternatives ) ) ;
    public final void ruleBoolean() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:690:2: ( ( ( rule__Boolean__Alternatives ) ) )
            // InternalEFactory.g:691:1: ( ( rule__Boolean__Alternatives ) )
            {
            // InternalEFactory.g:691:1: ( ( rule__Boolean__Alternatives ) )
            // InternalEFactory.g:692:1: ( rule__Boolean__Alternatives )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getBooleanAccess().getAlternatives()); 
            }
            // InternalEFactory.g:693:1: ( rule__Boolean__Alternatives )
            // InternalEFactory.g:693:2: rule__Boolean__Alternatives
            {
            pushFollow(FOLLOW_2);
            rule__Boolean__Alternatives();

            state._fsp--;
            if (state.failed) return ;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getBooleanAccess().getAlternatives()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "ruleBoolean"


    // $ANTLR start "entryRuleLong"
    // InternalEFactory.g:705:1: entryRuleLong : ruleLong EOF ;
    public final void entryRuleLong() throws RecognitionException {
        try {
            // InternalEFactory.g:706:1: ( ruleLong EOF )
            // InternalEFactory.g:707:1: ruleLong EOF
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getLongRule()); 
            }
            pushFollow(FOLLOW_1);
            ruleLong();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getLongRule()); 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "entryRuleLong"


    // $ANTLR start "ruleLong"
    // InternalEFactory.g:714:1: ruleLong : ( ( rule__Long__Group__0 ) ) ;
    public final void ruleLong() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:718:2: ( ( ( rule__Long__Group__0 ) ) )
            // InternalEFactory.g:719:1: ( ( rule__Long__Group__0 ) )
            {
            // InternalEFactory.g:719:1: ( ( rule__Long__Group__0 ) )
            // InternalEFactory.g:720:1: ( rule__Long__Group__0 )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getLongAccess().getGroup()); 
            }
            // InternalEFactory.g:721:1: ( rule__Long__Group__0 )
            // InternalEFactory.g:721:2: rule__Long__Group__0
            {
            pushFollow(FOLLOW_2);
            rule__Long__Group__0();

            state._fsp--;
            if (state.failed) return ;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getLongAccess().getGroup()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "ruleLong"


    // $ANTLR start "entryRuleDouble"
    // InternalEFactory.g:733:1: entryRuleDouble : ruleDouble EOF ;
    public final void entryRuleDouble() throws RecognitionException {
        try {
            // InternalEFactory.g:734:1: ( ruleDouble EOF )
            // InternalEFactory.g:735:1: ruleDouble EOF
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getDoubleRule()); 
            }
            pushFollow(FOLLOW_1);
            ruleDouble();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getDoubleRule()); 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "entryRuleDouble"


    // $ANTLR start "ruleDouble"
    // InternalEFactory.g:742:1: ruleDouble : ( ( rule__Double__Group__0 ) ) ;
    public final void ruleDouble() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:746:2: ( ( ( rule__Double__Group__0 ) ) )
            // InternalEFactory.g:747:1: ( ( rule__Double__Group__0 ) )
            {
            // InternalEFactory.g:747:1: ( ( rule__Double__Group__0 ) )
            // InternalEFactory.g:748:1: ( rule__Double__Group__0 )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getDoubleAccess().getGroup()); 
            }
            // InternalEFactory.g:749:1: ( rule__Double__Group__0 )
            // InternalEFactory.g:749:2: rule__Double__Group__0
            {
            pushFollow(FOLLOW_2);
            rule__Double__Group__0();

            state._fsp--;
            if (state.failed) return ;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getDoubleAccess().getGroup()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "ruleDouble"


    // $ANTLR start "entryRuleDate"
    // InternalEFactory.g:761:1: entryRuleDate : ruleDate EOF ;
    public final void entryRuleDate() throws RecognitionException {
        try {
            // InternalEFactory.g:762:1: ( ruleDate EOF )
            // InternalEFactory.g:763:1: ruleDate EOF
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getDateRule()); 
            }
            pushFollow(FOLLOW_1);
            ruleDate();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getDateRule()); 
            }
            match(input,EOF,FOLLOW_2); if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {
        }
        return ;
    }
    // $ANTLR end "entryRuleDate"


    // $ANTLR start "ruleDate"
    // InternalEFactory.g:770:1: ruleDate : ( ( rule__Date__Group__0 ) ) ;
    public final void ruleDate() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:774:2: ( ( ( rule__Date__Group__0 ) ) )
            // InternalEFactory.g:775:1: ( ( rule__Date__Group__0 ) )
            {
            // InternalEFactory.g:775:1: ( ( rule__Date__Group__0 ) )
            // InternalEFactory.g:776:1: ( rule__Date__Group__0 )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getDateAccess().getGroup()); 
            }
            // InternalEFactory.g:777:1: ( rule__Date__Group__0 )
            // InternalEFactory.g:777:2: rule__Date__Group__0
            {
            pushFollow(FOLLOW_2);
            rule__Date__Group__0();

            state._fsp--;
            if (state.failed) return ;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getDateAccess().getGroup()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "ruleDate"


    // $ANTLR start "rule__Value__Alternatives"
    // InternalEFactory.g:789:1: rule__Value__Alternatives : ( ( ruleMultiValue ) | ( ruleAttribute ) | ( ( rule__Value__Group_2__0 ) ) | ( ( ruleReference ) ) );
    public final void rule__Value__Alternatives() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:793:1: ( ( ruleMultiValue ) | ( ruleAttribute ) | ( ( rule__Value__Group_2__0 ) ) | ( ( ruleReference ) ) )
            int alt1=4;
            switch ( input.LA(1) ) {
            case 22:
                {
                alt1=1;
                }
                break;
            case RULE_STRING:
            case 13:
            case 14:
            case 21:
            case 26:
            case 27:
                {
                alt1=2;
                }
                break;
            case RULE_LONG_UNSIGNED:
                {
                int LA1_3 = input.LA(2);

                if ( (synpred2_InternalEFactory()) ) {
                    alt1=2;
                }
                else if ( (synpred3_InternalEFactory()) ) {
                    alt1=3;
                }
                else if ( (true) ) {
                    alt1=4;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return ;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 1, 3, input);

                    throw nvae;
                }
                }
                break;
            case RULE_ID:
                {
                int LA1_4 = input.LA(2);

                if ( (synpred3_InternalEFactory()) ) {
                    alt1=3;
                }
                else if ( (true) ) {
                    alt1=4;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return ;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 1, 4, input);

                    throw nvae;
                }
                }
                break;
            case RULE_LONG_ID:
                {
                int LA1_5 = input.LA(2);

                if ( (synpred3_InternalEFactory()) ) {
                    alt1=3;
                }
                else if ( (true) ) {
                    alt1=4;
                }
                else {
                    if (state.backtracking>0) {state.failed=true; return ;}
                    NoViableAltException nvae =
                        new NoViableAltException("", 1, 5, input);

                    throw nvae;
                }
                }
                break;
            case 18:
                {
                alt1=3;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return ;}
                NoViableAltException nvae =
                    new NoViableAltException("", 1, 0, input);

                throw nvae;
            }

            switch (alt1) {
                case 1 :
                    // InternalEFactory.g:794:1: ( ruleMultiValue )
                    {
                    // InternalEFactory.g:794:1: ( ruleMultiValue )
                    // InternalEFactory.g:795:1: ruleMultiValue
                    {
                    if ( state.backtracking==0 ) {
                       before(grammarAccess.getValueAccess().getMultiValueParserRuleCall_0()); 
                    }
                    pushFollow(FOLLOW_2);
                    ruleMultiValue();

                    state._fsp--;
                    if (state.failed) return ;
                    if ( state.backtracking==0 ) {
                       after(grammarAccess.getValueAccess().getMultiValueParserRuleCall_0()); 
                    }

                    }


                    }
                    break;
                case 2 :
                    // InternalEFactory.g:800:6: ( ruleAttribute )
                    {
                    // InternalEFactory.g:800:6: ( ruleAttribute )
                    // InternalEFactory.g:801:1: ruleAttribute
                    {
                    if ( state.backtracking==0 ) {
                       before(grammarAccess.getValueAccess().getAttributeParserRuleCall_1()); 
                    }
                    pushFollow(FOLLOW_2);
                    ruleAttribute();

                    state._fsp--;
                    if (state.failed) return ;
                    if ( state.backtracking==0 ) {
                       after(grammarAccess.getValueAccess().getAttributeParserRuleCall_1()); 
                    }

                    }


                    }
                    break;
                case 3 :
                    // InternalEFactory.g:806:6: ( ( rule__Value__Group_2__0 ) )
                    {
                    // InternalEFactory.g:806:6: ( ( rule__Value__Group_2__0 ) )
                    // InternalEFactory.g:807:1: ( rule__Value__Group_2__0 )
                    {
                    if ( state.backtracking==0 ) {
                       before(grammarAccess.getValueAccess().getGroup_2()); 
                    }
                    // InternalEFactory.g:808:1: ( rule__Value__Group_2__0 )
                    // InternalEFactory.g:808:2: rule__Value__Group_2__0
                    {
                    pushFollow(FOLLOW_2);
                    rule__Value__Group_2__0();

                    state._fsp--;
                    if (state.failed) return ;

                    }

                    if ( state.backtracking==0 ) {
                       after(grammarAccess.getValueAccess().getGroup_2()); 
                    }

                    }


                    }
                    break;
                case 4 :
                    // InternalEFactory.g:812:6: ( ( ruleReference ) )
                    {
                    // InternalEFactory.g:812:6: ( ( ruleReference ) )
                    // InternalEFactory.g:813:1: ( ruleReference )
                    {
                    if ( state.backtracking==0 ) {
                       before(grammarAccess.getValueAccess().getReferenceParserRuleCall_3()); 
                    }
                    // InternalEFactory.g:814:1: ( ruleReference )
                    // InternalEFactory.g:814:3: ruleReference
                    {
                    pushFollow(FOLLOW_2);
                    ruleReference();

                    state._fsp--;
                    if (state.failed) return ;

                    }

                    if ( state.backtracking==0 ) {
                       after(grammarAccess.getValueAccess().getReferenceParserRuleCall_3()); 
                    }

                    }


                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Value__Alternatives"


    // $ANTLR start "rule__Attribute__Alternatives"
    // InternalEFactory.g:823:1: rule__Attribute__Alternatives : ( ( ruleEnumAttribute ) | ( ruleStringAttribute ) | ( ruleIntegerAttribute ) | ( ruleBooleanAttribute ) | ( ruleDoubleAttribute ) | ( ruleDateAttribute ) | ( ruleNullAttribute ) );
    public final void rule__Attribute__Alternatives() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:827:1: ( ( ruleEnumAttribute ) | ( ruleStringAttribute ) | ( ruleIntegerAttribute ) | ( ruleBooleanAttribute ) | ( ruleDoubleAttribute ) | ( ruleDateAttribute ) | ( ruleNullAttribute ) )
            int alt2=7;
            alt2 = dfa2.predict(input);
            switch (alt2) {
                case 1 :
                    // InternalEFactory.g:828:1: ( ruleEnumAttribute )
                    {
                    // InternalEFactory.g:828:1: ( ruleEnumAttribute )
                    // InternalEFactory.g:829:1: ruleEnumAttribute
                    {
                    if ( state.backtracking==0 ) {
                       before(grammarAccess.getAttributeAccess().getEnumAttributeParserRuleCall_0()); 
                    }
                    pushFollow(FOLLOW_2);
                    ruleEnumAttribute();

                    state._fsp--;
                    if (state.failed) return ;
                    if ( state.backtracking==0 ) {
                       after(grammarAccess.getAttributeAccess().getEnumAttributeParserRuleCall_0()); 
                    }

                    }


                    }
                    break;
                case 2 :
                    // InternalEFactory.g:834:6: ( ruleStringAttribute )
                    {
                    // InternalEFactory.g:834:6: ( ruleStringAttribute )
                    // InternalEFactory.g:835:1: ruleStringAttribute
                    {
                    if ( state.backtracking==0 ) {
                       before(grammarAccess.getAttributeAccess().getStringAttributeParserRuleCall_1()); 
                    }
                    pushFollow(FOLLOW_2);
                    ruleStringAttribute();

                    state._fsp--;
                    if (state.failed) return ;
                    if ( state.backtracking==0 ) {
                       after(grammarAccess.getAttributeAccess().getStringAttributeParserRuleCall_1()); 
                    }

                    }


                    }
                    break;
                case 3 :
                    // InternalEFactory.g:840:6: ( ruleIntegerAttribute )
                    {
                    // InternalEFactory.g:840:6: ( ruleIntegerAttribute )
                    // InternalEFactory.g:841:1: ruleIntegerAttribute
                    {
                    if ( state.backtracking==0 ) {
                       before(grammarAccess.getAttributeAccess().getIntegerAttributeParserRuleCall_2()); 
                    }
                    pushFollow(FOLLOW_2);
                    ruleIntegerAttribute();

                    state._fsp--;
                    if (state.failed) return ;
                    if ( state.backtracking==0 ) {
                       after(grammarAccess.getAttributeAccess().getIntegerAttributeParserRuleCall_2()); 
                    }

                    }


                    }
                    break;
                case 4 :
                    // InternalEFactory.g:846:6: ( ruleBooleanAttribute )
                    {
                    // InternalEFactory.g:846:6: ( ruleBooleanAttribute )
                    // InternalEFactory.g:847:1: ruleBooleanAttribute
                    {
                    if ( state.backtracking==0 ) {
                       before(grammarAccess.getAttributeAccess().getBooleanAttributeParserRuleCall_3()); 
                    }
                    pushFollow(FOLLOW_2);
                    ruleBooleanAttribute();

                    state._fsp--;
                    if (state.failed) return ;
                    if ( state.backtracking==0 ) {
                       after(grammarAccess.getAttributeAccess().getBooleanAttributeParserRuleCall_3()); 
                    }

                    }


                    }
                    break;
                case 5 :
                    // InternalEFactory.g:852:6: ( ruleDoubleAttribute )
                    {
                    // InternalEFactory.g:852:6: ( ruleDoubleAttribute )
                    // InternalEFactory.g:853:1: ruleDoubleAttribute
                    {
                    if ( state.backtracking==0 ) {
                       before(grammarAccess.getAttributeAccess().getDoubleAttributeParserRuleCall_4()); 
                    }
                    pushFollow(FOLLOW_2);
                    ruleDoubleAttribute();

                    state._fsp--;
                    if (state.failed) return ;
                    if ( state.backtracking==0 ) {
                       after(grammarAccess.getAttributeAccess().getDoubleAttributeParserRuleCall_4()); 
                    }

                    }


                    }
                    break;
                case 6 :
                    // InternalEFactory.g:858:6: ( ruleDateAttribute )
                    {
                    // InternalEFactory.g:858:6: ( ruleDateAttribute )
                    // InternalEFactory.g:859:1: ruleDateAttribute
                    {
                    if ( state.backtracking==0 ) {
                       before(grammarAccess.getAttributeAccess().getDateAttributeParserRuleCall_5()); 
                    }
                    pushFollow(FOLLOW_2);
                    ruleDateAttribute();

                    state._fsp--;
                    if (state.failed) return ;
                    if ( state.backtracking==0 ) {
                       after(grammarAccess.getAttributeAccess().getDateAttributeParserRuleCall_5()); 
                    }

                    }


                    }
                    break;
                case 7 :
                    // InternalEFactory.g:864:6: ( ruleNullAttribute )
                    {
                    // InternalEFactory.g:864:6: ( ruleNullAttribute )
                    // InternalEFactory.g:865:1: ruleNullAttribute
                    {
                    if ( state.backtracking==0 ) {
                       before(grammarAccess.getAttributeAccess().getNullAttributeParserRuleCall_6()); 
                    }
                    pushFollow(FOLLOW_2);
                    ruleNullAttribute();

                    state._fsp--;
                    if (state.failed) return ;
                    if ( state.backtracking==0 ) {
                       after(grammarAccess.getAttributeAccess().getNullAttributeParserRuleCall_6()); 
                    }

                    }


                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Attribute__Alternatives"


    // $ANTLR start "rule__StringOrQualifiedNameWithWildcard__Alternatives"
    // InternalEFactory.g:875:1: rule__StringOrQualifiedNameWithWildcard__Alternatives : ( ( RULE_STRING ) | ( ruleQualifiedNameWithWildcard ) );
    public final void rule__StringOrQualifiedNameWithWildcard__Alternatives() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:879:1: ( ( RULE_STRING ) | ( ruleQualifiedNameWithWildcard ) )
            int alt3=2;
            int LA3_0 = input.LA(1);

            if ( (LA3_0==RULE_STRING) ) {
                alt3=1;
            }
            else if ( ((LA3_0>=RULE_ID && LA3_0<=RULE_LONG_UNSIGNED)) ) {
                alt3=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return ;}
                NoViableAltException nvae =
                    new NoViableAltException("", 3, 0, input);

                throw nvae;
            }
            switch (alt3) {
                case 1 :
                    // InternalEFactory.g:880:1: ( RULE_STRING )
                    {
                    // InternalEFactory.g:880:1: ( RULE_STRING )
                    // InternalEFactory.g:881:1: RULE_STRING
                    {
                    if ( state.backtracking==0 ) {
                       before(grammarAccess.getStringOrQualifiedNameWithWildcardAccess().getSTRINGTerminalRuleCall_0()); 
                    }
                    match(input,RULE_STRING,FOLLOW_2); if (state.failed) return ;
                    if ( state.backtracking==0 ) {
                       after(grammarAccess.getStringOrQualifiedNameWithWildcardAccess().getSTRINGTerminalRuleCall_0()); 
                    }

                    }


                    }
                    break;
                case 2 :
                    // InternalEFactory.g:886:6: ( ruleQualifiedNameWithWildcard )
                    {
                    // InternalEFactory.g:886:6: ( ruleQualifiedNameWithWildcard )
                    // InternalEFactory.g:887:1: ruleQualifiedNameWithWildcard
                    {
                    if ( state.backtracking==0 ) {
                       before(grammarAccess.getStringOrQualifiedNameWithWildcardAccess().getQualifiedNameWithWildcardParserRuleCall_1()); 
                    }
                    pushFollow(FOLLOW_2);
                    ruleQualifiedNameWithWildcard();

                    state._fsp--;
                    if (state.failed) return ;
                    if ( state.backtracking==0 ) {
                       after(grammarAccess.getStringOrQualifiedNameWithWildcardAccess().getQualifiedNameWithWildcardParserRuleCall_1()); 
                    }

                    }


                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__StringOrQualifiedNameWithWildcard__Alternatives"


    // $ANTLR start "rule__ValidID__Alternatives"
    // InternalEFactory.g:897:1: rule__ValidID__Alternatives : ( ( RULE_ID ) | ( RULE_LONG_ID ) | ( RULE_LONG_UNSIGNED ) );
    public final void rule__ValidID__Alternatives() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:901:1: ( ( RULE_ID ) | ( RULE_LONG_ID ) | ( RULE_LONG_UNSIGNED ) )
            int alt4=3;
            switch ( input.LA(1) ) {
            case RULE_ID:
                {
                alt4=1;
                }
                break;
            case RULE_LONG_ID:
                {
                alt4=2;
                }
                break;
            case RULE_LONG_UNSIGNED:
                {
                alt4=3;
                }
                break;
            default:
                if (state.backtracking>0) {state.failed=true; return ;}
                NoViableAltException nvae =
                    new NoViableAltException("", 4, 0, input);

                throw nvae;
            }

            switch (alt4) {
                case 1 :
                    // InternalEFactory.g:902:1: ( RULE_ID )
                    {
                    // InternalEFactory.g:902:1: ( RULE_ID )
                    // InternalEFactory.g:903:1: RULE_ID
                    {
                    if ( state.backtracking==0 ) {
                       before(grammarAccess.getValidIDAccess().getIDTerminalRuleCall_0()); 
                    }
                    match(input,RULE_ID,FOLLOW_2); if (state.failed) return ;
                    if ( state.backtracking==0 ) {
                       after(grammarAccess.getValidIDAccess().getIDTerminalRuleCall_0()); 
                    }

                    }


                    }
                    break;
                case 2 :
                    // InternalEFactory.g:908:6: ( RULE_LONG_ID )
                    {
                    // InternalEFactory.g:908:6: ( RULE_LONG_ID )
                    // InternalEFactory.g:909:1: RULE_LONG_ID
                    {
                    if ( state.backtracking==0 ) {
                       before(grammarAccess.getValidIDAccess().getLONG_IDTerminalRuleCall_1()); 
                    }
                    match(input,RULE_LONG_ID,FOLLOW_2); if (state.failed) return ;
                    if ( state.backtracking==0 ) {
                       after(grammarAccess.getValidIDAccess().getLONG_IDTerminalRuleCall_1()); 
                    }

                    }


                    }
                    break;
                case 3 :
                    // InternalEFactory.g:914:6: ( RULE_LONG_UNSIGNED )
                    {
                    // InternalEFactory.g:914:6: ( RULE_LONG_UNSIGNED )
                    // InternalEFactory.g:915:1: RULE_LONG_UNSIGNED
                    {
                    if ( state.backtracking==0 ) {
                       before(grammarAccess.getValidIDAccess().getLONG_UNSIGNEDTerminalRuleCall_2()); 
                    }
                    match(input,RULE_LONG_UNSIGNED,FOLLOW_2); if (state.failed) return ;
                    if ( state.backtracking==0 ) {
                       after(grammarAccess.getValidIDAccess().getLONG_UNSIGNEDTerminalRuleCall_2()); 
                    }

                    }


                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__ValidID__Alternatives"


    // $ANTLR start "rule__Boolean__Alternatives"
    // InternalEFactory.g:925:1: rule__Boolean__Alternatives : ( ( 'true' ) | ( 'false' ) );
    public final void rule__Boolean__Alternatives() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:929:1: ( ( 'true' ) | ( 'false' ) )
            int alt5=2;
            int LA5_0 = input.LA(1);

            if ( (LA5_0==13) ) {
                alt5=1;
            }
            else if ( (LA5_0==14) ) {
                alt5=2;
            }
            else {
                if (state.backtracking>0) {state.failed=true; return ;}
                NoViableAltException nvae =
                    new NoViableAltException("", 5, 0, input);

                throw nvae;
            }
            switch (alt5) {
                case 1 :
                    // InternalEFactory.g:930:1: ( 'true' )
                    {
                    // InternalEFactory.g:930:1: ( 'true' )
                    // InternalEFactory.g:931:1: 'true'
                    {
                    if ( state.backtracking==0 ) {
                       before(grammarAccess.getBooleanAccess().getTrueKeyword_0()); 
                    }
                    match(input,13,FOLLOW_2); if (state.failed) return ;
                    if ( state.backtracking==0 ) {
                       after(grammarAccess.getBooleanAccess().getTrueKeyword_0()); 
                    }

                    }


                    }
                    break;
                case 2 :
                    // InternalEFactory.g:938:6: ( 'false' )
                    {
                    // InternalEFactory.g:938:6: ( 'false' )
                    // InternalEFactory.g:939:1: 'false'
                    {
                    if ( state.backtracking==0 ) {
                       before(grammarAccess.getBooleanAccess().getFalseKeyword_1()); 
                    }
                    match(input,14,FOLLOW_2); if (state.failed) return ;
                    if ( state.backtracking==0 ) {
                       after(grammarAccess.getBooleanAccess().getFalseKeyword_1()); 
                    }

                    }


                    }
                    break;

            }
        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Boolean__Alternatives"


    // $ANTLR start "rule__Factory__Group__0"
    // InternalEFactory.g:953:1: rule__Factory__Group__0 : rule__Factory__Group__0__Impl rule__Factory__Group__1 ;
    public final void rule__Factory__Group__0() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:957:1: ( rule__Factory__Group__0__Impl rule__Factory__Group__1 )
            // InternalEFactory.g:958:2: rule__Factory__Group__0__Impl rule__Factory__Group__1
            {
            pushFollow(FOLLOW_3);
            rule__Factory__Group__0__Impl();

            state._fsp--;
            if (state.failed) return ;
            pushFollow(FOLLOW_2);
            rule__Factory__Group__1();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Factory__Group__0"


    // $ANTLR start "rule__Factory__Group__0__Impl"
    // InternalEFactory.g:965:1: rule__Factory__Group__0__Impl : ( ( rule__Factory__ImportsAssignment_0 )* ) ;
    public final void rule__Factory__Group__0__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:969:1: ( ( ( rule__Factory__ImportsAssignment_0 )* ) )
            // InternalEFactory.g:970:1: ( ( rule__Factory__ImportsAssignment_0 )* )
            {
            // InternalEFactory.g:970:1: ( ( rule__Factory__ImportsAssignment_0 )* )
            // InternalEFactory.g:971:1: ( rule__Factory__ImportsAssignment_0 )*
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getFactoryAccess().getImportsAssignment_0()); 
            }
            // InternalEFactory.g:972:1: ( rule__Factory__ImportsAssignment_0 )*
            loop6:
            do {
                int alt6=2;
                int LA6_0 = input.LA(1);

                if ( (LA6_0==16) ) {
                    alt6=1;
                }


                switch (alt6) {
            	case 1 :
            	    // InternalEFactory.g:972:2: rule__Factory__ImportsAssignment_0
            	    {
            	    pushFollow(FOLLOW_4);
            	    rule__Factory__ImportsAssignment_0();

            	    state._fsp--;
            	    if (state.failed) return ;

            	    }
            	    break;

            	default :
            	    break loop6;
                }
            } while (true);

            if ( state.backtracking==0 ) {
               after(grammarAccess.getFactoryAccess().getImportsAssignment_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Factory__Group__0__Impl"


    // $ANTLR start "rule__Factory__Group__1"
    // InternalEFactory.g:982:1: rule__Factory__Group__1 : rule__Factory__Group__1__Impl rule__Factory__Group__2 ;
    public final void rule__Factory__Group__1() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:986:1: ( rule__Factory__Group__1__Impl rule__Factory__Group__2 )
            // InternalEFactory.g:987:2: rule__Factory__Group__1__Impl rule__Factory__Group__2
            {
            pushFollow(FOLLOW_3);
            rule__Factory__Group__1__Impl();

            state._fsp--;
            if (state.failed) return ;
            pushFollow(FOLLOW_2);
            rule__Factory__Group__2();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Factory__Group__1"


    // $ANTLR start "rule__Factory__Group__1__Impl"
    // InternalEFactory.g:994:1: rule__Factory__Group__1__Impl : ( ( rule__Factory__EPackagesAssignment_1 )* ) ;
    public final void rule__Factory__Group__1__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:998:1: ( ( ( rule__Factory__EPackagesAssignment_1 )* ) )
            // InternalEFactory.g:999:1: ( ( rule__Factory__EPackagesAssignment_1 )* )
            {
            // InternalEFactory.g:999:1: ( ( rule__Factory__EPackagesAssignment_1 )* )
            // InternalEFactory.g:1000:1: ( rule__Factory__EPackagesAssignment_1 )*
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getFactoryAccess().getEPackagesAssignment_1()); 
            }
            // InternalEFactory.g:1001:1: ( rule__Factory__EPackagesAssignment_1 )*
            loop7:
            do {
                int alt7=2;
                int LA7_0 = input.LA(1);

                if ( (LA7_0==15) ) {
                    alt7=1;
                }


                switch (alt7) {
            	case 1 :
            	    // InternalEFactory.g:1001:2: rule__Factory__EPackagesAssignment_1
            	    {
            	    pushFollow(FOLLOW_5);
            	    rule__Factory__EPackagesAssignment_1();

            	    state._fsp--;
            	    if (state.failed) return ;

            	    }
            	    break;

            	default :
            	    break loop7;
                }
            } while (true);

            if ( state.backtracking==0 ) {
               after(grammarAccess.getFactoryAccess().getEPackagesAssignment_1()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Factory__Group__1__Impl"


    // $ANTLR start "rule__Factory__Group__2"
    // InternalEFactory.g:1011:1: rule__Factory__Group__2 : rule__Factory__Group__2__Impl rule__Factory__Group__3 ;
    public final void rule__Factory__Group__2() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1015:1: ( rule__Factory__Group__2__Impl rule__Factory__Group__3 )
            // InternalEFactory.g:1016:2: rule__Factory__Group__2__Impl rule__Factory__Group__3
            {
            pushFollow(FOLLOW_3);
            rule__Factory__Group__2__Impl();

            state._fsp--;
            if (state.failed) return ;
            pushFollow(FOLLOW_2);
            rule__Factory__Group__3();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Factory__Group__2"


    // $ANTLR start "rule__Factory__Group__2__Impl"
    // InternalEFactory.g:1023:1: rule__Factory__Group__2__Impl : ( ( rule__Factory__AnnotationsAssignment_2 )* ) ;
    public final void rule__Factory__Group__2__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1027:1: ( ( ( rule__Factory__AnnotationsAssignment_2 )* ) )
            // InternalEFactory.g:1028:1: ( ( rule__Factory__AnnotationsAssignment_2 )* )
            {
            // InternalEFactory.g:1028:1: ( ( rule__Factory__AnnotationsAssignment_2 )* )
            // InternalEFactory.g:1029:1: ( rule__Factory__AnnotationsAssignment_2 )*
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getFactoryAccess().getAnnotationsAssignment_2()); 
            }
            // InternalEFactory.g:1030:1: ( rule__Factory__AnnotationsAssignment_2 )*
            loop8:
            do {
                int alt8=2;
                int LA8_0 = input.LA(1);

                if ( (LA8_0==17) ) {
                    alt8=1;
                }


                switch (alt8) {
            	case 1 :
            	    // InternalEFactory.g:1030:2: rule__Factory__AnnotationsAssignment_2
            	    {
            	    pushFollow(FOLLOW_6);
            	    rule__Factory__AnnotationsAssignment_2();

            	    state._fsp--;
            	    if (state.failed) return ;

            	    }
            	    break;

            	default :
            	    break loop8;
                }
            } while (true);

            if ( state.backtracking==0 ) {
               after(grammarAccess.getFactoryAccess().getAnnotationsAssignment_2()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Factory__Group__2__Impl"


    // $ANTLR start "rule__Factory__Group__3"
    // InternalEFactory.g:1040:1: rule__Factory__Group__3 : rule__Factory__Group__3__Impl ;
    public final void rule__Factory__Group__3() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1044:1: ( rule__Factory__Group__3__Impl )
            // InternalEFactory.g:1045:2: rule__Factory__Group__3__Impl
            {
            pushFollow(FOLLOW_2);
            rule__Factory__Group__3__Impl();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Factory__Group__3"


    // $ANTLR start "rule__Factory__Group__3__Impl"
    // InternalEFactory.g:1051:1: rule__Factory__Group__3__Impl : ( ( rule__Factory__RootAssignment_3 ) ) ;
    public final void rule__Factory__Group__3__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1055:1: ( ( ( rule__Factory__RootAssignment_3 ) ) )
            // InternalEFactory.g:1056:1: ( ( rule__Factory__RootAssignment_3 ) )
            {
            // InternalEFactory.g:1056:1: ( ( rule__Factory__RootAssignment_3 ) )
            // InternalEFactory.g:1057:1: ( rule__Factory__RootAssignment_3 )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getFactoryAccess().getRootAssignment_3()); 
            }
            // InternalEFactory.g:1058:1: ( rule__Factory__RootAssignment_3 )
            // InternalEFactory.g:1058:2: rule__Factory__RootAssignment_3
            {
            pushFollow(FOLLOW_2);
            rule__Factory__RootAssignment_3();

            state._fsp--;
            if (state.failed) return ;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getFactoryAccess().getRootAssignment_3()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Factory__Group__3__Impl"


    // $ANTLR start "rule__PackageImport__Group__0"
    // InternalEFactory.g:1076:1: rule__PackageImport__Group__0 : rule__PackageImport__Group__0__Impl rule__PackageImport__Group__1 ;
    public final void rule__PackageImport__Group__0() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1080:1: ( rule__PackageImport__Group__0__Impl rule__PackageImport__Group__1 )
            // InternalEFactory.g:1081:2: rule__PackageImport__Group__0__Impl rule__PackageImport__Group__1
            {
            pushFollow(FOLLOW_7);
            rule__PackageImport__Group__0__Impl();

            state._fsp--;
            if (state.failed) return ;
            pushFollow(FOLLOW_2);
            rule__PackageImport__Group__1();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__PackageImport__Group__0"


    // $ANTLR start "rule__PackageImport__Group__0__Impl"
    // InternalEFactory.g:1088:1: rule__PackageImport__Group__0__Impl : ( 'use' ) ;
    public final void rule__PackageImport__Group__0__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1092:1: ( ( 'use' ) )
            // InternalEFactory.g:1093:1: ( 'use' )
            {
            // InternalEFactory.g:1093:1: ( 'use' )
            // InternalEFactory.g:1094:1: 'use'
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getPackageImportAccess().getUseKeyword_0()); 
            }
            match(input,15,FOLLOW_2); if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getPackageImportAccess().getUseKeyword_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__PackageImport__Group__0__Impl"


    // $ANTLR start "rule__PackageImport__Group__1"
    // InternalEFactory.g:1107:1: rule__PackageImport__Group__1 : rule__PackageImport__Group__1__Impl ;
    public final void rule__PackageImport__Group__1() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1111:1: ( rule__PackageImport__Group__1__Impl )
            // InternalEFactory.g:1112:2: rule__PackageImport__Group__1__Impl
            {
            pushFollow(FOLLOW_2);
            rule__PackageImport__Group__1__Impl();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__PackageImport__Group__1"


    // $ANTLR start "rule__PackageImport__Group__1__Impl"
    // InternalEFactory.g:1118:1: rule__PackageImport__Group__1__Impl : ( ( rule__PackageImport__EPackageAssignment_1 ) ) ;
    public final void rule__PackageImport__Group__1__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1122:1: ( ( ( rule__PackageImport__EPackageAssignment_1 ) ) )
            // InternalEFactory.g:1123:1: ( ( rule__PackageImport__EPackageAssignment_1 ) )
            {
            // InternalEFactory.g:1123:1: ( ( rule__PackageImport__EPackageAssignment_1 ) )
            // InternalEFactory.g:1124:1: ( rule__PackageImport__EPackageAssignment_1 )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getPackageImportAccess().getEPackageAssignment_1()); 
            }
            // InternalEFactory.g:1125:1: ( rule__PackageImport__EPackageAssignment_1 )
            // InternalEFactory.g:1125:2: rule__PackageImport__EPackageAssignment_1
            {
            pushFollow(FOLLOW_2);
            rule__PackageImport__EPackageAssignment_1();

            state._fsp--;
            if (state.failed) return ;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getPackageImportAccess().getEPackageAssignment_1()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__PackageImport__Group__1__Impl"


    // $ANTLR start "rule__NamespaceImport__Group__0"
    // InternalEFactory.g:1139:1: rule__NamespaceImport__Group__0 : rule__NamespaceImport__Group__0__Impl rule__NamespaceImport__Group__1 ;
    public final void rule__NamespaceImport__Group__0() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1143:1: ( rule__NamespaceImport__Group__0__Impl rule__NamespaceImport__Group__1 )
            // InternalEFactory.g:1144:2: rule__NamespaceImport__Group__0__Impl rule__NamespaceImport__Group__1
            {
            pushFollow(FOLLOW_7);
            rule__NamespaceImport__Group__0__Impl();

            state._fsp--;
            if (state.failed) return ;
            pushFollow(FOLLOW_2);
            rule__NamespaceImport__Group__1();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__NamespaceImport__Group__0"


    // $ANTLR start "rule__NamespaceImport__Group__0__Impl"
    // InternalEFactory.g:1151:1: rule__NamespaceImport__Group__0__Impl : ( 'import' ) ;
    public final void rule__NamespaceImport__Group__0__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1155:1: ( ( 'import' ) )
            // InternalEFactory.g:1156:1: ( 'import' )
            {
            // InternalEFactory.g:1156:1: ( 'import' )
            // InternalEFactory.g:1157:1: 'import'
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getNamespaceImportAccess().getImportKeyword_0()); 
            }
            match(input,16,FOLLOW_2); if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getNamespaceImportAccess().getImportKeyword_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__NamespaceImport__Group__0__Impl"


    // $ANTLR start "rule__NamespaceImport__Group__1"
    // InternalEFactory.g:1170:1: rule__NamespaceImport__Group__1 : rule__NamespaceImport__Group__1__Impl ;
    public final void rule__NamespaceImport__Group__1() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1174:1: ( rule__NamespaceImport__Group__1__Impl )
            // InternalEFactory.g:1175:2: rule__NamespaceImport__Group__1__Impl
            {
            pushFollow(FOLLOW_2);
            rule__NamespaceImport__Group__1__Impl();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__NamespaceImport__Group__1"


    // $ANTLR start "rule__NamespaceImport__Group__1__Impl"
    // InternalEFactory.g:1181:1: rule__NamespaceImport__Group__1__Impl : ( ( rule__NamespaceImport__ImportedNamespaceAssignment_1 ) ) ;
    public final void rule__NamespaceImport__Group__1__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1185:1: ( ( ( rule__NamespaceImport__ImportedNamespaceAssignment_1 ) ) )
            // InternalEFactory.g:1186:1: ( ( rule__NamespaceImport__ImportedNamespaceAssignment_1 ) )
            {
            // InternalEFactory.g:1186:1: ( ( rule__NamespaceImport__ImportedNamespaceAssignment_1 ) )
            // InternalEFactory.g:1187:1: ( rule__NamespaceImport__ImportedNamespaceAssignment_1 )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getNamespaceImportAccess().getImportedNamespaceAssignment_1()); 
            }
            // InternalEFactory.g:1188:1: ( rule__NamespaceImport__ImportedNamespaceAssignment_1 )
            // InternalEFactory.g:1188:2: rule__NamespaceImport__ImportedNamespaceAssignment_1
            {
            pushFollow(FOLLOW_2);
            rule__NamespaceImport__ImportedNamespaceAssignment_1();

            state._fsp--;
            if (state.failed) return ;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getNamespaceImportAccess().getImportedNamespaceAssignment_1()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__NamespaceImport__Group__1__Impl"


    // $ANTLR start "rule__CustomNameMapping__Group__0"
    // InternalEFactory.g:1202:1: rule__CustomNameMapping__Group__0 : rule__CustomNameMapping__Group__0__Impl rule__CustomNameMapping__Group__1 ;
    public final void rule__CustomNameMapping__Group__0() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1206:1: ( rule__CustomNameMapping__Group__0__Impl rule__CustomNameMapping__Group__1 )
            // InternalEFactory.g:1207:2: rule__CustomNameMapping__Group__0__Impl rule__CustomNameMapping__Group__1
            {
            pushFollow(FOLLOW_8);
            rule__CustomNameMapping__Group__0__Impl();

            state._fsp--;
            if (state.failed) return ;
            pushFollow(FOLLOW_2);
            rule__CustomNameMapping__Group__1();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__CustomNameMapping__Group__0"


    // $ANTLR start "rule__CustomNameMapping__Group__0__Impl"
    // InternalEFactory.g:1214:1: rule__CustomNameMapping__Group__0__Impl : ( '@Name' ) ;
    public final void rule__CustomNameMapping__Group__0__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1218:1: ( ( '@Name' ) )
            // InternalEFactory.g:1219:1: ( '@Name' )
            {
            // InternalEFactory.g:1219:1: ( '@Name' )
            // InternalEFactory.g:1220:1: '@Name'
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getCustomNameMappingAccess().getNameKeyword_0()); 
            }
            match(input,17,FOLLOW_2); if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getCustomNameMappingAccess().getNameKeyword_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__CustomNameMapping__Group__0__Impl"


    // $ANTLR start "rule__CustomNameMapping__Group__1"
    // InternalEFactory.g:1233:1: rule__CustomNameMapping__Group__1 : rule__CustomNameMapping__Group__1__Impl rule__CustomNameMapping__Group__2 ;
    public final void rule__CustomNameMapping__Group__1() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1237:1: ( rule__CustomNameMapping__Group__1__Impl rule__CustomNameMapping__Group__2 )
            // InternalEFactory.g:1238:2: rule__CustomNameMapping__Group__1__Impl rule__CustomNameMapping__Group__2
            {
            pushFollow(FOLLOW_3);
            rule__CustomNameMapping__Group__1__Impl();

            state._fsp--;
            if (state.failed) return ;
            pushFollow(FOLLOW_2);
            rule__CustomNameMapping__Group__2();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__CustomNameMapping__Group__1"


    // $ANTLR start "rule__CustomNameMapping__Group__1__Impl"
    // InternalEFactory.g:1245:1: rule__CustomNameMapping__Group__1__Impl : ( '{' ) ;
    public final void rule__CustomNameMapping__Group__1__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1249:1: ( ( '{' ) )
            // InternalEFactory.g:1250:1: ( '{' )
            {
            // InternalEFactory.g:1250:1: ( '{' )
            // InternalEFactory.g:1251:1: '{'
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getCustomNameMappingAccess().getLeftCurlyBracketKeyword_1()); 
            }
            match(input,18,FOLLOW_2); if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getCustomNameMappingAccess().getLeftCurlyBracketKeyword_1()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__CustomNameMapping__Group__1__Impl"


    // $ANTLR start "rule__CustomNameMapping__Group__2"
    // InternalEFactory.g:1264:1: rule__CustomNameMapping__Group__2 : rule__CustomNameMapping__Group__2__Impl rule__CustomNameMapping__Group__3 ;
    public final void rule__CustomNameMapping__Group__2() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1268:1: ( rule__CustomNameMapping__Group__2__Impl rule__CustomNameMapping__Group__3 )
            // InternalEFactory.g:1269:2: rule__CustomNameMapping__Group__2__Impl rule__CustomNameMapping__Group__3
            {
            pushFollow(FOLLOW_9);
            rule__CustomNameMapping__Group__2__Impl();

            state._fsp--;
            if (state.failed) return ;
            pushFollow(FOLLOW_2);
            rule__CustomNameMapping__Group__3();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__CustomNameMapping__Group__2"


    // $ANTLR start "rule__CustomNameMapping__Group__2__Impl"
    // InternalEFactory.g:1276:1: rule__CustomNameMapping__Group__2__Impl : ( ( rule__CustomNameMapping__EClassAssignment_2 ) ) ;
    public final void rule__CustomNameMapping__Group__2__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1280:1: ( ( ( rule__CustomNameMapping__EClassAssignment_2 ) ) )
            // InternalEFactory.g:1281:1: ( ( rule__CustomNameMapping__EClassAssignment_2 ) )
            {
            // InternalEFactory.g:1281:1: ( ( rule__CustomNameMapping__EClassAssignment_2 ) )
            // InternalEFactory.g:1282:1: ( rule__CustomNameMapping__EClassAssignment_2 )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getCustomNameMappingAccess().getEClassAssignment_2()); 
            }
            // InternalEFactory.g:1283:1: ( rule__CustomNameMapping__EClassAssignment_2 )
            // InternalEFactory.g:1283:2: rule__CustomNameMapping__EClassAssignment_2
            {
            pushFollow(FOLLOW_2);
            rule__CustomNameMapping__EClassAssignment_2();

            state._fsp--;
            if (state.failed) return ;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getCustomNameMappingAccess().getEClassAssignment_2()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__CustomNameMapping__Group__2__Impl"


    // $ANTLR start "rule__CustomNameMapping__Group__3"
    // InternalEFactory.g:1293:1: rule__CustomNameMapping__Group__3 : rule__CustomNameMapping__Group__3__Impl rule__CustomNameMapping__Group__4 ;
    public final void rule__CustomNameMapping__Group__3() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1297:1: ( rule__CustomNameMapping__Group__3__Impl rule__CustomNameMapping__Group__4 )
            // InternalEFactory.g:1298:2: rule__CustomNameMapping__Group__3__Impl rule__CustomNameMapping__Group__4
            {
            pushFollow(FOLLOW_3);
            rule__CustomNameMapping__Group__3__Impl();

            state._fsp--;
            if (state.failed) return ;
            pushFollow(FOLLOW_2);
            rule__CustomNameMapping__Group__4();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__CustomNameMapping__Group__3"


    // $ANTLR start "rule__CustomNameMapping__Group__3__Impl"
    // InternalEFactory.g:1305:1: rule__CustomNameMapping__Group__3__Impl : ( '=' ) ;
    public final void rule__CustomNameMapping__Group__3__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1309:1: ( ( '=' ) )
            // InternalEFactory.g:1310:1: ( '=' )
            {
            // InternalEFactory.g:1310:1: ( '=' )
            // InternalEFactory.g:1311:1: '='
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getCustomNameMappingAccess().getEqualsSignKeyword_3()); 
            }
            match(input,19,FOLLOW_2); if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getCustomNameMappingAccess().getEqualsSignKeyword_3()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__CustomNameMapping__Group__3__Impl"


    // $ANTLR start "rule__CustomNameMapping__Group__4"
    // InternalEFactory.g:1324:1: rule__CustomNameMapping__Group__4 : rule__CustomNameMapping__Group__4__Impl rule__CustomNameMapping__Group__5 ;
    public final void rule__CustomNameMapping__Group__4() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1328:1: ( rule__CustomNameMapping__Group__4__Impl rule__CustomNameMapping__Group__5 )
            // InternalEFactory.g:1329:2: rule__CustomNameMapping__Group__4__Impl rule__CustomNameMapping__Group__5
            {
            pushFollow(FOLLOW_10);
            rule__CustomNameMapping__Group__4__Impl();

            state._fsp--;
            if (state.failed) return ;
            pushFollow(FOLLOW_2);
            rule__CustomNameMapping__Group__5();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__CustomNameMapping__Group__4"


    // $ANTLR start "rule__CustomNameMapping__Group__4__Impl"
    // InternalEFactory.g:1336:1: rule__CustomNameMapping__Group__4__Impl : ( ( rule__CustomNameMapping__NameFeatureAssignment_4 ) ) ;
    public final void rule__CustomNameMapping__Group__4__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1340:1: ( ( ( rule__CustomNameMapping__NameFeatureAssignment_4 ) ) )
            // InternalEFactory.g:1341:1: ( ( rule__CustomNameMapping__NameFeatureAssignment_4 ) )
            {
            // InternalEFactory.g:1341:1: ( ( rule__CustomNameMapping__NameFeatureAssignment_4 ) )
            // InternalEFactory.g:1342:1: ( rule__CustomNameMapping__NameFeatureAssignment_4 )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getCustomNameMappingAccess().getNameFeatureAssignment_4()); 
            }
            // InternalEFactory.g:1343:1: ( rule__CustomNameMapping__NameFeatureAssignment_4 )
            // InternalEFactory.g:1343:2: rule__CustomNameMapping__NameFeatureAssignment_4
            {
            pushFollow(FOLLOW_2);
            rule__CustomNameMapping__NameFeatureAssignment_4();

            state._fsp--;
            if (state.failed) return ;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getCustomNameMappingAccess().getNameFeatureAssignment_4()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__CustomNameMapping__Group__4__Impl"


    // $ANTLR start "rule__CustomNameMapping__Group__5"
    // InternalEFactory.g:1353:1: rule__CustomNameMapping__Group__5 : rule__CustomNameMapping__Group__5__Impl ;
    public final void rule__CustomNameMapping__Group__5() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1357:1: ( rule__CustomNameMapping__Group__5__Impl )
            // InternalEFactory.g:1358:2: rule__CustomNameMapping__Group__5__Impl
            {
            pushFollow(FOLLOW_2);
            rule__CustomNameMapping__Group__5__Impl();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__CustomNameMapping__Group__5"


    // $ANTLR start "rule__CustomNameMapping__Group__5__Impl"
    // InternalEFactory.g:1364:1: rule__CustomNameMapping__Group__5__Impl : ( '}' ) ;
    public final void rule__CustomNameMapping__Group__5__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1368:1: ( ( '}' ) )
            // InternalEFactory.g:1369:1: ( '}' )
            {
            // InternalEFactory.g:1369:1: ( '}' )
            // InternalEFactory.g:1370:1: '}'
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getCustomNameMappingAccess().getRightCurlyBracketKeyword_5()); 
            }
            match(input,20,FOLLOW_2); if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getCustomNameMappingAccess().getRightCurlyBracketKeyword_5()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__CustomNameMapping__Group__5__Impl"


    // $ANTLR start "rule__Feature__Group__0"
    // InternalEFactory.g:1395:1: rule__Feature__Group__0 : rule__Feature__Group__0__Impl rule__Feature__Group__1 ;
    public final void rule__Feature__Group__0() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1399:1: ( rule__Feature__Group__0__Impl rule__Feature__Group__1 )
            // InternalEFactory.g:1400:2: rule__Feature__Group__0__Impl rule__Feature__Group__1
            {
            pushFollow(FOLLOW_11);
            rule__Feature__Group__0__Impl();

            state._fsp--;
            if (state.failed) return ;
            pushFollow(FOLLOW_2);
            rule__Feature__Group__1();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Feature__Group__0"


    // $ANTLR start "rule__Feature__Group__0__Impl"
    // InternalEFactory.g:1407:1: rule__Feature__Group__0__Impl : ( ( rule__Feature__EFeatureAssignment_0 ) ) ;
    public final void rule__Feature__Group__0__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1411:1: ( ( ( rule__Feature__EFeatureAssignment_0 ) ) )
            // InternalEFactory.g:1412:1: ( ( rule__Feature__EFeatureAssignment_0 ) )
            {
            // InternalEFactory.g:1412:1: ( ( rule__Feature__EFeatureAssignment_0 ) )
            // InternalEFactory.g:1413:1: ( rule__Feature__EFeatureAssignment_0 )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getFeatureAccess().getEFeatureAssignment_0()); 
            }
            // InternalEFactory.g:1414:1: ( rule__Feature__EFeatureAssignment_0 )
            // InternalEFactory.g:1414:2: rule__Feature__EFeatureAssignment_0
            {
            pushFollow(FOLLOW_2);
            rule__Feature__EFeatureAssignment_0();

            state._fsp--;
            if (state.failed) return ;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getFeatureAccess().getEFeatureAssignment_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Feature__Group__0__Impl"


    // $ANTLR start "rule__Feature__Group__1"
    // InternalEFactory.g:1424:1: rule__Feature__Group__1 : rule__Feature__Group__1__Impl rule__Feature__Group__2 ;
    public final void rule__Feature__Group__1() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1428:1: ( rule__Feature__Group__1__Impl rule__Feature__Group__2 )
            // InternalEFactory.g:1429:2: rule__Feature__Group__1__Impl rule__Feature__Group__2
            {
            pushFollow(FOLLOW_12);
            rule__Feature__Group__1__Impl();

            state._fsp--;
            if (state.failed) return ;
            pushFollow(FOLLOW_2);
            rule__Feature__Group__2();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Feature__Group__1"


    // $ANTLR start "rule__Feature__Group__1__Impl"
    // InternalEFactory.g:1436:1: rule__Feature__Group__1__Impl : ( ':' ) ;
    public final void rule__Feature__Group__1__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1440:1: ( ( ':' ) )
            // InternalEFactory.g:1441:1: ( ':' )
            {
            // InternalEFactory.g:1441:1: ( ':' )
            // InternalEFactory.g:1442:1: ':'
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getFeatureAccess().getColonKeyword_1()); 
            }
            match(input,21,FOLLOW_2); if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getFeatureAccess().getColonKeyword_1()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Feature__Group__1__Impl"


    // $ANTLR start "rule__Feature__Group__2"
    // InternalEFactory.g:1455:1: rule__Feature__Group__2 : rule__Feature__Group__2__Impl ;
    public final void rule__Feature__Group__2() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1459:1: ( rule__Feature__Group__2__Impl )
            // InternalEFactory.g:1460:2: rule__Feature__Group__2__Impl
            {
            pushFollow(FOLLOW_2);
            rule__Feature__Group__2__Impl();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Feature__Group__2"


    // $ANTLR start "rule__Feature__Group__2__Impl"
    // InternalEFactory.g:1466:1: rule__Feature__Group__2__Impl : ( ( rule__Feature__ValueAssignment_2 )? ) ;
    public final void rule__Feature__Group__2__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1470:1: ( ( ( rule__Feature__ValueAssignment_2 )? ) )
            // InternalEFactory.g:1471:1: ( ( rule__Feature__ValueAssignment_2 )? )
            {
            // InternalEFactory.g:1471:1: ( ( rule__Feature__ValueAssignment_2 )? )
            // InternalEFactory.g:1472:1: ( rule__Feature__ValueAssignment_2 )?
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getFeatureAccess().getValueAssignment_2()); 
            }
            // InternalEFactory.g:1473:1: ( rule__Feature__ValueAssignment_2 )?
            int alt9=2;
            alt9 = dfa9.predict(input);
            switch (alt9) {
                case 1 :
                    // InternalEFactory.g:1473:2: rule__Feature__ValueAssignment_2
                    {
                    pushFollow(FOLLOW_2);
                    rule__Feature__ValueAssignment_2();

                    state._fsp--;
                    if (state.failed) return ;

                    }
                    break;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getFeatureAccess().getValueAssignment_2()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Feature__Group__2__Impl"


    // $ANTLR start "rule__NewObject__Group__0"
    // InternalEFactory.g:1489:1: rule__NewObject__Group__0 : rule__NewObject__Group__0__Impl rule__NewObject__Group__1 ;
    public final void rule__NewObject__Group__0() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1493:1: ( rule__NewObject__Group__0__Impl rule__NewObject__Group__1 )
            // InternalEFactory.g:1494:2: rule__NewObject__Group__0__Impl rule__NewObject__Group__1
            {
            pushFollow(FOLLOW_13);
            rule__NewObject__Group__0__Impl();

            state._fsp--;
            if (state.failed) return ;
            pushFollow(FOLLOW_2);
            rule__NewObject__Group__1();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__NewObject__Group__0"


    // $ANTLR start "rule__NewObject__Group__0__Impl"
    // InternalEFactory.g:1501:1: rule__NewObject__Group__0__Impl : ( ( rule__NewObject__EClassAssignment_0 ) ) ;
    public final void rule__NewObject__Group__0__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1505:1: ( ( ( rule__NewObject__EClassAssignment_0 ) ) )
            // InternalEFactory.g:1506:1: ( ( rule__NewObject__EClassAssignment_0 ) )
            {
            // InternalEFactory.g:1506:1: ( ( rule__NewObject__EClassAssignment_0 ) )
            // InternalEFactory.g:1507:1: ( rule__NewObject__EClassAssignment_0 )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getNewObjectAccess().getEClassAssignment_0()); 
            }
            // InternalEFactory.g:1508:1: ( rule__NewObject__EClassAssignment_0 )
            // InternalEFactory.g:1508:2: rule__NewObject__EClassAssignment_0
            {
            pushFollow(FOLLOW_2);
            rule__NewObject__EClassAssignment_0();

            state._fsp--;
            if (state.failed) return ;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getNewObjectAccess().getEClassAssignment_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__NewObject__Group__0__Impl"


    // $ANTLR start "rule__NewObject__Group__1"
    // InternalEFactory.g:1518:1: rule__NewObject__Group__1 : rule__NewObject__Group__1__Impl rule__NewObject__Group__2 ;
    public final void rule__NewObject__Group__1() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1522:1: ( rule__NewObject__Group__1__Impl rule__NewObject__Group__2 )
            // InternalEFactory.g:1523:2: rule__NewObject__Group__1__Impl rule__NewObject__Group__2
            {
            pushFollow(FOLLOW_13);
            rule__NewObject__Group__1__Impl();

            state._fsp--;
            if (state.failed) return ;
            pushFollow(FOLLOW_2);
            rule__NewObject__Group__2();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__NewObject__Group__1"


    // $ANTLR start "rule__NewObject__Group__1__Impl"
    // InternalEFactory.g:1530:1: rule__NewObject__Group__1__Impl : ( ( rule__NewObject__NameAssignment_1 )? ) ;
    public final void rule__NewObject__Group__1__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1534:1: ( ( ( rule__NewObject__NameAssignment_1 )? ) )
            // InternalEFactory.g:1535:1: ( ( rule__NewObject__NameAssignment_1 )? )
            {
            // InternalEFactory.g:1535:1: ( ( rule__NewObject__NameAssignment_1 )? )
            // InternalEFactory.g:1536:1: ( rule__NewObject__NameAssignment_1 )?
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getNewObjectAccess().getNameAssignment_1()); 
            }
            // InternalEFactory.g:1537:1: ( rule__NewObject__NameAssignment_1 )?
            int alt10=2;
            int LA10_0 = input.LA(1);

            if ( ((LA10_0>=RULE_ID && LA10_0<=RULE_LONG_UNSIGNED)) ) {
                alt10=1;
            }
            switch (alt10) {
                case 1 :
                    // InternalEFactory.g:1537:2: rule__NewObject__NameAssignment_1
                    {
                    pushFollow(FOLLOW_2);
                    rule__NewObject__NameAssignment_1();

                    state._fsp--;
                    if (state.failed) return ;

                    }
                    break;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getNewObjectAccess().getNameAssignment_1()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__NewObject__Group__1__Impl"


    // $ANTLR start "rule__NewObject__Group__2"
    // InternalEFactory.g:1547:1: rule__NewObject__Group__2 : rule__NewObject__Group__2__Impl rule__NewObject__Group__3 ;
    public final void rule__NewObject__Group__2() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1551:1: ( rule__NewObject__Group__2__Impl rule__NewObject__Group__3 )
            // InternalEFactory.g:1552:2: rule__NewObject__Group__2__Impl rule__NewObject__Group__3
            {
            pushFollow(FOLLOW_14);
            rule__NewObject__Group__2__Impl();

            state._fsp--;
            if (state.failed) return ;
            pushFollow(FOLLOW_2);
            rule__NewObject__Group__3();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__NewObject__Group__2"


    // $ANTLR start "rule__NewObject__Group__2__Impl"
    // InternalEFactory.g:1559:1: rule__NewObject__Group__2__Impl : ( '{' ) ;
    public final void rule__NewObject__Group__2__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1563:1: ( ( '{' ) )
            // InternalEFactory.g:1564:1: ( '{' )
            {
            // InternalEFactory.g:1564:1: ( '{' )
            // InternalEFactory.g:1565:1: '{'
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getNewObjectAccess().getLeftCurlyBracketKeyword_2()); 
            }
            match(input,18,FOLLOW_2); if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getNewObjectAccess().getLeftCurlyBracketKeyword_2()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__NewObject__Group__2__Impl"


    // $ANTLR start "rule__NewObject__Group__3"
    // InternalEFactory.g:1578:1: rule__NewObject__Group__3 : rule__NewObject__Group__3__Impl rule__NewObject__Group__4 ;
    public final void rule__NewObject__Group__3() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1582:1: ( rule__NewObject__Group__3__Impl rule__NewObject__Group__4 )
            // InternalEFactory.g:1583:2: rule__NewObject__Group__3__Impl rule__NewObject__Group__4
            {
            pushFollow(FOLLOW_14);
            rule__NewObject__Group__3__Impl();

            state._fsp--;
            if (state.failed) return ;
            pushFollow(FOLLOW_2);
            rule__NewObject__Group__4();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__NewObject__Group__3"


    // $ANTLR start "rule__NewObject__Group__3__Impl"
    // InternalEFactory.g:1590:1: rule__NewObject__Group__3__Impl : ( ( rule__NewObject__FeaturesAssignment_3 )* ) ;
    public final void rule__NewObject__Group__3__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1594:1: ( ( ( rule__NewObject__FeaturesAssignment_3 )* ) )
            // InternalEFactory.g:1595:1: ( ( rule__NewObject__FeaturesAssignment_3 )* )
            {
            // InternalEFactory.g:1595:1: ( ( rule__NewObject__FeaturesAssignment_3 )* )
            // InternalEFactory.g:1596:1: ( rule__NewObject__FeaturesAssignment_3 )*
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getNewObjectAccess().getFeaturesAssignment_3()); 
            }
            // InternalEFactory.g:1597:1: ( rule__NewObject__FeaturesAssignment_3 )*
            loop11:
            do {
                int alt11=2;
                int LA11_0 = input.LA(1);

                if ( ((LA11_0>=RULE_ID && LA11_0<=RULE_LONG_UNSIGNED)) ) {
                    alt11=1;
                }


                switch (alt11) {
            	case 1 :
            	    // InternalEFactory.g:1597:2: rule__NewObject__FeaturesAssignment_3
            	    {
            	    pushFollow(FOLLOW_15);
            	    rule__NewObject__FeaturesAssignment_3();

            	    state._fsp--;
            	    if (state.failed) return ;

            	    }
            	    break;

            	default :
            	    break loop11;
                }
            } while (true);

            if ( state.backtracking==0 ) {
               after(grammarAccess.getNewObjectAccess().getFeaturesAssignment_3()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__NewObject__Group__3__Impl"


    // $ANTLR start "rule__NewObject__Group__4"
    // InternalEFactory.g:1607:1: rule__NewObject__Group__4 : rule__NewObject__Group__4__Impl ;
    public final void rule__NewObject__Group__4() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1611:1: ( rule__NewObject__Group__4__Impl )
            // InternalEFactory.g:1612:2: rule__NewObject__Group__4__Impl
            {
            pushFollow(FOLLOW_2);
            rule__NewObject__Group__4__Impl();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__NewObject__Group__4"


    // $ANTLR start "rule__NewObject__Group__4__Impl"
    // InternalEFactory.g:1618:1: rule__NewObject__Group__4__Impl : ( '}' ) ;
    public final void rule__NewObject__Group__4__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1622:1: ( ( '}' ) )
            // InternalEFactory.g:1623:1: ( '}' )
            {
            // InternalEFactory.g:1623:1: ( '}' )
            // InternalEFactory.g:1624:1: '}'
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getNewObjectAccess().getRightCurlyBracketKeyword_4()); 
            }
            match(input,20,FOLLOW_2); if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getNewObjectAccess().getRightCurlyBracketKeyword_4()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__NewObject__Group__4__Impl"


    // $ANTLR start "rule__Value__Group_2__0"
    // InternalEFactory.g:1647:1: rule__Value__Group_2__0 : rule__Value__Group_2__0__Impl rule__Value__Group_2__1 ;
    public final void rule__Value__Group_2__0() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1651:1: ( rule__Value__Group_2__0__Impl rule__Value__Group_2__1 )
            // InternalEFactory.g:1652:2: rule__Value__Group_2__0__Impl rule__Value__Group_2__1
            {
            pushFollow(FOLLOW_14);
            rule__Value__Group_2__0__Impl();

            state._fsp--;
            if (state.failed) return ;
            pushFollow(FOLLOW_2);
            rule__Value__Group_2__1();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Value__Group_2__0"


    // $ANTLR start "rule__Value__Group_2__0__Impl"
    // InternalEFactory.g:1659:1: rule__Value__Group_2__0__Impl : ( ( rule__Value__Group_2_0__0 ) ) ;
    public final void rule__Value__Group_2__0__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1663:1: ( ( ( rule__Value__Group_2_0__0 ) ) )
            // InternalEFactory.g:1664:1: ( ( rule__Value__Group_2_0__0 ) )
            {
            // InternalEFactory.g:1664:1: ( ( rule__Value__Group_2_0__0 ) )
            // InternalEFactory.g:1665:1: ( rule__Value__Group_2_0__0 )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getValueAccess().getGroup_2_0()); 
            }
            // InternalEFactory.g:1666:1: ( rule__Value__Group_2_0__0 )
            // InternalEFactory.g:1666:2: rule__Value__Group_2_0__0
            {
            pushFollow(FOLLOW_2);
            rule__Value__Group_2_0__0();

            state._fsp--;
            if (state.failed) return ;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getValueAccess().getGroup_2_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Value__Group_2__0__Impl"


    // $ANTLR start "rule__Value__Group_2__1"
    // InternalEFactory.g:1676:1: rule__Value__Group_2__1 : rule__Value__Group_2__1__Impl rule__Value__Group_2__2 ;
    public final void rule__Value__Group_2__1() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1680:1: ( rule__Value__Group_2__1__Impl rule__Value__Group_2__2 )
            // InternalEFactory.g:1681:2: rule__Value__Group_2__1__Impl rule__Value__Group_2__2
            {
            pushFollow(FOLLOW_14);
            rule__Value__Group_2__1__Impl();

            state._fsp--;
            if (state.failed) return ;
            pushFollow(FOLLOW_2);
            rule__Value__Group_2__2();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Value__Group_2__1"


    // $ANTLR start "rule__Value__Group_2__1__Impl"
    // InternalEFactory.g:1688:1: rule__Value__Group_2__1__Impl : ( ( rule__Value__FeaturesAssignment_2_1 )* ) ;
    public final void rule__Value__Group_2__1__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1692:1: ( ( ( rule__Value__FeaturesAssignment_2_1 )* ) )
            // InternalEFactory.g:1693:1: ( ( rule__Value__FeaturesAssignment_2_1 )* )
            {
            // InternalEFactory.g:1693:1: ( ( rule__Value__FeaturesAssignment_2_1 )* )
            // InternalEFactory.g:1694:1: ( rule__Value__FeaturesAssignment_2_1 )*
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getValueAccess().getFeaturesAssignment_2_1()); 
            }
            // InternalEFactory.g:1695:1: ( rule__Value__FeaturesAssignment_2_1 )*
            loop12:
            do {
                int alt12=2;
                int LA12_0 = input.LA(1);

                if ( ((LA12_0>=RULE_ID && LA12_0<=RULE_LONG_UNSIGNED)) ) {
                    alt12=1;
                }


                switch (alt12) {
            	case 1 :
            	    // InternalEFactory.g:1695:2: rule__Value__FeaturesAssignment_2_1
            	    {
            	    pushFollow(FOLLOW_15);
            	    rule__Value__FeaturesAssignment_2_1();

            	    state._fsp--;
            	    if (state.failed) return ;

            	    }
            	    break;

            	default :
            	    break loop12;
                }
            } while (true);

            if ( state.backtracking==0 ) {
               after(grammarAccess.getValueAccess().getFeaturesAssignment_2_1()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Value__Group_2__1__Impl"


    // $ANTLR start "rule__Value__Group_2__2"
    // InternalEFactory.g:1705:1: rule__Value__Group_2__2 : rule__Value__Group_2__2__Impl rule__Value__Group_2__3 ;
    public final void rule__Value__Group_2__2() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1709:1: ( rule__Value__Group_2__2__Impl rule__Value__Group_2__3 )
            // InternalEFactory.g:1710:2: rule__Value__Group_2__2__Impl rule__Value__Group_2__3
            {
            pushFollow(FOLLOW_1);
            rule__Value__Group_2__2__Impl();

            state._fsp--;
            if (state.failed) return ;
            pushFollow(FOLLOW_2);
            rule__Value__Group_2__3();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Value__Group_2__2"


    // $ANTLR start "rule__Value__Group_2__2__Impl"
    // InternalEFactory.g:1717:1: rule__Value__Group_2__2__Impl : ( '}' ) ;
    public final void rule__Value__Group_2__2__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1721:1: ( ( '}' ) )
            // InternalEFactory.g:1722:1: ( '}' )
            {
            // InternalEFactory.g:1722:1: ( '}' )
            // InternalEFactory.g:1723:1: '}'
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getValueAccess().getRightCurlyBracketKeyword_2_2()); 
            }
            match(input,20,FOLLOW_2); if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getValueAccess().getRightCurlyBracketKeyword_2_2()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Value__Group_2__2__Impl"


    // $ANTLR start "rule__Value__Group_2__3"
    // InternalEFactory.g:1736:1: rule__Value__Group_2__3 : rule__Value__Group_2__3__Impl ;
    public final void rule__Value__Group_2__3() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1740:1: ( rule__Value__Group_2__3__Impl )
            // InternalEFactory.g:1741:2: rule__Value__Group_2__3__Impl
            {
            pushFollow(FOLLOW_2);
            rule__Value__Group_2__3__Impl();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Value__Group_2__3"


    // $ANTLR start "rule__Value__Group_2__3__Impl"
    // InternalEFactory.g:1747:1: rule__Value__Group_2__3__Impl : ( () ) ;
    public final void rule__Value__Group_2__3__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1751:1: ( ( () ) )
            // InternalEFactory.g:1752:1: ( () )
            {
            // InternalEFactory.g:1752:1: ( () )
            // InternalEFactory.g:1753:1: ()
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getValueAccess().getContainmentValueAction_2_3()); 
            }
            // InternalEFactory.g:1754:1: ()
            // InternalEFactory.g:1756:1: 
            {
            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getValueAccess().getContainmentValueAction_2_3()); 
            }

            }


            }

        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Value__Group_2__3__Impl"


    // $ANTLR start "rule__Value__Group_2_0__0"
    // InternalEFactory.g:1774:1: rule__Value__Group_2_0__0 : rule__Value__Group_2_0__0__Impl ;
    public final void rule__Value__Group_2_0__0() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1778:1: ( rule__Value__Group_2_0__0__Impl )
            // InternalEFactory.g:1779:2: rule__Value__Group_2_0__0__Impl
            {
            pushFollow(FOLLOW_2);
            rule__Value__Group_2_0__0__Impl();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Value__Group_2_0__0"


    // $ANTLR start "rule__Value__Group_2_0__0__Impl"
    // InternalEFactory.g:1785:1: rule__Value__Group_2_0__0__Impl : ( ( rule__Value__Group_2_0_0__0 ) ) ;
    public final void rule__Value__Group_2_0__0__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1789:1: ( ( ( rule__Value__Group_2_0_0__0 ) ) )
            // InternalEFactory.g:1790:1: ( ( rule__Value__Group_2_0_0__0 ) )
            {
            // InternalEFactory.g:1790:1: ( ( rule__Value__Group_2_0_0__0 ) )
            // InternalEFactory.g:1791:1: ( rule__Value__Group_2_0_0__0 )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getValueAccess().getGroup_2_0_0()); 
            }
            // InternalEFactory.g:1792:1: ( rule__Value__Group_2_0_0__0 )
            // InternalEFactory.g:1792:2: rule__Value__Group_2_0_0__0
            {
            pushFollow(FOLLOW_2);
            rule__Value__Group_2_0_0__0();

            state._fsp--;
            if (state.failed) return ;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getValueAccess().getGroup_2_0_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Value__Group_2_0__0__Impl"


    // $ANTLR start "rule__Value__Group_2_0_0__0"
    // InternalEFactory.g:1804:1: rule__Value__Group_2_0_0__0 : rule__Value__Group_2_0_0__0__Impl rule__Value__Group_2_0_0__1 ;
    public final void rule__Value__Group_2_0_0__0() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1808:1: ( rule__Value__Group_2_0_0__0__Impl rule__Value__Group_2_0_0__1 )
            // InternalEFactory.g:1809:2: rule__Value__Group_2_0_0__0__Impl rule__Value__Group_2_0_0__1
            {
            pushFollow(FOLLOW_13);
            rule__Value__Group_2_0_0__0__Impl();

            state._fsp--;
            if (state.failed) return ;
            pushFollow(FOLLOW_2);
            rule__Value__Group_2_0_0__1();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Value__Group_2_0_0__0"


    // $ANTLR start "rule__Value__Group_2_0_0__0__Impl"
    // InternalEFactory.g:1816:1: rule__Value__Group_2_0_0__0__Impl : ( () ) ;
    public final void rule__Value__Group_2_0_0__0__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1820:1: ( ( () ) )
            // InternalEFactory.g:1821:1: ( () )
            {
            // InternalEFactory.g:1821:1: ( () )
            // InternalEFactory.g:1822:1: ()
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getValueAccess().getNewObjectAction_2_0_0_0()); 
            }
            // InternalEFactory.g:1823:1: ()
            // InternalEFactory.g:1825:1: 
            {
            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getValueAccess().getNewObjectAction_2_0_0_0()); 
            }

            }


            }

        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Value__Group_2_0_0__0__Impl"


    // $ANTLR start "rule__Value__Group_2_0_0__1"
    // InternalEFactory.g:1835:1: rule__Value__Group_2_0_0__1 : rule__Value__Group_2_0_0__1__Impl rule__Value__Group_2_0_0__2 ;
    public final void rule__Value__Group_2_0_0__1() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1839:1: ( rule__Value__Group_2_0_0__1__Impl rule__Value__Group_2_0_0__2 )
            // InternalEFactory.g:1840:2: rule__Value__Group_2_0_0__1__Impl rule__Value__Group_2_0_0__2
            {
            pushFollow(FOLLOW_13);
            rule__Value__Group_2_0_0__1__Impl();

            state._fsp--;
            if (state.failed) return ;
            pushFollow(FOLLOW_2);
            rule__Value__Group_2_0_0__2();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Value__Group_2_0_0__1"


    // $ANTLR start "rule__Value__Group_2_0_0__1__Impl"
    // InternalEFactory.g:1847:1: rule__Value__Group_2_0_0__1__Impl : ( ( rule__Value__EClassAssignment_2_0_0_1 )? ) ;
    public final void rule__Value__Group_2_0_0__1__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1851:1: ( ( ( rule__Value__EClassAssignment_2_0_0_1 )? ) )
            // InternalEFactory.g:1852:1: ( ( rule__Value__EClassAssignment_2_0_0_1 )? )
            {
            // InternalEFactory.g:1852:1: ( ( rule__Value__EClassAssignment_2_0_0_1 )? )
            // InternalEFactory.g:1853:1: ( rule__Value__EClassAssignment_2_0_0_1 )?
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getValueAccess().getEClassAssignment_2_0_0_1()); 
            }
            // InternalEFactory.g:1854:1: ( rule__Value__EClassAssignment_2_0_0_1 )?
            int alt13=2;
            switch ( input.LA(1) ) {
                case RULE_ID:
                    {
                    int LA13_1 = input.LA(2);

                    if ( (synpred21_InternalEFactory()) ) {
                        alt13=1;
                    }
                    }
                    break;
                case RULE_LONG_ID:
                    {
                    int LA13_2 = input.LA(2);

                    if ( (synpred21_InternalEFactory()) ) {
                        alt13=1;
                    }
                    }
                    break;
                case RULE_LONG_UNSIGNED:
                    {
                    int LA13_3 = input.LA(2);

                    if ( (synpred21_InternalEFactory()) ) {
                        alt13=1;
                    }
                    }
                    break;
            }

            switch (alt13) {
                case 1 :
                    // InternalEFactory.g:1854:2: rule__Value__EClassAssignment_2_0_0_1
                    {
                    pushFollow(FOLLOW_2);
                    rule__Value__EClassAssignment_2_0_0_1();

                    state._fsp--;
                    if (state.failed) return ;

                    }
                    break;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getValueAccess().getEClassAssignment_2_0_0_1()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Value__Group_2_0_0__1__Impl"


    // $ANTLR start "rule__Value__Group_2_0_0__2"
    // InternalEFactory.g:1864:1: rule__Value__Group_2_0_0__2 : rule__Value__Group_2_0_0__2__Impl rule__Value__Group_2_0_0__3 ;
    public final void rule__Value__Group_2_0_0__2() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1868:1: ( rule__Value__Group_2_0_0__2__Impl rule__Value__Group_2_0_0__3 )
            // InternalEFactory.g:1869:2: rule__Value__Group_2_0_0__2__Impl rule__Value__Group_2_0_0__3
            {
            pushFollow(FOLLOW_13);
            rule__Value__Group_2_0_0__2__Impl();

            state._fsp--;
            if (state.failed) return ;
            pushFollow(FOLLOW_2);
            rule__Value__Group_2_0_0__3();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Value__Group_2_0_0__2"


    // $ANTLR start "rule__Value__Group_2_0_0__2__Impl"
    // InternalEFactory.g:1876:1: rule__Value__Group_2_0_0__2__Impl : ( ( rule__Value__NameAssignment_2_0_0_2 )? ) ;
    public final void rule__Value__Group_2_0_0__2__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1880:1: ( ( ( rule__Value__NameAssignment_2_0_0_2 )? ) )
            // InternalEFactory.g:1881:1: ( ( rule__Value__NameAssignment_2_0_0_2 )? )
            {
            // InternalEFactory.g:1881:1: ( ( rule__Value__NameAssignment_2_0_0_2 )? )
            // InternalEFactory.g:1882:1: ( rule__Value__NameAssignment_2_0_0_2 )?
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getValueAccess().getNameAssignment_2_0_0_2()); 
            }
            // InternalEFactory.g:1883:1: ( rule__Value__NameAssignment_2_0_0_2 )?
            int alt14=2;
            int LA14_0 = input.LA(1);

            if ( ((LA14_0>=RULE_ID && LA14_0<=RULE_LONG_UNSIGNED)) ) {
                alt14=1;
            }
            switch (alt14) {
                case 1 :
                    // InternalEFactory.g:1883:2: rule__Value__NameAssignment_2_0_0_2
                    {
                    pushFollow(FOLLOW_2);
                    rule__Value__NameAssignment_2_0_0_2();

                    state._fsp--;
                    if (state.failed) return ;

                    }
                    break;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getValueAccess().getNameAssignment_2_0_0_2()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Value__Group_2_0_0__2__Impl"


    // $ANTLR start "rule__Value__Group_2_0_0__3"
    // InternalEFactory.g:1893:1: rule__Value__Group_2_0_0__3 : rule__Value__Group_2_0_0__3__Impl ;
    public final void rule__Value__Group_2_0_0__3() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1897:1: ( rule__Value__Group_2_0_0__3__Impl )
            // InternalEFactory.g:1898:2: rule__Value__Group_2_0_0__3__Impl
            {
            pushFollow(FOLLOW_2);
            rule__Value__Group_2_0_0__3__Impl();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Value__Group_2_0_0__3"


    // $ANTLR start "rule__Value__Group_2_0_0__3__Impl"
    // InternalEFactory.g:1904:1: rule__Value__Group_2_0_0__3__Impl : ( '{' ) ;
    public final void rule__Value__Group_2_0_0__3__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1908:1: ( ( '{' ) )
            // InternalEFactory.g:1909:1: ( '{' )
            {
            // InternalEFactory.g:1909:1: ( '{' )
            // InternalEFactory.g:1910:1: '{'
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getValueAccess().getLeftCurlyBracketKeyword_2_0_0_3()); 
            }
            match(input,18,FOLLOW_2); if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getValueAccess().getLeftCurlyBracketKeyword_2_0_0_3()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Value__Group_2_0_0__3__Impl"


    // $ANTLR start "rule__MultiValue__Group__0"
    // InternalEFactory.g:1931:1: rule__MultiValue__Group__0 : rule__MultiValue__Group__0__Impl rule__MultiValue__Group__1 ;
    public final void rule__MultiValue__Group__0() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1935:1: ( rule__MultiValue__Group__0__Impl rule__MultiValue__Group__1 )
            // InternalEFactory.g:1936:2: rule__MultiValue__Group__0__Impl rule__MultiValue__Group__1
            {
            pushFollow(FOLLOW_16);
            rule__MultiValue__Group__0__Impl();

            state._fsp--;
            if (state.failed) return ;
            pushFollow(FOLLOW_2);
            rule__MultiValue__Group__1();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__MultiValue__Group__0"


    // $ANTLR start "rule__MultiValue__Group__0__Impl"
    // InternalEFactory.g:1943:1: rule__MultiValue__Group__0__Impl : ( () ) ;
    public final void rule__MultiValue__Group__0__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1947:1: ( ( () ) )
            // InternalEFactory.g:1948:1: ( () )
            {
            // InternalEFactory.g:1948:1: ( () )
            // InternalEFactory.g:1949:1: ()
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getMultiValueAccess().getMultiValueAction_0()); 
            }
            // InternalEFactory.g:1950:1: ()
            // InternalEFactory.g:1952:1: 
            {
            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getMultiValueAccess().getMultiValueAction_0()); 
            }

            }


            }

        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__MultiValue__Group__0__Impl"


    // $ANTLR start "rule__MultiValue__Group__1"
    // InternalEFactory.g:1962:1: rule__MultiValue__Group__1 : rule__MultiValue__Group__1__Impl rule__MultiValue__Group__2 ;
    public final void rule__MultiValue__Group__1() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1966:1: ( rule__MultiValue__Group__1__Impl rule__MultiValue__Group__2 )
            // InternalEFactory.g:1967:2: rule__MultiValue__Group__1__Impl rule__MultiValue__Group__2
            {
            pushFollow(FOLLOW_17);
            rule__MultiValue__Group__1__Impl();

            state._fsp--;
            if (state.failed) return ;
            pushFollow(FOLLOW_2);
            rule__MultiValue__Group__2();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__MultiValue__Group__1"


    // $ANTLR start "rule__MultiValue__Group__1__Impl"
    // InternalEFactory.g:1974:1: rule__MultiValue__Group__1__Impl : ( '[' ) ;
    public final void rule__MultiValue__Group__1__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1978:1: ( ( '[' ) )
            // InternalEFactory.g:1979:1: ( '[' )
            {
            // InternalEFactory.g:1979:1: ( '[' )
            // InternalEFactory.g:1980:1: '['
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getMultiValueAccess().getLeftSquareBracketKeyword_1()); 
            }
            match(input,22,FOLLOW_2); if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getMultiValueAccess().getLeftSquareBracketKeyword_1()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__MultiValue__Group__1__Impl"


    // $ANTLR start "rule__MultiValue__Group__2"
    // InternalEFactory.g:1993:1: rule__MultiValue__Group__2 : rule__MultiValue__Group__2__Impl rule__MultiValue__Group__3 ;
    public final void rule__MultiValue__Group__2() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:1997:1: ( rule__MultiValue__Group__2__Impl rule__MultiValue__Group__3 )
            // InternalEFactory.g:1998:2: rule__MultiValue__Group__2__Impl rule__MultiValue__Group__3
            {
            pushFollow(FOLLOW_17);
            rule__MultiValue__Group__2__Impl();

            state._fsp--;
            if (state.failed) return ;
            pushFollow(FOLLOW_2);
            rule__MultiValue__Group__3();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__MultiValue__Group__2"


    // $ANTLR start "rule__MultiValue__Group__2__Impl"
    // InternalEFactory.g:2005:1: rule__MultiValue__Group__2__Impl : ( ( rule__MultiValue__ValuesAssignment_2 )* ) ;
    public final void rule__MultiValue__Group__2__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2009:1: ( ( ( rule__MultiValue__ValuesAssignment_2 )* ) )
            // InternalEFactory.g:2010:1: ( ( rule__MultiValue__ValuesAssignment_2 )* )
            {
            // InternalEFactory.g:2010:1: ( ( rule__MultiValue__ValuesAssignment_2 )* )
            // InternalEFactory.g:2011:1: ( rule__MultiValue__ValuesAssignment_2 )*
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getMultiValueAccess().getValuesAssignment_2()); 
            }
            // InternalEFactory.g:2012:1: ( rule__MultiValue__ValuesAssignment_2 )*
            loop15:
            do {
                int alt15=2;
                int LA15_0 = input.LA(1);

                if ( ((LA15_0>=RULE_STRING && LA15_0<=RULE_LONG_UNSIGNED)||(LA15_0>=13 && LA15_0<=14)||LA15_0==18||(LA15_0>=21 && LA15_0<=22)||(LA15_0>=26 && LA15_0<=27)) ) {
                    alt15=1;
                }


                switch (alt15) {
            	case 1 :
            	    // InternalEFactory.g:2012:2: rule__MultiValue__ValuesAssignment_2
            	    {
            	    pushFollow(FOLLOW_18);
            	    rule__MultiValue__ValuesAssignment_2();

            	    state._fsp--;
            	    if (state.failed) return ;

            	    }
            	    break;

            	default :
            	    break loop15;
                }
            } while (true);

            if ( state.backtracking==0 ) {
               after(grammarAccess.getMultiValueAccess().getValuesAssignment_2()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__MultiValue__Group__2__Impl"


    // $ANTLR start "rule__MultiValue__Group__3"
    // InternalEFactory.g:2022:1: rule__MultiValue__Group__3 : rule__MultiValue__Group__3__Impl ;
    public final void rule__MultiValue__Group__3() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2026:1: ( rule__MultiValue__Group__3__Impl )
            // InternalEFactory.g:2027:2: rule__MultiValue__Group__3__Impl
            {
            pushFollow(FOLLOW_2);
            rule__MultiValue__Group__3__Impl();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__MultiValue__Group__3"


    // $ANTLR start "rule__MultiValue__Group__3__Impl"
    // InternalEFactory.g:2033:1: rule__MultiValue__Group__3__Impl : ( ']' ) ;
    public final void rule__MultiValue__Group__3__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2037:1: ( ( ']' ) )
            // InternalEFactory.g:2038:1: ( ']' )
            {
            // InternalEFactory.g:2038:1: ( ']' )
            // InternalEFactory.g:2039:1: ']'
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getMultiValueAccess().getRightSquareBracketKeyword_3()); 
            }
            match(input,23,FOLLOW_2); if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getMultiValueAccess().getRightSquareBracketKeyword_3()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__MultiValue__Group__3__Impl"


    // $ANTLR start "rule__EnumAttribute__Group__0"
    // InternalEFactory.g:2060:1: rule__EnumAttribute__Group__0 : rule__EnumAttribute__Group__0__Impl rule__EnumAttribute__Group__1 ;
    public final void rule__EnumAttribute__Group__0() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2064:1: ( rule__EnumAttribute__Group__0__Impl rule__EnumAttribute__Group__1 )
            // InternalEFactory.g:2065:2: rule__EnumAttribute__Group__0__Impl rule__EnumAttribute__Group__1
            {
            pushFollow(FOLLOW_3);
            rule__EnumAttribute__Group__0__Impl();

            state._fsp--;
            if (state.failed) return ;
            pushFollow(FOLLOW_2);
            rule__EnumAttribute__Group__1();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__EnumAttribute__Group__0"


    // $ANTLR start "rule__EnumAttribute__Group__0__Impl"
    // InternalEFactory.g:2072:1: rule__EnumAttribute__Group__0__Impl : ( ':' ) ;
    public final void rule__EnumAttribute__Group__0__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2076:1: ( ( ':' ) )
            // InternalEFactory.g:2077:1: ( ':' )
            {
            // InternalEFactory.g:2077:1: ( ':' )
            // InternalEFactory.g:2078:1: ':'
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getEnumAttributeAccess().getColonKeyword_0()); 
            }
            match(input,21,FOLLOW_2); if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getEnumAttributeAccess().getColonKeyword_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__EnumAttribute__Group__0__Impl"


    // $ANTLR start "rule__EnumAttribute__Group__1"
    // InternalEFactory.g:2091:1: rule__EnumAttribute__Group__1 : rule__EnumAttribute__Group__1__Impl ;
    public final void rule__EnumAttribute__Group__1() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2095:1: ( rule__EnumAttribute__Group__1__Impl )
            // InternalEFactory.g:2096:2: rule__EnumAttribute__Group__1__Impl
            {
            pushFollow(FOLLOW_2);
            rule__EnumAttribute__Group__1__Impl();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__EnumAttribute__Group__1"


    // $ANTLR start "rule__EnumAttribute__Group__1__Impl"
    // InternalEFactory.g:2102:1: rule__EnumAttribute__Group__1__Impl : ( ( rule__EnumAttribute__ValueAssignment_1 ) ) ;
    public final void rule__EnumAttribute__Group__1__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2106:1: ( ( ( rule__EnumAttribute__ValueAssignment_1 ) ) )
            // InternalEFactory.g:2107:1: ( ( rule__EnumAttribute__ValueAssignment_1 ) )
            {
            // InternalEFactory.g:2107:1: ( ( rule__EnumAttribute__ValueAssignment_1 ) )
            // InternalEFactory.g:2108:1: ( rule__EnumAttribute__ValueAssignment_1 )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getEnumAttributeAccess().getValueAssignment_1()); 
            }
            // InternalEFactory.g:2109:1: ( rule__EnumAttribute__ValueAssignment_1 )
            // InternalEFactory.g:2109:2: rule__EnumAttribute__ValueAssignment_1
            {
            pushFollow(FOLLOW_2);
            rule__EnumAttribute__ValueAssignment_1();

            state._fsp--;
            if (state.failed) return ;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getEnumAttributeAccess().getValueAssignment_1()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__EnumAttribute__Group__1__Impl"


    // $ANTLR start "rule__QualifiedNameWithWildcard__Group__0"
    // InternalEFactory.g:2123:1: rule__QualifiedNameWithWildcard__Group__0 : rule__QualifiedNameWithWildcard__Group__0__Impl rule__QualifiedNameWithWildcard__Group__1 ;
    public final void rule__QualifiedNameWithWildcard__Group__0() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2127:1: ( rule__QualifiedNameWithWildcard__Group__0__Impl rule__QualifiedNameWithWildcard__Group__1 )
            // InternalEFactory.g:2128:2: rule__QualifiedNameWithWildcard__Group__0__Impl rule__QualifiedNameWithWildcard__Group__1
            {
            pushFollow(FOLLOW_19);
            rule__QualifiedNameWithWildcard__Group__0__Impl();

            state._fsp--;
            if (state.failed) return ;
            pushFollow(FOLLOW_2);
            rule__QualifiedNameWithWildcard__Group__1();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__QualifiedNameWithWildcard__Group__0"


    // $ANTLR start "rule__QualifiedNameWithWildcard__Group__0__Impl"
    // InternalEFactory.g:2135:1: rule__QualifiedNameWithWildcard__Group__0__Impl : ( ruleQualifiedName ) ;
    public final void rule__QualifiedNameWithWildcard__Group__0__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2139:1: ( ( ruleQualifiedName ) )
            // InternalEFactory.g:2140:1: ( ruleQualifiedName )
            {
            // InternalEFactory.g:2140:1: ( ruleQualifiedName )
            // InternalEFactory.g:2141:1: ruleQualifiedName
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getQualifiedNameWithWildcardAccess().getQualifiedNameParserRuleCall_0()); 
            }
            pushFollow(FOLLOW_2);
            ruleQualifiedName();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getQualifiedNameWithWildcardAccess().getQualifiedNameParserRuleCall_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__QualifiedNameWithWildcard__Group__0__Impl"


    // $ANTLR start "rule__QualifiedNameWithWildcard__Group__1"
    // InternalEFactory.g:2152:1: rule__QualifiedNameWithWildcard__Group__1 : rule__QualifiedNameWithWildcard__Group__1__Impl ;
    public final void rule__QualifiedNameWithWildcard__Group__1() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2156:1: ( rule__QualifiedNameWithWildcard__Group__1__Impl )
            // InternalEFactory.g:2157:2: rule__QualifiedNameWithWildcard__Group__1__Impl
            {
            pushFollow(FOLLOW_2);
            rule__QualifiedNameWithWildcard__Group__1__Impl();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__QualifiedNameWithWildcard__Group__1"


    // $ANTLR start "rule__QualifiedNameWithWildcard__Group__1__Impl"
    // InternalEFactory.g:2163:1: rule__QualifiedNameWithWildcard__Group__1__Impl : ( ( '.*' )? ) ;
    public final void rule__QualifiedNameWithWildcard__Group__1__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2167:1: ( ( ( '.*' )? ) )
            // InternalEFactory.g:2168:1: ( ( '.*' )? )
            {
            // InternalEFactory.g:2168:1: ( ( '.*' )? )
            // InternalEFactory.g:2169:1: ( '.*' )?
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getQualifiedNameWithWildcardAccess().getFullStopAsteriskKeyword_1()); 
            }
            // InternalEFactory.g:2170:1: ( '.*' )?
            int alt16=2;
            int LA16_0 = input.LA(1);

            if ( (LA16_0==24) ) {
                alt16=1;
            }
            switch (alt16) {
                case 1 :
                    // InternalEFactory.g:2171:2: '.*'
                    {
                    match(input,24,FOLLOW_2); if (state.failed) return ;

                    }
                    break;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getQualifiedNameWithWildcardAccess().getFullStopAsteriskKeyword_1()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__QualifiedNameWithWildcard__Group__1__Impl"


    // $ANTLR start "rule__QualifiedName__Group__0"
    // InternalEFactory.g:2186:1: rule__QualifiedName__Group__0 : rule__QualifiedName__Group__0__Impl rule__QualifiedName__Group__1 ;
    public final void rule__QualifiedName__Group__0() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2190:1: ( rule__QualifiedName__Group__0__Impl rule__QualifiedName__Group__1 )
            // InternalEFactory.g:2191:2: rule__QualifiedName__Group__0__Impl rule__QualifiedName__Group__1
            {
            pushFollow(FOLLOW_20);
            rule__QualifiedName__Group__0__Impl();

            state._fsp--;
            if (state.failed) return ;
            pushFollow(FOLLOW_2);
            rule__QualifiedName__Group__1();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__QualifiedName__Group__0"


    // $ANTLR start "rule__QualifiedName__Group__0__Impl"
    // InternalEFactory.g:2198:1: rule__QualifiedName__Group__0__Impl : ( ruleValidID ) ;
    public final void rule__QualifiedName__Group__0__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2202:1: ( ( ruleValidID ) )
            // InternalEFactory.g:2203:1: ( ruleValidID )
            {
            // InternalEFactory.g:2203:1: ( ruleValidID )
            // InternalEFactory.g:2204:1: ruleValidID
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getQualifiedNameAccess().getValidIDParserRuleCall_0()); 
            }
            pushFollow(FOLLOW_2);
            ruleValidID();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getQualifiedNameAccess().getValidIDParserRuleCall_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__QualifiedName__Group__0__Impl"


    // $ANTLR start "rule__QualifiedName__Group__1"
    // InternalEFactory.g:2215:1: rule__QualifiedName__Group__1 : rule__QualifiedName__Group__1__Impl ;
    public final void rule__QualifiedName__Group__1() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2219:1: ( rule__QualifiedName__Group__1__Impl )
            // InternalEFactory.g:2220:2: rule__QualifiedName__Group__1__Impl
            {
            pushFollow(FOLLOW_2);
            rule__QualifiedName__Group__1__Impl();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__QualifiedName__Group__1"


    // $ANTLR start "rule__QualifiedName__Group__1__Impl"
    // InternalEFactory.g:2226:1: rule__QualifiedName__Group__1__Impl : ( ( rule__QualifiedName__Group_1__0 )* ) ;
    public final void rule__QualifiedName__Group__1__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2230:1: ( ( ( rule__QualifiedName__Group_1__0 )* ) )
            // InternalEFactory.g:2231:1: ( ( rule__QualifiedName__Group_1__0 )* )
            {
            // InternalEFactory.g:2231:1: ( ( rule__QualifiedName__Group_1__0 )* )
            // InternalEFactory.g:2232:1: ( rule__QualifiedName__Group_1__0 )*
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getQualifiedNameAccess().getGroup_1()); 
            }
            // InternalEFactory.g:2233:1: ( rule__QualifiedName__Group_1__0 )*
            loop17:
            do {
                int alt17=2;
                int LA17_0 = input.LA(1);

                if ( (LA17_0==25) ) {
                    alt17=1;
                }


                switch (alt17) {
            	case 1 :
            	    // InternalEFactory.g:2233:2: rule__QualifiedName__Group_1__0
            	    {
            	    pushFollow(FOLLOW_21);
            	    rule__QualifiedName__Group_1__0();

            	    state._fsp--;
            	    if (state.failed) return ;

            	    }
            	    break;

            	default :
            	    break loop17;
                }
            } while (true);

            if ( state.backtracking==0 ) {
               after(grammarAccess.getQualifiedNameAccess().getGroup_1()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__QualifiedName__Group__1__Impl"


    // $ANTLR start "rule__QualifiedName__Group_1__0"
    // InternalEFactory.g:2247:1: rule__QualifiedName__Group_1__0 : rule__QualifiedName__Group_1__0__Impl rule__QualifiedName__Group_1__1 ;
    public final void rule__QualifiedName__Group_1__0() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2251:1: ( rule__QualifiedName__Group_1__0__Impl rule__QualifiedName__Group_1__1 )
            // InternalEFactory.g:2252:2: rule__QualifiedName__Group_1__0__Impl rule__QualifiedName__Group_1__1
            {
            pushFollow(FOLLOW_3);
            rule__QualifiedName__Group_1__0__Impl();

            state._fsp--;
            if (state.failed) return ;
            pushFollow(FOLLOW_2);
            rule__QualifiedName__Group_1__1();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__QualifiedName__Group_1__0"


    // $ANTLR start "rule__QualifiedName__Group_1__0__Impl"
    // InternalEFactory.g:2259:1: rule__QualifiedName__Group_1__0__Impl : ( ( '.' ) ) ;
    public final void rule__QualifiedName__Group_1__0__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2263:1: ( ( ( '.' ) ) )
            // InternalEFactory.g:2264:1: ( ( '.' ) )
            {
            // InternalEFactory.g:2264:1: ( ( '.' ) )
            // InternalEFactory.g:2265:1: ( '.' )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getQualifiedNameAccess().getFullStopKeyword_1_0()); 
            }
            // InternalEFactory.g:2266:1: ( '.' )
            // InternalEFactory.g:2267:2: '.'
            {
            match(input,25,FOLLOW_2); if (state.failed) return ;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getQualifiedNameAccess().getFullStopKeyword_1_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__QualifiedName__Group_1__0__Impl"


    // $ANTLR start "rule__QualifiedName__Group_1__1"
    // InternalEFactory.g:2278:1: rule__QualifiedName__Group_1__1 : rule__QualifiedName__Group_1__1__Impl ;
    public final void rule__QualifiedName__Group_1__1() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2282:1: ( rule__QualifiedName__Group_1__1__Impl )
            // InternalEFactory.g:2283:2: rule__QualifiedName__Group_1__1__Impl
            {
            pushFollow(FOLLOW_2);
            rule__QualifiedName__Group_1__1__Impl();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__QualifiedName__Group_1__1"


    // $ANTLR start "rule__QualifiedName__Group_1__1__Impl"
    // InternalEFactory.g:2289:1: rule__QualifiedName__Group_1__1__Impl : ( ruleValidID ) ;
    public final void rule__QualifiedName__Group_1__1__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2293:1: ( ( ruleValidID ) )
            // InternalEFactory.g:2294:1: ( ruleValidID )
            {
            // InternalEFactory.g:2294:1: ( ruleValidID )
            // InternalEFactory.g:2295:1: ruleValidID
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getQualifiedNameAccess().getValidIDParserRuleCall_1_1()); 
            }
            pushFollow(FOLLOW_2);
            ruleValidID();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getQualifiedNameAccess().getValidIDParserRuleCall_1_1()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__QualifiedName__Group_1__1__Impl"


    // $ANTLR start "rule__Long__Group__0"
    // InternalEFactory.g:2310:1: rule__Long__Group__0 : rule__Long__Group__0__Impl rule__Long__Group__1 ;
    public final void rule__Long__Group__0() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2314:1: ( rule__Long__Group__0__Impl rule__Long__Group__1 )
            // InternalEFactory.g:2315:2: rule__Long__Group__0__Impl rule__Long__Group__1
            {
            pushFollow(FOLLOW_22);
            rule__Long__Group__0__Impl();

            state._fsp--;
            if (state.failed) return ;
            pushFollow(FOLLOW_2);
            rule__Long__Group__1();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Long__Group__0"


    // $ANTLR start "rule__Long__Group__0__Impl"
    // InternalEFactory.g:2322:1: rule__Long__Group__0__Impl : ( ( '-' )? ) ;
    public final void rule__Long__Group__0__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2326:1: ( ( ( '-' )? ) )
            // InternalEFactory.g:2327:1: ( ( '-' )? )
            {
            // InternalEFactory.g:2327:1: ( ( '-' )? )
            // InternalEFactory.g:2328:1: ( '-' )?
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getLongAccess().getHyphenMinusKeyword_0()); 
            }
            // InternalEFactory.g:2329:1: ( '-' )?
            int alt18=2;
            int LA18_0 = input.LA(1);

            if ( (LA18_0==26) ) {
                alt18=1;
            }
            switch (alt18) {
                case 1 :
                    // InternalEFactory.g:2330:2: '-'
                    {
                    match(input,26,FOLLOW_2); if (state.failed) return ;

                    }
                    break;

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getLongAccess().getHyphenMinusKeyword_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Long__Group__0__Impl"


    // $ANTLR start "rule__Long__Group__1"
    // InternalEFactory.g:2341:1: rule__Long__Group__1 : rule__Long__Group__1__Impl ;
    public final void rule__Long__Group__1() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2345:1: ( rule__Long__Group__1__Impl )
            // InternalEFactory.g:2346:2: rule__Long__Group__1__Impl
            {
            pushFollow(FOLLOW_2);
            rule__Long__Group__1__Impl();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Long__Group__1"


    // $ANTLR start "rule__Long__Group__1__Impl"
    // InternalEFactory.g:2352:1: rule__Long__Group__1__Impl : ( RULE_LONG_UNSIGNED ) ;
    public final void rule__Long__Group__1__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2356:1: ( ( RULE_LONG_UNSIGNED ) )
            // InternalEFactory.g:2357:1: ( RULE_LONG_UNSIGNED )
            {
            // InternalEFactory.g:2357:1: ( RULE_LONG_UNSIGNED )
            // InternalEFactory.g:2358:1: RULE_LONG_UNSIGNED
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getLongAccess().getLONG_UNSIGNEDTerminalRuleCall_1()); 
            }
            match(input,RULE_LONG_UNSIGNED,FOLLOW_2); if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getLongAccess().getLONG_UNSIGNEDTerminalRuleCall_1()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Long__Group__1__Impl"


    // $ANTLR start "rule__Double__Group__0"
    // InternalEFactory.g:2373:1: rule__Double__Group__0 : rule__Double__Group__0__Impl rule__Double__Group__1 ;
    public final void rule__Double__Group__0() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2377:1: ( rule__Double__Group__0__Impl rule__Double__Group__1 )
            // InternalEFactory.g:2378:2: rule__Double__Group__0__Impl rule__Double__Group__1
            {
            pushFollow(FOLLOW_20);
            rule__Double__Group__0__Impl();

            state._fsp--;
            if (state.failed) return ;
            pushFollow(FOLLOW_2);
            rule__Double__Group__1();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Double__Group__0"


    // $ANTLR start "rule__Double__Group__0__Impl"
    // InternalEFactory.g:2385:1: rule__Double__Group__0__Impl : ( ruleLong ) ;
    public final void rule__Double__Group__0__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2389:1: ( ( ruleLong ) )
            // InternalEFactory.g:2390:1: ( ruleLong )
            {
            // InternalEFactory.g:2390:1: ( ruleLong )
            // InternalEFactory.g:2391:1: ruleLong
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getDoubleAccess().getLongParserRuleCall_0()); 
            }
            pushFollow(FOLLOW_2);
            ruleLong();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getDoubleAccess().getLongParserRuleCall_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Double__Group__0__Impl"


    // $ANTLR start "rule__Double__Group__1"
    // InternalEFactory.g:2402:1: rule__Double__Group__1 : rule__Double__Group__1__Impl rule__Double__Group__2 ;
    public final void rule__Double__Group__1() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2406:1: ( rule__Double__Group__1__Impl rule__Double__Group__2 )
            // InternalEFactory.g:2407:2: rule__Double__Group__1__Impl rule__Double__Group__2
            {
            pushFollow(FOLLOW_23);
            rule__Double__Group__1__Impl();

            state._fsp--;
            if (state.failed) return ;
            pushFollow(FOLLOW_2);
            rule__Double__Group__2();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Double__Group__1"


    // $ANTLR start "rule__Double__Group__1__Impl"
    // InternalEFactory.g:2414:1: rule__Double__Group__1__Impl : ( '.' ) ;
    public final void rule__Double__Group__1__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2418:1: ( ( '.' ) )
            // InternalEFactory.g:2419:1: ( '.' )
            {
            // InternalEFactory.g:2419:1: ( '.' )
            // InternalEFactory.g:2420:1: '.'
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getDoubleAccess().getFullStopKeyword_1()); 
            }
            match(input,25,FOLLOW_2); if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getDoubleAccess().getFullStopKeyword_1()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Double__Group__1__Impl"


    // $ANTLR start "rule__Double__Group__2"
    // InternalEFactory.g:2433:1: rule__Double__Group__2 : rule__Double__Group__2__Impl ;
    public final void rule__Double__Group__2() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2437:1: ( rule__Double__Group__2__Impl )
            // InternalEFactory.g:2438:2: rule__Double__Group__2__Impl
            {
            pushFollow(FOLLOW_2);
            rule__Double__Group__2__Impl();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Double__Group__2"


    // $ANTLR start "rule__Double__Group__2__Impl"
    // InternalEFactory.g:2444:1: rule__Double__Group__2__Impl : ( RULE_LONG_UNSIGNED ) ;
    public final void rule__Double__Group__2__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2448:1: ( ( RULE_LONG_UNSIGNED ) )
            // InternalEFactory.g:2449:1: ( RULE_LONG_UNSIGNED )
            {
            // InternalEFactory.g:2449:1: ( RULE_LONG_UNSIGNED )
            // InternalEFactory.g:2450:1: RULE_LONG_UNSIGNED
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getDoubleAccess().getLONG_UNSIGNEDTerminalRuleCall_2()); 
            }
            match(input,RULE_LONG_UNSIGNED,FOLLOW_2); if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getDoubleAccess().getLONG_UNSIGNEDTerminalRuleCall_2()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Double__Group__2__Impl"


    // $ANTLR start "rule__Date__Group__0"
    // InternalEFactory.g:2467:1: rule__Date__Group__0 : rule__Date__Group__0__Impl rule__Date__Group__1 ;
    public final void rule__Date__Group__0() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2471:1: ( rule__Date__Group__0__Impl rule__Date__Group__1 )
            // InternalEFactory.g:2472:2: rule__Date__Group__0__Impl rule__Date__Group__1
            {
            pushFollow(FOLLOW_20);
            rule__Date__Group__0__Impl();

            state._fsp--;
            if (state.failed) return ;
            pushFollow(FOLLOW_2);
            rule__Date__Group__1();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Date__Group__0"


    // $ANTLR start "rule__Date__Group__0__Impl"
    // InternalEFactory.g:2479:1: rule__Date__Group__0__Impl : ( RULE_LONG_UNSIGNED ) ;
    public final void rule__Date__Group__0__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2483:1: ( ( RULE_LONG_UNSIGNED ) )
            // InternalEFactory.g:2484:1: ( RULE_LONG_UNSIGNED )
            {
            // InternalEFactory.g:2484:1: ( RULE_LONG_UNSIGNED )
            // InternalEFactory.g:2485:1: RULE_LONG_UNSIGNED
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getDateAccess().getLONG_UNSIGNEDTerminalRuleCall_0()); 
            }
            match(input,RULE_LONG_UNSIGNED,FOLLOW_2); if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getDateAccess().getLONG_UNSIGNEDTerminalRuleCall_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Date__Group__0__Impl"


    // $ANTLR start "rule__Date__Group__1"
    // InternalEFactory.g:2496:1: rule__Date__Group__1 : rule__Date__Group__1__Impl rule__Date__Group__2 ;
    public final void rule__Date__Group__1() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2500:1: ( rule__Date__Group__1__Impl rule__Date__Group__2 )
            // InternalEFactory.g:2501:2: rule__Date__Group__1__Impl rule__Date__Group__2
            {
            pushFollow(FOLLOW_23);
            rule__Date__Group__1__Impl();

            state._fsp--;
            if (state.failed) return ;
            pushFollow(FOLLOW_2);
            rule__Date__Group__2();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Date__Group__1"


    // $ANTLR start "rule__Date__Group__1__Impl"
    // InternalEFactory.g:2508:1: rule__Date__Group__1__Impl : ( '.' ) ;
    public final void rule__Date__Group__1__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2512:1: ( ( '.' ) )
            // InternalEFactory.g:2513:1: ( '.' )
            {
            // InternalEFactory.g:2513:1: ( '.' )
            // InternalEFactory.g:2514:1: '.'
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getDateAccess().getFullStopKeyword_1()); 
            }
            match(input,25,FOLLOW_2); if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getDateAccess().getFullStopKeyword_1()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Date__Group__1__Impl"


    // $ANTLR start "rule__Date__Group__2"
    // InternalEFactory.g:2527:1: rule__Date__Group__2 : rule__Date__Group__2__Impl rule__Date__Group__3 ;
    public final void rule__Date__Group__2() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2531:1: ( rule__Date__Group__2__Impl rule__Date__Group__3 )
            // InternalEFactory.g:2532:2: rule__Date__Group__2__Impl rule__Date__Group__3
            {
            pushFollow(FOLLOW_20);
            rule__Date__Group__2__Impl();

            state._fsp--;
            if (state.failed) return ;
            pushFollow(FOLLOW_2);
            rule__Date__Group__3();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Date__Group__2"


    // $ANTLR start "rule__Date__Group__2__Impl"
    // InternalEFactory.g:2539:1: rule__Date__Group__2__Impl : ( RULE_LONG_UNSIGNED ) ;
    public final void rule__Date__Group__2__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2543:1: ( ( RULE_LONG_UNSIGNED ) )
            // InternalEFactory.g:2544:1: ( RULE_LONG_UNSIGNED )
            {
            // InternalEFactory.g:2544:1: ( RULE_LONG_UNSIGNED )
            // InternalEFactory.g:2545:1: RULE_LONG_UNSIGNED
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getDateAccess().getLONG_UNSIGNEDTerminalRuleCall_2()); 
            }
            match(input,RULE_LONG_UNSIGNED,FOLLOW_2); if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getDateAccess().getLONG_UNSIGNEDTerminalRuleCall_2()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Date__Group__2__Impl"


    // $ANTLR start "rule__Date__Group__3"
    // InternalEFactory.g:2556:1: rule__Date__Group__3 : rule__Date__Group__3__Impl rule__Date__Group__4 ;
    public final void rule__Date__Group__3() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2560:1: ( rule__Date__Group__3__Impl rule__Date__Group__4 )
            // InternalEFactory.g:2561:2: rule__Date__Group__3__Impl rule__Date__Group__4
            {
            pushFollow(FOLLOW_23);
            rule__Date__Group__3__Impl();

            state._fsp--;
            if (state.failed) return ;
            pushFollow(FOLLOW_2);
            rule__Date__Group__4();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Date__Group__3"


    // $ANTLR start "rule__Date__Group__3__Impl"
    // InternalEFactory.g:2568:1: rule__Date__Group__3__Impl : ( '.' ) ;
    public final void rule__Date__Group__3__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2572:1: ( ( '.' ) )
            // InternalEFactory.g:2573:1: ( '.' )
            {
            // InternalEFactory.g:2573:1: ( '.' )
            // InternalEFactory.g:2574:1: '.'
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getDateAccess().getFullStopKeyword_3()); 
            }
            match(input,25,FOLLOW_2); if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getDateAccess().getFullStopKeyword_3()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Date__Group__3__Impl"


    // $ANTLR start "rule__Date__Group__4"
    // InternalEFactory.g:2587:1: rule__Date__Group__4 : rule__Date__Group__4__Impl ;
    public final void rule__Date__Group__4() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2591:1: ( rule__Date__Group__4__Impl )
            // InternalEFactory.g:2592:2: rule__Date__Group__4__Impl
            {
            pushFollow(FOLLOW_2);
            rule__Date__Group__4__Impl();

            state._fsp--;
            if (state.failed) return ;

            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Date__Group__4"


    // $ANTLR start "rule__Date__Group__4__Impl"
    // InternalEFactory.g:2598:1: rule__Date__Group__4__Impl : ( RULE_LONG_UNSIGNED ) ;
    public final void rule__Date__Group__4__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2602:1: ( ( RULE_LONG_UNSIGNED ) )
            // InternalEFactory.g:2603:1: ( RULE_LONG_UNSIGNED )
            {
            // InternalEFactory.g:2603:1: ( RULE_LONG_UNSIGNED )
            // InternalEFactory.g:2604:1: RULE_LONG_UNSIGNED
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getDateAccess().getLONG_UNSIGNEDTerminalRuleCall_4()); 
            }
            match(input,RULE_LONG_UNSIGNED,FOLLOW_2); if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getDateAccess().getLONG_UNSIGNEDTerminalRuleCall_4()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Date__Group__4__Impl"


    // $ANTLR start "rule__Factory__ImportsAssignment_0"
    // InternalEFactory.g:2626:1: rule__Factory__ImportsAssignment_0 : ( ruleNamespaceImport ) ;
    public final void rule__Factory__ImportsAssignment_0() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2630:1: ( ( ruleNamespaceImport ) )
            // InternalEFactory.g:2631:1: ( ruleNamespaceImport )
            {
            // InternalEFactory.g:2631:1: ( ruleNamespaceImport )
            // InternalEFactory.g:2632:1: ruleNamespaceImport
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getFactoryAccess().getImportsNamespaceImportParserRuleCall_0_0()); 
            }
            pushFollow(FOLLOW_2);
            ruleNamespaceImport();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getFactoryAccess().getImportsNamespaceImportParserRuleCall_0_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Factory__ImportsAssignment_0"


    // $ANTLR start "rule__Factory__EPackagesAssignment_1"
    // InternalEFactory.g:2641:1: rule__Factory__EPackagesAssignment_1 : ( rulePackageImport ) ;
    public final void rule__Factory__EPackagesAssignment_1() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2645:1: ( ( rulePackageImport ) )
            // InternalEFactory.g:2646:1: ( rulePackageImport )
            {
            // InternalEFactory.g:2646:1: ( rulePackageImport )
            // InternalEFactory.g:2647:1: rulePackageImport
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getFactoryAccess().getEPackagesPackageImportParserRuleCall_1_0()); 
            }
            pushFollow(FOLLOW_2);
            rulePackageImport();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getFactoryAccess().getEPackagesPackageImportParserRuleCall_1_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Factory__EPackagesAssignment_1"


    // $ANTLR start "rule__Factory__AnnotationsAssignment_2"
    // InternalEFactory.g:2656:1: rule__Factory__AnnotationsAssignment_2 : ( ruleAnnotation ) ;
    public final void rule__Factory__AnnotationsAssignment_2() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2660:1: ( ( ruleAnnotation ) )
            // InternalEFactory.g:2661:1: ( ruleAnnotation )
            {
            // InternalEFactory.g:2661:1: ( ruleAnnotation )
            // InternalEFactory.g:2662:1: ruleAnnotation
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getFactoryAccess().getAnnotationsAnnotationParserRuleCall_2_0()); 
            }
            pushFollow(FOLLOW_2);
            ruleAnnotation();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getFactoryAccess().getAnnotationsAnnotationParserRuleCall_2_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Factory__AnnotationsAssignment_2"


    // $ANTLR start "rule__Factory__RootAssignment_3"
    // InternalEFactory.g:2671:1: rule__Factory__RootAssignment_3 : ( ruleNewObject ) ;
    public final void rule__Factory__RootAssignment_3() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2675:1: ( ( ruleNewObject ) )
            // InternalEFactory.g:2676:1: ( ruleNewObject )
            {
            // InternalEFactory.g:2676:1: ( ruleNewObject )
            // InternalEFactory.g:2677:1: ruleNewObject
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getFactoryAccess().getRootNewObjectParserRuleCall_3_0()); 
            }
            pushFollow(FOLLOW_2);
            ruleNewObject();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getFactoryAccess().getRootNewObjectParserRuleCall_3_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Factory__RootAssignment_3"


    // $ANTLR start "rule__PackageImport__EPackageAssignment_1"
    // InternalEFactory.g:2686:1: rule__PackageImport__EPackageAssignment_1 : ( ( ruleStringOrQualifiedNameWithWildcard ) ) ;
    public final void rule__PackageImport__EPackageAssignment_1() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2690:1: ( ( ( ruleStringOrQualifiedNameWithWildcard ) ) )
            // InternalEFactory.g:2691:1: ( ( ruleStringOrQualifiedNameWithWildcard ) )
            {
            // InternalEFactory.g:2691:1: ( ( ruleStringOrQualifiedNameWithWildcard ) )
            // InternalEFactory.g:2692:1: ( ruleStringOrQualifiedNameWithWildcard )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getPackageImportAccess().getEPackageEPackageCrossReference_1_0()); 
            }
            // InternalEFactory.g:2693:1: ( ruleStringOrQualifiedNameWithWildcard )
            // InternalEFactory.g:2694:1: ruleStringOrQualifiedNameWithWildcard
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getPackageImportAccess().getEPackageEPackageStringOrQualifiedNameWithWildcardParserRuleCall_1_0_1()); 
            }
            pushFollow(FOLLOW_2);
            ruleStringOrQualifiedNameWithWildcard();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getPackageImportAccess().getEPackageEPackageStringOrQualifiedNameWithWildcardParserRuleCall_1_0_1()); 
            }

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getPackageImportAccess().getEPackageEPackageCrossReference_1_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__PackageImport__EPackageAssignment_1"


    // $ANTLR start "rule__NamespaceImport__ImportedNamespaceAssignment_1"
    // InternalEFactory.g:2705:1: rule__NamespaceImport__ImportedNamespaceAssignment_1 : ( ruleQualifiedNameWithWildcard ) ;
    public final void rule__NamespaceImport__ImportedNamespaceAssignment_1() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2709:1: ( ( ruleQualifiedNameWithWildcard ) )
            // InternalEFactory.g:2710:1: ( ruleQualifiedNameWithWildcard )
            {
            // InternalEFactory.g:2710:1: ( ruleQualifiedNameWithWildcard )
            // InternalEFactory.g:2711:1: ruleQualifiedNameWithWildcard
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getNamespaceImportAccess().getImportedNamespaceQualifiedNameWithWildcardParserRuleCall_1_0()); 
            }
            pushFollow(FOLLOW_2);
            ruleQualifiedNameWithWildcard();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getNamespaceImportAccess().getImportedNamespaceQualifiedNameWithWildcardParserRuleCall_1_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__NamespaceImport__ImportedNamespaceAssignment_1"


    // $ANTLR start "rule__CustomNameMapping__EClassAssignment_2"
    // InternalEFactory.g:2720:1: rule__CustomNameMapping__EClassAssignment_2 : ( ( ruleQualifiedName ) ) ;
    public final void rule__CustomNameMapping__EClassAssignment_2() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2724:1: ( ( ( ruleQualifiedName ) ) )
            // InternalEFactory.g:2725:1: ( ( ruleQualifiedName ) )
            {
            // InternalEFactory.g:2725:1: ( ( ruleQualifiedName ) )
            // InternalEFactory.g:2726:1: ( ruleQualifiedName )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getCustomNameMappingAccess().getEClassEClassCrossReference_2_0()); 
            }
            // InternalEFactory.g:2727:1: ( ruleQualifiedName )
            // InternalEFactory.g:2728:1: ruleQualifiedName
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getCustomNameMappingAccess().getEClassEClassQualifiedNameParserRuleCall_2_0_1()); 
            }
            pushFollow(FOLLOW_2);
            ruleQualifiedName();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getCustomNameMappingAccess().getEClassEClassQualifiedNameParserRuleCall_2_0_1()); 
            }

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getCustomNameMappingAccess().getEClassEClassCrossReference_2_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__CustomNameMapping__EClassAssignment_2"


    // $ANTLR start "rule__CustomNameMapping__NameFeatureAssignment_4"
    // InternalEFactory.g:2739:1: rule__CustomNameMapping__NameFeatureAssignment_4 : ( ( ruleQualifiedName ) ) ;
    public final void rule__CustomNameMapping__NameFeatureAssignment_4() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2743:1: ( ( ( ruleQualifiedName ) ) )
            // InternalEFactory.g:2744:1: ( ( ruleQualifiedName ) )
            {
            // InternalEFactory.g:2744:1: ( ( ruleQualifiedName ) )
            // InternalEFactory.g:2745:1: ( ruleQualifiedName )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getCustomNameMappingAccess().getNameFeatureEAttributeCrossReference_4_0()); 
            }
            // InternalEFactory.g:2746:1: ( ruleQualifiedName )
            // InternalEFactory.g:2747:1: ruleQualifiedName
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getCustomNameMappingAccess().getNameFeatureEAttributeQualifiedNameParserRuleCall_4_0_1()); 
            }
            pushFollow(FOLLOW_2);
            ruleQualifiedName();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getCustomNameMappingAccess().getNameFeatureEAttributeQualifiedNameParserRuleCall_4_0_1()); 
            }

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getCustomNameMappingAccess().getNameFeatureEAttributeCrossReference_4_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__CustomNameMapping__NameFeatureAssignment_4"


    // $ANTLR start "rule__Feature__EFeatureAssignment_0"
    // InternalEFactory.g:2758:1: rule__Feature__EFeatureAssignment_0 : ( ( ruleQualifiedName ) ) ;
    public final void rule__Feature__EFeatureAssignment_0() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2762:1: ( ( ( ruleQualifiedName ) ) )
            // InternalEFactory.g:2763:1: ( ( ruleQualifiedName ) )
            {
            // InternalEFactory.g:2763:1: ( ( ruleQualifiedName ) )
            // InternalEFactory.g:2764:1: ( ruleQualifiedName )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getFeatureAccess().getEFeatureEStructuralFeatureCrossReference_0_0()); 
            }
            // InternalEFactory.g:2765:1: ( ruleQualifiedName )
            // InternalEFactory.g:2766:1: ruleQualifiedName
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getFeatureAccess().getEFeatureEStructuralFeatureQualifiedNameParserRuleCall_0_0_1()); 
            }
            pushFollow(FOLLOW_2);
            ruleQualifiedName();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getFeatureAccess().getEFeatureEStructuralFeatureQualifiedNameParserRuleCall_0_0_1()); 
            }

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getFeatureAccess().getEFeatureEStructuralFeatureCrossReference_0_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Feature__EFeatureAssignment_0"


    // $ANTLR start "rule__Feature__ValueAssignment_2"
    // InternalEFactory.g:2777:1: rule__Feature__ValueAssignment_2 : ( ruleValue ) ;
    public final void rule__Feature__ValueAssignment_2() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2781:1: ( ( ruleValue ) )
            // InternalEFactory.g:2782:1: ( ruleValue )
            {
            // InternalEFactory.g:2782:1: ( ruleValue )
            // InternalEFactory.g:2783:1: ruleValue
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getFeatureAccess().getValueValueParserRuleCall_2_0()); 
            }
            pushFollow(FOLLOW_2);
            ruleValue();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getFeatureAccess().getValueValueParserRuleCall_2_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Feature__ValueAssignment_2"


    // $ANTLR start "rule__NewObject__EClassAssignment_0"
    // InternalEFactory.g:2792:1: rule__NewObject__EClassAssignment_0 : ( ( ruleQualifiedName ) ) ;
    public final void rule__NewObject__EClassAssignment_0() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2796:1: ( ( ( ruleQualifiedName ) ) )
            // InternalEFactory.g:2797:1: ( ( ruleQualifiedName ) )
            {
            // InternalEFactory.g:2797:1: ( ( ruleQualifiedName ) )
            // InternalEFactory.g:2798:1: ( ruleQualifiedName )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getNewObjectAccess().getEClassEClassCrossReference_0_0()); 
            }
            // InternalEFactory.g:2799:1: ( ruleQualifiedName )
            // InternalEFactory.g:2800:1: ruleQualifiedName
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getNewObjectAccess().getEClassEClassQualifiedNameParserRuleCall_0_0_1()); 
            }
            pushFollow(FOLLOW_2);
            ruleQualifiedName();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getNewObjectAccess().getEClassEClassQualifiedNameParserRuleCall_0_0_1()); 
            }

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getNewObjectAccess().getEClassEClassCrossReference_0_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__NewObject__EClassAssignment_0"


    // $ANTLR start "rule__NewObject__NameAssignment_1"
    // InternalEFactory.g:2811:1: rule__NewObject__NameAssignment_1 : ( ruleValidID ) ;
    public final void rule__NewObject__NameAssignment_1() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2815:1: ( ( ruleValidID ) )
            // InternalEFactory.g:2816:1: ( ruleValidID )
            {
            // InternalEFactory.g:2816:1: ( ruleValidID )
            // InternalEFactory.g:2817:1: ruleValidID
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getNewObjectAccess().getNameValidIDParserRuleCall_1_0()); 
            }
            pushFollow(FOLLOW_2);
            ruleValidID();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getNewObjectAccess().getNameValidIDParserRuleCall_1_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__NewObject__NameAssignment_1"


    // $ANTLR start "rule__NewObject__FeaturesAssignment_3"
    // InternalEFactory.g:2826:1: rule__NewObject__FeaturesAssignment_3 : ( ruleFeature ) ;
    public final void rule__NewObject__FeaturesAssignment_3() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2830:1: ( ( ruleFeature ) )
            // InternalEFactory.g:2831:1: ( ruleFeature )
            {
            // InternalEFactory.g:2831:1: ( ruleFeature )
            // InternalEFactory.g:2832:1: ruleFeature
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getNewObjectAccess().getFeaturesFeatureParserRuleCall_3_0()); 
            }
            pushFollow(FOLLOW_2);
            ruleFeature();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getNewObjectAccess().getFeaturesFeatureParserRuleCall_3_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__NewObject__FeaturesAssignment_3"


    // $ANTLR start "rule__Value__EClassAssignment_2_0_0_1"
    // InternalEFactory.g:2841:1: rule__Value__EClassAssignment_2_0_0_1 : ( ( ruleQualifiedName ) ) ;
    public final void rule__Value__EClassAssignment_2_0_0_1() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2845:1: ( ( ( ruleQualifiedName ) ) )
            // InternalEFactory.g:2846:1: ( ( ruleQualifiedName ) )
            {
            // InternalEFactory.g:2846:1: ( ( ruleQualifiedName ) )
            // InternalEFactory.g:2847:1: ( ruleQualifiedName )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getValueAccess().getEClassEClassCrossReference_2_0_0_1_0()); 
            }
            // InternalEFactory.g:2848:1: ( ruleQualifiedName )
            // InternalEFactory.g:2849:1: ruleQualifiedName
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getValueAccess().getEClassEClassQualifiedNameParserRuleCall_2_0_0_1_0_1()); 
            }
            pushFollow(FOLLOW_2);
            ruleQualifiedName();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getValueAccess().getEClassEClassQualifiedNameParserRuleCall_2_0_0_1_0_1()); 
            }

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getValueAccess().getEClassEClassCrossReference_2_0_0_1_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Value__EClassAssignment_2_0_0_1"


    // $ANTLR start "rule__Value__NameAssignment_2_0_0_2"
    // InternalEFactory.g:2860:1: rule__Value__NameAssignment_2_0_0_2 : ( ruleValidID ) ;
    public final void rule__Value__NameAssignment_2_0_0_2() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2864:1: ( ( ruleValidID ) )
            // InternalEFactory.g:2865:1: ( ruleValidID )
            {
            // InternalEFactory.g:2865:1: ( ruleValidID )
            // InternalEFactory.g:2866:1: ruleValidID
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getValueAccess().getNameValidIDParserRuleCall_2_0_0_2_0()); 
            }
            pushFollow(FOLLOW_2);
            ruleValidID();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getValueAccess().getNameValidIDParserRuleCall_2_0_0_2_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Value__NameAssignment_2_0_0_2"


    // $ANTLR start "rule__Value__FeaturesAssignment_2_1"
    // InternalEFactory.g:2875:1: rule__Value__FeaturesAssignment_2_1 : ( ruleFeature ) ;
    public final void rule__Value__FeaturesAssignment_2_1() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2879:1: ( ( ruleFeature ) )
            // InternalEFactory.g:2880:1: ( ruleFeature )
            {
            // InternalEFactory.g:2880:1: ( ruleFeature )
            // InternalEFactory.g:2881:1: ruleFeature
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getValueAccess().getFeaturesFeatureParserRuleCall_2_1_0()); 
            }
            pushFollow(FOLLOW_2);
            ruleFeature();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getValueAccess().getFeaturesFeatureParserRuleCall_2_1_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Value__FeaturesAssignment_2_1"


    // $ANTLR start "rule__MultiValue__ValuesAssignment_2"
    // InternalEFactory.g:2890:1: rule__MultiValue__ValuesAssignment_2 : ( ruleValue ) ;
    public final void rule__MultiValue__ValuesAssignment_2() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2894:1: ( ( ruleValue ) )
            // InternalEFactory.g:2895:1: ( ruleValue )
            {
            // InternalEFactory.g:2895:1: ( ruleValue )
            // InternalEFactory.g:2896:1: ruleValue
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getMultiValueAccess().getValuesValueParserRuleCall_2_0()); 
            }
            pushFollow(FOLLOW_2);
            ruleValue();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getMultiValueAccess().getValuesValueParserRuleCall_2_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__MultiValue__ValuesAssignment_2"


    // $ANTLR start "rule__Reference__ValueAssignment"
    // InternalEFactory.g:2905:1: rule__Reference__ValueAssignment : ( ( ruleQualifiedName ) ) ;
    public final void rule__Reference__ValueAssignment() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2909:1: ( ( ( ruleQualifiedName ) ) )
            // InternalEFactory.g:2910:1: ( ( ruleQualifiedName ) )
            {
            // InternalEFactory.g:2910:1: ( ( ruleQualifiedName ) )
            // InternalEFactory.g:2911:1: ( ruleQualifiedName )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getReferenceAccess().getValueEObjectCrossReference_0()); 
            }
            // InternalEFactory.g:2912:1: ( ruleQualifiedName )
            // InternalEFactory.g:2913:1: ruleQualifiedName
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getReferenceAccess().getValueEObjectQualifiedNameParserRuleCall_0_1()); 
            }
            pushFollow(FOLLOW_2);
            ruleQualifiedName();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getReferenceAccess().getValueEObjectQualifiedNameParserRuleCall_0_1()); 
            }

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getReferenceAccess().getValueEObjectCrossReference_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__Reference__ValueAssignment"


    // $ANTLR start "rule__EnumAttribute__ValueAssignment_1"
    // InternalEFactory.g:2924:1: rule__EnumAttribute__ValueAssignment_1 : ( ( ruleQualifiedName ) ) ;
    public final void rule__EnumAttribute__ValueAssignment_1() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2928:1: ( ( ( ruleQualifiedName ) ) )
            // InternalEFactory.g:2929:1: ( ( ruleQualifiedName ) )
            {
            // InternalEFactory.g:2929:1: ( ( ruleQualifiedName ) )
            // InternalEFactory.g:2930:1: ( ruleQualifiedName )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getEnumAttributeAccess().getValueEEnumLiteralCrossReference_1_0()); 
            }
            // InternalEFactory.g:2931:1: ( ruleQualifiedName )
            // InternalEFactory.g:2932:1: ruleQualifiedName
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getEnumAttributeAccess().getValueEEnumLiteralQualifiedNameParserRuleCall_1_0_1()); 
            }
            pushFollow(FOLLOW_2);
            ruleQualifiedName();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getEnumAttributeAccess().getValueEEnumLiteralQualifiedNameParserRuleCall_1_0_1()); 
            }

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getEnumAttributeAccess().getValueEEnumLiteralCrossReference_1_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__EnumAttribute__ValueAssignment_1"


    // $ANTLR start "rule__StringAttribute__ValueAssignment"
    // InternalEFactory.g:2943:1: rule__StringAttribute__ValueAssignment : ( RULE_STRING ) ;
    public final void rule__StringAttribute__ValueAssignment() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2947:1: ( ( RULE_STRING ) )
            // InternalEFactory.g:2948:1: ( RULE_STRING )
            {
            // InternalEFactory.g:2948:1: ( RULE_STRING )
            // InternalEFactory.g:2949:1: RULE_STRING
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getStringAttributeAccess().getValueSTRINGTerminalRuleCall_0()); 
            }
            match(input,RULE_STRING,FOLLOW_2); if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getStringAttributeAccess().getValueSTRINGTerminalRuleCall_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__StringAttribute__ValueAssignment"


    // $ANTLR start "rule__IntegerAttribute__ValueAssignment"
    // InternalEFactory.g:2958:1: rule__IntegerAttribute__ValueAssignment : ( ruleLong ) ;
    public final void rule__IntegerAttribute__ValueAssignment() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2962:1: ( ( ruleLong ) )
            // InternalEFactory.g:2963:1: ( ruleLong )
            {
            // InternalEFactory.g:2963:1: ( ruleLong )
            // InternalEFactory.g:2964:1: ruleLong
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getIntegerAttributeAccess().getValueLongParserRuleCall_0()); 
            }
            pushFollow(FOLLOW_2);
            ruleLong();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getIntegerAttributeAccess().getValueLongParserRuleCall_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__IntegerAttribute__ValueAssignment"


    // $ANTLR start "rule__DoubleAttribute__ValueAssignment"
    // InternalEFactory.g:2973:1: rule__DoubleAttribute__ValueAssignment : ( ruleDouble ) ;
    public final void rule__DoubleAttribute__ValueAssignment() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2977:1: ( ( ruleDouble ) )
            // InternalEFactory.g:2978:1: ( ruleDouble )
            {
            // InternalEFactory.g:2978:1: ( ruleDouble )
            // InternalEFactory.g:2979:1: ruleDouble
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getDoubleAttributeAccess().getValueDoubleParserRuleCall_0()); 
            }
            pushFollow(FOLLOW_2);
            ruleDouble();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getDoubleAttributeAccess().getValueDoubleParserRuleCall_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__DoubleAttribute__ValueAssignment"


    // $ANTLR start "rule__DateAttribute__ValueAssignment"
    // InternalEFactory.g:2988:1: rule__DateAttribute__ValueAssignment : ( ruleDate ) ;
    public final void rule__DateAttribute__ValueAssignment() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:2992:1: ( ( ruleDate ) )
            // InternalEFactory.g:2993:1: ( ruleDate )
            {
            // InternalEFactory.g:2993:1: ( ruleDate )
            // InternalEFactory.g:2994:1: ruleDate
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getDateAttributeAccess().getValueDateParserRuleCall_0()); 
            }
            pushFollow(FOLLOW_2);
            ruleDate();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getDateAttributeAccess().getValueDateParserRuleCall_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__DateAttribute__ValueAssignment"


    // $ANTLR start "rule__NullAttribute__ValueAssignment"
    // InternalEFactory.g:3003:1: rule__NullAttribute__ValueAssignment : ( ( 'NULL' ) ) ;
    public final void rule__NullAttribute__ValueAssignment() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:3007:1: ( ( ( 'NULL' ) ) )
            // InternalEFactory.g:3008:1: ( ( 'NULL' ) )
            {
            // InternalEFactory.g:3008:1: ( ( 'NULL' ) )
            // InternalEFactory.g:3009:1: ( 'NULL' )
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getNullAttributeAccess().getValueNULLKeyword_0()); 
            }
            // InternalEFactory.g:3010:1: ( 'NULL' )
            // InternalEFactory.g:3011:1: 'NULL'
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getNullAttributeAccess().getValueNULLKeyword_0()); 
            }
            match(input,27,FOLLOW_2); if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getNullAttributeAccess().getValueNULLKeyword_0()); 
            }

            }

            if ( state.backtracking==0 ) {
               after(grammarAccess.getNullAttributeAccess().getValueNULLKeyword_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__NullAttribute__ValueAssignment"


    // $ANTLR start "rule__BooleanAttribute__ValueAssignment"
    // InternalEFactory.g:3026:1: rule__BooleanAttribute__ValueAssignment : ( ruleBoolean ) ;
    public final void rule__BooleanAttribute__ValueAssignment() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // InternalEFactory.g:3030:1: ( ( ruleBoolean ) )
            // InternalEFactory.g:3031:1: ( ruleBoolean )
            {
            // InternalEFactory.g:3031:1: ( ruleBoolean )
            // InternalEFactory.g:3032:1: ruleBoolean
            {
            if ( state.backtracking==0 ) {
               before(grammarAccess.getBooleanAttributeAccess().getValueBooleanParserRuleCall_0()); 
            }
            pushFollow(FOLLOW_2);
            ruleBoolean();

            state._fsp--;
            if (state.failed) return ;
            if ( state.backtracking==0 ) {
               after(grammarAccess.getBooleanAttributeAccess().getValueBooleanParserRuleCall_0()); 
            }

            }


            }

        }
        catch (RecognitionException re) {
            reportError(re);
            recover(input,re);
        }
        finally {

            	restoreStackSize(stackSize);

        }
        return ;
    }
    // $ANTLR end "rule__BooleanAttribute__ValueAssignment"

    // $ANTLR start synpred2_InternalEFactory
    public final void synpred2_InternalEFactory_fragment() throws RecognitionException {   
        // InternalEFactory.g:800:6: ( ( ruleAttribute ) )
        // InternalEFactory.g:800:6: ( ruleAttribute )
        {
        // InternalEFactory.g:800:6: ( ruleAttribute )
        // InternalEFactory.g:801:1: ruleAttribute
        {
        if ( state.backtracking==0 ) {
           before(grammarAccess.getValueAccess().getAttributeParserRuleCall_1()); 
        }
        pushFollow(FOLLOW_2);
        ruleAttribute();

        state._fsp--;
        if (state.failed) return ;

        }


        }
    }
    // $ANTLR end synpred2_InternalEFactory

    // $ANTLR start synpred3_InternalEFactory
    public final void synpred3_InternalEFactory_fragment() throws RecognitionException {   
        // InternalEFactory.g:806:6: ( ( ( rule__Value__Group_2__0 ) ) )
        // InternalEFactory.g:806:6: ( ( rule__Value__Group_2__0 ) )
        {
        // InternalEFactory.g:806:6: ( ( rule__Value__Group_2__0 ) )
        // InternalEFactory.g:807:1: ( rule__Value__Group_2__0 )
        {
        if ( state.backtracking==0 ) {
           before(grammarAccess.getValueAccess().getGroup_2()); 
        }
        // InternalEFactory.g:808:1: ( rule__Value__Group_2__0 )
        // InternalEFactory.g:808:2: rule__Value__Group_2__0
        {
        pushFollow(FOLLOW_2);
        rule__Value__Group_2__0();

        state._fsp--;
        if (state.failed) return ;

        }


        }


        }
    }
    // $ANTLR end synpred3_InternalEFactory

    // $ANTLR start synpred21_InternalEFactory
    public final void synpred21_InternalEFactory_fragment() throws RecognitionException {   
        // InternalEFactory.g:1854:2: ( rule__Value__EClassAssignment_2_0_0_1 )
        // InternalEFactory.g:1854:2: rule__Value__EClassAssignment_2_0_0_1
        {
        pushFollow(FOLLOW_2);
        rule__Value__EClassAssignment_2_0_0_1();

        state._fsp--;
        if (state.failed) return ;

        }
    }
    // $ANTLR end synpred21_InternalEFactory

    // Delegated rules

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
    public final boolean synpred21_InternalEFactory() {
        state.backtracking++;
        int start = input.mark();
        try {
            synpred21_InternalEFactory_fragment(); // can never throw exception
        } catch (RecognitionException re) {
            System.err.println("impossible: "+re);
        }
        boolean success = !state.failed;
        input.rewind(start);
        state.backtracking--;
        state.failed=false;
        return success;
    }


    protected DFA2 dfa2 = new DFA2(this);
    protected DFA9 dfa9 = new DFA9(this);
    static final String dfa_1s = "\15\uffff";
    static final String dfa_2s = "\4\uffff\1\11\2\uffff\1\11\3\uffff\1\12\1\uffff";
    static final String dfa_3s = "\1\4\2\uffff\1\7\1\4\2\uffff\1\4\1\7\2\uffff\1\4\1\uffff";
    static final String dfa_4s = "\1\33\2\uffff\1\7\1\33\2\uffff\1\33\1\7\2\uffff\1\33\1\uffff";
    static final String dfa_5s = "\1\uffff\1\1\1\2\2\uffff\1\4\1\7\2\uffff\1\3\1\5\1\uffff\1\6";
    static final String dfa_6s = "\15\uffff}>";
    static final String[] dfa_7s = {
            "\1\2\2\uffff\1\4\5\uffff\2\5\6\uffff\1\1\4\uffff\1\3\1\6",
            "",
            "",
            "\1\7",
            "\4\11\5\uffff\2\11\3\uffff\1\11\1\uffff\4\11\1\uffff\1\10\2\11",
            "",
            "",
            "\4\11\5\uffff\2\11\3\uffff\1\11\1\uffff\4\11\1\uffff\1\12\2\11",
            "\1\13",
            "",
            "",
            "\4\12\5\uffff\2\12\3\uffff\1\12\1\uffff\4\12\1\uffff\1\14\2\12",
            ""
    };

    static final short[] dfa_1 = DFA.unpackEncodedString(dfa_1s);
    static final short[] dfa_2 = DFA.unpackEncodedString(dfa_2s);
    static final char[] dfa_3 = DFA.unpackEncodedStringToUnsignedChars(dfa_3s);
    static final char[] dfa_4 = DFA.unpackEncodedStringToUnsignedChars(dfa_4s);
    static final short[] dfa_5 = DFA.unpackEncodedString(dfa_5s);
    static final short[] dfa_6 = DFA.unpackEncodedString(dfa_6s);
    static final short[][] dfa_7 = unpackEncodedStringArray(dfa_7s);

    class DFA2 extends DFA {

        public DFA2(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 2;
            this.eot = dfa_1;
            this.eof = dfa_2;
            this.min = dfa_3;
            this.max = dfa_4;
            this.accept = dfa_5;
            this.special = dfa_6;
            this.transition = dfa_7;
        }
        public String getDescription() {
            return "823:1: rule__Attribute__Alternatives : ( ( ruleEnumAttribute ) | ( ruleStringAttribute ) | ( ruleIntegerAttribute ) | ( ruleBooleanAttribute ) | ( ruleDoubleAttribute ) | ( ruleDateAttribute ) | ( ruleNullAttribute ) );";
        }
    }
    static final String dfa_8s = "\16\uffff";
    static final String dfa_9s = "\1\5\1\uffff\3\1\3\uffff\4\1\1\uffff\1\1";
    static final String dfa_10s = "\1\4\1\uffff\3\5\1\uffff\10\5";
    static final String dfa_11s = "\1\33\1\uffff\3\31\1\uffff\2\7\4\31\1\7\1\31";
    static final String dfa_12s = "\1\uffff\1\1\3\uffff\1\2\10\uffff";
    static final String dfa_13s = "\16\uffff}>";
    static final String[] dfa_14s = {
            "\1\1\1\3\1\4\1\2\5\uffff\2\1\3\uffff\1\1\1\uffff\1\5\2\1\3\uffff\2\1",
            "",
            "\3\1\12\uffff\1\1\1\uffff\1\1\1\5\3\uffff\1\6",
            "\3\1\12\uffff\1\1\1\uffff\1\1\1\5\3\uffff\1\7",
            "\3\1\12\uffff\1\1\1\uffff\1\1\1\5\3\uffff\1\7",
            "",
            "\1\10\1\11\1\12",
            "\1\10\1\11\1\13",
            "\3\1\12\uffff\1\1\1\uffff\1\1\1\5\3\uffff\1\7",
            "\3\1\12\uffff\1\1\1\uffff\1\1\1\5\3\uffff\1\7",
            "\3\1\12\uffff\1\1\1\uffff\1\1\1\5\3\uffff\1\14",
            "\3\1\12\uffff\1\1\1\uffff\1\1\1\5\3\uffff\1\7",
            "\1\10\1\11\1\15",
            "\3\1\12\uffff\1\1\1\uffff\1\1\1\5\3\uffff\1\7"
    };

    static final short[] dfa_8 = DFA.unpackEncodedString(dfa_8s);
    static final short[] dfa_9 = DFA.unpackEncodedString(dfa_9s);
    static final char[] dfa_10 = DFA.unpackEncodedStringToUnsignedChars(dfa_10s);
    static final char[] dfa_11 = DFA.unpackEncodedStringToUnsignedChars(dfa_11s);
    static final short[] dfa_12 = DFA.unpackEncodedString(dfa_12s);
    static final short[] dfa_13 = DFA.unpackEncodedString(dfa_13s);
    static final short[][] dfa_14 = unpackEncodedStringArray(dfa_14s);

    class DFA9 extends DFA {

        public DFA9(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 9;
            this.eot = dfa_8;
            this.eof = dfa_9;
            this.min = dfa_10;
            this.max = dfa_11;
            this.accept = dfa_12;
            this.special = dfa_13;
            this.transition = dfa_14;
        }
        public String getDescription() {
            return "1473:1: ( rule__Feature__ValueAssignment_2 )?";
        }
    }
 

    public static final BitSet FOLLOW_1 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_2 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_3 = new BitSet(new long[]{0x00000000000280E0L});
    public static final BitSet FOLLOW_4 = new BitSet(new long[]{0x0000000000010002L});
    public static final BitSet FOLLOW_5 = new BitSet(new long[]{0x0000000000008002L});
    public static final BitSet FOLLOW_6 = new BitSet(new long[]{0x0000000000020002L});
    public static final BitSet FOLLOW_7 = new BitSet(new long[]{0x00000000000280F0L});
    public static final BitSet FOLLOW_8 = new BitSet(new long[]{0x0000000000040000L});
    public static final BitSet FOLLOW_9 = new BitSet(new long[]{0x0000000000080000L});
    public static final BitSet FOLLOW_10 = new BitSet(new long[]{0x0000000000100000L});
    public static final BitSet FOLLOW_11 = new BitSet(new long[]{0x0000000000200000L});
    public static final BitSet FOLLOW_12 = new BitSet(new long[]{0x000000000C66E0F0L});
    public static final BitSet FOLLOW_13 = new BitSet(new long[]{0x00000000000680E0L});
    public static final BitSet FOLLOW_14 = new BitSet(new long[]{0x00000000001280E0L});
    public static final BitSet FOLLOW_15 = new BitSet(new long[]{0x00000000000280E2L});
    public static final BitSet FOLLOW_16 = new BitSet(new long[]{0x0000000000400000L});
    public static final BitSet FOLLOW_17 = new BitSet(new long[]{0x000000000CE6E0F0L});
    public static final BitSet FOLLOW_18 = new BitSet(new long[]{0x000000000C66E0F2L});
    public static final BitSet FOLLOW_19 = new BitSet(new long[]{0x0000000001000000L});
    public static final BitSet FOLLOW_20 = new BitSet(new long[]{0x0000000002000000L});
    public static final BitSet FOLLOW_21 = new BitSet(new long[]{0x0000000002000002L});
    public static final BitSet FOLLOW_22 = new BitSet(new long[]{0x0000000004000080L});
    public static final BitSet FOLLOW_23 = new BitSet(new long[]{0x0000000000000080L});

}