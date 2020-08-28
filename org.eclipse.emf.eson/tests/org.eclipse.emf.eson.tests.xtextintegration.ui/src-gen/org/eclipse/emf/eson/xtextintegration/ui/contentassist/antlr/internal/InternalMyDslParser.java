package org.eclipse.emf.eson.xtextintegration.ui.contentassist.antlr.internal; 

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
import org.eclipse.emf.eson.xtextintegration.services.MyDslGrammarAccess;



import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

@SuppressWarnings("all")
public class InternalMyDslParser extends AbstractInternalContentAssistParser {
    public static final String[] tokenNames = new String[] {
        "<invalid>", "<EOR>", "<DOWN>", "<UP>", "RULE_ID", "RULE_INT", "RULE_STRING", "RULE_ML_COMMENT", "RULE_SL_COMMENT", "RULE_WS", "RULE_ANY_OTHER", "'Greetings'", "'REF'", "'REFs'", "'Hello'", "'!'"
    };
    public static final int RULE_ID=4;
    public static final int RULE_STRING=6;
    public static final int T__15=15;
    public static final int T__12=12;
    public static final int T__11=11;
    public static final int T__14=14;
    public static final int T__13=13;
    public static final int RULE_ANY_OTHER=10;
    public static final int RULE_INT=5;
    public static final int RULE_WS=9;
    public static final int RULE_SL_COMMENT=8;
    public static final int EOF=-1;
    public static final int RULE_ML_COMMENT=7;

    // delegates
    // delegators


        public InternalMyDslParser(TokenStream input) {
            this(input, new RecognizerSharedState());
        }
        public InternalMyDslParser(TokenStream input, RecognizerSharedState state) {
            super(input, state);
             
        }
        

    public String[] getTokenNames() { return InternalMyDslParser.tokenNames; }
    public String getGrammarFileName() { return "../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g"; }


     
     	private MyDslGrammarAccess grammarAccess;
     	
        public void setGrammarAccess(MyDslGrammarAccess grammarAccess) {
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




    // $ANTLR start "entryRuleModel"
    // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:60:1: entryRuleModel : ruleModel EOF ;
    public final void entryRuleModel() throws RecognitionException {
        try {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:61:1: ( ruleModel EOF )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:62:1: ruleModel EOF
            {
             before(grammarAccess.getModelRule()); 
            pushFollow(FOLLOW_ruleModel_in_entryRuleModel61);
            ruleModel();

            state._fsp--;

             after(grammarAccess.getModelRule()); 
            match(input,EOF,FOLLOW_EOF_in_entryRuleModel68); 

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
    // $ANTLR end "entryRuleModel"


    // $ANTLR start "ruleModel"
    // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:69:1: ruleModel : ( ( rule__Model__Group__0 ) ) ;
    public final void ruleModel() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:73:2: ( ( ( rule__Model__Group__0 ) ) )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:74:1: ( ( rule__Model__Group__0 ) )
            {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:74:1: ( ( rule__Model__Group__0 ) )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:75:1: ( rule__Model__Group__0 )
            {
             before(grammarAccess.getModelAccess().getGroup()); 
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:76:1: ( rule__Model__Group__0 )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:76:2: rule__Model__Group__0
            {
            pushFollow(FOLLOW_rule__Model__Group__0_in_ruleModel94);
            rule__Model__Group__0();

            state._fsp--;


            }

             after(grammarAccess.getModelAccess().getGroup()); 

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
    // $ANTLR end "ruleModel"


    // $ANTLR start "entryRuleGreeting"
    // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:88:1: entryRuleGreeting : ruleGreeting EOF ;
    public final void entryRuleGreeting() throws RecognitionException {
        try {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:89:1: ( ruleGreeting EOF )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:90:1: ruleGreeting EOF
            {
             before(grammarAccess.getGreetingRule()); 
            pushFollow(FOLLOW_ruleGreeting_in_entryRuleGreeting121);
            ruleGreeting();

            state._fsp--;

             after(grammarAccess.getGreetingRule()); 
            match(input,EOF,FOLLOW_EOF_in_entryRuleGreeting128); 

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
    // $ANTLR end "entryRuleGreeting"


    // $ANTLR start "ruleGreeting"
    // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:97:1: ruleGreeting : ( ( rule__Greeting__Group__0 ) ) ;
    public final void ruleGreeting() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:101:2: ( ( ( rule__Greeting__Group__0 ) ) )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:102:1: ( ( rule__Greeting__Group__0 ) )
            {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:102:1: ( ( rule__Greeting__Group__0 ) )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:103:1: ( rule__Greeting__Group__0 )
            {
             before(grammarAccess.getGreetingAccess().getGroup()); 
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:104:1: ( rule__Greeting__Group__0 )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:104:2: rule__Greeting__Group__0
            {
            pushFollow(FOLLOW_rule__Greeting__Group__0_in_ruleGreeting154);
            rule__Greeting__Group__0();

            state._fsp--;


            }

             after(grammarAccess.getGreetingAccess().getGroup()); 

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
    // $ANTLR end "ruleGreeting"


    // $ANTLR start "rule__Model__Group__0"
    // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:118:1: rule__Model__Group__0 : rule__Model__Group__0__Impl rule__Model__Group__1 ;
    public final void rule__Model__Group__0() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:122:1: ( rule__Model__Group__0__Impl rule__Model__Group__1 )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:123:2: rule__Model__Group__0__Impl rule__Model__Group__1
            {
            pushFollow(FOLLOW_rule__Model__Group__0__Impl_in_rule__Model__Group__0188);
            rule__Model__Group__0__Impl();

            state._fsp--;

            pushFollow(FOLLOW_rule__Model__Group__1_in_rule__Model__Group__0191);
            rule__Model__Group__1();

            state._fsp--;


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
    // $ANTLR end "rule__Model__Group__0"


    // $ANTLR start "rule__Model__Group__0__Impl"
    // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:130:1: rule__Model__Group__0__Impl : ( 'Greetings' ) ;
    public final void rule__Model__Group__0__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:134:1: ( ( 'Greetings' ) )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:135:1: ( 'Greetings' )
            {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:135:1: ( 'Greetings' )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:136:1: 'Greetings'
            {
             before(grammarAccess.getModelAccess().getGreetingsKeyword_0()); 
            match(input,11,FOLLOW_11_in_rule__Model__Group__0__Impl219); 
             after(grammarAccess.getModelAccess().getGreetingsKeyword_0()); 

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
    // $ANTLR end "rule__Model__Group__0__Impl"


    // $ANTLR start "rule__Model__Group__1"
    // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:149:1: rule__Model__Group__1 : rule__Model__Group__1__Impl rule__Model__Group__2 ;
    public final void rule__Model__Group__1() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:153:1: ( rule__Model__Group__1__Impl rule__Model__Group__2 )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:154:2: rule__Model__Group__1__Impl rule__Model__Group__2
            {
            pushFollow(FOLLOW_rule__Model__Group__1__Impl_in_rule__Model__Group__1250);
            rule__Model__Group__1__Impl();

            state._fsp--;

            pushFollow(FOLLOW_rule__Model__Group__2_in_rule__Model__Group__1253);
            rule__Model__Group__2();

            state._fsp--;


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
    // $ANTLR end "rule__Model__Group__1"


    // $ANTLR start "rule__Model__Group__1__Impl"
    // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:161:1: rule__Model__Group__1__Impl : ( ( rule__Model__NameAssignment_1 ) ) ;
    public final void rule__Model__Group__1__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:165:1: ( ( ( rule__Model__NameAssignment_1 ) ) )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:166:1: ( ( rule__Model__NameAssignment_1 ) )
            {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:166:1: ( ( rule__Model__NameAssignment_1 ) )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:167:1: ( rule__Model__NameAssignment_1 )
            {
             before(grammarAccess.getModelAccess().getNameAssignment_1()); 
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:168:1: ( rule__Model__NameAssignment_1 )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:168:2: rule__Model__NameAssignment_1
            {
            pushFollow(FOLLOW_rule__Model__NameAssignment_1_in_rule__Model__Group__1__Impl280);
            rule__Model__NameAssignment_1();

            state._fsp--;


            }

             after(grammarAccess.getModelAccess().getNameAssignment_1()); 

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
    // $ANTLR end "rule__Model__Group__1__Impl"


    // $ANTLR start "rule__Model__Group__2"
    // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:178:1: rule__Model__Group__2 : rule__Model__Group__2__Impl rule__Model__Group__3 ;
    public final void rule__Model__Group__2() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:182:1: ( rule__Model__Group__2__Impl rule__Model__Group__3 )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:183:2: rule__Model__Group__2__Impl rule__Model__Group__3
            {
            pushFollow(FOLLOW_rule__Model__Group__2__Impl_in_rule__Model__Group__2310);
            rule__Model__Group__2__Impl();

            state._fsp--;

            pushFollow(FOLLOW_rule__Model__Group__3_in_rule__Model__Group__2313);
            rule__Model__Group__3();

            state._fsp--;


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
    // $ANTLR end "rule__Model__Group__2"


    // $ANTLR start "rule__Model__Group__2__Impl"
    // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:190:1: rule__Model__Group__2__Impl : ( ( rule__Model__Group_2__0 )? ) ;
    public final void rule__Model__Group__2__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:194:1: ( ( ( rule__Model__Group_2__0 )? ) )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:195:1: ( ( rule__Model__Group_2__0 )? )
            {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:195:1: ( ( rule__Model__Group_2__0 )? )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:196:1: ( rule__Model__Group_2__0 )?
            {
             before(grammarAccess.getModelAccess().getGroup_2()); 
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:197:1: ( rule__Model__Group_2__0 )?
            int alt1=2;
            int LA1_0 = input.LA(1);

            if ( (LA1_0==12) ) {
                alt1=1;
            }
            switch (alt1) {
                case 1 :
                    // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:197:2: rule__Model__Group_2__0
                    {
                    pushFollow(FOLLOW_rule__Model__Group_2__0_in_rule__Model__Group__2__Impl340);
                    rule__Model__Group_2__0();

                    state._fsp--;


                    }
                    break;

            }

             after(grammarAccess.getModelAccess().getGroup_2()); 

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
    // $ANTLR end "rule__Model__Group__2__Impl"


    // $ANTLR start "rule__Model__Group__3"
    // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:207:1: rule__Model__Group__3 : rule__Model__Group__3__Impl rule__Model__Group__4 ;
    public final void rule__Model__Group__3() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:211:1: ( rule__Model__Group__3__Impl rule__Model__Group__4 )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:212:2: rule__Model__Group__3__Impl rule__Model__Group__4
            {
            pushFollow(FOLLOW_rule__Model__Group__3__Impl_in_rule__Model__Group__3371);
            rule__Model__Group__3__Impl();

            state._fsp--;

            pushFollow(FOLLOW_rule__Model__Group__4_in_rule__Model__Group__3374);
            rule__Model__Group__4();

            state._fsp--;


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
    // $ANTLR end "rule__Model__Group__3"


    // $ANTLR start "rule__Model__Group__3__Impl"
    // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:219:1: rule__Model__Group__3__Impl : ( ( rule__Model__Group_3__0 )? ) ;
    public final void rule__Model__Group__3__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:223:1: ( ( ( rule__Model__Group_3__0 )? ) )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:224:1: ( ( rule__Model__Group_3__0 )? )
            {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:224:1: ( ( rule__Model__Group_3__0 )? )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:225:1: ( rule__Model__Group_3__0 )?
            {
             before(grammarAccess.getModelAccess().getGroup_3()); 
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:226:1: ( rule__Model__Group_3__0 )?
            int alt2=2;
            int LA2_0 = input.LA(1);

            if ( (LA2_0==13) ) {
                alt2=1;
            }
            switch (alt2) {
                case 1 :
                    // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:226:2: rule__Model__Group_3__0
                    {
                    pushFollow(FOLLOW_rule__Model__Group_3__0_in_rule__Model__Group__3__Impl401);
                    rule__Model__Group_3__0();

                    state._fsp--;


                    }
                    break;

            }

             after(grammarAccess.getModelAccess().getGroup_3()); 

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
    // $ANTLR end "rule__Model__Group__3__Impl"


    // $ANTLR start "rule__Model__Group__4"
    // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:236:1: rule__Model__Group__4 : rule__Model__Group__4__Impl ;
    public final void rule__Model__Group__4() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:240:1: ( rule__Model__Group__4__Impl )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:241:2: rule__Model__Group__4__Impl
            {
            pushFollow(FOLLOW_rule__Model__Group__4__Impl_in_rule__Model__Group__4432);
            rule__Model__Group__4__Impl();

            state._fsp--;


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
    // $ANTLR end "rule__Model__Group__4"


    // $ANTLR start "rule__Model__Group__4__Impl"
    // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:247:1: rule__Model__Group__4__Impl : ( ( rule__Model__GreetingsAssignment_4 )* ) ;
    public final void rule__Model__Group__4__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:251:1: ( ( ( rule__Model__GreetingsAssignment_4 )* ) )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:252:1: ( ( rule__Model__GreetingsAssignment_4 )* )
            {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:252:1: ( ( rule__Model__GreetingsAssignment_4 )* )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:253:1: ( rule__Model__GreetingsAssignment_4 )*
            {
             before(grammarAccess.getModelAccess().getGreetingsAssignment_4()); 
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:254:1: ( rule__Model__GreetingsAssignment_4 )*
            loop3:
            do {
                int alt3=2;
                int LA3_0 = input.LA(1);

                if ( (LA3_0==14) ) {
                    alt3=1;
                }


                switch (alt3) {
            	case 1 :
            	    // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:254:2: rule__Model__GreetingsAssignment_4
            	    {
            	    pushFollow(FOLLOW_rule__Model__GreetingsAssignment_4_in_rule__Model__Group__4__Impl459);
            	    rule__Model__GreetingsAssignment_4();

            	    state._fsp--;


            	    }
            	    break;

            	default :
            	    break loop3;
                }
            } while (true);

             after(grammarAccess.getModelAccess().getGreetingsAssignment_4()); 

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
    // $ANTLR end "rule__Model__Group__4__Impl"


    // $ANTLR start "rule__Model__Group_2__0"
    // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:274:1: rule__Model__Group_2__0 : rule__Model__Group_2__0__Impl rule__Model__Group_2__1 ;
    public final void rule__Model__Group_2__0() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:278:1: ( rule__Model__Group_2__0__Impl rule__Model__Group_2__1 )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:279:2: rule__Model__Group_2__0__Impl rule__Model__Group_2__1
            {
            pushFollow(FOLLOW_rule__Model__Group_2__0__Impl_in_rule__Model__Group_2__0500);
            rule__Model__Group_2__0__Impl();

            state._fsp--;

            pushFollow(FOLLOW_rule__Model__Group_2__1_in_rule__Model__Group_2__0503);
            rule__Model__Group_2__1();

            state._fsp--;


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
    // $ANTLR end "rule__Model__Group_2__0"


    // $ANTLR start "rule__Model__Group_2__0__Impl"
    // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:286:1: rule__Model__Group_2__0__Impl : ( 'REF' ) ;
    public final void rule__Model__Group_2__0__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:290:1: ( ( 'REF' ) )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:291:1: ( 'REF' )
            {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:291:1: ( 'REF' )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:292:1: 'REF'
            {
             before(grammarAccess.getModelAccess().getREFKeyword_2_0()); 
            match(input,12,FOLLOW_12_in_rule__Model__Group_2__0__Impl531); 
             after(grammarAccess.getModelAccess().getREFKeyword_2_0()); 

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
    // $ANTLR end "rule__Model__Group_2__0__Impl"


    // $ANTLR start "rule__Model__Group_2__1"
    // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:305:1: rule__Model__Group_2__1 : rule__Model__Group_2__1__Impl ;
    public final void rule__Model__Group_2__1() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:309:1: ( rule__Model__Group_2__1__Impl )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:310:2: rule__Model__Group_2__1__Impl
            {
            pushFollow(FOLLOW_rule__Model__Group_2__1__Impl_in_rule__Model__Group_2__1562);
            rule__Model__Group_2__1__Impl();

            state._fsp--;


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
    // $ANTLR end "rule__Model__Group_2__1"


    // $ANTLR start "rule__Model__Group_2__1__Impl"
    // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:316:1: rule__Model__Group_2__1__Impl : ( ( rule__Model__RefModelAssignment_2_1 ) ) ;
    public final void rule__Model__Group_2__1__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:320:1: ( ( ( rule__Model__RefModelAssignment_2_1 ) ) )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:321:1: ( ( rule__Model__RefModelAssignment_2_1 ) )
            {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:321:1: ( ( rule__Model__RefModelAssignment_2_1 ) )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:322:1: ( rule__Model__RefModelAssignment_2_1 )
            {
             before(grammarAccess.getModelAccess().getRefModelAssignment_2_1()); 
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:323:1: ( rule__Model__RefModelAssignment_2_1 )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:323:2: rule__Model__RefModelAssignment_2_1
            {
            pushFollow(FOLLOW_rule__Model__RefModelAssignment_2_1_in_rule__Model__Group_2__1__Impl589);
            rule__Model__RefModelAssignment_2_1();

            state._fsp--;


            }

             after(grammarAccess.getModelAccess().getRefModelAssignment_2_1()); 

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
    // $ANTLR end "rule__Model__Group_2__1__Impl"


    // $ANTLR start "rule__Model__Group_3__0"
    // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:337:1: rule__Model__Group_3__0 : rule__Model__Group_3__0__Impl rule__Model__Group_3__1 ;
    public final void rule__Model__Group_3__0() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:341:1: ( rule__Model__Group_3__0__Impl rule__Model__Group_3__1 )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:342:2: rule__Model__Group_3__0__Impl rule__Model__Group_3__1
            {
            pushFollow(FOLLOW_rule__Model__Group_3__0__Impl_in_rule__Model__Group_3__0623);
            rule__Model__Group_3__0__Impl();

            state._fsp--;

            pushFollow(FOLLOW_rule__Model__Group_3__1_in_rule__Model__Group_3__0626);
            rule__Model__Group_3__1();

            state._fsp--;


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
    // $ANTLR end "rule__Model__Group_3__0"


    // $ANTLR start "rule__Model__Group_3__0__Impl"
    // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:349:1: rule__Model__Group_3__0__Impl : ( 'REFs' ) ;
    public final void rule__Model__Group_3__0__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:353:1: ( ( 'REFs' ) )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:354:1: ( 'REFs' )
            {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:354:1: ( 'REFs' )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:355:1: 'REFs'
            {
             before(grammarAccess.getModelAccess().getREFsKeyword_3_0()); 
            match(input,13,FOLLOW_13_in_rule__Model__Group_3__0__Impl654); 
             after(grammarAccess.getModelAccess().getREFsKeyword_3_0()); 

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
    // $ANTLR end "rule__Model__Group_3__0__Impl"


    // $ANTLR start "rule__Model__Group_3__1"
    // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:368:1: rule__Model__Group_3__1 : rule__Model__Group_3__1__Impl ;
    public final void rule__Model__Group_3__1() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:372:1: ( rule__Model__Group_3__1__Impl )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:373:2: rule__Model__Group_3__1__Impl
            {
            pushFollow(FOLLOW_rule__Model__Group_3__1__Impl_in_rule__Model__Group_3__1685);
            rule__Model__Group_3__1__Impl();

            state._fsp--;


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
    // $ANTLR end "rule__Model__Group_3__1"


    // $ANTLR start "rule__Model__Group_3__1__Impl"
    // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:379:1: rule__Model__Group_3__1__Impl : ( ( ( rule__Model__RefModelsAssignment_3_1 ) ) ( ( rule__Model__RefModelsAssignment_3_1 )* ) ) ;
    public final void rule__Model__Group_3__1__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:383:1: ( ( ( ( rule__Model__RefModelsAssignment_3_1 ) ) ( ( rule__Model__RefModelsAssignment_3_1 )* ) ) )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:384:1: ( ( ( rule__Model__RefModelsAssignment_3_1 ) ) ( ( rule__Model__RefModelsAssignment_3_1 )* ) )
            {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:384:1: ( ( ( rule__Model__RefModelsAssignment_3_1 ) ) ( ( rule__Model__RefModelsAssignment_3_1 )* ) )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:385:1: ( ( rule__Model__RefModelsAssignment_3_1 ) ) ( ( rule__Model__RefModelsAssignment_3_1 )* )
            {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:385:1: ( ( rule__Model__RefModelsAssignment_3_1 ) )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:386:1: ( rule__Model__RefModelsAssignment_3_1 )
            {
             before(grammarAccess.getModelAccess().getRefModelsAssignment_3_1()); 
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:387:1: ( rule__Model__RefModelsAssignment_3_1 )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:387:2: rule__Model__RefModelsAssignment_3_1
            {
            pushFollow(FOLLOW_rule__Model__RefModelsAssignment_3_1_in_rule__Model__Group_3__1__Impl714);
            rule__Model__RefModelsAssignment_3_1();

            state._fsp--;


            }

             after(grammarAccess.getModelAccess().getRefModelsAssignment_3_1()); 

            }

            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:390:1: ( ( rule__Model__RefModelsAssignment_3_1 )* )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:391:1: ( rule__Model__RefModelsAssignment_3_1 )*
            {
             before(grammarAccess.getModelAccess().getRefModelsAssignment_3_1()); 
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:392:1: ( rule__Model__RefModelsAssignment_3_1 )*
            loop4:
            do {
                int alt4=2;
                int LA4_0 = input.LA(1);

                if ( (LA4_0==RULE_ID) ) {
                    alt4=1;
                }


                switch (alt4) {
            	case 1 :
            	    // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:392:2: rule__Model__RefModelsAssignment_3_1
            	    {
            	    pushFollow(FOLLOW_rule__Model__RefModelsAssignment_3_1_in_rule__Model__Group_3__1__Impl726);
            	    rule__Model__RefModelsAssignment_3_1();

            	    state._fsp--;


            	    }
            	    break;

            	default :
            	    break loop4;
                }
            } while (true);

             after(grammarAccess.getModelAccess().getRefModelsAssignment_3_1()); 

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
    // $ANTLR end "rule__Model__Group_3__1__Impl"


    // $ANTLR start "rule__Greeting__Group__0"
    // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:407:1: rule__Greeting__Group__0 : rule__Greeting__Group__0__Impl rule__Greeting__Group__1 ;
    public final void rule__Greeting__Group__0() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:411:1: ( rule__Greeting__Group__0__Impl rule__Greeting__Group__1 )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:412:2: rule__Greeting__Group__0__Impl rule__Greeting__Group__1
            {
            pushFollow(FOLLOW_rule__Greeting__Group__0__Impl_in_rule__Greeting__Group__0763);
            rule__Greeting__Group__0__Impl();

            state._fsp--;

            pushFollow(FOLLOW_rule__Greeting__Group__1_in_rule__Greeting__Group__0766);
            rule__Greeting__Group__1();

            state._fsp--;


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
    // $ANTLR end "rule__Greeting__Group__0"


    // $ANTLR start "rule__Greeting__Group__0__Impl"
    // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:419:1: rule__Greeting__Group__0__Impl : ( 'Hello' ) ;
    public final void rule__Greeting__Group__0__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:423:1: ( ( 'Hello' ) )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:424:1: ( 'Hello' )
            {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:424:1: ( 'Hello' )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:425:1: 'Hello'
            {
             before(grammarAccess.getGreetingAccess().getHelloKeyword_0()); 
            match(input,14,FOLLOW_14_in_rule__Greeting__Group__0__Impl794); 
             after(grammarAccess.getGreetingAccess().getHelloKeyword_0()); 

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
    // $ANTLR end "rule__Greeting__Group__0__Impl"


    // $ANTLR start "rule__Greeting__Group__1"
    // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:438:1: rule__Greeting__Group__1 : rule__Greeting__Group__1__Impl rule__Greeting__Group__2 ;
    public final void rule__Greeting__Group__1() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:442:1: ( rule__Greeting__Group__1__Impl rule__Greeting__Group__2 )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:443:2: rule__Greeting__Group__1__Impl rule__Greeting__Group__2
            {
            pushFollow(FOLLOW_rule__Greeting__Group__1__Impl_in_rule__Greeting__Group__1825);
            rule__Greeting__Group__1__Impl();

            state._fsp--;

            pushFollow(FOLLOW_rule__Greeting__Group__2_in_rule__Greeting__Group__1828);
            rule__Greeting__Group__2();

            state._fsp--;


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
    // $ANTLR end "rule__Greeting__Group__1"


    // $ANTLR start "rule__Greeting__Group__1__Impl"
    // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:450:1: rule__Greeting__Group__1__Impl : ( ( rule__Greeting__NameAssignment_1 ) ) ;
    public final void rule__Greeting__Group__1__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:454:1: ( ( ( rule__Greeting__NameAssignment_1 ) ) )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:455:1: ( ( rule__Greeting__NameAssignment_1 ) )
            {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:455:1: ( ( rule__Greeting__NameAssignment_1 ) )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:456:1: ( rule__Greeting__NameAssignment_1 )
            {
             before(grammarAccess.getGreetingAccess().getNameAssignment_1()); 
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:457:1: ( rule__Greeting__NameAssignment_1 )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:457:2: rule__Greeting__NameAssignment_1
            {
            pushFollow(FOLLOW_rule__Greeting__NameAssignment_1_in_rule__Greeting__Group__1__Impl855);
            rule__Greeting__NameAssignment_1();

            state._fsp--;


            }

             after(grammarAccess.getGreetingAccess().getNameAssignment_1()); 

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
    // $ANTLR end "rule__Greeting__Group__1__Impl"


    // $ANTLR start "rule__Greeting__Group__2"
    // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:467:1: rule__Greeting__Group__2 : rule__Greeting__Group__2__Impl ;
    public final void rule__Greeting__Group__2() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:471:1: ( rule__Greeting__Group__2__Impl )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:472:2: rule__Greeting__Group__2__Impl
            {
            pushFollow(FOLLOW_rule__Greeting__Group__2__Impl_in_rule__Greeting__Group__2885);
            rule__Greeting__Group__2__Impl();

            state._fsp--;


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
    // $ANTLR end "rule__Greeting__Group__2"


    // $ANTLR start "rule__Greeting__Group__2__Impl"
    // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:478:1: rule__Greeting__Group__2__Impl : ( '!' ) ;
    public final void rule__Greeting__Group__2__Impl() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:482:1: ( ( '!' ) )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:483:1: ( '!' )
            {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:483:1: ( '!' )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:484:1: '!'
            {
             before(grammarAccess.getGreetingAccess().getExclamationMarkKeyword_2()); 
            match(input,15,FOLLOW_15_in_rule__Greeting__Group__2__Impl913); 
             after(grammarAccess.getGreetingAccess().getExclamationMarkKeyword_2()); 

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
    // $ANTLR end "rule__Greeting__Group__2__Impl"


    // $ANTLR start "rule__Model__NameAssignment_1"
    // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:504:1: rule__Model__NameAssignment_1 : ( RULE_ID ) ;
    public final void rule__Model__NameAssignment_1() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:508:1: ( ( RULE_ID ) )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:509:1: ( RULE_ID )
            {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:509:1: ( RULE_ID )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:510:1: RULE_ID
            {
             before(grammarAccess.getModelAccess().getNameIDTerminalRuleCall_1_0()); 
            match(input,RULE_ID,FOLLOW_RULE_ID_in_rule__Model__NameAssignment_1955); 
             after(grammarAccess.getModelAccess().getNameIDTerminalRuleCall_1_0()); 

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
    // $ANTLR end "rule__Model__NameAssignment_1"


    // $ANTLR start "rule__Model__RefModelAssignment_2_1"
    // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:519:1: rule__Model__RefModelAssignment_2_1 : ( ( RULE_ID ) ) ;
    public final void rule__Model__RefModelAssignment_2_1() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:523:1: ( ( ( RULE_ID ) ) )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:524:1: ( ( RULE_ID ) )
            {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:524:1: ( ( RULE_ID ) )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:525:1: ( RULE_ID )
            {
             before(grammarAccess.getModelAccess().getRefModelModelCrossReference_2_1_0()); 
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:526:1: ( RULE_ID )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:527:1: RULE_ID
            {
             before(grammarAccess.getModelAccess().getRefModelModelIDTerminalRuleCall_2_1_0_1()); 
            match(input,RULE_ID,FOLLOW_RULE_ID_in_rule__Model__RefModelAssignment_2_1990); 
             after(grammarAccess.getModelAccess().getRefModelModelIDTerminalRuleCall_2_1_0_1()); 

            }

             after(grammarAccess.getModelAccess().getRefModelModelCrossReference_2_1_0()); 

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
    // $ANTLR end "rule__Model__RefModelAssignment_2_1"


    // $ANTLR start "rule__Model__RefModelsAssignment_3_1"
    // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:538:1: rule__Model__RefModelsAssignment_3_1 : ( ( RULE_ID ) ) ;
    public final void rule__Model__RefModelsAssignment_3_1() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:542:1: ( ( ( RULE_ID ) ) )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:543:1: ( ( RULE_ID ) )
            {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:543:1: ( ( RULE_ID ) )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:544:1: ( RULE_ID )
            {
             before(grammarAccess.getModelAccess().getRefModelsModelCrossReference_3_1_0()); 
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:545:1: ( RULE_ID )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:546:1: RULE_ID
            {
             before(grammarAccess.getModelAccess().getRefModelsModelIDTerminalRuleCall_3_1_0_1()); 
            match(input,RULE_ID,FOLLOW_RULE_ID_in_rule__Model__RefModelsAssignment_3_11029); 
             after(grammarAccess.getModelAccess().getRefModelsModelIDTerminalRuleCall_3_1_0_1()); 

            }

             after(grammarAccess.getModelAccess().getRefModelsModelCrossReference_3_1_0()); 

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
    // $ANTLR end "rule__Model__RefModelsAssignment_3_1"


    // $ANTLR start "rule__Model__GreetingsAssignment_4"
    // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:557:1: rule__Model__GreetingsAssignment_4 : ( ruleGreeting ) ;
    public final void rule__Model__GreetingsAssignment_4() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:561:1: ( ( ruleGreeting ) )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:562:1: ( ruleGreeting )
            {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:562:1: ( ruleGreeting )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:563:1: ruleGreeting
            {
             before(grammarAccess.getModelAccess().getGreetingsGreetingParserRuleCall_4_0()); 
            pushFollow(FOLLOW_ruleGreeting_in_rule__Model__GreetingsAssignment_41064);
            ruleGreeting();

            state._fsp--;

             after(grammarAccess.getModelAccess().getGreetingsGreetingParserRuleCall_4_0()); 

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
    // $ANTLR end "rule__Model__GreetingsAssignment_4"


    // $ANTLR start "rule__Greeting__NameAssignment_1"
    // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:572:1: rule__Greeting__NameAssignment_1 : ( RULE_ID ) ;
    public final void rule__Greeting__NameAssignment_1() throws RecognitionException {

        		int stackSize = keepStackSize();
            
        try {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:576:1: ( ( RULE_ID ) )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:577:1: ( RULE_ID )
            {
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:577:1: ( RULE_ID )
            // ../org.eclipse.emf.eson.tests.xtextintegration.ui/src-gen/org/eclipse/emf/eson/xtextintegration/ui/contentassist/antlr/internal/InternalMyDsl.g:578:1: RULE_ID
            {
             before(grammarAccess.getGreetingAccess().getNameIDTerminalRuleCall_1_0()); 
            match(input,RULE_ID,FOLLOW_RULE_ID_in_rule__Greeting__NameAssignment_11095); 
             after(grammarAccess.getGreetingAccess().getNameIDTerminalRuleCall_1_0()); 

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
    // $ANTLR end "rule__Greeting__NameAssignment_1"

    // Delegated rules


 

    public static final BitSet FOLLOW_ruleModel_in_entryRuleModel61 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_entryRuleModel68 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_rule__Model__Group__0_in_ruleModel94 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleGreeting_in_entryRuleGreeting121 = new BitSet(new long[]{0x0000000000000000L});
    public static final BitSet FOLLOW_EOF_in_entryRuleGreeting128 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_rule__Greeting__Group__0_in_ruleGreeting154 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_rule__Model__Group__0__Impl_in_rule__Model__Group__0188 = new BitSet(new long[]{0x0000000000000010L});
    public static final BitSet FOLLOW_rule__Model__Group__1_in_rule__Model__Group__0191 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_11_in_rule__Model__Group__0__Impl219 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_rule__Model__Group__1__Impl_in_rule__Model__Group__1250 = new BitSet(new long[]{0x0000000000007000L});
    public static final BitSet FOLLOW_rule__Model__Group__2_in_rule__Model__Group__1253 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_rule__Model__NameAssignment_1_in_rule__Model__Group__1__Impl280 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_rule__Model__Group__2__Impl_in_rule__Model__Group__2310 = new BitSet(new long[]{0x0000000000007000L});
    public static final BitSet FOLLOW_rule__Model__Group__3_in_rule__Model__Group__2313 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_rule__Model__Group_2__0_in_rule__Model__Group__2__Impl340 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_rule__Model__Group__3__Impl_in_rule__Model__Group__3371 = new BitSet(new long[]{0x0000000000007000L});
    public static final BitSet FOLLOW_rule__Model__Group__4_in_rule__Model__Group__3374 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_rule__Model__Group_3__0_in_rule__Model__Group__3__Impl401 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_rule__Model__Group__4__Impl_in_rule__Model__Group__4432 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_rule__Model__GreetingsAssignment_4_in_rule__Model__Group__4__Impl459 = new BitSet(new long[]{0x0000000000004002L});
    public static final BitSet FOLLOW_rule__Model__Group_2__0__Impl_in_rule__Model__Group_2__0500 = new BitSet(new long[]{0x0000000000000010L});
    public static final BitSet FOLLOW_rule__Model__Group_2__1_in_rule__Model__Group_2__0503 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_12_in_rule__Model__Group_2__0__Impl531 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_rule__Model__Group_2__1__Impl_in_rule__Model__Group_2__1562 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_rule__Model__RefModelAssignment_2_1_in_rule__Model__Group_2__1__Impl589 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_rule__Model__Group_3__0__Impl_in_rule__Model__Group_3__0623 = new BitSet(new long[]{0x0000000000000010L});
    public static final BitSet FOLLOW_rule__Model__Group_3__1_in_rule__Model__Group_3__0626 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_13_in_rule__Model__Group_3__0__Impl654 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_rule__Model__Group_3__1__Impl_in_rule__Model__Group_3__1685 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_rule__Model__RefModelsAssignment_3_1_in_rule__Model__Group_3__1__Impl714 = new BitSet(new long[]{0x0000000000000012L});
    public static final BitSet FOLLOW_rule__Model__RefModelsAssignment_3_1_in_rule__Model__Group_3__1__Impl726 = new BitSet(new long[]{0x0000000000000012L});
    public static final BitSet FOLLOW_rule__Greeting__Group__0__Impl_in_rule__Greeting__Group__0763 = new BitSet(new long[]{0x0000000000000010L});
    public static final BitSet FOLLOW_rule__Greeting__Group__1_in_rule__Greeting__Group__0766 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_14_in_rule__Greeting__Group__0__Impl794 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_rule__Greeting__Group__1__Impl_in_rule__Greeting__Group__1825 = new BitSet(new long[]{0x0000000000008000L});
    public static final BitSet FOLLOW_rule__Greeting__Group__2_in_rule__Greeting__Group__1828 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_rule__Greeting__NameAssignment_1_in_rule__Greeting__Group__1__Impl855 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_rule__Greeting__Group__2__Impl_in_rule__Greeting__Group__2885 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_15_in_rule__Greeting__Group__2__Impl913 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_RULE_ID_in_rule__Model__NameAssignment_1955 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_RULE_ID_in_rule__Model__RefModelAssignment_2_1990 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_RULE_ID_in_rule__Model__RefModelsAssignment_3_11029 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_ruleGreeting_in_rule__Model__GreetingsAssignment_41064 = new BitSet(new long[]{0x0000000000000002L});
    public static final BitSet FOLLOW_RULE_ID_in_rule__Greeting__NameAssignment_11095 = new BitSet(new long[]{0x0000000000000002L});

}