package org.eclipse.emf.eson.ui.contentassist.antlr.internal;

// Hack: Use our own Lexer superclass by means of import. 
// Currently there is no other way to specify the superclass for the lexer.
import org.eclipse.xtext.ui.editor.contentassist.antlr.internal.Lexer;


import org.antlr.runtime.*;
import java.util.Stack;
import java.util.List;
import java.util.ArrayList;

@SuppressWarnings("all")
public class InternalEFactoryLexer extends Lexer {
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

    public InternalEFactoryLexer() {;} 
    public InternalEFactoryLexer(CharStream input) {
        this(input, new RecognizerSharedState());
    }
    public InternalEFactoryLexer(CharStream input, RecognizerSharedState state) {
        super(input,state);

    }
    public String getGrammarFileName() { return "../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g"; }

    // $ANTLR start "T__13"
    public final void mT__13() throws RecognitionException {
        try {
            int _type = T__13;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:11:7: ( 'true' )
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:11:9: 'true'
            {
            match("true"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__13"

    // $ANTLR start "T__14"
    public final void mT__14() throws RecognitionException {
        try {
            int _type = T__14;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:12:7: ( 'false' )
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:12:9: 'false'
            {
            match("false"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__14"

    // $ANTLR start "T__15"
    public final void mT__15() throws RecognitionException {
        try {
            int _type = T__15;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:13:7: ( 'use' )
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:13:9: 'use'
            {
            match("use"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__15"

    // $ANTLR start "T__16"
    public final void mT__16() throws RecognitionException {
        try {
            int _type = T__16;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:14:7: ( 'import' )
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:14:9: 'import'
            {
            match("import"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__16"

    // $ANTLR start "T__17"
    public final void mT__17() throws RecognitionException {
        try {
            int _type = T__17;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:15:7: ( '@Name' )
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:15:9: '@Name'
            {
            match("@Name"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__17"

    // $ANTLR start "T__18"
    public final void mT__18() throws RecognitionException {
        try {
            int _type = T__18;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:16:7: ( '{' )
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:16:9: '{'
            {
            match('{'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__18"

    // $ANTLR start "T__19"
    public final void mT__19() throws RecognitionException {
        try {
            int _type = T__19;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:17:7: ( '=' )
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:17:9: '='
            {
            match('='); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__19"

    // $ANTLR start "T__20"
    public final void mT__20() throws RecognitionException {
        try {
            int _type = T__20;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:18:7: ( '}' )
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:18:9: '}'
            {
            match('}'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__20"

    // $ANTLR start "T__21"
    public final void mT__21() throws RecognitionException {
        try {
            int _type = T__21;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:19:7: ( ':' )
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:19:9: ':'
            {
            match(':'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__21"

    // $ANTLR start "T__22"
    public final void mT__22() throws RecognitionException {
        try {
            int _type = T__22;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:20:7: ( '[' )
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:20:9: '['
            {
            match('['); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__22"

    // $ANTLR start "T__23"
    public final void mT__23() throws RecognitionException {
        try {
            int _type = T__23;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:21:7: ( ']' )
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:21:9: ']'
            {
            match(']'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__23"

    // $ANTLR start "T__24"
    public final void mT__24() throws RecognitionException {
        try {
            int _type = T__24;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:22:7: ( '.*' )
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:22:9: '.*'
            {
            match(".*"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__24"

    // $ANTLR start "T__25"
    public final void mT__25() throws RecognitionException {
        try {
            int _type = T__25;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:23:7: ( '.' )
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:23:9: '.'
            {
            match('.'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__25"

    // $ANTLR start "T__26"
    public final void mT__26() throws RecognitionException {
        try {
            int _type = T__26;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:24:7: ( '-' )
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:24:9: '-'
            {
            match('-'); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__26"

    // $ANTLR start "T__27"
    public final void mT__27() throws RecognitionException {
        try {
            int _type = T__27;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:25:7: ( 'NULL' )
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:25:9: 'NULL'
            {
            match("NULL"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "T__27"

    // $ANTLR start "RULE_LONG_ID"
    public final void mRULE_LONG_ID() throws RecognitionException {
        try {
            int _type = RULE_LONG_ID;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3042:14: ( RULE_LONG_UNSIGNED RULE_ID )
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3042:16: RULE_LONG_UNSIGNED RULE_ID
            {
            mRULE_LONG_UNSIGNED(); 
            mRULE_ID(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "RULE_LONG_ID"

    // $ANTLR start "RULE_LONG_UNSIGNED"
    public final void mRULE_LONG_UNSIGNED() throws RecognitionException {
        try {
            int _type = RULE_LONG_UNSIGNED;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3044:20: ( ( '0' .. '9' )+ )
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3044:22: ( '0' .. '9' )+
            {
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3044:22: ( '0' .. '9' )+
            int cnt1=0;
            loop1:
            do {
                int alt1=2;
                int LA1_0 = input.LA(1);

                if ( ((LA1_0>='0' && LA1_0<='9')) ) {
                    alt1=1;
                }


                switch (alt1) {
            	case 1 :
            	    // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3044:23: '0' .. '9'
            	    {
            	    matchRange('0','9'); 

            	    }
            	    break;

            	default :
            	    if ( cnt1 >= 1 ) break loop1;
                        EarlyExitException eee =
                            new EarlyExitException(1, input);
                        throw eee;
                }
                cnt1++;
            } while (true);


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "RULE_LONG_UNSIGNED"

    // $ANTLR start "RULE_ID"
    public final void mRULE_ID() throws RecognitionException {
        try {
            int _type = RULE_ID;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3046:9: ( ( '^' )? ( 'a' .. 'z' | 'A' .. 'Z' | '_' | ',' | '-' ) ( 'a' .. 'z' | 'A' .. 'Z' | '_' | ',' | '-' | '0' .. '9' )* )
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3046:11: ( '^' )? ( 'a' .. 'z' | 'A' .. 'Z' | '_' | ',' | '-' ) ( 'a' .. 'z' | 'A' .. 'Z' | '_' | ',' | '-' | '0' .. '9' )*
            {
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3046:11: ( '^' )?
            int alt2=2;
            int LA2_0 = input.LA(1);

            if ( (LA2_0=='^') ) {
                alt2=1;
            }
            switch (alt2) {
                case 1 :
                    // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3046:11: '^'
                    {
                    match('^'); 

                    }
                    break;

            }

            if ( (input.LA(1)>=',' && input.LA(1)<='-')||(input.LA(1)>='A' && input.LA(1)<='Z')||input.LA(1)=='_'||(input.LA(1)>='a' && input.LA(1)<='z') ) {
                input.consume();

            }
            else {
                MismatchedSetException mse = new MismatchedSetException(null,input);
                recover(mse);
                throw mse;}

            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3046:48: ( 'a' .. 'z' | 'A' .. 'Z' | '_' | ',' | '-' | '0' .. '9' )*
            loop3:
            do {
                int alt3=2;
                int LA3_0 = input.LA(1);

                if ( ((LA3_0>=',' && LA3_0<='-')||(LA3_0>='0' && LA3_0<='9')||(LA3_0>='A' && LA3_0<='Z')||LA3_0=='_'||(LA3_0>='a' && LA3_0<='z')) ) {
                    alt3=1;
                }


                switch (alt3) {
            	case 1 :
            	    // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:
            	    {
            	    if ( (input.LA(1)>=',' && input.LA(1)<='-')||(input.LA(1)>='0' && input.LA(1)<='9')||(input.LA(1)>='A' && input.LA(1)<='Z')||input.LA(1)=='_'||(input.LA(1)>='a' && input.LA(1)<='z') ) {
            	        input.consume();

            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;}


            	    }
            	    break;

            	default :
            	    break loop3;
                }
            } while (true);


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "RULE_ID"

    // $ANTLR start "RULE_INT"
    public final void mRULE_INT() throws RecognitionException {
        try {
            int _type = RULE_INT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3048:10: ( ( '0' .. '9' )+ )
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3048:12: ( '0' .. '9' )+
            {
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3048:12: ( '0' .. '9' )+
            int cnt4=0;
            loop4:
            do {
                int alt4=2;
                int LA4_0 = input.LA(1);

                if ( ((LA4_0>='0' && LA4_0<='9')) ) {
                    alt4=1;
                }


                switch (alt4) {
            	case 1 :
            	    // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3048:13: '0' .. '9'
            	    {
            	    matchRange('0','9'); 

            	    }
            	    break;

            	default :
            	    if ( cnt4 >= 1 ) break loop4;
                        EarlyExitException eee =
                            new EarlyExitException(4, input);
                        throw eee;
                }
                cnt4++;
            } while (true);


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "RULE_INT"

    // $ANTLR start "RULE_STRING"
    public final void mRULE_STRING() throws RecognitionException {
        try {
            int _type = RULE_STRING;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3050:13: ( ( '\"' ( '\\\\' . | ~ ( ( '\\\\' | '\"' ) ) )* '\"' | '\\'' ( '\\\\' . | ~ ( ( '\\\\' | '\\'' ) ) )* '\\'' ) )
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3050:15: ( '\"' ( '\\\\' . | ~ ( ( '\\\\' | '\"' ) ) )* '\"' | '\\'' ( '\\\\' . | ~ ( ( '\\\\' | '\\'' ) ) )* '\\'' )
            {
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3050:15: ( '\"' ( '\\\\' . | ~ ( ( '\\\\' | '\"' ) ) )* '\"' | '\\'' ( '\\\\' . | ~ ( ( '\\\\' | '\\'' ) ) )* '\\'' )
            int alt7=2;
            int LA7_0 = input.LA(1);

            if ( (LA7_0=='\"') ) {
                alt7=1;
            }
            else if ( (LA7_0=='\'') ) {
                alt7=2;
            }
            else {
                NoViableAltException nvae =
                    new NoViableAltException("", 7, 0, input);

                throw nvae;
            }
            switch (alt7) {
                case 1 :
                    // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3050:16: '\"' ( '\\\\' . | ~ ( ( '\\\\' | '\"' ) ) )* '\"'
                    {
                    match('\"'); 
                    // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3050:20: ( '\\\\' . | ~ ( ( '\\\\' | '\"' ) ) )*
                    loop5:
                    do {
                        int alt5=3;
                        int LA5_0 = input.LA(1);

                        if ( (LA5_0=='\\') ) {
                            alt5=1;
                        }
                        else if ( ((LA5_0>='\u0000' && LA5_0<='!')||(LA5_0>='#' && LA5_0<='[')||(LA5_0>=']' && LA5_0<='\uFFFF')) ) {
                            alt5=2;
                        }


                        switch (alt5) {
                    	case 1 :
                    	    // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3050:21: '\\\\' .
                    	    {
                    	    match('\\'); 
                    	    matchAny(); 

                    	    }
                    	    break;
                    	case 2 :
                    	    // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3050:28: ~ ( ( '\\\\' | '\"' ) )
                    	    {
                    	    if ( (input.LA(1)>='\u0000' && input.LA(1)<='!')||(input.LA(1)>='#' && input.LA(1)<='[')||(input.LA(1)>=']' && input.LA(1)<='\uFFFF') ) {
                    	        input.consume();

                    	    }
                    	    else {
                    	        MismatchedSetException mse = new MismatchedSetException(null,input);
                    	        recover(mse);
                    	        throw mse;}


                    	    }
                    	    break;

                    	default :
                    	    break loop5;
                        }
                    } while (true);

                    match('\"'); 

                    }
                    break;
                case 2 :
                    // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3050:48: '\\'' ( '\\\\' . | ~ ( ( '\\\\' | '\\'' ) ) )* '\\''
                    {
                    match('\''); 
                    // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3050:53: ( '\\\\' . | ~ ( ( '\\\\' | '\\'' ) ) )*
                    loop6:
                    do {
                        int alt6=3;
                        int LA6_0 = input.LA(1);

                        if ( (LA6_0=='\\') ) {
                            alt6=1;
                        }
                        else if ( ((LA6_0>='\u0000' && LA6_0<='&')||(LA6_0>='(' && LA6_0<='[')||(LA6_0>=']' && LA6_0<='\uFFFF')) ) {
                            alt6=2;
                        }


                        switch (alt6) {
                    	case 1 :
                    	    // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3050:54: '\\\\' .
                    	    {
                    	    match('\\'); 
                    	    matchAny(); 

                    	    }
                    	    break;
                    	case 2 :
                    	    // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3050:61: ~ ( ( '\\\\' | '\\'' ) )
                    	    {
                    	    if ( (input.LA(1)>='\u0000' && input.LA(1)<='&')||(input.LA(1)>='(' && input.LA(1)<='[')||(input.LA(1)>=']' && input.LA(1)<='\uFFFF') ) {
                    	        input.consume();

                    	    }
                    	    else {
                    	        MismatchedSetException mse = new MismatchedSetException(null,input);
                    	        recover(mse);
                    	        throw mse;}


                    	    }
                    	    break;

                    	default :
                    	    break loop6;
                        }
                    } while (true);

                    match('\''); 

                    }
                    break;

            }


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "RULE_STRING"

    // $ANTLR start "RULE_ML_COMMENT"
    public final void mRULE_ML_COMMENT() throws RecognitionException {
        try {
            int _type = RULE_ML_COMMENT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3052:17: ( '/*' ( options {greedy=false; } : . )* '*/' )
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3052:19: '/*' ( options {greedy=false; } : . )* '*/'
            {
            match("/*"); 

            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3052:24: ( options {greedy=false; } : . )*
            loop8:
            do {
                int alt8=2;
                int LA8_0 = input.LA(1);

                if ( (LA8_0=='*') ) {
                    int LA8_1 = input.LA(2);

                    if ( (LA8_1=='/') ) {
                        alt8=2;
                    }
                    else if ( ((LA8_1>='\u0000' && LA8_1<='.')||(LA8_1>='0' && LA8_1<='\uFFFF')) ) {
                        alt8=1;
                    }


                }
                else if ( ((LA8_0>='\u0000' && LA8_0<=')')||(LA8_0>='+' && LA8_0<='\uFFFF')) ) {
                    alt8=1;
                }


                switch (alt8) {
            	case 1 :
            	    // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3052:52: .
            	    {
            	    matchAny(); 

            	    }
            	    break;

            	default :
            	    break loop8;
                }
            } while (true);

            match("*/"); 


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "RULE_ML_COMMENT"

    // $ANTLR start "RULE_SL_COMMENT"
    public final void mRULE_SL_COMMENT() throws RecognitionException {
        try {
            int _type = RULE_SL_COMMENT;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3054:17: ( '//' (~ ( ( '\\n' | '\\r' ) ) )* ( ( '\\r' )? '\\n' )? )
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3054:19: '//' (~ ( ( '\\n' | '\\r' ) ) )* ( ( '\\r' )? '\\n' )?
            {
            match("//"); 

            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3054:24: (~ ( ( '\\n' | '\\r' ) ) )*
            loop9:
            do {
                int alt9=2;
                int LA9_0 = input.LA(1);

                if ( ((LA9_0>='\u0000' && LA9_0<='\t')||(LA9_0>='\u000B' && LA9_0<='\f')||(LA9_0>='\u000E' && LA9_0<='\uFFFF')) ) {
                    alt9=1;
                }


                switch (alt9) {
            	case 1 :
            	    // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3054:24: ~ ( ( '\\n' | '\\r' ) )
            	    {
            	    if ( (input.LA(1)>='\u0000' && input.LA(1)<='\t')||(input.LA(1)>='\u000B' && input.LA(1)<='\f')||(input.LA(1)>='\u000E' && input.LA(1)<='\uFFFF') ) {
            	        input.consume();

            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;}


            	    }
            	    break;

            	default :
            	    break loop9;
                }
            } while (true);

            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3054:40: ( ( '\\r' )? '\\n' )?
            int alt11=2;
            int LA11_0 = input.LA(1);

            if ( (LA11_0=='\n'||LA11_0=='\r') ) {
                alt11=1;
            }
            switch (alt11) {
                case 1 :
                    // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3054:41: ( '\\r' )? '\\n'
                    {
                    // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3054:41: ( '\\r' )?
                    int alt10=2;
                    int LA10_0 = input.LA(1);

                    if ( (LA10_0=='\r') ) {
                        alt10=1;
                    }
                    switch (alt10) {
                        case 1 :
                            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3054:41: '\\r'
                            {
                            match('\r'); 

                            }
                            break;

                    }

                    match('\n'); 

                    }
                    break;

            }


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "RULE_SL_COMMENT"

    // $ANTLR start "RULE_WS"
    public final void mRULE_WS() throws RecognitionException {
        try {
            int _type = RULE_WS;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3056:9: ( ( ' ' | '\\t' | '\\r' | '\\n' )+ )
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3056:11: ( ' ' | '\\t' | '\\r' | '\\n' )+
            {
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3056:11: ( ' ' | '\\t' | '\\r' | '\\n' )+
            int cnt12=0;
            loop12:
            do {
                int alt12=2;
                int LA12_0 = input.LA(1);

                if ( ((LA12_0>='\t' && LA12_0<='\n')||LA12_0=='\r'||LA12_0==' ') ) {
                    alt12=1;
                }


                switch (alt12) {
            	case 1 :
            	    // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:
            	    {
            	    if ( (input.LA(1)>='\t' && input.LA(1)<='\n')||input.LA(1)=='\r'||input.LA(1)==' ' ) {
            	        input.consume();

            	    }
            	    else {
            	        MismatchedSetException mse = new MismatchedSetException(null,input);
            	        recover(mse);
            	        throw mse;}


            	    }
            	    break;

            	default :
            	    if ( cnt12 >= 1 ) break loop12;
                        EarlyExitException eee =
                            new EarlyExitException(12, input);
                        throw eee;
                }
                cnt12++;
            } while (true);


            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "RULE_WS"

    // $ANTLR start "RULE_ANY_OTHER"
    public final void mRULE_ANY_OTHER() throws RecognitionException {
        try {
            int _type = RULE_ANY_OTHER;
            int _channel = DEFAULT_TOKEN_CHANNEL;
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3058:16: ( . )
            // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:3058:18: .
            {
            matchAny(); 

            }

            state.type = _type;
            state.channel = _channel;
        }
        finally {
        }
    }
    // $ANTLR end "RULE_ANY_OTHER"

    public void mTokens() throws RecognitionException {
        // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:1:8: ( T__13 | T__14 | T__15 | T__16 | T__17 | T__18 | T__19 | T__20 | T__21 | T__22 | T__23 | T__24 | T__25 | T__26 | T__27 | RULE_LONG_ID | RULE_LONG_UNSIGNED | RULE_ID | RULE_INT | RULE_STRING | RULE_ML_COMMENT | RULE_SL_COMMENT | RULE_WS | RULE_ANY_OTHER )
        int alt13=24;
        alt13 = dfa13.predict(input);
        switch (alt13) {
            case 1 :
                // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:1:10: T__13
                {
                mT__13(); 

                }
                break;
            case 2 :
                // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:1:16: T__14
                {
                mT__14(); 

                }
                break;
            case 3 :
                // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:1:22: T__15
                {
                mT__15(); 

                }
                break;
            case 4 :
                // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:1:28: T__16
                {
                mT__16(); 

                }
                break;
            case 5 :
                // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:1:34: T__17
                {
                mT__17(); 

                }
                break;
            case 6 :
                // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:1:40: T__18
                {
                mT__18(); 

                }
                break;
            case 7 :
                // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:1:46: T__19
                {
                mT__19(); 

                }
                break;
            case 8 :
                // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:1:52: T__20
                {
                mT__20(); 

                }
                break;
            case 9 :
                // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:1:58: T__21
                {
                mT__21(); 

                }
                break;
            case 10 :
                // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:1:64: T__22
                {
                mT__22(); 

                }
                break;
            case 11 :
                // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:1:70: T__23
                {
                mT__23(); 

                }
                break;
            case 12 :
                // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:1:76: T__24
                {
                mT__24(); 

                }
                break;
            case 13 :
                // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:1:82: T__25
                {
                mT__25(); 

                }
                break;
            case 14 :
                // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:1:88: T__26
                {
                mT__26(); 

                }
                break;
            case 15 :
                // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:1:94: T__27
                {
                mT__27(); 

                }
                break;
            case 16 :
                // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:1:100: RULE_LONG_ID
                {
                mRULE_LONG_ID(); 

                }
                break;
            case 17 :
                // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:1:113: RULE_LONG_UNSIGNED
                {
                mRULE_LONG_UNSIGNED(); 

                }
                break;
            case 18 :
                // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:1:132: RULE_ID
                {
                mRULE_ID(); 

                }
                break;
            case 19 :
                // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:1:140: RULE_INT
                {
                mRULE_INT(); 

                }
                break;
            case 20 :
                // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:1:149: RULE_STRING
                {
                mRULE_STRING(); 

                }
                break;
            case 21 :
                // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:1:161: RULE_ML_COMMENT
                {
                mRULE_ML_COMMENT(); 

                }
                break;
            case 22 :
                // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:1:177: RULE_SL_COMMENT
                {
                mRULE_SL_COMMENT(); 

                }
                break;
            case 23 :
                // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:1:193: RULE_WS
                {
                mRULE_WS(); 

                }
                break;
            case 24 :
                // ../org.eclipse.emf.eson.ui/src-gen/org/eclipse/emf/eson/ui/contentassist/antlr/internal/InternalEFactory.g:1:201: RULE_ANY_OTHER
                {
                mRULE_ANY_OTHER(); 

                }
                break;

        }

    }


    protected DFA13 dfa13 = new DFA13(this);
    static final String DFA13_eotS =
        "\1\uffff\4\30\1\26\6\uffff\1\44\1\45\1\30\1\47\1\26\1\uffff\3\26\2\uffff\1\30\1\uffff\3\30\12\uffff\1\30\1\uffff\1\47\5\uffff\2\30\1\65\2\30\1\70\1\30\1\uffff\1\30\1\73\1\uffff\1\74\1\30\2\uffff\1\76\1\uffff";
    static final String DFA13_eofS =
        "\77\uffff";
    static final String DFA13_minS =
        "\1\0\1\162\1\141\1\163\1\155\1\116\6\uffff\1\52\1\54\1\125\2\54\1\uffff\2\0\1\52\2\uffff\1\165\1\uffff\1\154\1\145\1\160\12\uffff\1\114\1\uffff\1\54\5\uffff\1\145\1\163\1\54\1\157\1\114\1\54\1\145\1\uffff\1\162\1\54\1\uffff\1\54\1\164\2\uffff\1\54\1\uffff";
    static final String DFA13_maxS =
        "\1\uffff\1\162\1\141\1\163\1\155\1\116\6\uffff\1\52\1\172\1\125\2\172\1\uffff\2\uffff\1\57\2\uffff\1\165\1\uffff\1\154\1\145\1\160\12\uffff\1\114\1\uffff\1\172\5\uffff\1\145\1\163\1\172\1\157\1\114\1\172\1\145\1\uffff\1\162\1\172\1\uffff\1\172\1\164\2\uffff\1\172\1\uffff";
    static final String DFA13_acceptS =
        "\6\uffff\1\6\1\7\1\10\1\11\1\12\1\13\5\uffff\1\22\3\uffff\1\27\1\30\1\uffff\1\22\3\uffff\1\5\1\6\1\7\1\10\1\11\1\12\1\13\1\14\1\15\1\16\1\uffff\1\21\1\uffff\1\20\1\24\1\25\1\26\1\27\7\uffff\1\3\2\uffff\1\1\2\uffff\1\17\1\2\1\uffff\1\4";
    static final String DFA13_specialS =
        "\1\1\21\uffff\1\0\1\2\53\uffff}>";
    static final String[] DFA13_transitionS = {
            "\11\26\2\25\2\26\1\25\22\26\1\25\1\26\1\22\4\26\1\23\4\26\1\21\1\15\1\14\1\24\12\17\1\11\2\26\1\7\2\26\1\5\15\21\1\16\14\21\1\12\1\26\1\13\1\20\1\21\1\26\5\21\1\2\2\21\1\4\12\21\1\1\1\3\5\21\1\6\1\26\1\10\uff82\26",
            "\1\27",
            "\1\31",
            "\1\32",
            "\1\33",
            "\1\34",
            "",
            "",
            "",
            "",
            "",
            "",
            "\1\43",
            "\2\30\2\uffff\12\30\7\uffff\32\30\4\uffff\1\30\1\uffff\32\30",
            "\1\46",
            "\2\51\2\uffff\12\50\7\uffff\32\51\3\uffff\2\51\1\uffff\32\51",
            "\2\30\23\uffff\32\30\4\uffff\1\30\1\uffff\32\30",
            "",
            "\0\52",
            "\0\52",
            "\1\53\4\uffff\1\54",
            "",
            "",
            "\1\56",
            "",
            "\1\57",
            "\1\60",
            "\1\61",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "\1\62",
            "",
            "\2\51\2\uffff\12\50\7\uffff\32\51\3\uffff\2\51\1\uffff\32\51",
            "",
            "",
            "",
            "",
            "",
            "\1\63",
            "\1\64",
            "\2\30\2\uffff\12\30\7\uffff\32\30\4\uffff\1\30\1\uffff\32\30",
            "\1\66",
            "\1\67",
            "\2\30\2\uffff\12\30\7\uffff\32\30\4\uffff\1\30\1\uffff\32\30",
            "\1\71",
            "",
            "\1\72",
            "\2\30\2\uffff\12\30\7\uffff\32\30\4\uffff\1\30\1\uffff\32\30",
            "",
            "\2\30\2\uffff\12\30\7\uffff\32\30\4\uffff\1\30\1\uffff\32\30",
            "\1\75",
            "",
            "",
            "\2\30\2\uffff\12\30\7\uffff\32\30\4\uffff\1\30\1\uffff\32\30",
            ""
    };

    static final short[] DFA13_eot = DFA.unpackEncodedString(DFA13_eotS);
    static final short[] DFA13_eof = DFA.unpackEncodedString(DFA13_eofS);
    static final char[] DFA13_min = DFA.unpackEncodedStringToUnsignedChars(DFA13_minS);
    static final char[] DFA13_max = DFA.unpackEncodedStringToUnsignedChars(DFA13_maxS);
    static final short[] DFA13_accept = DFA.unpackEncodedString(DFA13_acceptS);
    static final short[] DFA13_special = DFA.unpackEncodedString(DFA13_specialS);
    static final short[][] DFA13_transition;

    static {
        int numStates = DFA13_transitionS.length;
        DFA13_transition = new short[numStates][];
        for (int i=0; i<numStates; i++) {
            DFA13_transition[i] = DFA.unpackEncodedString(DFA13_transitionS[i]);
        }
    }

    class DFA13 extends DFA {

        public DFA13(BaseRecognizer recognizer) {
            this.recognizer = recognizer;
            this.decisionNumber = 13;
            this.eot = DFA13_eot;
            this.eof = DFA13_eof;
            this.min = DFA13_min;
            this.max = DFA13_max;
            this.accept = DFA13_accept;
            this.special = DFA13_special;
            this.transition = DFA13_transition;
        }
        public String getDescription() {
            return "1:1: Tokens : ( T__13 | T__14 | T__15 | T__16 | T__17 | T__18 | T__19 | T__20 | T__21 | T__22 | T__23 | T__24 | T__25 | T__26 | T__27 | RULE_LONG_ID | RULE_LONG_UNSIGNED | RULE_ID | RULE_INT | RULE_STRING | RULE_ML_COMMENT | RULE_SL_COMMENT | RULE_WS | RULE_ANY_OTHER );";
        }
        public int specialStateTransition(int s, IntStream _input) throws NoViableAltException {
            IntStream input = _input;
        	int _s = s;
            switch ( s ) {
                    case 0 : 
                        int LA13_18 = input.LA(1);

                        s = -1;
                        if ( ((LA13_18>='\u0000' && LA13_18<='\uFFFF')) ) {s = 42;}

                        else s = 22;

                        if ( s>=0 ) return s;
                        break;
                    case 1 : 
                        int LA13_0 = input.LA(1);

                        s = -1;
                        if ( (LA13_0=='t') ) {s = 1;}

                        else if ( (LA13_0=='f') ) {s = 2;}

                        else if ( (LA13_0=='u') ) {s = 3;}

                        else if ( (LA13_0=='i') ) {s = 4;}

                        else if ( (LA13_0=='@') ) {s = 5;}

                        else if ( (LA13_0=='{') ) {s = 6;}

                        else if ( (LA13_0=='=') ) {s = 7;}

                        else if ( (LA13_0=='}') ) {s = 8;}

                        else if ( (LA13_0==':') ) {s = 9;}

                        else if ( (LA13_0=='[') ) {s = 10;}

                        else if ( (LA13_0==']') ) {s = 11;}

                        else if ( (LA13_0=='.') ) {s = 12;}

                        else if ( (LA13_0=='-') ) {s = 13;}

                        else if ( (LA13_0=='N') ) {s = 14;}

                        else if ( ((LA13_0>='0' && LA13_0<='9')) ) {s = 15;}

                        else if ( (LA13_0=='^') ) {s = 16;}

                        else if ( (LA13_0==','||(LA13_0>='A' && LA13_0<='M')||(LA13_0>='O' && LA13_0<='Z')||LA13_0=='_'||(LA13_0>='a' && LA13_0<='e')||(LA13_0>='g' && LA13_0<='h')||(LA13_0>='j' && LA13_0<='s')||(LA13_0>='v' && LA13_0<='z')) ) {s = 17;}

                        else if ( (LA13_0=='\"') ) {s = 18;}

                        else if ( (LA13_0=='\'') ) {s = 19;}

                        else if ( (LA13_0=='/') ) {s = 20;}

                        else if ( ((LA13_0>='\t' && LA13_0<='\n')||LA13_0=='\r'||LA13_0==' ') ) {s = 21;}

                        else if ( ((LA13_0>='\u0000' && LA13_0<='\b')||(LA13_0>='\u000B' && LA13_0<='\f')||(LA13_0>='\u000E' && LA13_0<='\u001F')||LA13_0=='!'||(LA13_0>='#' && LA13_0<='&')||(LA13_0>='(' && LA13_0<='+')||(LA13_0>=';' && LA13_0<='<')||(LA13_0>='>' && LA13_0<='?')||LA13_0=='\\'||LA13_0=='`'||LA13_0=='|'||(LA13_0>='~' && LA13_0<='\uFFFF')) ) {s = 22;}

                        if ( s>=0 ) return s;
                        break;
                    case 2 : 
                        int LA13_19 = input.LA(1);

                        s = -1;
                        if ( ((LA13_19>='\u0000' && LA13_19<='\uFFFF')) ) {s = 42;}

                        else s = 22;

                        if ( s>=0 ) return s;
                        break;
            }
            NoViableAltException nvae =
                new NoViableAltException(getDescription(), 13, _s, input);
            error(nvae);
            throw nvae;
        }
    }
 

}