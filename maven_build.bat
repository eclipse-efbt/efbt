cd regdna\eclipse_model
call mvn install
cd ..\..  
cd regdna\dsl\org.eclipse.efbt.regdna.dsl.parent
call mvn install
cd ..\..\..
cd regpot_desktop\components
call mvn install
cd ..\..  
cd sirius_ui
call mvn install
cd ..
cd regpot_desktop\installation
call mvn install
cd ..\..
