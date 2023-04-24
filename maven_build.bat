cd ecore4reg\java_model
call mvn install
cd ..\.. 
cd ecore4reg\java_examples
call mvn install
cd ..\..  
cd ecore4reg\dsl\org.eclipse.efbt.ecore4reg.dsl.parent
call mvn install
cd ..\..\..  
cd ecore4reg\sirius_ui
call mvn install
cd ..\..
cd installations\complete_repo\
call mvn install
cd ..\..