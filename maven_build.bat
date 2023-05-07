cd ecore4reg\eclipse_model
call mvn install
cd ..\.. 
cd ecore4reg\eclipse_examples
call mvn install
cd ..\..  
cd ecore4reg\dsl\org.eclipse.efbt.ecore4reg.dsl.parent
call mvn install
cd ..\..\..  
cd installations\complete_repo\
call mvn install
cd ..\..