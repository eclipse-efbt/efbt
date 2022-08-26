cd openregspecs\java_model
call mvn install
cd ..\.. 
cd openregspecs\java_examples
call mvn install
cd ..\..  
cd openregspecs\dsl\org.eclipse.efbt.openregspecs.dsl.parent
call mvn install
cd ..\..\..  
cd openregspecs\sirius_ui
call mvn install
cd ..\..
cd installations\complete_repo\
call mvn install
cd ..\..