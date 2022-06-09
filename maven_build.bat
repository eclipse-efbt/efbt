cd openregspecs\java_model
call mvn install
cd ..\..
cd openregspecs\java_query
call mvn install
cd ..\.. 
cd openregspecs\java_examples
call mvn install
cd ..\..  
cd openregspecs\dsl\org.eclipse.efbt.openregspecs.dsl.parent
call mvn install
cd ..\..\..  
cd controller\ldm
call mvn install
cd ..\..
cd controller\smcubes\access_dependencies_external
call mvn install
cd ..\..\..
cd controller\smcubes\access_dependencies_plugin
call mvn install
cd ..\..\..
cd controller\smcubes\component
call mvn install
cd ..\..\..
cd controller\platformcall\component
call mvn install
cd ..\..\..
cd openregspecs\sirius_ui
call mvn install
cd ..\..
cd installations\complete_repo\
call mvn install
cd ..\..