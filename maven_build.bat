cd openregspecs\model
call mvn install
cd ..\..
cd openregspecs\query
call mvn install
cd ..\..
cd openregspecs\sirius_ui
call mvn install
cd ..\..
cd cocalimo\examples
call mvn install 
cd ..\..                  
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
cd openregspecs\platform_call\ui_sirius
call mvn install
cd ..\..\..
cd installations\complete_repo\
call mvn install
cd ..\..