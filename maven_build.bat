cd openregspecs\core\model
call mvn install
cd ..\..\..
cd openregspecs\smcubes\model
call mvn install
cd ..\..\..
cd openregspecs\testing\model
call mvn install
cd ..\..\..
cd openregspecs\testing\ui_sirius
call mvn install
cd ..\..\..
cd openregspecs\platform_call\model
call mvn install
cd ..\..\..
 cd openregspecs\smcubes\modelquery
call mvn install
cd ..\..\..
cd openregspecs\smcubes\ui_sirius
call mvn install
cd ..\..\..
cd openregspecs\examples
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