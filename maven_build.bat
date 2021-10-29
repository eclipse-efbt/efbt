cd cocalimo\core\model
call mvn install
cd ..\..\..
cd cocalimo\smcubes\model
call mvn install
cd ..\..\..
 cd cocalimo\smcubes\modelquery
call mvn install
cd ..\..\..
cd cocalimo\smcubes\ui_sirius
call mvn install
cd ..\..\..
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
cd controller\platformcall\ui_sirius
call mvn install
cd ..\..\..
cd installations\complete_repo\
call mvn install
cd ..\..