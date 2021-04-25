cd cocalimo\core\model
call mvn install
cd ..\..\..
cd cocalimo\core\ui_sirius
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
cd cocalimo\smcubes_with_lineage\model
call mvn install
cd ..\..\..
cd cocalimo\smcubes_with_lineage\modelquery
call mvn install
cd ..\..\..
cd cocalimo\smcubes_with_lineage\ui_sirius
call mvn install
cd ..\..\..
cd cocalimo\dtm\model
call mvn install
cd ..\..\..
cd cocalimo\dtm\modelquery
call mvn install
cd ..\..\..
cd cocalimo\dtm\ui_sirius
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
cd controller\dtm\component
call mvn install
cd ..\..\..
cd controller\dtm\ui_sirius
call mvn install
cd ..\..\..
cd installations\complete_repo\
call mvn install
cd ..\..