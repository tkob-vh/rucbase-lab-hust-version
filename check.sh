#!/bin/bash
cd rmdb
if [ -f build ];then
rm -rf build
fi

mkdir build
cd build
cmake .. 
make 
if [ $? == 0 ]; then 
if [ -f bin/disk_manager_test ]; then
chmod +x bin/disk_manager_teset
./bin/disk_manager_test
else 
echo "没有编译生成评测程序，请检查代码！"
fi
echo "make没有成功，请根据make提示，修改后程序"
fi

