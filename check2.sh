#!/bin/bash
cd rmdb

if [ -d build ];then
rm -rf build
fi

mkdir build
cd build
touch cmake_err.txt
touch make_err.txt
cmake .. > cmake_err.txt 2>&1 >/dev/null
if [ $? == 0 ]; then 
make > make_err.txt 2>&1 >/dev/null
if [ $? == 0 ]; then 
if [ -f ./bin/disk_manager_test ]; then
chmod +x ./bin/disk_manager_test
./bin/lru_replacer_test --gtest_print_time=0
else 
echo "没有编译生成评测程序，请检查代码！"
fi
else 
echo "make没有成功，请根据make提示，修改后程序:"
cat make_err.txt
fi
else 
echo "cmake出错，以下是提示信息："
cat cmake_err.txt
fi
