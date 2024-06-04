cmake_minimum_required(VERSION 3.16)
project(r-sql)

set(CMAKE_CXX_STANDARD 20)
find_package(Threads REQUIRED)



add_executable(r-sql main.cpp)


target_link_libraries(r-sql
        pthread readline 
)