cmake_minimum_required(VERSION 3.10)
project(EmbeddedPythonUDF)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find Python
find_package(Python3 REQUIRED COMPONENTS Interpreter Development NumPy)

# Find JNI
find_package(JNI REQUIRED)

# Print found paths for debugging
message(STATUS "Python3_INCLUDE_DIRS: ${Python3_INCLUDE_DIRS}")
message(STATUS "Python3_LIBRARIES: ${Python3_LIBRARIES}")
message(STATUS "Python3_NumPy_INCLUDE_DIRS: ${Python3_NumPy_INCLUDE_DIRS}")
message(STATUS "JNI_INCLUDE_DIRS: ${JNI_INCLUDE_DIRS}")

# Create shared library
add_library(embedded_python_udf SHARED embedded_python.cpp)

target_include_directories(embedded_python_udf PRIVATE
    ${Python3_INCLUDE_DIRS}
    ${Python3_NumPy_INCLUDE_DIRS}
    ${JNI_INCLUDE_DIRS}
)

target_link_libraries(embedded_python_udf PRIVATE
    ${Python3_LIBRARIES}
)

# Set output directory
set_target_properties(embedded_python_udf PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/../lib
)
