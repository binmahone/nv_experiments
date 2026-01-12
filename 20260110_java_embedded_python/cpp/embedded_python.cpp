/**
 * Embedded Python UDF - Generic version supporting String[] + double[]
 * Fair comparison: also does json.dumps() on dict results
 */

#define NPY_NO_DEPRECATED_API NPY_1_7_API_VERSION
#define PY_ARRAY_UNIQUE_SYMBOL embedded_python_ARRAY_API

#include <Python.h>
#include <jni.h>
#include <numpy/arrayobject.h>
#include <string>
#include <chrono>

extern "C" {
    JNIEXPORT void JNICALL Java_EmbeddedPythonUDF_initPython(JNIEnv*, jclass);
    JNIEXPORT void JNICALL Java_EmbeddedPythonUDF_finalizePython(JNIEnv*, jclass);
    JNIEXPORT jlong JNICALL Java_EmbeddedPythonUDF_benchmarkEmbeddedGenericUDF(
        JNIEnv*, jclass, jobjectArray, jdoubleArray, jstring, jint);
}

static PyObject* g_numpy_module = nullptr;
static PyObject* g_json_module = nullptr;
static PyObject* g_json_dumps = nullptr;
static bool g_initialized = false;
static PyThreadState* g_main_thread_state = nullptr;

static int init_numpy() { import_array(); return 0; }

JNIEXPORT void JNICALL Java_EmbeddedPythonUDF_initPython(JNIEnv *env, jclass cls) {
    if (g_initialized) return;
    
    const char* pythonHome = getenv("PYTHONHOME");
    if (pythonHome) {
        static wchar_t pythonHomeWide[1024];
        mbstowcs(pythonHomeWide, pythonHome, 1024);
        Py_SetPythonHome(pythonHomeWide);
    }
    
    Py_Initialize();
    PyEval_InitThreads();
    
    if (init_numpy() < 0) {
        PyErr_Print();
        return;
    }
    
    g_numpy_module = PyImport_ImportModule("numpy");
    g_json_module = PyImport_ImportModule("json");
    if (g_json_module) {
        g_json_dumps = PyObject_GetAttrString(g_json_module, "dumps");
    }
    
    // Import commonly used modules
    PyRun_SimpleString("import re\nimport hashlib\nimport numpy as np\nimport json");
    
    g_main_thread_state = PyEval_SaveThread();
    g_initialized = true;
}

JNIEXPORT void JNICALL Java_EmbeddedPythonUDF_finalizePython(JNIEnv *env, jclass cls) {
    if (!g_initialized) return;
    PyEval_RestoreThread(g_main_thread_state);
    Py_XDECREF(g_json_dumps);
    Py_XDECREF(g_json_module);
    Py_XDECREF(g_numpy_module);
    Py_Finalize();
    g_initialized = false;
}

class GILAcquire {
public:
    GILAcquire() { gstate = PyGILState_Ensure(); }
    ~GILAcquire() { PyGILState_Release(gstate); }
private:
    PyGILState_STATE gstate;
};

/**
 * Serialize result to JSON strings (for fair comparison with cross-process)
 * If result is a list of dicts, call json.dumps() on each element
 */
static void serializeResultToJson(PyObject* result, PyObject* jsonDumps) {
    if (!result || !PyList_Check(result) || !jsonDumps) return;
    
    Py_ssize_t len = PyList_Size(result);
    for (Py_ssize_t i = 0; i < len; i++) {
        PyObject* item = PyList_GetItem(result, i);  // borrowed ref
        
        // If item is a dict or list, serialize to JSON
        if (PyDict_Check(item) || PyList_Check(item)) {
            PyObject* args = PyTuple_Pack(1, item);
            PyObject* jsonStr = PyObject_CallObject(jsonDumps, args);
            Py_DECREF(args);
            
            if (jsonStr) {
                // We got the JSON string, now we'd return it to JVM
                // For benchmarking, just decref it
                Py_DECREF(jsonStr);
            }
        }
        // For simple types (str, bool, float), no need to json.dumps
    }
}

/**
 * Generic UDF: accepts String[] + double[], returns results
 */
JNIEXPORT jlong JNICALL Java_EmbeddedPythonUDF_benchmarkEmbeddedGenericUDF(
    JNIEnv *env, jclass cls, 
    jobjectArray strings, 
    jdoubleArray numbers,
    jstring udfCode, 
    jint iterations) {
    
    if (!g_initialized) return -1;
    
    jsize strLen = env->GetArrayLength(strings);
    jsize numLen = env->GetArrayLength(numbers);
    jdouble* numData = env->GetDoubleArrayElements(numbers, nullptr);
    const char* udfCodeStr = env->GetStringUTFChars(udfCode, nullptr);
    
    long duration_ns = 0;
    {
        GILAcquire gil;
        
        PyObject* globalDict = PyModule_GetDict(PyImport_AddModule("__main__"));
        // Execute UDF code which defines a function named 'udf'
        std::string code = udfCodeStr;
        if (PyRun_String(code.c_str(), Py_file_input, globalDict, globalDict) == nullptr) {
            PyErr_Print();
            env->ReleaseDoubleArrayElements(numbers, numData, JNI_ABORT);
            env->ReleaseStringUTFChars(udfCode, udfCodeStr);
            return -1;
        }
        PyObject* udfFunc = PyDict_GetItemString(globalDict, "udf");
        
        if (!udfFunc) {
            PyErr_Print();
            env->ReleaseDoubleArrayElements(numbers, numData, JNI_ABORT);
            env->ReleaseStringUTFChars(udfCode, udfCodeStr);
            return -1;
        }
        
        auto start = std::chrono::high_resolution_clock::now();
        
        for (int iter = 0; iter < iterations; iter++) {
            // Create Python list for strings
            PyObject* strList = PyList_New(strLen);
            for (jsize i = 0; i < strLen; i++) {
                jstring jstr = (jstring)env->GetObjectArrayElement(strings, i);
                const char* cstr = env->GetStringUTFChars(jstr, nullptr);
                PyList_SetItem(strList, i, PyUnicode_FromString(cstr));
                env->ReleaseStringUTFChars(jstr, cstr);
            }
            
            // Create numpy array for numbers (zero-copy)
            npy_intp dims[1] = {numLen};
            PyObject* numArray = PyArray_SimpleNewFromData(1, dims, NPY_DOUBLE, numData);
            
            // Call UDF(texts, nums)
            PyObject* args = PyTuple_Pack(2, strList, numArray);
            PyObject* result = PyObject_CallObject(udfFunc, args);
            
            // Fair comparison: serialize dict results to JSON (same as cross-process)
            if (result) {
                serializeResultToJson(result, g_json_dumps);
            }
            
            Py_XDECREF(result);
            Py_DECREF(args);
            Py_DECREF(numArray);
            Py_DECREF(strList);
        }
        
        auto end = std::chrono::high_resolution_clock::now();
        duration_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    }
    
    env->ReleaseDoubleArrayElements(numbers, numData, JNI_ABORT);
    env->ReleaseStringUTFChars(udfCode, udfCodeStr);
    return duration_ns;
}
