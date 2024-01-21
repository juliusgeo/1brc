#define PY_SSIZE_T_CLEAN
#include "Python.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

static PyObject* update_dict(PyObject *self, PyObject *args) {
    PyObject *value_obj;
    PyObject *name_obj;
    PyObject *station_list = NULL;
    PyObject *stations_dict = PyDict_New();

    long start, end;
    char* filename;
    char *name, *value_str;
    double value;
    double min_obj, max_obj;
    if (!PyArg_ParseTuple(args, "llz", &start, &end, &filename)) {
            return NULL;
    }

    FILE *fp = fopen(filename, "r");
    fseek(fp, start, SEEK_SET);
    // Iterate over the items
    int num_bytes_read = 0;
    int num_bytes_to_read = end - start;
    size_t len = 0;
    ssize_t read;
    while (num_bytes_read < num_bytes_to_read) {
        char* row = NULL;
        read = getline(&row, &len, fp);
        if (read == -1) {
            break; // End of file or error
        }
        num_bytes_read += read;
        name = strtok(row, ";");
        value_str = strtok(NULL, ";");

        // Convert value to double
        value = strtod(value_str, NULL);

        // Convert C strings back to Python objects
        value_obj = PyFloat_FromDouble(value);

        // Check if name is in stations
        station_list = PyDict_GetItemString(stations_dict, name);
        if (station_list == NULL) {
            // Name not in stations, create a new list
            station_list = Py_BuildValue("[dddd]", value, value, 1.0, value);
            PyDict_SetItemString(stations_dict, name, station_list);
            Py_DECREF(value_obj);
            free(row);
            continue;
        }

        // Update the station list

        min_obj = PyFloat_AS_DOUBLE(PyList_GET_ITEM(station_list, 0));
        max_obj = PyFloat_AS_DOUBLE(PyList_GET_ITEM(station_list, 1));
        if (value < min_obj) {
            PyList_SetItem(station_list, 0, value_obj);
            Py_INCREF(value_obj);
        }
        else if (value > max_obj) {
            PyList_SetItem(station_list, 1, value_obj);
            Py_INCREF(value_obj);
        }
        PyList_SetItem(station_list, 2, PyFloat_FromDouble(PyFloat_AS_DOUBLE(PyList_GET_ITEM(station_list, 2)) + 1));
        PyList_SetItem(station_list, 3, PyFloat_FromDouble(PyFloat_AS_DOUBLE(PyList_GET_ITEM(station_list, 3)) + value));
        Py_DECREF(value_obj);
        free(row);
    }
    fclose(fp);
    return stations_dict;
}

static PyMethodDef brcMethods[] = {
    {"update_dict",  update_dict, METH_VARARGS,
     "Update dictionary"},
    {NULL, NULL, 0, NULL}        /* Sentinel */
};

static struct PyModuleDef brc_module = {
    PyModuleDef_HEAD_INIT,
    "brc",   /* name of module */
    NULL, /* module documentation, may be NULL */
    0,       /* size of per-interpreter state of the module,
                 or -1 if the module keeps state in global variables. */
    brcMethods,
};
PyMODINIT_FUNC
PyInit_brc(void)
{
    return PyModuleDef_Init(&brc_module);
}