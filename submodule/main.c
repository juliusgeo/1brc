#define PY_SSIZE_T_CLEAN
#include "Python.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

static PyObject* update_dict(PyObject *self, PyObject *args) {
    PyObject *iterator, *iterable, *row, *stations_dict, *value_obj, *name_obj;
    PyObject *station_list = NULL, *min_obj = NULL, *max_obj = NULL;
    PyObject *row_obj;
    if (!PyArg_ParseTuple(args, "OO", &stations_dict, &iterable)) {
            return NULL;
    }
    iterator = PyObject_GetIter(iterable);
    if (iterator == NULL) {
        return NULL; // Not an iterable
    }

    // Iterate over the items
    while ((row_obj = PyIter_Next(iterator)) != NULL) {
        char *name, *value_str;
        double value;
        int is_new_station = 0;
        row = PyBytes_AsString(row_obj);
        if (row == NULL) {
            return NULL;
        }
        name = strtok(row, ";");
        value_str = strtok(NULL, ";");
        if (name == NULL || value_str == NULL) {
            PyErr_SetString(PyExc_ValueError, "Row must contain two parts separated by ';'");
            return NULL;
        }

        // Convert value to float
        value = strtod(value_str, NULL);

        // Convert C strings back to Python objects
        name_obj = PyBytes_FromString(name);
        value_obj = PyFloat_FromDouble(value);
        if (name_obj == NULL || value_obj == NULL) {
            Py_XDECREF(name_obj);
            Py_XDECREF(value_obj);
            return NULL;
        }

        // Check if name is in stations
        station_list = PyDict_GetItem(stations_dict, name_obj);
        if (station_list == NULL) {
            // Name not in stations, create a new list
            is_new_station = 1;
            station_list = Py_BuildValue("[dddd]", value, value, 0.0, 0.0);
            if (station_list == NULL) {
                return NULL;
            }
        } else {
            Py_INCREF(station_list); // Increment ref count
        }

        // Update the station list
        min_obj = PyList_GetItem(station_list, 0);
        max_obj = PyList_GetItem(station_list, 1);
        if (PyObject_RichCompareBool(value_obj, min_obj, Py_LT)) {
            PyList_SetItem(station_list, 0, value_obj);
            Py_INCREF(value_obj);
        }
        if (PyObject_RichCompareBool(value_obj, max_obj, Py_GT)) {
            PyList_SetItem(station_list, 1, value_obj);
            Py_INCREF(value_obj);
        }
        PyList_SetItem(station_list, 2, PyFloat_FromDouble(PyFloat_AsDouble(PyList_GetItem(station_list, 2)) + 1));
        PyList_SetItem(station_list, 3, PyFloat_FromDouble(PyFloat_AsDouble(PyList_GetItem(station_list, 3)) + value));

        // Update the dictionary
        if (is_new_station) {
            PyDict_SetItem(stations_dict, name_obj, station_list);
        }
        Py_DECREF(name_obj);
        Py_DECREF(value_obj);
        Py_DECREF(row_obj);
    }

    // Check for errors during iteration
    if (PyErr_Occurred()) {
        Py_DECREF(iterator);
        return NULL;
    }

    // Decrement the iterator's reference count
    Py_DECREF(iterator);

    // Decrement ref counts
    Py_DECREF(station_list);

    Py_RETURN_NONE;
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