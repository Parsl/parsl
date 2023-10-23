#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <iostream>
#include <string>

#include <cassert>

#include "common.h"

#include "chronolog_client.h"

static PyObject *
chronolog_start(PyObject *self, PyObject *args)
{
    // const char *command;
    int sts = 7;

    // if (!PyArg_ParseTuple(args, "s", &command))
    //    return NULL;

    //    sts = system(command);

    std::cout << "hello\n";

    std::string conf_file_path="default.json";

    ChronoLog::ConfigurationManager confManager(conf_file_path);
    chronolog::Client *client = new chronolog::Client(confManager);
   
    int ret = client->Connect();

    assert(ret == CL_SUCCESS);

    std::unordered_map<std::string, std::string> chronicle_attrs;
    chronicle_attrs.emplace("Priority", "High");
    int flags = 1;

    std::string chronicle_name = "parslmon";
    // TOOD: "parslmon" chronicle name should be more dynamic
    ret = client->CreateChronicle(chronicle_name, chronicle_attrs, flags);
     
    // assert(ret == CL_SUCCESS);

    std::unordered_map<std::string, std::string> story_attrs;

     // TODO this is a workaround for acuiqrestory uniqueness bug
    std::string story_name = gen_random(32);
    flags = 2;
    std::pair<int, chronolog::StoryHandle*> acquire_ret = client->AcquireStory(chronicle_name, story_name, story_attrs, flags);

    chronolog::StoryHandle* story = acquire_ret.second;
   
    story->log_event("starting chronopy");


    client->ReleaseStory(chronicle_name, story_name);

    client->Disconnect();
 
    return PyLong_FromLong(sts);
}

static PyMethodDef SpamMethods[] = {
    {"start",  chronolog_start, METH_VARARGS,
     "start chronolog enough for parsl"},
    {NULL, NULL, 0, NULL}        /* Sentinel */
};

static struct PyModuleDef chronologmodule = {
    PyModuleDef_HEAD_INIT,
    "chronopy",   /* name of module */
    NULL, /* module documentation, may be NULL */
    -1,       /* size of per-interpreter state of the module,
                 or -1 if the module keeps state in global variables. */
    SpamMethods
};

PyMODINIT_FUNC
PyInit_chronopy(void)
{
    return PyModule_Create(&chronologmodule);
}

