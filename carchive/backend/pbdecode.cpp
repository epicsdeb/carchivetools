#define PY_SSIZE_T_CLEAN

#include <Python.h>
#include <numpy/arrayobject.h>

#include "EPICSEvent.pb.h"

namespace {

/* quick scoped pointer for PyObject and friends */
template<class C>
class Ref {
    C *obj;
public:
    Ref(C *P) : obj(P) {}
    ~Ref() {Py_XDECREF((PyObject*)obj);}
    inline C& operator*() const { return *obj; }
    inline C* operator->() const { return obj; }
    inline C* release() { C* ret=obj; obj=NULL; return ret; }
    inline C* get() const { return obj; }
    inline PyObject* releasepy() { C* ret=obj; obj=NULL; return (PyObject*)ret; }
    inline PyObject* py() const { return (PyObject*)obj; }
};

}

static
Py_ssize_t unescape_plan(const char *in, Py_ssize_t inlen)
{
    Py_ssize_t outlen = inlen;

    while(inlen-- && outlen>=0) {
        if(*in++ == 0x1b) {
            // skip next
            in++;
            inlen--;
            // remove one from output
            outlen--;
        }
    }

    if(outlen<0) {
        PyErr_Format(PyExc_ValueError, "Invalid escaping");
        return -1;
    }

    return outlen;
}

static
int unescape(const char *in, Py_ssize_t inlen, char *out, Py_ssize_t outlen)
{
    char *initout = out;
    int escape = 0;

    for(; inlen; inlen--, in++) {
        char I = *in;

        if(escape) {
            escape = 0;
            switch(I) {
            case 1: *out++ = 0x1b; break;
            case 2: *out++ = '\n'; break;
            case 3: *out++ = '\r'; break;
            default:               return 1;
            }
        } else if(I==0x1b){
            escape = 1;
        } else {
            *out++ = I;
        }
    }
    if(initout+outlen!=out)
        return 2;
    return 0;
}

PyObject* PBD_unescape(PyObject *unused, PyObject *args)
{
    const char *inbuf;
    Py_ssize_t inbuflen;

    if(!PyArg_ParseTuple(args, "s#", &inbuf, &inbuflen))
        return NULL;

    Py_ssize_t outbuflen = unescape_plan(inbuf, inbuflen);
    if(outbuflen<0)
        return NULL;

    Ref<PyObject> ret(PyString_FromStringAndSize(NULL, outbuflen));
    if(!ret.get())
        return NULL;

    char *outbuf = PyString_AS_STRING(ret.get());

    int err;
    Py_BEGIN_ALLOW_THREADS {

        err = unescape(inbuf, inbuflen, outbuf, outbuflen);

    } Py_END_ALLOW_THREADS

    if(err)
        return PyErr_Format(PyExc_ValueError, "Invalid escape sequence in input (%d)", err);

    return ret.releasepy();
}

namespace {

static
PyObject* PBD_decode_double(PyObject *unused, PyObject *args)
{
    PyObject *lines;

    // PyString_Type

    if(!PyArg_ParseTuple(args, "O!",
                         &PyList_Type, &lines
                         ))
        return NULL;

    Py_ssize_t nlines = PyList_Size(lines);
    if(nlines<0)
        return NULL;

    for(Py_ssize_t i=0; i<nlines; i++) {
        PyObject *line = PyList_GET_ITEM(lines, i);

        if(!PyString_Check(line) || PyString_GET_SIZE(line)<0) {
            return PyErr_Format(PyExc_TypeError, "Input list item is not string");
        }
    }

    Ref<PyObject> vals(PyList_New(nlines)),
                  metas(PyList_New(nlines));

    if(!vals.get() || !metas.get())
        return NULL;

    EPICS::ScalarDouble decoder;

    for(Py_ssize_t i=0; i<nlines; i++) {
        PyObject *line = PyList_GET_ITEM(lines, i);
        const char *buf = PyString_AS_STRING(line);
        Py_ssize_t buflen = PyString_GET_SIZE(line);

        try {
            decoder.Clear();
            if(!decoder.ParseFromArray((const void*)buf, buflen))
                return PyErr_Format(PyExc_ValueError, "Decode error in element %lu", (unsigned long)i);
        } catch(...) {
            return PyErr_Format(PyExc_RuntimeError, "C++ exception");
        }

        Ref<PyObject> val = PyFloat_FromDouble(decoder.val()),
                      meta = Py_BuildValue("iiII", decoder.severity(), decoder.status(),
                                                   decoder.secondsintoyear(), decoder.nano());
        if(!val.get() || !meta.get())
            return NULL;

        PyList_SET_ITEM(vals.get(), i, val.release());
        PyList_SET_ITEM(metas.get(), i, meta.release());

    }

    return Py_BuildValue("OO", vals.release(), metas.release());
}

}

static PyMethodDef PBDMethods[] = {
    {"decode_double", PBD_decode_double, METH_VARARGS,
     "Decode protobuf stream into numpy arrays"},
    {"unescape", PBD_unescape, METH_VARARGS,
     "Unescape a byte string"},
    {NULL}
};

PyMODINIT_FUNC
initpbdecode(void)
{
    PyObject *mod;

    GOOGLE_PROTOBUF_VERIFY_VERSION;

    mod = Py_InitModule("carchive.backend.pbdecode", PBDMethods);
    if(!mod)
        return;
    import_array();
}
