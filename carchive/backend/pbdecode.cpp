#define PY_SSIZE_T_CLEAN

#include <Python.h>
#include <numpy/arrayobject.h>

#include <string.h>

#include <vector>
#include <string>
#include <typeinfo>

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

/* must match definition of dbr_time in carchive.dtype */
struct meta {
    uint32_t severity;
    uint16_t status;
    uint32_t sec;
    uint32_t nano;
} __attribute__((packed));

template<typename E> struct store {
    static Py_ssize_t esize() { return sizeof(E); }
    static char* next(char* V) { return V+sizeof(E); }
};
template<> struct store<std::string> {
    static Py_ssize_t esize() { return 40; }
    static char* next(char* V) { return V+40; }
};

template<typename E> struct frompb {
    template<class PB>
    static void decodeval(char* V, PB& pb) { *(E*)V = pb.val(); }
};
template<> struct frompb<std::string> {
    static void decodeval(char* V, EPICS::ScalarString &pb) {
        strncpy(V, pb.val().c_str(), 40);
        V[39]='\0';
    }
};
template<> struct frompb<char> {
    static void decodeval(char* V, EPICS::ScalarByte &pb) {
        *V = pb.val()[0];
    }
};

template<typename E, class PB>
PyObject* PBD_decode_scalar(PyObject *unused, PyObject *args)
{
    PyObject *lines;
    PyArrayObject *valarr, *metaarr;

    if(!PyArg_ParseTuple(args, "O!O!O!",
                         &PyList_Type, &lines,
                         &PyArray_Type, &valarr,
                         &PyArray_Type, &metaarr
                         ))
        return NULL;

    Py_ssize_t nlines = PyList_Size(lines);
    if(nlines<0)
        return NULL;

    if(!PyArray_ISCARRAY(valarr) || !PyArray_ISCARRAY(metaarr))
        return PyErr_Format(PyExc_ValueError, "output arrays must be C-contingious and writable");

    if(nlines!=PyArray_SIZE(valarr) || nlines!=PyArray_SIZE(metaarr))
        return PyErr_Format(PyExc_ValueError, "output arrays must have the same length of input lines");

    if(store<E>::esize()!=PyArray_ITEMSIZE(valarr) ||
            sizeof(meta)!=PyArray_ITEMSIZE(metaarr))
        return PyErr_Format(PyExc_ValueError, "output item sizes must be consistent with type %s. %lu %lu %lu %lu",
                            typeid(E).name(),
                            (unsigned long)store<E>::esize(),
                            (unsigned long)PyArray_ITEMSIZE(valarr),
                            (unsigned long)sizeof(meta),
                            (unsigned long)PyArray_ITEMSIZE(metaarr)
                            );

    /* check that all elements of input list are strings */
    for(Py_ssize_t i=0; i<nlines; i++) {
        PyObject *line = PyList_GET_ITEM(lines, i);

        if(!PyString_Check(line) || PyString_GET_SIZE(line)<0) {
            return PyErr_Format(PyExc_TypeError, "Input list item is not string");
        }
    }

    char *curval=PyArray_BYTES(valarr);
    meta* curmeta = (meta*)PyArray_BYTES(metaarr);

    PB decoder;

    for(Py_ssize_t i=0; i<nlines; i++) {
        PyObject *line = PyList_GET_ITEM(lines, i);
        const char *buf = PyString_AS_STRING(line);
        Py_ssize_t buflen = PyString_GET_SIZE(line);

        try {
            decoder.Clear();
            if(!decoder.ParseFromArray((const void*)buf, buflen))
                return PyErr_Format(PyExc_ValueError, "Decode error in element %lu", (unsigned long)i);

            frompb<E>::decodeval(curval, decoder);
            curmeta->severity = decoder.severity();
            curmeta->status = decoder.status();
            curmeta->sec = decoder.secondsintoyear();
            curmeta->nano = decoder.nano();

        } catch(...) {
            return PyErr_Format(PyExc_RuntimeError, "C++ exception");
        }

        curmeta += 1;
        curval = store<E>::next(curval);
    }

    Py_RETURN_NONE;
}

}

static PyMethodDef PBDMethods[] = {
    {"unescape", PBD_unescape, METH_VARARGS,
     "Unescape a byte string"},

    {"decode_string", PBD_decode_scalar<std::string, EPICS::ScalarString>, METH_VARARGS,
     "Decode protobuf stream into numpy array of strings"},
    {"decode_byte", PBD_decode_scalar<char, EPICS::ScalarByte>, METH_VARARGS,
     "Decode protobuf stream into numpy array of int8"},
    {"decode_short", PBD_decode_scalar<short, EPICS::ScalarShort>, METH_VARARGS,
     "Decode protobuf stream into numpy array of int16"},
    {"decode_int", PBD_decode_scalar<int32_t, EPICS::ScalarInt>, METH_VARARGS,
     "Decode protobuf stream into numpy array of int32"},
    {"decode_enum", PBD_decode_scalar<int32_t, EPICS::ScalarEnum>, METH_VARARGS,
     "Decode protobuf stream into numpy array of int32"},
    {"decode_float", PBD_decode_scalar<float, EPICS::ScalarFloat>, METH_VARARGS,
     "Decode protobuf stream into numpy array of float32"},
    {"decode_double", PBD_decode_scalar<double, EPICS::ScalarDouble>, METH_VARARGS,
     "Decode protobuf stream into numpy array of float64"},

    {NULL}
};

struct mapent {
    const char *v;
    int k;
};
static const mapent decodemap[] = {
    {"decode_string", EPICS::SCALAR_STRING},
    {"decode_byte", EPICS::SCALAR_BYTE},
    {"decode_short", EPICS::SCALAR_SHORT},
    {"decode_int", EPICS::SCALAR_INT},
    {"decode_enum", EPICS::SCALAR_ENUM},
    {"decode_float", EPICS::SCALAR_FLOAT},
    {"decode_double", EPICS::SCALAR_DOUBLE},
    {NULL}
};

PyMODINIT_FUNC
initpbdecode(void)
{
    Ref<PyObject> map(PyDict_New());
    PyObject *mod;

    if(!map.get())
        return;

    GOOGLE_PROTOBUF_VERIFY_VERSION;

    mod = Py_InitModule("carchive.backend.pbdecode", PBDMethods);
    if(!mod)
        return;
    import_array();

    /* build a dictionary mapping PayloadType to decoder function */
    for(const mapent *pcur = decodemap; pcur->v; pcur++) {
        PyObject *meth = PyObject_GetAttrString(mod, pcur->v);
        if(!meth)
            break;
        PyObject *pint = PyInt_FromLong(pcur->k);
        if(!pint)
            break;
        if(PyDict_SetItem(map.get(), pint, meth)==-1) {
            Py_DECREF(pint);
            break;
        }
    }

    PyModule_AddObject(mod, "decoders", map.release());
}
