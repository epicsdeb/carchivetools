#define PY_SSIZE_T_CLEAN

#include <Python.h>
#include <numpy/arrayobject.h>

#include <string.h>
#include <sstream>

#include <vector>
#include <string>
#include <typeinfo>

#include "carchive/backend/EPICSEvent.pb.h"

static PyObject *decoderError;

namespace {

/* quick scoped pointer for PyObject and friends */
class PyRef {
    PyObject *obj;
public:
    PyRef(PyObject *P) : obj(P) {}
    ~PyRef() {Py_XDECREF(obj);}
    //inline PyObject& operator*() const { return *obj; }
    inline PyObject* operator->() const { return obj; }
    inline PyObject* release() { PyObject* ret=obj; obj=NULL; return ret; }
    inline PyObject* get() const { return obj; }
    inline bool isnull() const { return obj==NULL; }
    template<class C>
    inline C* as() const { return (C*)obj; }
    template<class C>
    inline C* releaseas() { C* ret=as<C>(); obj=NULL; return ret; }
};

class GIL {
    PyThreadState *_save;
public:
    GIL():_save(NULL) {unlock();}
    ~GIL() { lock(); }
    void unlock() { if(!_save) _save=PyEval_SaveThread(); }
    void lock() { if(_save) PyEval_RestoreThread(_save); _save=NULL; }
};

}

/* compute the size of the unescaped string */
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

    if(outlen<0)
        return -1;

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
        return PyErr_Format(PyExc_ValueError, "Invalid escaping");

    PyRef ret(PyString_FromStringAndSize(NULL, outbuflen));
    if(!ret.get())
        return NULL;

    char *outbuf = PyString_AS_STRING(ret.get());

    int err = unescape(inbuf, inbuflen, outbuf, outbuflen);
    if(err)
        return PyErr_Format(PyExc_ValueError, "Invalid escape sequence in input (%d)", err);

    return ret.release();
}

namespace {

/* must match definition of dbr_time in carchive.dtype */
struct meta {
    uint32_t severity;
    uint16_t status;
    uint32_t sec;
    uint32_t nano;
} __attribute__((packed));

static PyArray_Descr* dtype_meta;
static PyArray_Descr* dtype_val_str;

// map type to NPY_* type code

template<typename E> struct type2npy {};
#define TYPE2NPY(type, TYPE) template<> struct type2npy<type> { enum {code=NPY_ ## TYPE}; }
TYPE2NPY(char, BYTE);
TYPE2NPY(short, SHORT);
TYPE2NPY(int32_t, INT32);
TYPE2NPY(float, FLOAT32);
TYPE2NPY(double, FLOAT64);
#undef TYPE2NPY

// give a numpy type descriptor for the given C++ type

template<typename E> struct npytype {
    static PyArray_Descr* get() {
        return PyArray_DescrFromType((int)type2npy<E>::code);
    }
};

template<> struct npytype<std::string> {
    static PyArray_Descr* get() { Py_INCREF((PyObject*)dtype_val_str); return dtype_val_str; }
};

// value typep specific operations

template<typename E> struct store {
    // element size
    static Py_ssize_t esize() { return sizeof(E); }
    // advance iterator to next item in element array.
    static char* next(char* V) { return V+sizeof(E); }
    // copy a single value from a the appropriate protobuf container
    // xx.val() return type.
    template<class PBV>
    static void decodeval(char* V, const PBV& pb) { *(E*)V = pb; }
};
template<> struct store<std::string> {
    static Py_ssize_t esize() { return 40; }
    static char* next(char* V) { return V+40; }
    static void decodeval(char* V, const std::string& pb) {
        strncpy(V, pb.c_str(), 40);
        V[39]='\0';
    }
};
template<> struct store<char> {
    static Py_ssize_t esize() { return 1; }
    static char* next(char* V) { return V+1; }
    static void decodeval(char* V, const std::string& pb) {
        *V = pb[0];
    }
};

template<typename E, class PB, bool vect> struct vectop {
    static size_t nelem(const PB& pb) {return pb.val_size();}
    static void store(const PB& pb, char *out) {
        for(int j=0; j<pb.val_size(); j++, out+=::store<E>::esize()) {
            /*TODO: memcpy */
            ::store<E>::decodeval(out, pb.val(j));
        }
    }
};

template<typename E, class PB> struct vectop<E, PB, false> {
    static size_t nelem(const PB& pb) {return 1;}
    static void store(const PB& pb, char *out) {
        ::store<E>::decodeval(out, pb.val());
    }
};

template<typename E, class PB, bool vect>
PyObject* PBD_decode_X(PyObject *unused, PyObject *args)
{
    PyObject *lines;
    int cadismod;

    if(!PyArg_ParseTuple(args, "O!i",
                         &PyList_Type, &lines,
                         &cadismod
                         ))
        return NULL;

    Py_ssize_t nlines = PyList_Size(lines);
    if(nlines<0)
        return NULL;

    if(cadismod<0 || cadismod>1)
        return PyErr_Format(PyExc_ValueError, "CA disconnect mode not recognised: %d", cadismod);

    GIL locker;

    std::vector<PB> decoders(nlines);
    // keep track of which lines expand to more than one sample.
    std::vector<bool> extrasamp(nlines);
    Py_ssize_t nextrasamp = 0; // # of output lines
    size_t maxelements = 0;

    /* check that all elements of input list are strings */
    for(Py_ssize_t i=0; i<nlines; i++) {
        PyObject *line = PyList_GET_ITEM(lines, i);

        if(!PyString_Check(line)) {
            locker.lock();
            return PyErr_Format(PyExc_TypeError, "Input list item must be a string");
        }

        const char *buf = PyString_AS_STRING(line);
        Py_ssize_t buflen = PyString_GET_SIZE(line);
        PB& D = decoders[i];

        try {
            const char *outbuf = buf;
            std::vector<char> unE;

            Py_ssize_t outbuflen = unescape_plan(buf, buflen);

            if(outbuflen<0) {
                locker.lock();
                return PyErr_Format(PyExc_ValueError, "Invalid escaping");

            } else if(outbuflen!=buflen) {
                unE.resize(outbuflen);
                outbuf = &unE[0];
                int err = unescape(buf, buflen, &unE[0], outbuflen);
                if(err) {
                    locker.lock();
                    return PyErr_Format(PyExc_ValueError, "Invalid escape sequence in input (%d)", err);
                }
                buflen = outbuflen;
            }

            if(!D.ParseFromArray((const void*)outbuf, buflen)) {
                locker.lock();
                PyErr_SetObject(decoderError, PyByteArray_FromStringAndSize(outbuf, buflen));
                return NULL;
            }

            maxelements = std::max(maxelements, vectop<E,PB,vect>::nelem(D));

            for(int j=0; j<D.fieldvalues_size(); ++j) {
                const ::EPICS::FieldValue& FV = D.fieldvalues(j);

                if(FV.name()=="cnxregainedepsecs" ||
                   FV.name()=="cnxlostepsecs")
                {
                    extrasamp[i] = true;
                    nextrasamp++;
                    break;
                }
            }

        } catch(...) {
            locker.lock();
            return PyErr_Format(PyExc_RuntimeError, "C++ exception");
        }
    }

    // allocate output buffers

    npy_intp valdims[2], metadims;
    valdims[0] = metadims = nlines;
    valdims[1] = maxelements;

    if(cadismod==0) {
        // inject artificial samples
        valdims[0] = metadims = nlines + nextrasamp;
    }

    locker.lock();

    PyRef outval(PyArray_Zeros(2, valdims, npytype<E>::get(), 0));
    Py_INCREF(dtype_meta);
    PyRef outmeta(PyArray_Zeros(1, &metadims, dtype_meta, 0));

    if(outval.isnull() || outmeta.isnull())
        return NULL;

    locker.unlock();

    for(Py_ssize_t i=0, j=0; i<nlines; i++, j++) {
        const PB& D = decoders[i];
        meta *M = (meta*)PyArray_GETPTR1(outmeta.get(), j);
        char *val = (char*)PyArray_GETPTR2(outval.get(), j, 0);

        if(cadismod==0 && extrasamp[i]) {
            bool havets = false;
            // inject disconnect event here
/*
 *TODO: use provided time
            for(int k=0; k<D.fieldvalues_size(); ++k) {
                const ::EPICS::FieldValue& FV = D.fieldvalues(k);

                // use supplied connection loss time if possible
                // TODO, wrong time base
                if(FV.name()=="cnxlostepsecs")
                {
                    // TODO: check for backwards time...
                    std::istringstream m(FV.val());
                    unsigned int I;
                    m >> I;
                    M->sec = I;
                    M->nano = 0;
                    if(!m.bad()) {
                        havets = true;
                        break;
                    }
                }
            }
*/
            if(!havets || M->sec>D.secondsintoyear()) {
                M->sec = D.secondsintoyear();
                M->nano = D.nano();
                if(M->nano)
                    M->nano--;
                else {
                    M->sec--;
                    M->nano=999999999;
                }
            }
            M->severity = 3904;

            j++;
            M = (meta*)PyArray_GETPTR1(outmeta.get(), j);
            val = (char*)PyArray_GETPTR2(outval.get(), j, 0);
        }

        vectop<E,PB,vect>::store(D, val);

        if(cadismod==1 && extrasamp[i]) {
            M->severity = 3904; // maximize severity
        } else {
            M->severity = D.severity();
        }
        M->status = D.status();
        M->sec = D.secondsintoyear();
        M->nano = D.nano();

    }

    return Py_BuildValue("OO", outval.release(), outmeta.release());
}

}

static PyMethodDef PBDMethods[] = {
    {"unescape", PBD_unescape, METH_VARARGS,
     "Unescape a byte string"},

    {"decode_scalar_string", PBD_decode_X<std::string, EPICS::ScalarString, false>, METH_VARARGS,
     "Decode protobuf stream into numpy array of strings"},
    {"decode_scalar_byte", PBD_decode_X<char, EPICS::ScalarByte, false>, METH_VARARGS,
     "Decode protobuf stream into numpy array of int8"},
    {"decode_scalar_short", PBD_decode_X<short, EPICS::ScalarShort, false>, METH_VARARGS,
     "Decode protobuf stream into numpy array of int16"},
    {"decode_scalar_int", PBD_decode_X<int32_t, EPICS::ScalarInt, false>, METH_VARARGS,
     "Decode protobuf stream into numpy array of int32"},
    {"decode_scalar_enum", PBD_decode_X<int32_t, EPICS::ScalarEnum, false>, METH_VARARGS,
     "Decode protobuf stream into numpy array of int32"},
    {"decode_scalar_float", PBD_decode_X<float, EPICS::ScalarFloat, false>, METH_VARARGS,
     "Decode protobuf stream into numpy array of float32"},
    {"decode_scalar_double", PBD_decode_X<double, EPICS::ScalarDouble, false>, METH_VARARGS,
     "Decode protobuf stream into numpy array of float64"},

    {"decode_vector_string", PBD_decode_X<std::string, EPICS::VectorString, true>, METH_VARARGS,
     "Decode protobuf stream into numpy array"},
    {"decode_vector_short", PBD_decode_X<short, EPICS::VectorShort, true>, METH_VARARGS,
     "Decode protobuf stream into numpy array"},
    {"decode_vector_int", PBD_decode_X<int32_t, EPICS::VectorInt, true>, METH_VARARGS,
     "Decode protobuf stream into numpy array"},
    {"decode_vector_enum", PBD_decode_X<int32_t, EPICS::VectorEnum, true>, METH_VARARGS,
     "Decode protobuf stream into numpy array"},
    {"decode_vector_float", PBD_decode_X<float, EPICS::VectorFloat, true>, METH_VARARGS,
     "Decode protobuf stream into numpy array"},
    {"decode_vector_double", PBD_decode_X<double, EPICS::VectorDouble, true>, METH_VARARGS,
     "Decode protobuf stream into numpy array"},

    {NULL}
};

struct mapent {
    const char *v;
    int k;
};
static const mapent decodemap[] = {
    {"decode_scalar_string", EPICS::SCALAR_STRING},
    {"decode_scalar_byte", EPICS::SCALAR_BYTE},
    {"decode_scalar_short", EPICS::SCALAR_SHORT},
    {"decode_scalar_int", EPICS::SCALAR_INT},
    {"decode_scalar_enum", EPICS::SCALAR_ENUM},
    {"decode_scalar_float", EPICS::SCALAR_FLOAT},
    {"decode_scalar_double", EPICS::SCALAR_DOUBLE},

    {"decode_vector_string", EPICS::WAVEFORM_STRING},
    {"decode_vector_short", EPICS::WAVEFORM_SHORT},
    {"decode_vector_int", EPICS::WAVEFORM_INT},
    {"decode_vector_enum", EPICS::WAVEFORM_ENUM},
    {"decode_vector_float", EPICS::WAVEFORM_FLOAT},
    {"decode_vector_double", EPICS::WAVEFORM_DOUBLE},
    {NULL}
};

char decoderErrorName[] = "carchive.backend.pbdecode.DecodeError";

PyMODINIT_FUNC
initpbdecode(void)
{
    PyRef map(PyDict_New());
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

    // create dtype for struct meta
    PyArray_Descr* tval = PyArray_DescrNewFromType(NPY_VOID);
    assert(tval);
    tval->elsize = sizeof(meta);
    dtype_meta = tval;
    Py_INCREF(tval); // keep one extra ref for the C global variable
    PyModule_AddObject(mod, "metatype", (PyObject*)tval);

    // create dtype for char[40]
    tval = PyArray_DescrNewFromType(NPY_STRING);
    assert(tval);
    tval->elsize = 40;
    dtype_val_str = tval;
    Py_INCREF(tval);
    PyModule_AddObject(mod, "strtype", (PyObject*)tval);

    PyModule_AddObject(mod, "decoders", map.release());

    decoderError = PyErr_NewException(decoderErrorName,
                                      PyExc_ValueError, NULL);
    Py_XINCREF(decoderError);
    PyModule_AddObject(mod, "DecodeError", decoderError);
}
