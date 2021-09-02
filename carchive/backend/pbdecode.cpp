#define PY_SSIZE_T_CLEAN

#include <Python.h>
#include <numpy/arrayobject.h>

#include <string.h>
#include <sstream>

#include <vector>
#include <string>
#include <typeinfo>

#include "carchive/backend/EPICSEvent.pb.h"

#if PY_MAJOR_VERSION >= 3
#  define PyInt_FromLong PyLong_FromLong
#endif

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
    void reset(PyObject* n) {
        Py_XDECREF(obj);
        Py_XINCREF(n);
        obj = n;
    }
};

class GIL {
    PyThreadState *_save;
public:
    GIL():_save(NULL) {unlock();}
    ~GIL() { lock(); }
    void unlock() { if(!_save) _save=PyEval_SaveThread(); }
    void lock() { if(_save) PyEval_RestoreThread(_save); _save=NULL; }
};

static
void LogBadSample(const char *filename, int line,
                   const char* buf, size_t buflen);

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

static
PyObject* PBD_unescape(PyObject *unused, PyObject *args)
{
    const char *inbuf;
    Py_ssize_t inbuflen;

    if(!PyArg_ParseTuple(args, "s#", &inbuf, &inbuflen))
        return NULL;

    Py_ssize_t outbuflen = unescape_plan(inbuf, inbuflen);
    if(outbuflen<0)
        return PyErr_Format(PyExc_ValueError, "Invalid escaping");

    PyRef ret(PyBytes_FromStringAndSize(NULL, outbuflen));
    if(!ret.get())
        return NULL;

    char *outbuf = PyBytes_AS_STRING(ret.get());

    int err = unescape(inbuf, inbuflen, outbuf, outbuflen);
    if(err)
        return PyErr_Format(PyExc_ValueError, "Invalid escape sequence in input (%d)", err);

    return ret.release();
}

/* compute the size of the escaped string */
static
Py_ssize_t escape_plan(const char *in, Py_ssize_t inlen)
{
    Py_ssize_t outlen = inlen;

    while(inlen--) {
        switch(*in++) {
        case '\x1b':
        case '\n':
        case '\r':
            outlen++;
        default:
            break;
        }
    }
    return outlen;
}

static
void escape(const char *in, Py_ssize_t inlen, char *out, Py_ssize_t outlen)
{
    const char *lastin = in+inlen;
    char *lastout = out + outlen;
    while(in<lastin && out<lastout) {
        char I = *in++;
        switch(I) {
        case '\x1b':
        case '\n':
        case '\r':
            *out++ = '\x1b';
            break;
        default:
            *out++ = I;
            continue;
        }
        switch(I) {
        case '\x1b': *out++ = 1; break;
        case '\n': *out++ = 2; break;
        case '\r': *out++ = 3; break;
        default:
            break;
        }
    }
    assert(in==lastin && out==lastout);
}

static
PyObject* PBD_escape(PyObject *unused, PyObject *args)
{
    const char *inbuf;
    Py_ssize_t inbuflen;

    if(!PyArg_ParseTuple(args, "s#", &inbuf, &inbuflen))
        return NULL;

    Py_ssize_t outbuflen = escape_plan(inbuf, inbuflen);

    PyRef ret(PyBytes_FromStringAndSize(NULL, outbuflen));
    if(!ret.get())
        return NULL;

    char *outbuf = PyBytes_AS_STRING(ret.get());

    escape(inbuf, inbuflen, outbuf, outbuflen);

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
    unsigned long sectoyear = 0;

    if(!PyArg_ParseTuple(args, "O!i|k",
                         &PyList_Type, &lines,
                         &cadismod,
                         &sectoyear
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

        if(!PyBytes_Check(line)) {
            locker.lock();
            return PyErr_Format(PyExc_TypeError, "Input list item must be a string");
        }

        const char *buf = PyBytes_AS_STRING(line);
        Py_ssize_t buflen = PyBytes_GET_SIZE(line);
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
                // Mark invalid sample
                LogBadSample(__FILE__, __LINE__, outbuf, buflen);
                D.Clear();
                D.set_severity(103);
            }

            maxelements = std::max(maxelements, vectop<E,PB,vect>::nelem(D));

            for(int j=0; j<D.fieldvalues_size(); ++j) {
                const ::EPICS::FieldValue& FV = D.fieldvalues(j);

                if(FV.name()=="cnxlostepsecs")
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
                    M->sec = I - sectoyear;
                    M->nano = 0;
                    if(m.bad())
                        break;

                    unsigned int prevS=0, nextS = D.secondsintoyear(),
                                 prevNS=0, nextNS = D.nano();
                    if(j>0) {
                        meta *prevM = (meta*)PyArray_GETPTR1(outmeta.get(), j-1);
                        prevS = prevM->sec;
                        prevNS= prevM->nano;
                    }

                    // try to preserve monotonic time for the disconnect event
                    if(prevS==nextS) {
                        M->sec = prevS;
                        M->nano= (nextNS+prevNS)/2;
                    } else if(prevS==M->sec) {
                        if(prevNS>=999999999) {
                            M->sec = prevS+1;
                            M->nano= 0;
                        } else {
                            M->sec = prevS;
                            M->nano= prevNS+1;
                        }
                    } else if(nextS==M->sec) {
                        if(nextNS==0) {
                            M->sec = nextS-1;
                            M->nano= 999999999;
                        } else {
                            M->sec = nextS;
                            M->nano= nextNS-1;
                        }
                    }

                    havets = true;
                    break;
                }
            }

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

            // increment for the real sample
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

    locker.lock();
    return Py_BuildValue("OO", outval.release(), outmeta.release());
}

static
PyObject *splitter(PyObject *unused, PyObject *args)
{
    PyObject *lines;
    if(!PyArg_ParseTuple(args, "O!", &PyList_Type, &lines))
        return NULL;

    const Py_ssize_t numin = PyList_Size(lines);
    if(numin<0)
        return NULL;

    PyRef groups(PyList_New(0)),
          current(PyList_New(0));
    if(groups.isnull() || current.isnull())
        return NULL;

    for(Py_ssize_t i=0; i<numin; i++) {
        PyObject *cur = PyList_GET_ITEM(lines, i);
        Py_ssize_t esize = PyObject_Length(cur);
        if(esize<=0) {
            // start a new sub-list
            if(PyList_Append(groups.get(), current.release()))
                return NULL;
            if(PyList_Append(groups.get(), Py_None))
                return NULL;
            current.reset(PyList_New(0));
            if(current.isnull())
                return NULL;
        } else {
            if(PyList_Append(current.get(), cur))
                return NULL;
        }
    }

    if(PyList_Append(groups.get(), current.release()))
        return NULL;

    return groups.release();
}

static
char decoderErrorName[] = "carchive.backend.pbdecode.DecodeError";

static
const char moduleName[] = "carchive.backend.pbdecode";

static
PyObject *pblogger;

static
void LogBadSample(const char *filename, int line,
                   const char* buf, size_t buflen)
{
    if(!pblogger) return;
    PyGILState_STATE tstate;
    tstate = PyGILState_Ensure();
    PyRef arr(PyByteArray_FromStringAndSize(buf,buflen));
    PyRef junk(PyObject_CallMethod(pblogger, "error", "ssiO", "protobuf decode fails: %s:%d: %s",
                        filename, line, arr.get()));
    if(junk.isnull()) {
        PyErr_Print();
        PyErr_Clear();
    }
    PyGILState_Release(tstate);
}

static
void PBLog(google::protobuf::LogLevel L,
           const char *filename, int line,
           const std::string& msg)
{
    if(!pblogger) return;
    PyGILState_STATE tstate;
    tstate = PyGILState_Ensure();
    PyRef junk(PyObject_CallMethod(pblogger, "error", "ssis", "protobuf: %s:%d: %s",
                        filename, line, msg.c_str()));
    if(junk.isnull()) {
        PyErr_Print();
        PyErr_Clear();
    }
    PyGILState_Release(tstate);
}

static
PyObject* cleanupLogger(PyObject *unused, PyObject *unused2)
{
    Py_XDECREF(pblogger);
    pblogger = NULL;
    Py_RETURN_NONE;
}

static
PyObject* getLog(PyObject *unused, PyObject *unused2)
{
    PyObject *R=pblogger;
    if(!R)
        R = Py_None;
    Py_INCREF(R);
    return R;
}

static
bool initLogger(PyObject *modself)
{
    PyRef logging(PyImport_ImportModule("logging")),
          atexit(PyImport_ImportModule("atexit"));
    if(logging.isnull() || atexit.isnull())
        return false;
    // reference to the cleanupLogger method
    PyRef cleanup(PyObject_GetAttrString(modself, "_cleanupLogger"));
    if(cleanup.isnull())
        return false;
    PyRef junk1(PyObject_CallMethod(atexit.get(), "register", "O", cleanup.get()));
    if(junk1.isnull())
        return false;
    pblogger = PyObject_CallMethod(logging.get(), "getLogger", "s", moduleName);
    if(pblogger)
        google::protobuf::SetLogHandler(&PBLog);
    return !!pblogger;
}

}//namespace

static PyMethodDef PBDMethods[] = {
    {"unescape", PBD_unescape, METH_VARARGS,
     "Unescape a byte string"},
    {"escape", PBD_escape, METH_VARARGS,
     "Escape a byte string"},

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

    {"linesplitter", splitter, METH_VARARGS, "Group AA PB lines"},

    {"_getLogger", getLog, METH_NOARGS, "Fetch extension module logger"},
    {"_cleanupLogger", cleanupLogger, METH_NOARGS, "Remove extension module logger"},
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

#if PY_MAJOR_VERSION >= 3
static struct PyModuleDef pbdmod = {
    PyModuleDef_HEAD_INIT,
    moduleName,
    NULL,
    -1,
    PBDMethods,
};
#  define MODRETURN return mod
#else
#  define MODRETURN return
#endif

PyMODINIT_FUNC
initpbdecode(void)
{
    PyRef map(PyDict_New());
    PyObject *mod;

    if(!map.get())
        MODRETURN;

    GOOGLE_PROTOBUF_VERIFY_VERSION;

#if PY_MAJOR_VERSION >= 3
    mod - PyModule_Create(&pbdmod);
#else
    mod = Py_InitModule(moduleName, PBDMethods);
#endif
    if(!mod)
        MODRETURN;
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

    if(!initLogger(mod)) {
        PyErr_Print();
        PyErr_Clear();
    }

    MODRETURN;
}
