from libcpp.string cimport string as libcpp_string
from libcpp.string cimport string as libcpp_utf8_string
from libcpp cimport bool
from dictionary cimport Dictionary
from std_smart_ptr cimport shared_ptr
from match cimport Match
from match_iterator cimport MatchIteratorPair as _MatchIteratorPair

ctypedef bool (*match_acceptor_t)(Match& m)

cdef extern from "keyvi/dictionary/completion/multiword_completion.h" namespace "keyvi::dictionary::completion":
    cdef cppclass MultiWordCompletion:
        MultiWordCompletion(shared_ptr[Dictionary]) except +
        _MatchIteratorPair GetCompletions(libcpp_utf8_string)
        _MatchIteratorPair GetCompletions(libcpp_utf8_string, int)
        _MatchIteratorPair GetCompletions(libcpp_utf8_string, int, match_acceptor_t)


