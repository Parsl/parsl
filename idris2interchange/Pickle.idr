module Pickle

||| Takes some representation of a byte sequence containing a pickle and
||| executes that pickle program, leaving something more like an AST of
||| the final object. That AST can be turned into an idris2 value by some
||| later function.
export
unpickle : ?ByteRepr -> ?PickleAST
unpickle i = ?notimpl
