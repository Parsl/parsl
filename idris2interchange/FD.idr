module FD

import Control.App
import Generics.Derive
import System.FFI

import Bytes
import Logging

-- got this ambiguity error on index when adding an extra log line, i guess
-- because that added in an extra >> which then made ambiguity resolution
-- need one more step.
{-
Error: While processing right hand side of poll. Maximum ambiguity depth exceeded in FD.poll:
FD.PollOutput.(.fd) --> Prelude.Interfaces.(>>) --> Prelude.Interfaces.(>>) --> Prelude.Interfaces.(>>) 

FD:151:29--151:34
 147 |      -- we aren't verifying in the type system that it is the *correct*
 148 |      -- n that we intended - that still happens by human reasoning.
 149 |      -- (also theres no '.fd is an unknown attribute' runtime error...
 150 |      -- that's also a compile time error)
 151 |      logv "FD at this pos" (index i inputs).fd
                                   ^^^^^

Suggestion: the default ambiguity depth limit is 3, the %ambiguity_depth pragma can be used to extend this limit, but beware
compilation times can be severely impacted.
-}
%ambiguity_depth 6

%language ElabReflection
-- %default total


-- bits for interfacing to linux poll

-- implementing this interface is a "unsafe" statement that the relevant file
-- descriptor type can be polled by kernel interfaces. It is a transcription
-- of that piece of the kernel type model into idris2.
public export interface Pollable p where

public export data ReadableFD : Type where

public export data PidFD : Type where


public export Pollable PidFD where



-- TODO: can we do something to protect the lifetime of this int,
-- eg to treat it linearly (or linearly with dup-ing?) so that we
-- don't end up with bad lifetimes?
public export
data FD x = MkFD Int

public export
stdin : FD ReadableFD
stdin = MkFD 0  -- well-known fd number

-- cast is deliberatly one-way: we can easily remove the FD-ness of
-- the value, but not easily/accidentally add it onto a particular
-- arbitrary integer without declaring it using MkFD.
-- This is syntactically the same, but at a human level is a bit more
-- serious looking than cast - going along with the concept that you
-- can always cast an FD to an int meaningfully, but not the other way
-- round.
Cast (FD x) Int where
  cast (MkFD fd) = fd

-- we need to derive Generic and Meta here, before Show will also
-- be derivable
%runElab derive "FD" [Generic, Meta, Show]

-- this is what poll takes: but revents is an output
-- so model it as input and output types that are internally
-- converted to/from what happens to be a single pollfd struct...
--       struct pollfd {
--           int   fd;         /* file descriptor */
--           short events;     /* requested events */
--           short revents;    /* returned events */
--       };

public export
record PollInput where
  -- I experimented with PollInput being parameterised by fd type, and putting a constraint on that type.
  -- but we don't need the type exposed here, and it makes eg. a vector of PollInputs be non-homogenous, as
  -- there can be different FD types in there.
  -- https://stackoverflow.com/questions/47418867/idris-dependent-records-with-interface-constraint-on-type-constructor-parameter
  -- so instead look at getting the FD constraint inside the PollInput.
  -- that is, i guess, a fd of type fd_type, but also a Pollable instance for that fd, found automatically?
  constructor MkPollInput


  -- proof that the fd is pollable
  fd : FD fd_type
  {auto pollable_fd : Pollable fd_type}

  -- hoping that Bits16 is the same size as a C short...
  -- that's not so defined in general
  events: Bits16


public export
record PollOutput where
  -- not constraining fd_type to pollable here, because have not
  -- encountered need. it is, however, implicitly pollable because
  -- it came out of poll, at the implementation level, so maybe
  -- its interesting to convey that at type level sometime.
  -- but! should the fd_type be stored in poll output, or else
  -- how do I safely convey the fd type? someone somewhere has
  -- to assert the syscall maps the input and output FDs together
  -- (perhaps with a runtime checkabout assert = or something)
  constructor MkPollOutput
  fd: FD fd_type

  revents: Bits16   -- see events short note above


-- time in milliseconds - are there other ways to represent
-- time units in idris? using a unit-forced type not int pushes
-- against some common coding mistakes I've encountered in Parsl
-- (in Parsl proper, as recommended by Kevin, a different approach
-- is to ensure the *name* of all parameters and variables contains
-- the unit - with less automatic type checking there)
public export
data TimeMS = MkTimeMS Int

-- poll looks like this:
--        int poll(struct pollfd *fds, nfds_t nfds, int timeout);

-- in addition to being the same size, every entry in the input
-- corresponds to the entry in the output
-- (aka the FD key sequence of the input and the output is the
-- same...)
-- is there some better structure for that malloc than hoping
-- we did a free at the end? `with` style bracket or something
-- linear?

-- n is the number of (struct pollfd) in this memory allocation
-- so that we can have statically checked bounds checks (even
-- when calling into C code, as long as the interface is written
-- right)
data PollMemPtr : (n: Nat) -> Type where
  MkPollMemPtr : AnyPtr -> PollMemPtr n

%foreign "C:pollhelper_allocate_memory,pollhelper"
prim_pollhelper_allocate_memory: Int -> PrimIO AnyPtr

pollhelper_allocate_memory: HasErr AppHasIO es => (n: Nat) -> App es (PollMemPtr n)
pollhelper_allocate_memory n = do
  ptr <- primIO $ primIO $ prim_pollhelper_allocate_memory (cast n)
  pure (MkPollMemPtr ptr)

pollhelper_free_memory : HasErr AppHasIO es => PollMemPtr n -> App es ()
pollhelper_free_memory (MkPollMemPtr ptr) = primIO $ free ptr


%foreign "C:pollhelper_set_entry,pollhelper"
prim_pollhelper_set_entry : AnyPtr -> Int -> Int -> PrimIO ()

pollhelper_set_entry : HasErr AppHasIO es => PollMemPtr n -> Fin n -> PollInput -> App es ()
pollhelper_set_entry (MkPollMemPtr ptr) pos pi = do
  -- the cast for pos from Fin n to Int is unchecked and will break
  -- at runtime if the number is too big for Int... not compile time
  -- checked... more specifically it's the Integer to Int cast that
  -- breaks, by returning the wrong number (probably a binary bitwise
  -- least-significant-bits thing?)
  primIO $ primIO $ prim_pollhelper_set_entry ptr (cast (the Integer (cast pos))) (cast pi.fd) -- TODO: flags, if I want anything other than hardcoded POLLIN

%foreign "C:pollhelper_get_entry,pollhelper"
prim_pollhelper_get_entry : AnyPtr -> Int -> PrimIO Bits16

pollhelper_get_entry : HasErr AppHasIO es => PollMemPtr n -> Fin n -> App es Bits16
pollhelper_get_entry (MkPollMemPtr ptr) pos =
  primIO $ primIO $ prim_pollhelper_get_entry ptr (cast (the Integer (cast pos)))

%foreign "C:poll,libc"
prim_poll : AnyPtr -> Int -> Int -> PrimIO Int

pollhelper_poll : HasErr AppHasIO es => {n : Nat} -> PollMemPtr n -> TimeMS -> App es Int
pollhelper_poll (MkPollMemPtr ptr) (MkTimeMS t) =
  primIO $ primIO $ prim_poll ptr (cast n) t

public export
poll: (State LogConfig LogConfig es, HasErr AppHasIO es) => {n: Nat} -> Vect n PollInput -> TimeMS -> App es (Vect n PollOutput)
poll inputs timeout = do
   -- we can't do this alloc using idris2 memory alloc because
   -- we don't have a sizeof operator or calloc (or equiv)
   buf <- pollhelper_allocate_memory n

   -- this for needs to be index by n so that we can claim that
   -- we're updating the right place
   for_ (Data.Fin.List.allFins n) $ \i => the (App es ()) $ do
     -- in here, we know that i is in the range of n
     -- and so then can be used safely to index inputs
     -- as used in print statement below
     log "---"
     log "Preparing poll structure"
     logv "AllFins member" i
     -- ... here index will fail at compile time if it cannot statically
     -- verify that i is in range for inputs - there's no notion of a
     -- runtime out of range error for this index call.
     -- we aren't verifying in the type system that it is the *correct*
     -- n that we intended - that still happens by human reasoning.
     -- (also theres no '.fd is an unknown attribute' runtime error...
     -- that's also a compile time error)
     logv "FD at this pos" (index i inputs).fd
     -- it also doesn't check we are passing in the right memory block
     -- to pollhelper_set_entry that happens to have the same count/size
     pollhelper_set_entry buf i (index i inputs)

   -- the above "allocate and set values later" looks quite like the
   -- linear immutable hole filling stuff talked about by Arnauld at tweag,
   -- although the other (.revents) part of this struct *is* mutable...

   log "About to call poll"
   poll_ret <- pollhelper_poll buf timeout -- TODO: something with the return result
   log "Poll returned..."
   logv "... Poll returned this return value" poll_ret

   -- contrast Data.Vect.alLFins here with Data.Fin.List.allFins above..
   -- we could use Data.Vect.allFins in both places I think... the reason
   -- for Data.Vect here is so the output is the desired Data.Vect too...
   r <- for (Data.Vect.allFins n) $ \i => do
        let inp = index i inputs
        logv "Extracting result for poll index" i
        logv "FD" inp.fd
        revents <- pollhelper_get_entry buf i
        logv "revents" revents
        pure (MkPollOutput inp.fd revents)

   pollhelper_free_memory buf
   pure r


-- unix read() looks like:
--        ssize_t read(int fd, void buf[.count], size_t count);
-- type safe use would require buf to have [.count] enforced on it by the
-- type system. which is maybe OK in small areas.
-- or I could always allocate a fresh buffer here, with the given size?

%foreign "C:read,libc"
prim_read : Int -> AnyPtr -> Int -> PrimIO Int
-- TODO: are ssize_t and size_t suitable to be Ints?

public export
read : HasErr AppHasIO es => FD ReadableFD -> Nat -> App1 es ByteBlock
read (MkFD fd_int) count = do
  -- allocate memory and read into it. that probably isn't the right
  -- way to do things linearly... or the right way to do things with
  -- garbage collection
  let count_int = the Int (cast count)
  memory <- app $ primIO $ malloc count_int
  -- TODO: do I need to check memory is not NULL? (because unix malloc can
  -- return NULL...)
  read_count <- app $ primIO $ primIO $ prim_read fd_int memory count_int
  pure1 (MkByteBlock memory (cast read_count))


%foreign "C:pidfd_open,libc"
prim_pidfd_open : Int -> Int -> PrimIO Int
-- this assumes pid_t and unsigned int both work as `Int`

public export
pidfd_open : (HasErr AppHasIO es, HasErr String es) => Int -> App es (FD PidFD)
pidfd_open pid = do
  pidfd <- primIO $ primIO $ prim_pidfd_open pid 0
  -- the (App es ()) $ throw "lol"
  -- ^ ISSUE: another case of having to force the type of the action, but only
  -- in testing/experimenting.
  -- parsl/tests/test_callables.py Error: Unsolved holes:
  -- FD.{a:6106} introduced at: 
  -- FD:268:3--268:14
  -- 264 | public export
  -- 265 | pidfd_open : (HasErr AppHasIO es, HasErr String es) => Int -> App es (FD PidFD)
  -- 266 | pidfd_open pid = do
  -- 267 |   pidfd <- primIO $ primIO $ prim_pidfd_open pid 0
  -- 268 |   throw "lol"
  --         ^^^^^^^^^^^
  -- I think whats happening: We dont' actually care what the return type of
  -- the App is, but it has to be *something* concrete and there isn't any
  -- information to pick that something -- the App es () could be
  -- App es ANYTHING but the ANYTHING has to be solvable even though it is
  -- never needed here because its discarded.
  -- In practice, maybe that's not so much of a problem, because (as in the
  -- case statement below) a throw would often be one branch of a case
  -- statement that provides the type of throw via another branch - eg below
  -- its easy to see the return value of the `throw` action is an (FD PidFD)
  case pidfd of
    -1 => throw "error_pidfd_open_returned_error"
    p => pure (MkFD p)
