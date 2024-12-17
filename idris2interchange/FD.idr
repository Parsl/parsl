module FD

import Control.App
import Generics.Derive
import System.FFI

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
%ambiguity_depth 4

%language ElabReflection
-- %default total

-- bits for interfacing to linux poll

-- TODO: can we do something to protect the lifetime of this int,
-- eg to treat it linearly (or linearly with dup-ing?) so that we
-- don't end up with bad lifetimes?
public export
data FD = MkFD Int


-- cast is deliberatly one-way: we can easily remove the FD-ness of
-- the value, but not easily/accidentally add it onto a particular
-- arbitrary integer without declaring it using MkFD.
-- This is syntactically the same, but at a human level is a bit more
-- serious looking than cast - going along with the concept that you
-- can always cast an FD to an int meaningfully, but not the other way
-- round.
Cast FD Int where
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
  constructor MkPollInput
  fd: FD

  -- hoping that Bits16 is the same size as a C short...
  -- that's not so defined in general
  events: Bits16

public export
record PollOutput where
  constructor MkPollOutput
  fd: FD

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
poll: HasErr AppHasIO es => {n: Nat} -> Vect n PollInput -> TimeMS -> App es (Vect n PollOutput)
poll inputs timeout = do
   -- we can't do this alloc using idris2 memory alloc because
   -- we don't have a sizeof operator or calloc (or equiv)
   buf <- pollhelper_allocate_memory n

   -- this for needs to be index by n so that we can claim that
   -- we're updating the right place
   for_ (Data.Fin.List.allFins n) $ \i => do
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


