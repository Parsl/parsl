#include <assert.h>
#include <stdlib.h>
#include <string.h>

char readByteAt(char *p) {
  return *p;
}

void *incPtrBy(int n, void *p) {
  return p+n;
}

char *str_from_bytes(int n, char *p) {
  char *b = malloc(n+1);
  assert(b != NULL);
  memcpy(b, p, n);
  b[n] = '\0';
  return b;
  // idris2 runtime will allegedly de-alloc this malloc as part
  // of the FFI definition. but see notes elsewhere and valgrind,
  // it looks like it doesn't.
}

char *str_str_id(char *s) {
  // this is a helper used as part of casting a Ptr String to a
  // plain String in the FFI interface. To do with releasing the
  // memory allocated in str_from_bytes above.
  return s;
}


// copy_and_append: AnyPtr -> Int -> Bits8 -> PrimIO AnyPtr

void *copy_and_append(void *old_p, int old_n, char new_v) {
  char *new_p = malloc(old_n+1);
  assert(new_p != NULL);
  memcpy(new_p, old_p, old_n);
  new_p[old_n] = new_v;
  return new_p;
}

void *shift_down_one(void *p, int n) {
  void *old_p = p;

  memmove(p, p+1, n);

  // this realloc is optional: it isn't needed for
  // correctness, but tells the memory allocator about
  // reduced need for memory.
  // this memory can move due to the realloc...
  // so slightly weird to memmove *and* get a likely
  // realloc copy...
  // p = realloc(p, n);
  // printf("shift_down_one realloc: old_p = %p, (new) p = %p\n", old_p, p);
  // assert(p == old_p); // we'd expect the memory to not move. although for weird reasons it might?
  return p;
}

/*
%foreign "C:unicode_byte_len,bytes"
prim__unicode_byte_len : String -> PrimIO Int

%foreign "C:unicode_bytes,bytes"
prim__unicode_bytes : String -> PrimIO AnyPtr
*/

int unicode_byte_len(char *s) {
  return strlen(s); // strlen should return a byte count, not a unicode codepoint count, I think
}

void *unicode_bytes(char *s) {
  // does not need to be null terminated
  int n = strlen(s);
  char *b = malloc(n);
  memcpy(b, s, n);

  return b;
}


void *duplicate_block(void *p, int n) {
  void *p2 = malloc(n);
  assert(p2 != NULL);
  memcpy(p2, p, n);
}
