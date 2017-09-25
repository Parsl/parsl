#ifndef DEBUG_H
#define DEBUG_H

extern int GLOB_DEBUG;

#ifdef DEBUG
   #define debug(format, ...) if (GLOB_DEBUG == 1) fprintf(stdout, format , __VA_ARGS__)
#else
   #define debug(format, ...)
#endif

#define error(format, ...) fprintf(stderr, format , __VA_ARGS__)

#endif
