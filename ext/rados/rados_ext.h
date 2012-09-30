#ifndef RADOS_EXT
#define RADOS_EXT

// tell rbx not to use it's caching compat layer
// by doing this we're making a promize to RBX that
// we'll never modify the pointers we get back from RSTRING_PTR
#define RSTRING_NOT_MODIFIED
#include <ruby.h>
#include <fcntl.h>

#ifndef HAVE_UINT
#define HAVE_UINT
typedef unsigned short    ushort;
typedef unsigned int    uint;
#endif

#include <rados/librados.h>

#ifdef HAVE_RUBY_ENCODING_H
#include <ruby/encoding.h>
#endif

#include <cluster.h>
#include <io_context.h>

#endif
