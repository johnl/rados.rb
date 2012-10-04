#ifndef RADOS_CLUSTER_H
#define RADOS_CLUSTER_H

/*
 * partial emulation of the 1.9 rb_thread_blocking_region under 1.8,
 * this is enough for dealing with blocking I/O functions in the
 * presence of threads.
 */
#ifndef HAVE_RB_THREAD_BLOCKING_REGION

#include <rubysig.h>
#define RUBY_UBF_IO ((rb_unblock_function_t *)-1)
typedef void rb_unblock_function_t(void *);
typedef VALUE rb_blocking_function_t(void *);
static VALUE
rb_thread_blocking_region(
  rb_blocking_function_t *func, void *data1,
  RB_RADOS_UNUSED rb_unblock_function_t *ubf,
  RB_RADOS_UNUSED void *data2)
{
  VALUE rv;

  TRAP_BEG;
  rv = func(data1);
  TRAP_END;

  return rv;
}

#endif /* ! HAVE_RB_THREAD_BLOCKING_REGION */

void init_rados_cluster();

typedef struct {
  VALUE active_thread; /* rb_thread_current() or Qnil */
  int reconnect_enabled;
  int connected;
  int initialized;
  rados_t *cluster;
} rados_cluster_wrapper;

#endif
