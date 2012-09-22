#include <rados_ext.h>
#include <rados/librados.h>
#include <cluster.h>
#include <errno.h>

VALUE cRadosCluster;
extern VALUE mRados, cRadosError;

#define GET_CLUSTER(self) \
  rados_cluster_wrapper *wrapper; \
  Data_Get_Struct(self, rados_cluster_wrapper, wrapper)


static void rb_rados_cluster_mark(void * wrapper) {
  rados_cluster_wrapper * w = wrapper;
  if (w) {
    rb_gc_mark(w->active_thread);
  }
}

static VALUE nogvl_close(void *ptr) {
  rados_cluster_wrapper *wrapper;
  wrapper = ptr;
  if (wrapper->connected) {
    wrapper->active_thread = Qnil;
    wrapper->connected = 0;
		// FIXME: need rados_aio_flush() on all open contexts first
		rados_shutdown(*wrapper->cluster);
  }
	xfree(wrapper->cluster);
  return Qnil;
}


static void rb_rados_cluster_free(void * ptr) {
  rados_cluster_wrapper *wrapper = (rados_cluster_wrapper *)ptr;

	nogvl_close(wrapper);
  xfree(ptr);
}

static VALUE allocate(VALUE klass) {
  VALUE obj;
  rados_cluster_wrapper * wrapper;
  obj = Data_Make_Struct(klass, rados_cluster_wrapper, rb_rados_cluster_mark, rb_rados_cluster_free, wrapper);
  wrapper->active_thread = Qnil;
  wrapper->connected = 0; // means that a database connection is open
  wrapper->initialized = 0; // means that that the wrapper is initialized
	wrapper->cluster = (rados_t*)xmalloc(sizeof(rados_t));
  return obj;
}

static VALUE initialize_ext(VALUE self) {
	int err;
	GET_CLUSTER(self);

	// FIXME: can specify user id here for auth!
	err = rados_create(wrapper->cluster, NULL);
	if (err < 0) {
		rb_raise(cRadosError, "cannot create a cluster handle: %s", strerror(-err));
	}
	// FIXME: Allow specifying config filename
	err = rados_conf_read_file(*wrapper->cluster, NULL);
	if (err < 0) {
		rb_raise(cRadosError, "cannot read config file: %s", strerror(-err));
	}
	// FIXME: should release global lock
	err = rados_connect(*wrapper->cluster);
	if (err < 0) {
		rb_raise(cRadosError, "cannot connect: %s", strerror(-err));
	}
	wrapper->connected = 1;

	return self;
}

void init_rados_cluster() {
	cRadosCluster = rb_define_class_under(mRados, "Cluster", rb_cObject);
  rb_define_alloc_func(cRadosCluster, allocate);
	rb_define_private_method(cRadosCluster, "initialize_ext", initialize_ext, 0);
}
