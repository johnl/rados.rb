#include <rados_ext.h>
#include <rados/librados.h>
#include <cluster.h>
#include <io_context.h>
#include <errno.h>

VALUE cRadosIoContext;
extern VALUE mRados, cRadosError;

#define GET_IOCTX(self) \
  rados_ioctx_wrapper *wrapper; \
  Data_Get_Struct(self, rados_ioctx_wrapper, wrapper)

static void rb_rados_ioctx_mark(void *wrapper) {
	rados_ioctx_wrapper *w = wrapper;
	if (w) {
		rb_gc_mark(w->cluster);
	}
}

static void rb_rados_ioctx_free(void * ptr) {
	rados_ioctx_wrapper *wrapper = (rados_ioctx_wrapper *)ptr;
	rados_ioctx_destroy(wrapper->ioctx);
	// rados_ioctx_destroy apparently handles freeing :/
	//	xfree(ptr);
}


static VALUE allocate(VALUE klass) {
	VALUE obj;
	rados_ioctx_wrapper * wrapper;
	obj = Data_Make_Struct(klass, rados_ioctx_wrapper, rb_rados_ioctx_mark, rb_rados_ioctx_free, wrapper);
	wrapper->ioctx = (rados_ioctx_t*)xmalloc(sizeof(rados_ioctx_t));
	return obj;
}

VALUE rb_rados_ioctx_initialize(VALUE self, VALUE cluster, VALUE pool_name) {
	GET_IOCTX(self);
	int err;
	rados_cluster_wrapper *cwrapper;
	Check_Type(pool_name, T_STRING);
	char *cpool_name = StringValuePtr(pool_name);
	// FIXME: Check type of cluster
	wrapper->cluster = cluster;
 	Data_Get_Struct(cluster, rados_cluster_wrapper, cwrapper);
	// FIXME: Check cluster is initialized/connected?
 	err = rados_ioctx_create(*cwrapper->cluster, cpool_name, wrapper->ioctx);
	if (err < 0) {
		rb_raise(cRadosError, "error creating IoContext for pool '%s': %s", cpool_name, strerror(-err));
	}
	return Qtrue;
}

VALUE rb_rados_ioctx_get_id(VALUE self) {
	GET_IOCTX(self);
	int64_t id;
	id = rados_ioctx_get_id(*wrapper->ioctx);
	if (id < 0) {
		rb_raise(cRadosError, "error getting id of pool from IoContext: %s", strerror(-id));
	}
	return INT2NUM(id);
}


void init_rados_io_context() {
	cRadosIoContext = rb_define_class_under(mRados, "IoContext", rb_cObject);
	rb_define_alloc_func(cRadosIoContext, allocate);
	rb_define_method(cRadosIoContext, "initialize", rb_rados_ioctx_initialize, 2);
	rb_define_method(cRadosIoContext, "get_id", rb_rados_ioctx_get_id, 0);
}
