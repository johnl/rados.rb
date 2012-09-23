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

static VALUE rb_rados_cluster_stats(VALUE self) {
	int err;
	VALUE h;
	struct rados_cluster_stat_t result;
	GET_CLUSTER(self);

	err = rados_cluster_stat(*wrapper->cluster, &result);
	if (err < 0) {
		rb_raise(cRadosError, "cannot get stats: %s", strerror(-err));
	}
	h = rb_hash_new();
	rb_hash_aset(h, ID2SYM(rb_intern("num_objects")), INT2NUM(result.num_objects));
	rb_hash_aset(h, ID2SYM(rb_intern("kb")), INT2NUM(result.kb));
	rb_hash_aset(h, ID2SYM(rb_intern("kb_used")), INT2NUM(result.kb_used));
	rb_hash_aset(h, ID2SYM(rb_intern("kb_avail")), INT2NUM(result.kb_avail));
	return h;
}

static VALUE rb_rados_cluster_pool_list(VALUE self) {
	VALUE a;
	int i;
	GET_CLUSTER(self);
	int buf_s = rados_pool_list(*wrapper->cluster, NULL, 0);
	char *buf = xmalloc(buf_s);
	char *buf_p;
	int r = rados_pool_list(*wrapper->cluster, buf, buf_s);
	if (r != buf_s) {
		xfree(buf);
		rb_raise(cRadosError, "mismatch retrieving pool list");
	}
	a = rb_ary_new();
	rb_ary_push(a, rb_str_new2(buf));

	for (i = 1; i < buf_s; i++) {
		if ((buf[i - 1] == 0) && (buf[i] != 0) ) {
			buf_p = buf + i;
			rb_ary_push(a, rb_str_new2(buf_p));
		}
	}

	xfree(buf);
	return a;
}

static VALUE rb_rados_cluster_pool_lookup(VALUE self, VALUE pool_name) {
	GET_CLUSTER(self);
	int64_t id;
	Check_Type(pool_name, T_STRING);
	char *cpool_name = StringValuePtr(pool_name);
	id = rados_pool_lookup(*wrapper->cluster, cpool_name);
	if (id == -2) {
		rb_raise(rb_const_get(mRados, rb_intern("PoolNotFound")), "%s", cpool_name);
	} else if (id < 0) {
		rb_raise(cRadosError, "error looking up lookup pool '%s': %s", cpool_name, strerror(-id));
	}
	return INT2NUM(id);
}

static VALUE rb_rados_cluster_pool_create(VALUE self, VALUE pool_name) {
	GET_CLUSTER(self);
	int err;
	Check_Type(pool_name, T_STRING);
	char *cpool_name = StringValuePtr(pool_name);
	err = rados_pool_create(*wrapper->cluster, cpool_name);
	if (err < 0) {
		rb_raise(rb_const_get(mRados, rb_intern("PoolError")), "error creating pool '%s': %s", cpool_name, strerror(-err));
	}
	return Qtrue;
}

static VALUE rb_rados_cluster_pool_delete(VALUE self, VALUE pool_name) {
	GET_CLUSTER(self);
	int err;
	Check_Type(pool_name, T_STRING);
	char *cpool_name = StringValuePtr(pool_name);
	err = rados_pool_delete(*wrapper->cluster, cpool_name);
	if (err == -2) {
		rb_raise(rb_const_get(mRados, rb_intern("PoolNotFound")), "%s", cpool_name);
	} else if (err < 0) {
		rb_raise(rb_const_get(mRados, rb_intern("PoolError")), "error deleting pool '%s': %s", cpool_name, strerror(-err));
	}
	return Qtrue;
}

void init_rados_cluster() {
	cRadosCluster = rb_define_class_under(mRados, "Cluster", rb_cObject);
	rb_define_alloc_func(cRadosCluster, allocate);
	rb_define_private_method(cRadosCluster, "initialize_ext", initialize_ext, 0);
	rb_define_method(cRadosCluster, "stats", rb_rados_cluster_stats, 0);
	rb_define_method(cRadosCluster, "pool_list", rb_rados_cluster_pool_list, 0);
	rb_define_method(cRadosCluster, "pool_lookup", rb_rados_cluster_pool_lookup, 1);
	rb_define_method(cRadosCluster, "pool_create", rb_rados_cluster_pool_create, 1);
	rb_define_method(cRadosCluster, "pool_delete", rb_rados_cluster_pool_delete, 1);
}
