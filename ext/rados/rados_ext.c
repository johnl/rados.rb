#include <rados_ext.h>

VALUE mRados, cRadosError;

/* Ruby Extension initializer */
void Init_rados() {
	mRados      = rb_define_module("Rados");
	cRadosError = rb_const_get(mRados, rb_intern("Error"));

	init_rados_cluster();
	init_rados_io_context();
}
