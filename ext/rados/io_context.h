#ifndef RADOS_IOCONTEXT_H
#define RADOS_IOCONTEXT_H

typedef struct {
	rados_ioctx_t *ioctx;
	VALUE cluster;
} rados_ioctx_wrapper;

void init_rados_io_context();

#endif
