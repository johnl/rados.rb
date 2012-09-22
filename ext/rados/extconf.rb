# encoding: UTF-8
require 'mkmf'

$CFLAGS='-Wall'

# 1.9-only
have_func('rb_thread_blocking_region')
have_func('rb_wait_for_single_fd')

if have_header('rados.h') then
  prefix = nil
elsif have_header('rados/librados.h') then
  prefix = 'rados'
else
  abort "rados.h is missing.  please check your installation of rados and try again.\n-----"
end

have_library("rados")

create_makefile('rados/rados')
