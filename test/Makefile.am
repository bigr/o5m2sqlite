INCLUDES  = @CHECK_CFLAGS@
AUTOMAKE_OPTIONS = foreign
if HAVE_CHECK
TESTS = check_segmenter
else
TESTS =
endif
check_PROGRAMS = check_segmenter
check_segmenter_SOURCES = check_segmenter.c $(top_builddir)/src/segmenter.h $(top_builddir)/src/segmenter.c
check_segmenter_CFLAGS = @CHECK_CFLAGS@
check_segmenter_LDADD = @CHECK_LIBS@
CLEANFILES = check_segmenter.log
