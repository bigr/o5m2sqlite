#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.69])
AC_INIT(o5m2sqlite, 0.0.0, ja@bigr.cz)
AM_INIT_AUTOMAKE(o5m2sqlite, 0.0.0)
AC_CONFIG_SRCDIR([src/segmenter.h])
AC_CONFIG_HEADERS([config.h])

# Checks for programs.
AC_PROG_CC

# Checks for libraries.
PKG_CHECK_MODULES([DEPS], [libo5mreader, sqlite3, spatialite])

# Checks for header files.
AC_CHECK_HEADERS([float.h memory.h stdint.h stdlib.h string.h uthash.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_TYPE_INT32_T
AC_TYPE_INT64_T
AC_TYPE_UINT64_T
AC_TYPE_UINT8_T

# Checks for library functions.
AC_FUNC_MALLOC
AC_FUNC_REALLOC
AC_CHECK_FUNCS([memmove])

AC_CONFIG_FILES([Makefile
                 src/Makefile])
AC_OUTPUT
