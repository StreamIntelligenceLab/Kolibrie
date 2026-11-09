#!/usr/bin/env bash
# Put Python's shared library on the dynamic loader's path, for the `ml` feature.
#
# Source this file; do not execute it.
#
# Why it is derived rather than written down: a hardcoded path names one exact
# Python installation, which breaks on an upgrade, on a machine laid out
# differently, and on Linux, where the loader does not read DYLD_LIBRARY_PATH.
# Deriving it also picks the libpython the build actually linked against, since
# DYLD_LIBRARY_PATH takes precedence over the rpath pyo3 bakes in.
#
# Scope: the default suite does not need this, because pyo3's build script bakes an
# absolute rpath into the binaries it links. The `ml` feature does, because that
# rpath is not emitted transitively; without it the `ml` tests and
# `kolibrie-http-server` cannot load libpython.
#
# An existing value wins: the derived directory is appended rather than prepended,
# so anyone who has already aimed this at a particular interpreter keeps it.

# The same interpreter pyo3 resolves, so the directory holds the libpython the build
# links against rather than whichever one happens to be installed.
kolibrie_python_libdir() {
  "${PYO3_PYTHON:-python3}" -c \
    'import sysconfig; print(sysconfig.get_config_var("LIBDIR") or "")' 2>/dev/null
}

case "$(uname -s)" in
  Darwin) _kolibrie_ld_var=DYLD_LIBRARY_PATH ;;
  *)      _kolibrie_ld_var=LD_LIBRARY_PATH   ;;
esac

_kolibrie_libdir="$(kolibrie_python_libdir)"
if [ -n "$_kolibrie_libdir" ] && [ -d "$_kolibrie_libdir" ]; then
  eval "_kolibrie_current=\${$_kolibrie_ld_var:-}"
  if [ -n "$_kolibrie_current" ]; then
    export "$_kolibrie_ld_var=$_kolibrie_current:$_kolibrie_libdir"
  else
    export "$_kolibrie_ld_var=$_kolibrie_libdir"
  fi
  unset _kolibrie_current
fi
unset _kolibrie_libdir
