#!/usr/bin/env bash
# Put Python's shared library on the dynamic loader's path, for the `ml` feature.
#
# Source this file; do not execute it.
#
# Why it is derived rather than written down: the three scripts that need this each
# carried the same hardcoded string, naming an exact Homebrew patch version
# (`python@3.11/3.11.11`) and `$HOME/miniconda3`. That breaks on `brew upgrade`, on
# any machine laid out differently, and on Linux, where the loader does not read
# DYLD_LIBRARY_PATH at all -- so a CI runner silently got nothing useful.
#
# It was also wrong in a way that happened to work. It listed Homebrew's python@3.11
# first, while the binaries on the machine it was written for link against
# miniconda's 3.11: `otool -l` shows the rpath pyo3 baked in pointing at
# `$HOME/miniconda3/lib`. DYLD_LIBRARY_PATH takes precedence over rpath, so the
# hardcoded value was shadowing the libpython the build had actually chosen with a
# different copy that merely shared its ABI.
#
# Scope, measured. The default suite does not need this at all: `cargo test
# --workspace` passes all 773 tests with the variable unset, because pyo3's build
# script bakes an absolute rpath into the binaries it links. The `ml` feature does
# need it, because that rpath is not emitted transitively -- without it `cargo test
# -p kolibrie --features ml` fails 34 targets with "Library not loaded:
# @rpath/libpython3.11.dylib", and `kolibrie-http-server` exits 127.
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
