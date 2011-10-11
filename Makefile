OBJ=hiredis/net.o hiredis/hiredis.o hiredis/sds.o hiredis/async.o
BINS=redis_parse
LIBNAME=hiredis/libhiredis

HIREDIS_MAJOR=0
HIREDIS_MINOR=10

# Fallback to gcc when $CC is not in $PATH.
CC:=$(shell sh -c 'type $(CC) >/dev/null 2>/dev/null && echo $(CC) || echo gcc')
OPTIMIZATION?=-O3
WARNINGS=-Wall -W -Wstrict-prototypes -Wwrite-strings
DEBUG?= -g -ggdb
REAL_CFLAGS=$(OPTIMIZATION) -fPIC $(CFLAGS) $(WARNINGS) $(DEBUG)
REAL_LDFLAGS=$(LDFLAGS)

DYLIBSUFFIX=so
STLIBSUFFIX=a
DYLIB_MINOR_NAME=$(LIBNAME).$(DYLIBSUFFIX).$(HIREDIS_MAJOR).$(HIREDIS_MINOR)
DYLIB_MAJOR_NAME=$(LIBNAME).$(DYLIBSUFFIX).$(HIREDIS_MAJOR)
DYLIBNAME=$(LIBNAME).$(DYLIBSUFFIX)
DYLIB_MAKE_CMD=$(CC) -shared -Wl,-soname,$(DYLIB_MINOR_NAME) -o $(DYLIBNAME) $(LDFLAGS)
STLIBNAME=$(LIBNAME).$(STLIBSUFFIX)
STLIB_MAKE_CMD=ar rcs $(STLIBNAME)

# Platform-specific overrides
uname_S := $(shell sh -c 'uname -s 2>/dev/null || echo not')
ifeq ($(uname_S),SunOS)
  REAL_LDFLAGS+= -ldl -lnsl -lsocket
  DYLIB_MAKE_CMD=$(CC) -G -o $(DYLIBNAME) -h $(DYLIB_MINOR_NAME) $(LDFLAGS)
  INSTALL= cp -r
endif
ifeq ($(uname_S),Darwin)
  DYLIBSUFFIX=dylib
  DYLIB_MINOR_NAME=$(LIBNAME).$(HIREDIS_MAJOR).$(HIREDIS_MINOR).$(DYLIBSUFFIX)
  DYLIB_MAJOR_NAME=$(LIBNAME).$(HIREDIS_MAJOR).$(DYLIBSUFFIX)
  DYLIB_MAKE_CMD=$(CC) -shared -Wl,-install_name,$(DYLIB_MINOR_NAME) -o $(DYLIBNAME) $(LDFLAGS)
endif

all: $(DYLIBNAME) $(BINS)

# Deps (use make dep to generate this)
redis_parse.o: redis_parse.c hiredis/hiredis.h

$(DYLIBNAME): $(OBJ)
	$(DYLIB_MAKE_CMD) $(OBJ)

$(STLIBNAME): $(OBJ)
	$(STLIB_MAKE_CMD) $(OBJ)

dynamic: $(DYLIBNAME)
static: $(STLIBNAME)

# Binaries:
redis_parse: redis_parse.o $(STLIBNAME)
	$(CC) -o $@ $(REAL_LDFLAGS) $< $(STLIBNAME)