CC_FILES	:= $(shell find . -type f -name '*.cc')
OBJ_FILES	:= $(patsubst ./%.cc, .build/%.o, $(CC_FILES))

LIBDIR		:= ../..
GBENCH		:= gbench

CXXFLAGS	:= -I. -I$(LIBDIR) -I$(LIBDIR)/vendor -Ifixtures -std=c++11 -Wall -Wpedantic -Wextra
LDFLAGS		:= -L$(LIBDIR)/.build -lbenchmark -lreindexer -lleveldb -lsnappy -lpthread -ltcmalloc

ifeq ($(DEBUG_BUILD),1)
CXXFLAGS    := $(CXXFLAGS) -fsanitize=address -O0
LDFLAGS     := $(LDFLAGS) -fsanitize=address
else
CXXFLAGS    := $(CXXFLAGS) -DNDEBUG -O3 -Ofast -flto
CFLAGS      := $(CFLAGS) -DNDEBUG -O3  -Ofast -flto
endif

all: $(GBENCH)

lib:
$(LIBDIR)/.build/libreindexer.a:
	+make -s -C $(LIBDIR)

.build/%.o: ./%.cc
	@mkdir -p $(dir $@)
	@echo CXX $<
	@$(CXX) $(CXXFLAGS) -c $< -o $@

$(LIBDIR)/.build/libreindexer.a: lib

$(GBENCH): $(OBJ_FILES)  $(LIBDIR)/.build/libreindexer.a
	@echo LD $@
	@$(CXX) $^ $(LDFLAGS) -o $@

clean:
	rm -rf .build .depend

.depend: $(CC_FILES)
	@$(CXX) -MM $(CXXFLAGS) $^ | sed "s/^\(.*\): \(.*\)\.\([cp]*\) /\.build\/\2.o: \2.\3 /" >.depend

-include .depend