
# EXTRA_CFLAGS := -DREINDEX_SINGLETHREAD -DREINDEX_NOTIMING -DNDEBUG

CC_FILES  := $(shell find ./core ./tools ./vendor ./estl ./debug -type f -name '*.cc')
C_FILES  := $(shell find ./core ./vendor ./tools ./debug -type f -name '*.c')
OBJ_FILES := $(patsubst ./%.cc, .build/%.o, $(CC_FILES)) $(patsubst ./%.c, .build/%.o, $(C_FILES))
LIBREINDEXER := .build/libreindexer.a
CXXFLAGS    := -I. -Ivendor -std=c++11 -g -Wall -Wextra -Wold-style-cast -Werror $(EXTRA_CFLAGS)
CFLAGS      := -I. -Ivendor -std=c99 -g -Wall -Wextra -Werror $(EXTRA_CFLAGS)

ifeq ($(DEBUG_BUILD),1)
CXXFLAGS    := $(CXXFLAGS) -O1
CFLAGS      := $(CFLAGS) -O1
else
CXXFLAGS    := $(CXXFLAGS) -O3 -Ofast
CFLAGS      := $(CFLAGS) -O3 -Ofast
endif

ifeq ($(COVERAGE_BUILD),1)
CXXFLAGS	:= $(CXXFLAGS) --coverage
CFLAGS 		:= $(CFLAGS) --coverage
# LDFLAGS
endif

all: $(LIBREINDEXER)

.build/%.o: ./%.cc
	@mkdir -p $(dir $@)
	@echo CXX $<
	@$(CXX) $(CXXFLAGS) -c $< -o $@

.build/%.o: ./%.c
	@mkdir -p $(dir $@)
	@echo CC $<
	@$(CC) $(CFLAGS) -c $< -o $@

$(LIBREINDEXER): $(OBJ_FILES)
	@rm -f $@
	@echo AR $@
	@$(AR) cr $@ $^
clean:
	rm -rf .build .depend

.depend: $(CC_FILES)
	@$(CXX) -MM $(CXXFLAGS) $^ | sed "s/^\(.*\): \(.*\)\.\([cp]*\) /\.build\/\2.o: \2.\3 /" >.depend

-include .depend
