
CC_FILES  := $(shell find ./core ./algoritm ./query ./cbinding ./tools ./vendor -type f -name '*.cc')
OBJ_FILES := $(patsubst ./%.cc, .build/%.o, $(CC_FILES))
LIBREINDEXER := .build/libreindexer.a
CXXFLAGS    := -I. -Ivendor -std=c++11 -g -O3 -Wall -Wextra -DNDEBUG

all: $(LIBREINDEXER)

.build/%.o: ./%.cc
	@mkdir -p $(dir $@)
	@echo CXX $<
	@$(CXX) $(CXXFLAGS) -c $< -o $@

$(LIBREINDEXER): $(OBJ_FILES)
	@rm -f $@
	@echo AR $@
	@$(AR) cr $@ $^

clean:
	rm -rf .build .depend

.depend: $(CC_FILES)
	@$(CXX) -MM $(CXXFLAGS) $^ | sed "s/^\(.*\): \(.*\)\.\([cp]*\) /\.build\/\2.o: \2.\3 /" >.depend

-include .depend
