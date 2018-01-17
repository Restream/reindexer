
CC_FILES  := $(shell find . -type f -name '*.cc')

OBJ_FILES := $(patsubst ./%.cc, .build/%.o, $(CC_FILES))

LIBDIR    := ../..
REINDEXER_SERVER := reindexer_server
SWAGGER_VERSION ?= 2.x

CXXFLAGS  := -I. -I$(LIBDIR) -I$(LIBDIR)/vendor -std=c++11 -Wall -Wpedantic -Wextra
LDFLAGS   :=  -L$(LIBDIR)/.build -lreindexer -lleveldb -lsnappy -lev -lpthread -ltcmalloc

ifeq ($(DEBUG_BUILD),1)
CXXFLAGS    := $(CXXFLAGS) -fsanitize=thread -O0
LDFLAGS     := $(LDFLAGS) -fsanitize=thread
else
CXXFLAGS    := $(CXXFLAGS) -DNDEBUG -O3 -Ofast -flto
CFLAGS      := $(CFLAGS) -DNDEBUG -O3  -Ofast -flto
#LDFLAGS     := $(LDFLAGS) -flto
#EXTRA_CFLAGS := -DREINDEX_SINGLETHREAD -DREINDEX_NOTIMING -DNDEBUG -flto
endif


all: $(REINDEXER_SERVER)

lib:
$(LIBDIR)/.build/libreindexer.a:
	+make -s -C $(LIBDIR)

.build/%.o: ./%.cc
	@mkdir -p $(dir $@)
	@echo CXX $<
	@$(CXX) $(CXXFLAGS) -c $< -o $@

$(LIBDIR)/.build/libreindexer.a: lib

$(REINDEXER_SERVER): $(OBJ_FILES)  $(LIBDIR)/.build/libreindexer.a
	@echo LD $@
	@$(CXX) $^ $(LDFLAGS) -o $@

bench:
	./reindexer_server &
	sleep 1
	wrk -d5s -s pipeline.lua -t7 -c200 http://127.0.0.1:8000/query?q=select%20*%20from%20test_item%20where%20id%3d10 -- 50
	killall reindexer_server

clean:
	rm -rf .build .depend doc

swagger:
	@mkdir -p doc
	curl https://codeload.github.com/swagger-api/swagger-ui/tar.gz/$(SWAGGER_VERSION) | tar xzv -C doc swagger-ui-$(SWAGGER_VERSION)/dist
	mv -f doc/swagger-ui-$(SWAGGER_VERSION)/dist/* doc/
	rm -rf doc/swagger-ui-$(SWAGGER_VERSION) doc/swagger-ui.js
	sed -i_ "s/swagger-ui\.js/swagger-ui\.min\.js/" doc/index.html
	sed -i_ "s/swagger\.json/swagger\.yml/" doc/index.html
	sed -i_ "s/http:\/\/petstore\.swagger\.io\/v2\///" doc/index.html
	sed -i_ "s/\(ui = SwaggerUIBundle..\)/\1docExpansion:\"none\",/" doc/index.html
	rm -f doc/*_
	cp server.yml doc/swagger.yml


.depend: $(CC_FILES)
	@$(CXX) -MM $(CXXFLAGS) $^ | sed "s/^\(.*\): \(.*\)\.\([cp]*\) /\.build\/\2.o: \2.\3 /" >.depend

-include .depend
