CC      = gcc
CFLAGS  = -std=gnu11 -Wall -Wextra -g
LDFLAGS = -pthread

# Biblioteca del DSM: todo src/ menos el programa del nodo
LIB_OBJ = src/config.o src/pager.o src/coherence.o src/alloc.o src/stats.o src/node.o src/network.o src/dsm.o
LIB  = libdsm.a
BIN  = dsm_system
CONF = dsm.conf
NODE = 0

TESTS = tests/test_config tests/test_pager tests/test_cluster tests/test_api

.PHONY: all run test asan clean

all: $(LIB) $(BIN)

$(LIB): $(LIB_OBJ)
	ar rcs $@ $^

$(BIN): src/main.o $(LIB)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

# recompila si cambia el .c o algún .h
src/%.o: src/%.c src/*.h include/*.h
	$(CC) $(CFLAGS) -Iinclude -c $< -o $@

run: $(BIN)
	./$(BIN) $(CONF)

tests/%.o: tests/%.c src/*.h include/*.h
	$(CC) $(CFLAGS) -Iinclude -Isrc -c $< -o $@

tests/test_config: tests/test_config.o src/config.o
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

tests/test_pager: tests/test_pager.o $(LIB)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

tests/test_cluster: tests/test_cluster.o
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

tests/test_api: tests/test_api.o $(LIB)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

# pruebas de la configuración, del paginador como el nodo NODE (make test NODE=1), del
# clúster con tres nodos locales y de la API (usan los puertos de $(CONF))
test: $(BIN) $(TESTS)
	./tests/test_config $(CONF)
	./tests/test_pager $(CONF) $(NODE)
	./tests/test_cluster ./$(BIN) $(CONF)
	./tests/test_api ./$(BIN) $(CONF)

asan: clean
	$(MAKE) all $(TESTS) CFLAGS="$(CFLAGS) -fsanitize=address -fno-omit-frame-pointer" LDFLAGS="$(LDFLAGS) -fsanitize=address"

clean:
	rm -f src/*.o tests/*.o $(LIB) $(BIN) $(TESTS)
