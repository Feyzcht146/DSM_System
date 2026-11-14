CC      = gcc
CFLAGS  = -std=gnu11 -Wall -Wextra -g
LDFLAGS = -pthread

LIB  = src/config.o src/pager.o src/coherence.o src/node.o src/network.o
SRC  = src/main.c
OBJ  = $(SRC:.c=.o) $(LIB)
BIN  = dsm_system
CONF = dsm.conf
NODE = 0

TESTS = tests/test_config tests/test_pager tests/test_cluster

.PHONY: all run test asan clean

all: $(BIN)

$(BIN): $(OBJ)
	$(CC) $(CFLAGS) $(OBJ) -o $@ $(LDFLAGS)

# recompila si cambia el .c o algún .h de src/
src/%.o: src/%.c src/*.h
	$(CC) $(CFLAGS) -c $< -o $@

run: $(BIN)
	./$(BIN) $(CONF)

tests/%.o: tests/%.c src/*.h
	$(CC) $(CFLAGS) -Isrc -c $< -o $@

tests/test_config: tests/test_config.o src/config.o
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

tests/test_pager: tests/test_pager.o $(LIB)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

tests/test_cluster: tests/test_cluster.o
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

# pruebas de la configuración, del paginador como el nodo NODE (make test NODE=1) y
# del clúster con tres nodos locales (usa los puertos de $(CONF))
test: $(BIN) $(TESTS)
	./tests/test_config $(CONF)
	./tests/test_pager $(CONF) $(NODE)
	./tests/test_cluster ./$(BIN) $(CONF)

asan: clean
	$(MAKE) CFLAGS="$(CFLAGS) -fsanitize=address -fno-omit-frame-pointer" LDFLAGS="$(LDFLAGS) -fsanitize=address"

clean:
	rm -f src/*.o tests/*.o $(BIN) $(TESTS)
