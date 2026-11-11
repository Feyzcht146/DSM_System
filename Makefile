CC      = gcc
CFLAGS  = -std=gnu11 -Wall -Wextra -g
LDFLAGS = -pthread

SRC  = src/main.c src/config.c
OBJ  = $(SRC:.c=.o)
BIN  = dsm_system
CONF = dsm.conf

.PHONY: all run asan clean

all: $(BIN)

$(BIN): $(OBJ)
	$(CC) $(CFLAGS) $(OBJ) -o $@ $(LDFLAGS)

# recompila si cambia el .c o algún .h de src/
src/%.o: src/%.c src/*.h
	$(CC) $(CFLAGS) -c $< -o $@

run: $(BIN)
	./$(BIN) $(CONF)

asan: clean
    $(MAKE) CFLAGS="$(CFLAGS) -fsanitize=address -fno-omit-frame-pointer" LDFLAGS="$(LDFLAGS) -fsanitize=address"

clean:
	rm -f $(OBJ) $(BIN)
