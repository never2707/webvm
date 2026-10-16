CC := gcc
CFLAGS := -O3 -Wall -Wextra -std=c11
LDLIBS := -pthread -lrt

SOURCE := lab1.c
BINARY := lab1

all: $(BINARY)

$(BINARY): $(SOURCE) lab1.h
	$(CC) $(CFLAGS) $(SOURCE) -o $(BINARY) $(LDLIBS)

.PHONY: all clean
clean:
	rm -f $(BINARY)
