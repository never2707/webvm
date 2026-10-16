#ifndef LAB1_H
#define LAB1_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/shm.h>
#include <semaphore.h>
#include <time.h>
#include <errno.h>

#define MSG_PASSING 1
#define SHARED_MEM 2
#define MAX_MSG_SIZE 1024
#define DATA_MESSAGE 1L
#define EXIT_MESSAGE 2L

typedef struct {
    int flag;
    union {
        int msqid;
        char *shm_addr;
    } storage;
} mailbox_t;

typedef struct {
    long mType;
    /* One extra byte for the terminating NUL. */
    char msgText[MAX_MSG_SIZE + 1];
} message_t;

void receive(message_t *message_ptr, mailbox_t *mailbox_ptr);
void send(message_t message, mailbox_t *mailbox_ptr);

#endif
