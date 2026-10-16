#define _POSIX_C_SOURCE 200809L
#include "lab1.h"

static double communication_time;
static sem_t *sender_sem = SEM_FAILED;
static sem_t *receiver_sem = SEM_FAILED;
static char sender_name[96], receiver_name[96];
static int own_sender_sem, own_receiver_sem;
static int owned_queue = -1, owned_shm = -1;
static void *attached_shm = (void *) -1;
static volatile sig_atomic_t interrupted;

static void cleanup(void)
{
    if (attached_shm != (void *) -1)
        shmdt(attached_shm);
    if (sender_sem != SEM_FAILED)
        sem_close(sender_sem);
    if (receiver_sem != SEM_FAILED)
        sem_close(receiver_sem);
    if (own_sender_sem)
        sem_unlink(sender_name);
    if (own_receiver_sem)
        sem_unlink(receiver_name);
    if (owned_queue != -1)
        msgctl(owned_queue, IPC_RMID, NULL);
    if (owned_shm != -1)
        shmctl(owned_shm, IPC_RMID, NULL);
}

static void fail(const char *operation)
{
    perror(operation);
    exit(EXIT_FAILURE);
}

static void on_signal(int signal_number)
{
    interrupted = signal_number;
}

static void check_interrupted(void)
{
    if (interrupted) {
        fprintf(stderr, "Interrupted by signal %d.\n", (int) interrupted);
        exit(EXIT_FAILURE);
    }
}

static void wait_for(sem_t *sem)
{
    check_interrupted();
    while (sem_wait(sem) == -1) {
        if (errno != EINTR)
            fail("sem_wait");
        check_interrupted();
    }
    check_interrupted();
}

static void notify(sem_t *sem)
{
    if (sem_post(sem) == -1)
        fail("sem_post");
}

static void timestamp(struct timespec *value)
{
    if (clock_gettime(CLOCK_MONOTONIC, value) == -1)
        fail("clock_gettime");
}

static void add_time(struct timespec start, struct timespec end)
{
    communication_time += (double) (end.tv_sec - start.tv_sec)
                        + (double) (end.tv_nsec - start.tv_nsec) / 1e9;
}

void send(message_t message, mailbox_t *mailbox_ptr)
{
    struct timespec start, end;
    int result = 0;

    timestamp(&start);
    if (mailbox_ptr->flag == MSG_PASSING) {
        /* msgsz excludes mType. The semaphore guarantees an empty queue. */
        result = msgsnd(mailbox_ptr->storage.msqid, &message,
                        sizeof(message.msgText), IPC_NOWAIT);
    } else {
        memcpy(mailbox_ptr->storage.shm_addr, &message, sizeof(message));
    }
    timestamp(&end);
    if (result == -1)
        fail("msgsnd");
    add_time(start, end);
}

void receive(message_t *message_ptr, mailbox_t *mailbox_ptr)
{
    struct timespec start, end;
    ssize_t result = 0;

    timestamp(&start);
    if (mailbox_ptr->flag == MSG_PASSING) {
        /* The receiver semaphore guarantees a message is already available. */
        result = msgrcv(mailbox_ptr->storage.msqid, message_ptr,
                        sizeof(message_ptr->msgText), 0, IPC_NOWAIT);
    } else {
        memcpy(message_ptr, mailbox_ptr->storage.shm_addr,
               sizeof(*message_ptr));
    }
    timestamp(&end);
    if (result == -1)
        fail("msgrcv");
    add_time(start, end);
}

static sem_t *open_when_ready(const char *name)
{
    for (;;) {
        check_interrupted();
        sem_t *sem = sem_open(name, 0);
        if (sem != SEM_FAILED)
            return sem;
        if (errno != ENOENT)
            fail("sem_open receiver");
        struct timespec delay = {0, 10000000};
        nanosleep(&delay, NULL);
    }
}

static void setup(mailbox_t *mailbox, int is_sender)
{
    /* Both terminals must run from the same directory. */
    key_t key = ftok(".", mailbox->flag);
    if (key == (key_t) -1)
        fail("ftok");

    snprintf(sender_name, sizeof(sender_name), "/oslab1_%lu_%lx_sender",
             (unsigned long) getuid(), (unsigned long) (unsigned int) key);
    snprintf(receiver_name, sizeof(receiver_name), "/oslab1_%lu_%lx_receiver",
             (unsigned long) getuid(), (unsigned long) (unsigned int) key);

    if (is_sender) {
        if (mailbox->flag == MSG_PASSING) {
            owned_queue = msgget(key, IPC_CREAT | IPC_EXCL | 0600);
            if (owned_queue == -1)
                fail("msgget (another or unfinished run may exist)");
            mailbox->storage.msqid = owned_queue;
        } else {
            owned_shm = shmget(key, sizeof(message_t),
                               IPC_CREAT | IPC_EXCL | 0600);
            if (owned_shm == -1)
                fail("shmget (another or unfinished run may exist)");
            attached_shm = shmat(owned_shm, NULL, 0);
            if (attached_shm == (void *) -1)
                fail("shmat");
            mailbox->storage.shm_addr = attached_shm;
        }

        sender_sem = sem_open(sender_name, O_CREAT | O_EXCL, 0600, 1);
        if (sender_sem == SEM_FAILED)
            fail("sem_open sender (another or unfinished run may exist)");
        own_sender_sem = 1;

        /* Created LAST so the receiver only proceeds after setup is ready. */
        receiver_sem = sem_open(receiver_name, O_CREAT | O_EXCL, 0600, 0);
        if (receiver_sem == SEM_FAILED)
            fail("sem_open receiver (another or unfinished run may exist)");
        own_receiver_sem = 1;
    } else {
        receiver_sem = open_when_ready(receiver_name);
        sender_sem = sem_open(sender_name, 0);
        if (sender_sem == SEM_FAILED)
            fail("sem_open sender");

        if (mailbox->flag == MSG_PASSING) {
            mailbox->storage.msqid = msgget(key, 0600);
            if (mailbox->storage.msqid == -1)
                fail("msgget");
        } else {
            int shmid = shmget(key, sizeof(message_t), 0600);
            if (shmid == -1)
                fail("shmget");
            attached_shm = shmat(shmid, NULL, SHM_RDONLY);
            if (attached_shm == (void *) -1)
                fail("shmat");
            mailbox->storage.shm_addr = attached_shm;
        }
    }
}

static int run_sender(FILE *input, mailbox_t *mailbox)
{
    char *line = NULL;
    size_t capacity = 0;
    ssize_t length;
    int status = EXIT_SUCCESS;
    message_t message;

    while ((length = getline(&line, &capacity, input)) != -1) {
        check_interrupted();
        if (length > 0 && line[length - 1] == '\n')
            --length;
        if (length > 0 && line[length - 1] == '\r')
            --length;
        if (length < 1 || length > MAX_MSG_SIZE ||
            memchr(line, '\0', (size_t) length) != NULL) {
            fprintf(stderr, "Each line must contain 1-%d bytes of text.\n",
                    MAX_MSG_SIZE);
            status = EXIT_FAILURE;
            break;
        }

        memset(&message, 0, sizeof(message));
        message.mType = DATA_MESSAGE;
        memcpy(message.msgText, line, (size_t) length);

        wait_for(sender_sem);
        send(message, mailbox);
        printf("Sent: %s\n", message.msgText);
        notify(receiver_sem);
    }
    if (ferror(input)) {
        perror("read input");
        status = EXIT_FAILURE;
    }
    free(line);

    /* Wait for the last data message, then send the EOF control message. */
    wait_for(sender_sem);
    memset(&message, 0, sizeof(message));
    message.mType = EXIT_MESSAGE;
    send(message, mailbox);
    notify(receiver_sem);

    /* Do not remove IPC resources before the receiver has read EOF. */
    wait_for(sender_sem);
    printf("Total time in sending message: %.9fs\n", communication_time);
    return status;
}

static int run_receiver(mailbox_t *mailbox)
{
    message_t message;
    for (;;) {
        wait_for(receiver_sem);
        receive(&message, mailbox);
        if (message.mType == EXIT_MESSAGE) {
            notify(sender_sem);
            break;
        }
        printf("Received: %s\n", message.msgText);
        notify(sender_sem);
    }
    printf("Total time in receiving message: %.9fs\n", communication_time);
    return EXIT_SUCCESS;
}

int main(int argc, char *argv[])
{
    if (argc < 3) {
        fprintf(stderr, "Usage: %s sender <1|2> input_file\n"
                        "       %s receiver <1|2>\n", argv[0], argv[0]);
        return EXIT_FAILURE;
    }
    int is_sender = strcmp(argv[1], "sender") == 0;
    if ((!is_sender && strcmp(argv[1], "receiver") != 0) ||
        argc != (is_sender ? 4 : 3) ||
        (strcmp(argv[2], "1") != 0 && strcmp(argv[2], "2") != 0)) {
        fprintf(stderr, "Invalid arguments: use sender 1|2 input_file, "
                        "or receiver 1|2.\n");
        return EXIT_FAILURE;
    }

    FILE *input = NULL;
    if (is_sender) {
        input = fopen(argv[3], "r");
        if (input == NULL)
            fail("fopen");
    }
    if (atexit(cleanup) != 0) {
        fprintf(stderr, "Cannot register cleanup.\n");
        return EXIT_FAILURE;
    }
    struct sigaction action = {0};
    action.sa_handler = on_signal;
    sigemptyset(&action.sa_mask);
    if (sigaction(SIGINT, &action, NULL) == -1 ||
        sigaction(SIGTERM, &action, NULL) == -1)
        fail("sigaction");

    mailbox_t mailbox = {0};
    mailbox.flag = atoi(argv[2]);
    printf("%s\n", mailbox.flag == MSG_PASSING
                       ? "Message passing" : "Shared memory");
    fflush(stdout);
    setup(&mailbox, is_sender);
    int status = is_sender ? run_sender(input, &mailbox)
                           : run_receiver(&mailbox);
    if (input != NULL)
        fclose(input);
    return status;
}
