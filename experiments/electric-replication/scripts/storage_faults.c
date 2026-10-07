/* Test-only LD_PRELOAD interposer. Never linked into the server or benchmark.
 * Fault file: '<eio-sync|eio-dir-sync|enospc-write|short-write> <path substring>'.
 * Only descriptors below DS_TEST_DATA_ROOT are eligible. Short writes fire
 * once; persistent sync/write errors remain armed until the harness disarms.
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

static int (*real_fsync)(int);
static int (*real_fdatasync)(int);
static ssize_t (*real_write)(int, const void *, size_t);
static ssize_t (*real_pwrite)(int, const void *, size_t, off_t);
static ssize_t (*real_pwrite64)(int, const void *, size_t, off64_t);
static pthread_once_t once = PTHREAD_ONCE_INIT;
static pthread_mutex_t fault_lock = PTHREAD_MUTEX_INITIALIZER;

static void resolve(void) {
    real_fsync = dlsym(RTLD_NEXT, "fsync");
    real_fdatasync = dlsym(RTLD_NEXT, "fdatasync");
    real_write = dlsym(RTLD_NEXT, "write");
    real_pwrite = dlsym(RTLD_NEXT, "pwrite");
    real_pwrite64 = dlsym(RTLD_NEXT, "pwrite64");
    if (!real_fsync || !real_fdatasync || !real_write || !real_pwrite || !real_pwrite64) _exit(126);
}

/* 0 = normal, 1 = one short write, -1 = injected errno. */
static int fault(int fd, const char *operation, int writing, size_t count) {
    const char *root = getenv("DS_TEST_DATA_ROOT");
    const char *flag = getenv("DS_TEST_FAULT_FILE");
    const char *log = getenv("DS_TEST_FAULT_LOG");
    if (!root || !flag || !log) return 0;
    char link[64], path[PATH_MAX], buffer[256], mode[32], pattern[160];
    snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
    ssize_t len = readlink(link, path, sizeof(path)-1);
    if (len < 0) return 0;
    path[len] = 0;
    if (strncmp(path, root, strlen(root)) || path[strlen(root)] != '/') return 0;
    pthread_mutex_lock(&fault_lock);
    int file = open(flag, O_RDONLY);
    ssize_t n = file < 0 ? -1 : read(file, buffer, sizeof(buffer)-1);
    if (file >= 0) close(file);
    int result = 0, code = 0;
    if (n > 0) {
        buffer[n] = 0;
        if (sscanf(buffer, "%31s %159s", mode, pattern) == 2 && strstr(path, pattern)) {
            if (!writing && !strcmp(mode,"eio-sync")) code = EIO;
            if (!writing && !strcmp(mode,"eio-dir-sync")) {
                struct stat metadata;
                if (!fstat(fd,&metadata) && S_ISDIR(metadata.st_mode)) code = EIO;
            }
            if (writing && !strcmp(mode,"enospc-write")) code = ENOSPC;
            if (writing && count > 1 && !strcmp(mode,"short-write") && unlink(flag) == 0) result = 1;
        }
    }
    if (code || result) {
        struct timespec time;
        clock_gettime(CLOCK_MONOTONIC, &time);
        char record[PATH_MAX+256];
        int size = snprintf(record, sizeof(record),
            "{\"operation\":\"%s\",\"fault\":\"%s\",\"path\":\"%s\",\"errno\":%d,\"seconds\":%ld,\"nanoseconds\":%ld}\n",
            operation, mode, path, code, time.tv_sec, time.tv_nsec);
        file = open(log, O_CREAT|O_WRONLY|O_APPEND, 0600);
        if (file < 0 || real_write(file, record, size) != size) _exit(125);
        close(file);
    }
    pthread_mutex_unlock(&fault_lock);
    if (code) { errno = code; return -1; }
    return result;
}

int fsync(int fd) {
    pthread_once(&once, resolve);
    return fault(fd,"fsync",0,0) < 0 ? -1 : real_fsync(fd);
}
int fdatasync(int fd) {
    pthread_once(&once, resolve);
    return fault(fd,"fdatasync",0,0) < 0 ? -1 : real_fdatasync(fd);
}
ssize_t write(int fd, const void *buf, size_t count) {
    pthread_once(&once, resolve);
    int action = fault(fd,"write",1,count);
    if (action < 0) return -1;
    return real_write(fd,buf,action == 1 ? count/2 : count);
}
ssize_t pwrite(int fd, const void *buf, size_t count, off_t pos) {
    pthread_once(&once, resolve);
    int action = fault(fd,"pwrite",1,count);
    if (action < 0) return -1;
    return real_pwrite(fd,buf,action == 1 ? count/2 : count,pos);
}
ssize_t pwrite64(int fd, const void *buf, size_t count, off64_t pos) {
    pthread_once(&once, resolve);
    int action = fault(fd,"pwrite64",1,count);
    if (action < 0) return -1;
    return real_pwrite64(fd,buf,action == 1 ? count/2 : count,pos);
}
