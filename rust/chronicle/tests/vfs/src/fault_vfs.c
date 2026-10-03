/* Test-only, process-global VFS. Register/retarget only with every connection
 * closed. sqlite3_io_methods always receives our FaultFile; parent methods
 * receive real. The one-shot atomics may be armed from the test while the
 * storage actor runs.
 */
#include <sqlite3.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

typedef struct FaultFile {
  sqlite3_file base;
  sqlite3_file *real;
  int scoped;
} FaultFile;

static sqlite3_vfs wrapper;
static sqlite3_vfs *parent;
static char *target;
static _Atomic int armed_write;
static _Atomic int armed_sync;
static _Atomic int fired_write;
static _Atomic int fired_sync;

static FaultFile *ff(sqlite3_file *f) { return (FaultFile *)f; }
static int consume(_Atomic int *armed, _Atomic int *fired) {
  int expected = 1;
  if (atomic_compare_exchange_strong(armed, &expected, 0)) {
    atomic_fetch_add(fired, 1);
    return 1;
  }
  return 0;
}
static int scoped_name(const char *name) {
  if (!name || !target)
    return 0;
  size_t n = strlen(target);
  return strncmp(name, target, n) == 0 &&
         (name[n] == '\0' || strcmp(name + n, "-wal") == 0 ||
          strcmp(name + n, "-journal") == 0);
}

#define REAL(f) (ff(f)->real)
static int io_close(sqlite3_file *f) {
  int rc = REAL(f)->pMethods->xClose(REAL(f));
  free(REAL(f));
  return rc;
}
static int io_read(sqlite3_file *f, void *b, int n, sqlite3_int64 o) {
  return REAL(f)->pMethods->xRead(REAL(f), b, n, o);
}
static int io_write(sqlite3_file *f, const void *b, int n, sqlite3_int64 o) {
  if (ff(f)->scoped && consume(&armed_write, &fired_write))
    return SQLITE_IOERR_WRITE;
  return REAL(f)->pMethods->xWrite(REAL(f), b, n, o);
}
static int io_truncate(sqlite3_file *f, sqlite3_int64 n) {
  return REAL(f)->pMethods->xTruncate(REAL(f), n);
}
static int io_sync(sqlite3_file *f, int flags) {
  if (ff(f)->scoped && consume(&armed_sync, &fired_sync))
    return SQLITE_IOERR_FSYNC;
  return REAL(f)->pMethods->xSync(REAL(f), flags);
}
static int io_size(sqlite3_file *f, sqlite3_int64 *n) {
  return REAL(f)->pMethods->xFileSize(REAL(f), n);
}
static int io_lock(sqlite3_file *f, int n) {
  return REAL(f)->pMethods->xLock(REAL(f), n);
}
static int io_unlock(sqlite3_file *f, int n) {
  return REAL(f)->pMethods->xUnlock(REAL(f), n);
}
static int io_reserved(sqlite3_file *f, int *n) {
  return REAL(f)->pMethods->xCheckReservedLock(REAL(f), n);
}
static int io_control(sqlite3_file *f, int op, void *arg) {
  return REAL(f)->pMethods->xFileControl(REAL(f), op, arg);
}
static int io_sector(sqlite3_file *f) {
  return REAL(f)->pMethods->xSectorSize(REAL(f));
}
static int io_device(sqlite3_file *f) {
  return REAL(f)->pMethods->xDeviceCharacteristics(REAL(f));
}
static int io_shm_map(sqlite3_file *f, int p, int s, int e, void volatile **v) {
  return REAL(f)->pMethods->xShmMap(REAL(f), p, s, e, v);
}
static int io_shm_lock(sqlite3_file *f, int o, int n, int flags) {
  return REAL(f)->pMethods->xShmLock(REAL(f), o, n, flags);
}
static void io_shm_barrier(sqlite3_file *f) {
  REAL(f)->pMethods->xShmBarrier(REAL(f));
}
static int io_shm_unmap(sqlite3_file *f, int del) {
  return REAL(f)->pMethods->xShmUnmap(REAL(f), del);
}
static int io_fetch(sqlite3_file *f, sqlite3_int64 o, int n, void **p) {
  return REAL(f)->pMethods->xFetch(REAL(f), o, n, p);
}
static int io_unfetch(sqlite3_file *f, sqlite3_int64 o, void *p) {
  return REAL(f)->pMethods->xUnfetch(REAL(f), o, p);
}

static const sqlite3_io_methods methods_v3 = {
    3,         io_close,   io_read,     io_write,       io_truncate,  io_sync,
    io_size,   io_lock,    io_unlock,   io_reserved,    io_control,   io_sector,
    io_device, io_shm_map, io_shm_lock, io_shm_barrier, io_shm_unmap, io_fetch,
    io_unfetch};
static const sqlite3_io_methods methods_v2 = {
    2,         io_close,   io_read,     io_write,       io_truncate,  io_sync,
    io_size,   io_lock,    io_unlock,   io_reserved,    io_control,   io_sector,
    io_device, io_shm_map, io_shm_lock, io_shm_barrier, io_shm_unmap, NULL,
    NULL};
static const sqlite3_io_methods methods_v1 = {
    1,          io_close,  io_read,   io_write,  io_truncate,
    io_sync,    io_size,   io_lock,   io_unlock, io_reserved,
    io_control, io_sector, io_device, NULL,      NULL,
    NULL,       NULL,      NULL,      NULL};

static int vfs_open(sqlite3_vfs *v, const char *name, sqlite3_file *f,
                    int flags, int *out) {
  (void)v;
  FaultFile *file = (FaultFile *)f;
  memset(file, 0, sizeof(*file));
  file->real = calloc(1, (size_t)parent->szOsFile);
  if (!file->real)
    return SQLITE_NOMEM;
  int rc = parent->xOpen(parent, name, file->real, flags, out);
  if (rc != SQLITE_OK) {
    if (file->real->pMethods)
      file->real->pMethods->xClose(file->real);
    free(file->real);
    file->real = NULL;
    return rc;
  }
  file->scoped = scoped_name(name);
  file->base.pMethods =
      file->real->pMethods->iVersion >= 3
          ? &methods_v3
          : (file->real->pMethods->iVersion == 2 ? &methods_v2 : &methods_v1);
  return SQLITE_OK;
}

int chronicle_fault_vfs_register(void) {
  if (parent)
    return SQLITE_OK;
  parent = sqlite3_vfs_find(NULL);
  if (!parent)
    return SQLITE_NOTFOUND;
  wrapper = *parent;
  wrapper.pNext = NULL;
  wrapper.zName = "chronicle-fault";
  wrapper.szOsFile = (int)sizeof(FaultFile);
  wrapper.xOpen = vfs_open;
  return sqlite3_vfs_register(&wrapper, 1);
}
int chronicle_fault_vfs_target(const char *path) {
  char *copy;
  if (!path)
    return SQLITE_MISUSE;
  copy = malloc(strlen(path) + 1);
  if (!copy)
    return SQLITE_NOMEM;
  strcpy(copy, path);
  free(target);
  target = copy;
  atomic_store(&armed_write, 0);
  atomic_store(&armed_sync, 0);
  atomic_store(&fired_write, 0);
  atomic_store(&fired_sync, 0);
  return SQLITE_OK;
}
void chronicle_fault_vfs_arm(int kind) {
  atomic_store(kind == 1 ? &armed_write : &armed_sync, 1);
}
int chronicle_fault_vfs_fired(int kind) {
  return atomic_load(kind == 1 ? &fired_write : &fired_sync);
}
