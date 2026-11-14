// Paginador del DSM: reserva del espacio global, permisos por página y fallos de página.
// El contenido y el permiso de cada página los concede su home (coherence).
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <ucontext.h>
#include <unistd.h>

#include "coherence.h"
#include "pager.h"

// Una escritura de hasta PIPE_BUF bytes en un pipe es atómica: los avisos no se mezclan
_Static_assert(sizeof(fault_record_t) <= PIPE_BUF, "fault_record_t debe caber en PIPE_BUF");

// Estado del paginador. Lo escriben solo pager_init y pager_shutdown, cuando ningún otro
// hilo usa el DSM; después es de solo lectura (por eso el manejador puede leerlo), salvo
// pages[] (lock de cada página) y slots[] (slots_lock).
static int       ready;
static uint16_t  self_node, node_count;
static uint32_t  page_size, page_shift, n_pages;
static uint32_t  first_page[DSM_MAX_NODES], node_pages[DSM_MAX_NODES];
static uintptr_t base;
static size_t    total_bytes;       // bytes del espacio global

static int   memfd = -1;            // contenido de las páginas, mapeado dos veces
static char *app_view;              // permisos según el estado MSI; la usa la aplicación
static char *sys_view;              // siempre lectura/escritura; solo hilos internos
static local_page_t *pages;         // una entrada por página global
static uint32_t n_locks;            // mutexes de pages[] ya inicializados

static thread_slot_t slots[DSM_MAX_THREADS];
static uint16_t n_slots;
static pthread_mutex_t slot_lock = PTHREAD_MUTEX_INITIALIZER;

static int fault_pipe[2] = { -1, -1 };   // manejador -> servicio de fallos
static pthread_t fault_threads[UINT8_MAX];
static int n_fault_threads;
static struct sigaction old_action;
static int handler_installed;

static __thread int my_slot = -1;   // slot del hilo actual; -1 = no registrado

/* ----- manejador de SIGSEGV: solo funciones async-signal-safe ----- */

static void die(const char *msg) {
    size_t len = 0;
    while (msg[len]) len++;
    ssize_t n = write(STDERR_FILENO, msg, len);
    (void)n;
    abort();
}

// Tipo de acceso que falló. x86_64 lo informa en el bit 1 del código de error;
// en otra arquitectura lo deduce el servicio de fallos a partir del estado de la página.
static uint8_t fault_access(const void *context) {
#if defined(__x86_64__)
    const ucontext_t *uc = context;
    return (uc->uc_mcontext.gregs[REG_ERR] & 2) ? ACCESS_WRITE : ACCESS_READ;
#else
    (void)context;
    return ACCESS_UNKNOWN;
#endif
}

static void sigsegv_handler(int sig, siginfo_t *info, void *context) {
    (void)sig;
    int saved_errno = errno;
    uintptr_t addr = (uintptr_t)info->si_addr;

    // Fallo ajeno al DSM: vuelve la acción previa y el acceso, al repetirse, termina con ella
    if (addr < base || addr - base >= total_bytes) {
        sigaction(SIGSEGV, &old_action, NULL);
        errno = saved_errno;
        return;
    }
    if (my_slot < 0) die("DSM: fallo de página en un hilo no registrado\n");

    fault_record_t rec = {
        .page   = (uint32_t)((addr - base) >> page_shift),
        .access = fault_access(context),
        .slot   = (uint16_t)my_slot,
        .addr   = addr,
    };
    ssize_t n;
    do n = write(fault_pipe[1], &rec, sizeof rec); while (n < 0 && errno == EINTR);
    if (n != (ssize_t)sizeof rec) die("DSM: no se pudo avisar el fallo de página\n");

    // El hilo queda pausado aquí hasta que el servicio de fallos responda
    char ok = 0;
    do n = read(slots[my_slot].reply_r, &ok, 1); while (n < 0 && errno == EINTR);
    if (n != 1 || !ok) die("DSM: no se pudo resolver el fallo de página\n");

    errno = saved_errno;            // al retornar se reintenta la instrucción que falló
}

/* ----- servicio de fallos: hilos normales ----- */

static uint16_t page_home(uint32_t page) {
    for (uint16_t n = 0; n < node_count; n++)
        if (page >= first_page[n] && page - first_page[n] < node_pages[n]) return n;
    return UINT16_MAX;
}

static int access_allowed(uint8_t state, uint8_t access) {
    return state == PAGE_MODIFIED || (state == PAGE_SHARED && access == ACCESS_READ);
}

// Cambia el estado y el permiso juntos; se llama con el lock de la página tomado
static int set_page_state(uint32_t page, uint8_t state) {
    int prot = state == PAGE_MODIFIED ? PROT_READ | PROT_WRITE
             : state == PAGE_SHARED   ? PROT_READ : PROT_NONE;
    if (mprotect(app_view + ((size_t)page << page_shift), page_size, prot)) {
        perror("pager: mprotect");
        return -1;
    }
    pages[page].state = state;
    return 0;
}

// Pide la página a su home con el acceso indicado; al volver, el contenido vigente
// está en el memfd (lo copió el listener por la vista de sistema). 0 si se logró.
static int obtain_page(const fault_record_t *rec, uint8_t access, uint32_t *req_id) {
    int rc = coherence_acquire(rec->page, access, req_id);
    if (rc == ST_OK) return 0;
    if (rc < 0) {   // sin red: las páginas de este nodo ya están en su memfd
        if (page_home(rec->page) == self_node) return 0;
        fprintf(stderr, "pager: la página %u (0x%lx) es de otro nodo y no hay red\n",
                rec->page, (unsigned long)rec->addr);
        return -1;
    }
    fprintf(stderr, "pager: no se pudo obtener la página %u (0x%lx) del nodo %u (error %d)\n",
            rec->page, (unsigned long)rec->addr, page_home(rec->page), rc);
    return -1;
}

static void wake(uint16_t slot, char ok) {
    ssize_t n;
    do n = write(slots[slot].reply_w, &ok, 1); while (n < 0 && errno == EINTR);
    if (n != 1) perror("pager: no se pudo despertar al hilo");
}

static void handle_fault(const fault_record_t *rec) {
    local_page_t *pg = &pages[rec->page];
    uint64_t bit = 1ULL << rec->slot;

    pthread_mutex_lock(&pg->lock);
    // Sin el dato de la arquitectura: una página en I se pide para lectura; si ya se
    // podía leer, el fallo solo pudo ser una escritura (escribir en I cuesta dos fallos)
    uint8_t access = rec->access;
    if (access == ACCESS_UNKNOWN)
        access = pg->state == PAGE_INVALID ? ACCESS_READ : ACCESS_WRITE;
    if (access == ACCESS_WRITE) pg->write_faults++; else pg->read_faults++;

    // Otro fallo ya dejó el permiso necesario
    if (access_allowed(pg->state, access)) {
        pthread_mutex_unlock(&pg->lock);
        wake(rec->slot, 1);
        return;
    }
    // Ya hay un fallo en resolución para esta página: el hilo se suma a la espera
    if (pg->pending) {
        pg->waiters_mask |= bit;
        pthread_mutex_unlock(&pg->lock);
        return;
    }
    pg->pending = 1;
    pg->pending_access = access;
    pg->waiters_mask = bit;
    pthread_mutex_unlock(&pg->lock);

    uint32_t req_id = 0;
    int ok = obtain_page(rec, access, &req_id) == 0;   // sin lock: espera la respuesta del home

    pthread_mutex_lock(&pg->lock);
    if (ok) ok = set_page_state(rec->page, pg->pending_access == ACCESS_WRITE ? PAGE_MODIFIED
                                                                              : PAGE_SHARED) == 0;
    uint64_t waiters = pg->waiters_mask;
    pg->waiters_mask = 0;
    pg->pending = 0;
    pthread_mutex_unlock(&pg->lock);

    // Quien esperaba escribir y solo obtuvo lectura vuelve a fallar al reintentar
    for (uint16_t slot = 0; slot < DSM_MAX_THREADS; slot++)
        if (waiters >> slot & 1) wake(slot, (char)ok);

    // Después de despertar a los hilos: la confirmación le permite al home atender la
    // siguiente solicitud, que puede volver a quitarle la página a este nodo
    coherence_confirm(rec->page, req_id, ok);
}

static void *fault_service(void *arg) {
    (void)arg;
    fault_record_t rec;
    for (;;) {
        ssize_t n = read(fault_pipe[0], &rec, sizeof rec);
        if (n < 0 && errno == EINTR) continue;
        if (n != (ssize_t)sizeof rec) break;   // 0 = pipe cerrado por pager_shutdown
        handle_fault(&rec);
    }
    return NULL;
}

/* ----- registro de hilos ----- */

int pager_thread_register(void) {
    if (!ready || my_slot >= 0) return -1;

    int slot = -1, fds[2];
    pthread_mutex_lock(&slot_lock);
    for (int i = 0; i < n_slots && slot < 0; i++)
        if (!slots[i].in_use) slot = i;
    if (slot >= 0) {
        if (pipe2(fds, O_CLOEXEC)) {
            perror("pager: pipe");
            slot = -1;
        } else {
            slots[slot].reply_r = fds[0];
            slots[slot].reply_w = fds[1];
            slots[slot].tid     = pthread_self();
            slots[slot].in_use  = 1;
        }
    } else {
        fprintf(stderr, "pager: no hay slots de hilo libres (max_threads=%u)\n", n_slots);
    }
    pthread_mutex_unlock(&slot_lock);

    if (slot < 0) return -1;
    my_slot = slot;
    return 0;
}

int pager_thread_unregister(void) {
    if (my_slot < 0) return -1;
    int slot = my_slot;
    my_slot = -1;
    if (!ready) return -1;   // pager_shutdown ya cerró el pipe de este slot

    pthread_mutex_lock(&slot_lock);
    close(slots[slot].reply_r);
    close(slots[slot].reply_w);
    slots[slot].in_use = 0;
    pthread_mutex_unlock(&slot_lock);
    return 0;
}

/* ----- consultas ----- */

uint32_t pager_page_count(void) {
    return n_pages;
}

void *pager_app_addr(uint32_t page) {
    if (!ready || page >= n_pages) return NULL;
    return app_view + ((size_t)page << page_shift);
}

void *pager_sys_addr(uint32_t page) {
    if (!ready || page >= n_pages) return NULL;
    return sys_view + ((size_t)page << page_shift);
}

int pager_node_pages(uint16_t node, uint32_t *first, uint32_t *count) {
    if (!ready || node >= node_count) return -1;
    *first = first_page[node];
    *count = node_pages[node];
    return 0;
}

uint16_t pager_page_home(uint32_t page) {
    if (!ready) return UINT16_MAX;
    return page_home(page);
}

int pager_page_info(uint32_t page, local_page_t *out) {
    if (!ready || page >= n_pages) return -1;
    local_page_t *pg = &pages[page];
    pthread_mutex_lock(&pg->lock);
    out->waiters_mask   = pg->waiters_mask;
    out->state          = pg->state;
    out->pending        = pg->pending;
    out->pending_access = pg->pending_access;
    out->flags          = pg->flags;
    out->read_faults    = pg->read_faults;
    out->write_faults   = pg->write_faults;
    out->invalidations  = pg->invalidations;
    pthread_mutex_unlock(&pg->lock);
    return 0;
}

/* ----- acciones que ordena el home de una página ----- */

int pager_downgrade(uint32_t page, uint8_t state) {
    if (!ready || page >= n_pages) return -1;
    local_page_t *pg = &pages[page];
    pthread_mutex_lock(&pg->lock);
    int old = pg->state;
    if (old > state) {
        set_page_state(page, state);
        if (state == PAGE_INVALID) pg->invalidations++;
    }
    pthread_mutex_unlock(&pg->lock);
    return old;
}

void pager_drop_range(uint32_t first, uint32_t count) {
    if (!ready || first >= n_pages || count > n_pages - first) return;
    for (uint32_t p = first; p < first + count; p++) {
        pthread_mutex_lock(&pages[p].lock);
        if (pages[p].state != PAGE_INVALID) set_page_state(p, PAGE_INVALID);
        pthread_mutex_unlock(&pages[p].lock);
    }
    // Devuelve al sistema la memoria de esas copias; si se vuelven a usar, empiezan en cero
    if (fallocate(memfd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                  (off_t)first << page_shift, (off_t)count << page_shift))
        perror("pager: fallocate");
}

/* ----- inicio y liberación ----- */

// Libera lo que exista y deja el estado como antes de pager_init; sirve para
// pager_shutdown y para un pager_init que falla a medias
static void release_all(void) {
    // Cerrar la escritura del pipe da EOF a los hilos de fallos, que terminan
    if (fault_pipe[1] >= 0) close(fault_pipe[1]);
    fault_pipe[1] = -1;
    for (int i = 0; i < n_fault_threads; i++) pthread_join(fault_threads[i], NULL);
    n_fault_threads = 0;
    if (handler_installed) sigaction(SIGSEGV, &old_action, NULL);
    handler_installed = 0;
    if (fault_pipe[0] >= 0) close(fault_pipe[0]);
    fault_pipe[0] = -1;

    for (int i = 0; i < n_slots; i++) {
        if (!slots[i].in_use) continue;
        close(slots[i].reply_r);
        close(slots[i].reply_w);
        slots[i].in_use = 0;
    }
    n_slots = 0;

    if (app_view) munmap(app_view, total_bytes);
    if (sys_view) munmap(sys_view, total_bytes);
    app_view = sys_view = NULL;
    if (memfd >= 0) close(memfd);
    memfd = -1;

    for (uint32_t p = 0; p < n_locks; p++) pthread_mutex_destroy(&pages[p].lock);
    n_locks = 0;
    free(pages);
    pages = NULL;

    n_pages = 0;
    total_bytes = 0;
    ready = 0;
    my_slot = -1;
}

void pager_shutdown(void) {
    if (ready) release_all();
}

// Espacio global: la memoria de cada nodo, una tras otra en orden de node_id
static int build_layout(const dsm_config_t *c) {
    uint64_t count = 0;
    for (uint16_t n = 0; n < c->node_count; n++) {
        uint64_t in_node = c->nodes[n].memory_bytes >> page_shift;
        if (count + in_node > UINT32_MAX) {
            fprintf(stderr, "pager: el espacio global supera %u páginas\n", UINT32_MAX);
            return -1;
        }
        first_page[n] = (uint32_t)count;
        node_pages[n] = (uint32_t)in_node;
        count += in_node;
    }
    n_pages = (uint32_t)count;
    total_bytes = (size_t)count << page_shift;
    return 0;
}

// El contenido vive en un memfd mapeado dos veces: los hilos internos copian páginas
// por sys_view sin tener que abrir permisos en la vista que usa la aplicación
static int map_views(void) {
    memfd = memfd_create("dsm", MFD_CLOEXEC);
    if (memfd < 0 || ftruncate(memfd, (off_t)total_bytes)) {
        perror("pager: memfd");
        return -1;
    }
    // MAP_FIXED_NOREPLACE: si la zona está ocupada falla, en vez de pisar otro mapeo
    void *app = mmap((void *)base, total_bytes, PROT_NONE,
                     MAP_SHARED | MAP_FIXED_NOREPLACE, memfd, 0);
    if (app == MAP_FAILED) {
        fprintf(stderr, "pager: no se pudo reservar base_address=0x%lx: %s\n",
                (unsigned long)base, strerror(errno));
        return -1;
    }
    app_view = app;
    if ((uintptr_t)app != base) {   // kernel anterior a 4.17: ignora NOREPLACE
        fprintf(stderr, "pager: el kernel no mapeó en base_address=0x%lx\n",
                (unsigned long)base);
        return -1;
    }
    void *sys = mmap(NULL, total_bytes, PROT_READ | PROT_WRITE, MAP_SHARED, memfd, 0);
    if (sys == MAP_FAILED) {
        perror("pager: mmap de la vista de sistema");
        return -1;
    }
    sys_view = sys;
    return 0;
}

static int start_fault_handling(uint8_t n_threads) {
    if (pipe2(fault_pipe, O_CLOEXEC)) {
        fault_pipe[0] = fault_pipe[1] = -1;
        perror("pager: pipe");
        return -1;
    }
    struct sigaction action;
    memset(&action, 0, sizeof action);
    action.sa_sigaction = sigsegv_handler;
    action.sa_flags = SA_SIGINFO;
    sigemptyset(&action.sa_mask);
    if (sigaction(SIGSEGV, &action, &old_action)) {
        perror("pager: sigaction");
        return -1;
    }
    handler_installed = 1;

    for (int i = 0; i < n_threads; i++) {
        int err = pthread_create(&fault_threads[i], NULL, fault_service, NULL);
        if (err) {
            fprintf(stderr, "pager: no se pudo crear un hilo de fallos: %s\n", strerror(err));
            return -1;
        }
        n_fault_threads++;
    }
    return 0;
}

int pager_init(const dsm_config_t *c, uint16_t self) {
    if (ready) {
        fprintf(stderr, "pager: ya está inicializado\n");
        return -1;
    }
    if (self >= c->node_count) {
        fprintf(stderr, "pager: el nodo %u no existe (hay %u nodos)\n", self, c->node_count);
        return -1;
    }
    if (c->max_threads == 0 || c->max_threads > DSM_MAX_THREADS) {
        fprintf(stderr, "pager: max_threads debe estar entre 1 y %d\n", DSM_MAX_THREADS);
        return -1;
    }
    if (c->fault_threads == 0) {
        fprintf(stderr, "pager: fault_threads debe ser al menos 1\n");
        return -1;
    }

    self_node  = self;
    node_count = c->node_count;
    page_size  = c->page_size;
    page_shift = (uint32_t)__builtin_ctz(c->page_size);   // page_size es potencia de 2
    base       = (uintptr_t)c->base_address;
    n_slots    = c->max_threads;

    if (build_layout(c) || map_views()) goto fail;

    pages = calloc(n_pages, sizeof *pages);   // calloc: todas en PAGE_INVALID
    if (!pages) {
        perror("pager: tabla de páginas");
        goto fail;
    }
    for (uint32_t p = 0; p < n_pages; p++) {
        if (pthread_mutex_init(&pages[p].lock, NULL)) {
            fprintf(stderr, "pager: no se pudo crear el lock de la página %u\n", p);
            goto fail;
        }
        n_locks++;
    }

    if (start_fault_handling(c->fault_threads)) goto fail;

    ready = 1;
    if (pager_thread_register()) goto fail;   // el hilo que inicializa queda registrado
    return 0;

fail:
    release_all();
    return -1;
}
