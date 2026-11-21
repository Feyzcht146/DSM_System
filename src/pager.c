// Paginador del DSM: reserva el espacio global, controla los permisos de cada página y
// atiende los fallos de página. El contenido y el permiso los concede el home (coherence.c).
#define _GNU_SOURCE   // REG_ERR, memfd_create, MAP_FIXED_NOREPLACE, pipe2, fallocate
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <time.h>
#include <ucontext.h>
#include <unistd.h>

#include "coherence.h"
#include "pager.h"
#include "stats.h"

// El kernel garantiza que una escritura de hasta PIPE_BUF bytes en un pipe es atómica.
// Así los avisos de varios hilos que fallan a la vez nunca se mezclan en fault_pipe.
_Static_assert(sizeof(fault_record_t) <= PIPE_BUF, "fault_record_t debe caber en PIPE_BUF");

// Estado del paginador. Solo pager_init y pager_shutdown lo escriben, y lo hacen cuando ningún
// otro hilo usa el DSM. El resto del tiempo es de solo lectura, por eso el manejador de señales
// lo lee sin locks. Excepciones: pages[] (protegida por el lock de cada página) y el tid de cada
// slot (atómico).
static int       ready;
static uint16_t  self_node, node_count;
static uint32_t  page_size, page_shift, n_pages;
static uint32_t  first_page[DSM_MAX_NODES], node_pages[DSM_MAX_NODES];
static uintptr_t base;
static size_t    total_bytes;       // bytes del espacio global

static int   memfd = -1;            // contenido de las páginas
static char* app_view;              // permisos según el estado MSI; la usa la aplicación
static char* sys_view;              // siempre lectura/escritura; solo para el propio DSM e hilos internos
static local_page_t* pages;         // una entrada por página global
static uint32_t n_locks;            // mutexes de pages[] ya inicializados

static thread_slot_t slots[DSM_MAX_THREADS];
static uint16_t n_slots;            // slots con su pipe ya creado

static int fault_pipe[2] = { -1, -1 };   // manejador -> servicio de fallos
static pthread_t fault_threads[UINT8_MAX];
static int n_fault_threads;
static struct sigaction old_action;      // acción de SIGSEGV previa al DSM; se restaura al salir
static int handler_installed;

// __thread: cada hilo tiene su propia copia de estas variables
static __thread int my_slot = -1;   // slot del hilo actual; -1 = todavía sin slot
static __thread int32_t my_tid;     // id de este hilo en el kernel; 0 = sin consultar

/* ----- manejador de SIGSEGV: solo funciones async-signal-safe ----- */

// Escribe el mensaje y aborta. Usa write porque printf no es seguro en un manejador.
static void die(const char *msg) {
    size_t len = 0;
    while (msg[len]) len++;
    ssize_t n = write(STDERR_FILENO, msg, len);
    (void)n;
    abort();
}

// Devuelve si el acceso que falló fue lectura o escritura. En x86_64 el procesador lo indica
// en el bit 1 del código de error (1 = escritura); en otra arquitectura devuelve ACCESS_UNKNOWN
// y handle_fault lo deduce del estado de la página.
static uint8_t fault_access(const void *context) {
#if defined(__x86_64__)
    const ucontext_t *uc = context;
    return (uc->uc_mcontext.gregs[REG_ERR] & 2) ? ACCESS_WRITE : ACCESS_READ;
#else
    (void)context;
    return ACCESS_UNKNOWN;
#endif
}

// Asigna un slot al hilo actual y lo deja en my_slot. Un slot pertenece al hilo cuyo tid
// guarda (0 = libre). Se llama desde el manejador de SIGSEGV, por eso solo usa atómicos y
// syscall. Devuelve 0, o -1 si todos los slots son de hilos que siguen vivos.
static int claim_slot(void) {
    if (!my_tid) my_tid = (int32_t)syscall(SYS_gettid);

    // 1) Ya hay un slot con este tid: lo dejó un hilo muerto cuyo id el kernel reusó
    for (int i = 0; i < n_slots; i++) {
        if (atomic_load(&slots[i].tid) != my_tid) continue;
        my_slot = i;
        return 0;
    }
    // 2) Toma el primer slot libre
    for (int i = 0; i < n_slots; i++) {
        int32_t owner = 0;
        if (!atomic_compare_exchange_strong(&slots[i].tid, &owner, my_tid)) continue;
        my_slot = i;
        return 0;
    }
    // 3) No hay libres: toma el de un hilo que ya terminó sin desregistrarse.
    // tgkill con señal 0 no envía nada, solo consulta: ESRCH significa que el hilo no existe.
    pid_t pid = getpid();
    for (int i = 0; i < n_slots; i++) {
        int32_t owner = atomic_load(&slots[i].tid);
        if (syscall(SYS_tgkill, pid, owner, 0) == 0 || errno != ESRCH) continue;
        if (!atomic_compare_exchange_strong(&slots[i].tid, &owner, my_tid)) continue;
        my_slot = i;
        return 0;
    }
    return -1;
}

// 1 si el hilo actual tiene un slot y sigue siendo suyo
static int has_slot(void) {
    return my_slot >= 0 && atomic_load(&slots[my_slot].tid) == my_tid;
}

// Manejador de SIGSEGV. Avisa el fallo al servicio de fallos y bloquea al hilo hasta que la
// página tenga el permiso necesario.
static void sigsegv_handler(int sig, siginfo_t *info, void *context) {
    (void)sig;
    int saved_errno = errno;        // el manejador no debe alterar el errno del código interrumpido
    uintptr_t addr = (uintptr_t)info->si_addr;   // dirección que se intentó tocar

    // Fallo fuera del DSM (un error real del programa): restaura la acción previa y retorna.
    // La instrucción se repite, vuelve a fallar y esta vez el proceso termina normalmente.
    if (addr < base || addr - base >= total_bytes) {
        sigaction(SIGSEGV, &old_action, NULL);
        errno = saved_errno;
        return;
    }
    // El hilo toma su slot la primera vez que falla: la app no tiene que registrarlo antes
    if (!has_slot() && claim_slot()) die("DSM: demasiados hilos usan el DSM (max_threads)\n");

    // Aviso al servicio de fallos: qué página, qué acceso y a qué slot responder
    fault_record_t rec = {
        .page   = (uint32_t)((addr - base) >> page_shift),
        .access = fault_access(context),
        .slot   = (uint16_t)my_slot,
        .addr   = addr,
    };
    ssize_t n;
    do n = write(fault_pipe[1], &rec, sizeof rec); while (n < 0 && errno == EINTR);
    if (n != (ssize_t)sizeof rec) die("DSM: no se pudo avisar el fallo de página\n");

    // El hilo queda bloqueado en este read hasta que el servicio de fallos escriba en su pipe
    char ok = 0;
    do n = read(slots[my_slot].reply_r, &ok, 1); while (n < 0 && errno == EINTR);
    if (n != 1 || !ok) die("DSM: no se pudo resolver el fallo de página\n");

    errno = saved_errno;            // al retornar se reintenta la instrucción que falló
}

/* ----- servicio de fallos: hilos normales ----- */

// Devuelve el nodo home de la página (el que la aporta al espacio global)
static uint16_t page_home(uint32_t page) {
    for (uint16_t n = 0; n < node_count; n++)
        if (page >= first_page[n] && page - first_page[n] < node_pages[n]) return n;
    return UINT16_MAX;
}

// 1 si el estado ya permite el acceso: M permite todo, S solo lectura, I nada
static int access_allowed(uint8_t state, uint8_t access) {
    return state == PAGE_MODIFIED || (state == PAGE_SHARED && access == ACCESS_READ);
}

// Cambia el estado MSI de la página y su permiso en app_view (M = rw, S = r, I = ninguno).
// Se llama con el lock de la página tomado, para que estado y permiso nunca difieran.
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

// Pide la página a su home con el acceso indicado. Al volver, el contenido vigente ya está en
// el memfd (el listener lo copió por sys_view). Devuelve 0 si se logró, -1 si no.
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

// Despierta al hilo del slot escribiendo en su pipe: 1 = reintentar el acceso, 0 = abortar
static void wake(uint16_t slot, char ok) {
    ssize_t n;
    do n = write(slots[slot].reply_w, &ok, 1); while (n < 0 && errno == EINTR);
    if (n != 1) perror("pager: no se pudo despertar al hilo");
}

// Resuelve un fallo: consigue la página con el permiso pedido y despierta a los hilos que
// esperaban por ella. Varios hilos que fallan en la misma página generan una sola solicitud.
static void handle_fault(const fault_record_t *rec) {
    local_page_t *pg = &pages[rec->page];
    uint64_t bit = 1ULL << rec->slot;   // bit de este hilo en waiters_mask

    pthread_mutex_lock(&pg->lock);
    // Sin el dato de la arquitectura: una página en I se pide para lectura. Si ya se podía
    // leer, el fallo solo pudo ser una escritura (escribir en I cuesta entonces dos fallos).
    uint8_t access = rec->access;
    if (access == ACCESS_UNKNOWN)
        access = pg->state == PAGE_INVALID ? ACCESS_READ : ACCESS_WRITE;
    if (access == ACCESS_WRITE) pg->write_faults++; else pg->read_faults++;
    atomic_fetch_add(access == ACCESS_WRITE ? &dsm_counters.write_faults : &dsm_counters.read_faults, 1);

    // Otro fallo ya dejó el permiso necesario mientras este aviso esperaba en el pipe
    if (access_allowed(pg->state, access)) {
        pthread_mutex_unlock(&pg->lock);
        wake(rec->slot, 1);
        return;
    }
    // Ya hay una solicitud en curso para esta página: el hilo se suma a la espera y no pide
    // nada; lo despertará quien la resuelva
    if (pg->pending) {
        pg->waiters_mask |= bit;
        pthread_mutex_unlock(&pg->lock);
        return;
    }
    // Este fallo es el primero: marca la página como pendiente y hace la solicitud
    pg->pending = 1;
    pg->pending_access = access;
    pg->waiters_mask = bit;
    pthread_mutex_unlock(&pg->lock);

    uint32_t req_id = 0;
    struct timespec t0, t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);
    // Sin lock mientras espera al home: otros fallos de esta página pueden sumarse a waiters_mask
    int ok = obtain_page(rec, access, &req_id) == 0;

    pthread_mutex_lock(&pg->lock);
    if (ok) ok = set_page_state(rec->page, pg->pending_access == ACCESS_WRITE ? PAGE_MODIFIED
                                                                              : PAGE_SHARED) == 0;
    uint64_t waiters = pg->waiters_mask;
    pg->waiters_mask = 0;
    pg->pending = 0;
    pthread_mutex_unlock(&pg->lock);
    clock_gettime(CLOCK_MONOTONIC, &t1);
    stats_fault((uint64_t)((t1.tv_sec - t0.tv_sec) * 1000000000LL + (t1.tv_nsec - t0.tv_nsec)));

    // Despierta a todos los que esperaban. Quien quería escribir y solo se obtuvo lectura
    // vuelve a fallar al reintentar y hace su propia solicitud de escritura.
    for (uint16_t slot = 0; slot < DSM_MAX_THREADS; slot++)
        if (waiters >> slot & 1) wake(slot, (char)ok);

    // Confirma al home después de despertar a los hilos. El home no atiende otra solicitud de
    // esta página hasta recibir la confirmación; si confirmara antes, otro nodo podría quitarle
    // la página antes de que estos hilos alcancen a usarla.
    coherence_confirm(rec->page, req_id, ok);
}

// Hilo del servicio de fallos: lee avisos de fault_pipe y los resuelve uno por uno.
// Hay fault_threads hilos, así fallos de páginas distintas se resuelven en paralelo.
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

// Registro opcional: reserva el slot antes del primer fallo (el manejador lo hace solo)
int pager_thread_register(void) {
    if (!ready || has_slot()) return -1;
    if (claim_slot()) {
        fprintf(stderr, "pager: no hay slots de hilo libres (max_threads=%u)\n", n_slots);
        return -1;
    }
    return 0;
}

// Libera el slot del hilo actual para que otro hilo lo use
int pager_thread_unregister(void) {
    if (!ready || !has_slot()) return -1;
    atomic_store(&slots[my_slot].tid, 0);
    my_slot = -1;
    return 0;
}

/* ----- consultas ----- */

// Cantidad de slots ocupados (puede incluir hilos muertos que no se desregistraron)
uint32_t pager_thread_count(void) {
    uint32_t n = 0;
    for (int i = 0; i < n_slots; i++)
        if (atomic_load(&slots[i].tid) != 0) n++;
    return n;
}

uint32_t pager_page_count(void) {
    return n_pages;
}

// Dirección de la página en la vista de la aplicación (con permisos MSI)
void *pager_app_addr(uint32_t page) {
    if (!ready || page >= n_pages) return NULL;
    return app_view + ((size_t)page << page_shift);
}

// Dirección de la página en la vista de sistema (siempre rw); solo para hilos internos
void *pager_sys_addr(uint32_t page) {
    if (!ready || page >= n_pages) return NULL;
    return sys_view + ((size_t)page << page_shift);
}

// Primera página y cantidad de páginas que aporta el nodo al espacio global
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

// Copia el estado local de la página bajo su lock (sin copiar el mutex)
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

// Baja la página a un estado menor (M -> S, M/S -> I) cuando el home lo ordena.
// Nunca sube el estado. Devuelve el estado que tenía antes.
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

// Invalida un rango de páginas y borra su contenido (se usa al liberar una asignación)
void pager_drop_range(uint32_t first, uint32_t count) {
    if (!ready || first >= n_pages || count > n_pages - first) return;
    for (uint32_t p = first; p < first + count; p++) {
        pthread_mutex_lock(&pages[p].lock);
        if (pages[p].state != PAGE_INVALID) set_page_state(p, PAGE_INVALID);
        pthread_mutex_unlock(&pages[p].lock);
    }
    // Devuelve al sistema la RAM de esas páginas; si se vuelven a usar, empiezan en cero.
    // KEEP_SIZE: el memfd conserva su tamaño, solo se vacía ese tramo.
    if (fallocate(memfd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                  (off_t)first << page_shift, (off_t)count << page_shift))
        perror("pager: fallocate");
}

/* ----- inicio y liberación ----- */

// Libera lo que exista y deja todo como antes de pager_init. Sirve para pager_shutdown y
// para un pager_init que falla a medias: cada paso revisa si su recurso llegó a crearse.
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
        close(slots[i].reply_r);
        close(slots[i].reply_w);
        atomic_store(&slots[i].tid, 0);
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

// Arma el espacio global: las páginas de cada nodo, una tras otra en orden de node_id.
// Ej.: nodo 0 con 256 páginas y nodo 1 con 128 -> nodo 0 = páginas 0..255, nodo 1 = 256..383.
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

// Crea el memfd y lo mapea dos veces: app_view en base_address (sin permisos al inicio) y
// sys_view donde el kernel elija (siempre rw).
static int map_views(void) {
    // ftruncate fija el tamaño; la RAM se ocupa recién cuando cada página se toca por primera vez
    memfd = memfd_create("dsm", MFD_CLOEXEC);
    if (memfd < 0 || ftruncate(memfd, (off_t)total_bytes)) {
        perror("pager: memfd");
        return -1;
    }
    // app_view va en base_address, la misma en todos los nodos: así un puntero al DSM vale lo
    // mismo en cualquier nodo. MAP_FIXED_NOREPLACE falla si la zona está ocupada, en vez de
    // pisar otro mapeo.
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

// Crea los pipes, instala el manejador de SIGSEGV y arranca los hilos del servicio de fallos
static int start_fault_handling(uint8_t n_threads, uint16_t max_threads) {
    if (pipe2(fault_pipe, O_CLOEXEC)) {
        fault_pipe[0] = fault_pipe[1] = -1;
        perror("pager: pipe");
        return -1;
    }
    // Un pipe de respuesta por slot. Se crean todos aquí porque el manejador no puede crearlos.
    while (n_slots < max_threads) {
        int fds[2];
        if (pipe2(fds, O_CLOEXEC)) {
            perror("pager: pipe");
            return -1;
        }
        slots[n_slots].reply_r = fds[0];
        slots[n_slots].reply_w = fds[1];
        n_slots++;
    }
    struct sigaction action;
    memset(&action, 0, sizeof action);
    action.sa_sigaction = sigsegv_handler;
    action.sa_flags = SA_SIGINFO;   // pasa siginfo (dirección) y contexto (código de error)
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

// Inicia el paginador del nodo self: valida la configuración, reserva el espacio global,
// crea la tabla de páginas y empieza a atender fallos. Si algo falla, no deja nada abierto.
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

    if (start_fault_handling(c->fault_threads, c->max_threads)) goto fail;

    ready = 1;
    return 0;

fail:
    release_all();
    return -1;
}
