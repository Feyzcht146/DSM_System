// API del DSM (include/dsm.h): la única interfaz que usan las aplicaciones. Cada función
// valida el estado y los argumentos y llama a los módulos internos.
#include <pthread.h>
#include <signal.h>
#include <stdlib.h>
#include <string.h>

#include "alloc.h"
#include "coherence.h"
#include "config.h"
#include "dsm.h"
#include "network.h"
#include "node.h"
#include "pager.h"
#include "stats.h"

static dsm_config_t config;
static int running;                 // 1 entre dsm_init y dsm_shutdown
static int self_id;
// init y shutdown lo toman para escribir; las demás funciones, para leer. Así varios hilos
// usan la API a la vez, pero el DSM nunca se libera mientras otro hilo está dentro de una llamada.
static pthread_rwlock_t state_lock = PTHREAD_RWLOCK_INITIALIZER;
// Error de la última función que devuelve puntero (NULL no dice por qué falló). __thread: cada
// hilo ve su propio error, como errno.
static __thread int last_error;

// Elige el nodo para DSM_NODE_AUTO: el de la variable DSM_NODE, o la primera línea node cuya
// IP es de este equipo y cuyos puertos están libres. -1 si no hay ninguno.
static int choose_node(void) {
    const char *env = getenv("DSM_NODE");
    if (env && *env) {
        char *end;
        unsigned long id = strtoul(env, &end, 10);
        return *end == '\0' && id < config.node_count ? (int)id : -1;
    }
    for (int id = 0; id < config.node_count; id++) {
        const node_config_t *n = &config.nodes[id];
        if (net_can_listen(n->host, n->data_port) && net_can_listen(n->host, n->hb_port)) return id;
    }
    return -1;
}

// Arranca los módulos en orden de dependencia (paginador -> tablas -> red -> unión). Si uno
// falla, detiene los anteriores y no deja nada a medias.
static int start_node(uint16_t id) {
    if (pager_init(&config, id)) return DSM_ERR_MEMORY;
    stats_init(&config, id);

    int rc = DSM_OK;
    if (node_init(&config, id) || coherence_init(&config, id) || alloc_init(&config, id)) rc = DSM_ERR_MEMORY;
    else if (net_init(&config, id)) rc = DSM_ERR_NETWORK;
    else if (node_join_cluster()) {
        rc = DSM_ERR_REJECTED;
        node_leave_cluster();
        net_shutdown();
    }
    if (rc != DSM_OK) {
        alloc_shutdown();
        coherence_shutdown();
        node_shutdown();
        pager_shutdown();
    }
    return rc;
}

// Carga la configuración, elige el nodo y lo arranca. DSM_OK o un código de error.
int dsm_init(const char *config_path, int node_id) {
    pthread_rwlock_wrlock(&state_lock);
    int rc = DSM_OK;
    if (running) rc = DSM_ERR_STATE;
    else if (dsm_config_load(config_path, &config)) rc = DSM_ERR_CONFIG;
    else {
        if (node_id == DSM_NODE_AUTO) node_id = choose_node();
        if (node_id < 0 || node_id >= config.node_count) rc = DSM_ERR_NODE;
    }
    if (rc == DSM_OK) {
        dsm_log_setup(config.log_level, (uint16_t)node_id);

        // Los hilos internos heredan la máscara de señales del hilo que los crea. Se bloquean
        // todas mientras se crean: así Ctrl+C y kill los recibe solo la aplicación. SIGSEGV y
        // las demás señales de fallo no se bloquean porque las provoca la propia instrucción.
        // Al final se restaura la máscara original del hilo que llamó a dsm_init.
        sigset_t internal, previous;
        sigfillset(&internal);
        sigdelset(&internal, SIGSEGV);
        sigdelset(&internal, SIGBUS);
        sigdelset(&internal, SIGFPE);
        sigdelset(&internal, SIGILL);
        pthread_sigmask(SIG_BLOCK, &internal, &previous);
        rc = start_node((uint16_t)node_id);
        pthread_sigmask(SIG_SETMASK, &previous, NULL);
    }
    if (rc == DSM_OK) {
        self_id = node_id;
        running = 1;
    }
    pthread_rwlock_unlock(&state_lock);
    return rc;
}

// Salida ordenada: libera las asignaciones sin nombre, avisa a los demás nodos y detiene todo
// en orden inverso al de inicio
int dsm_shutdown(void) {
    pthread_rwlock_wrlock(&state_lock);
    int rc = running ? DSM_OK : DSM_ERR_STATE;
    if (running) {
        alloc_release_own();    // las asignaciones sin nombre terminan con el proceso
        node_leave_cluster();   // avisa y espera a que los demás recuperen sus páginas
        net_shutdown();
        alloc_shutdown();
        coherence_shutdown();
        node_shutdown();
        pager_shutdown();
        running = 0;
        dsm_log(LOG_INFO, "nodo detenido");
    }
    pthread_rwlock_unlock(&state_lock);
    return rc;
}

int dsm_node_id(void) {
    pthread_rwlock_rdlock(&state_lock);
    int id = running ? self_id : DSM_ERR_STATE;
    pthread_rwlock_unlock(&state_lock);
    return id;
}

int dsm_thread_register(void) {
    pthread_rwlock_rdlock(&state_lock);
    int rc = !running ? DSM_ERR_STATE : pager_thread_register() ? DSM_ERR_THREADS : DSM_OK;
    pthread_rwlock_unlock(&state_lock);
    return rc;
}

int dsm_thread_unregister(void) {
    pthread_rwlock_rdlock(&state_lock);
    int rc = !running ? DSM_ERR_STATE : pager_thread_unregister() ? DSM_ERR_THREADS : DSM_OK;
    pthread_rwlock_unlock(&state_lock);
    return rc;
}

/* ----- asignación de memoria ----- */

// Un nombre válido no está vacío y cabe en DSM_NAME_MAX caracteres
static int valid_name(const char *name) {
    return name && name[0] != '\0' && strlen(name) <= DSM_NAME_MAX;
}

// Crea la asignación y devuelve su dirección; con el state_lock ya tomado
static void *create(size_t size, const char *tag, uint16_t flags) {
    alloc_record_t r;
    if (alloc_create(size, tag, flags, &r)) {
        last_error = DSM_ERR_NOMEM;
        return NULL;
    }
    return pager_app_addr(r.first_page);
}

// Asigna size bytes de memoria compartida; tag es solo una etiqueta para las consultas.
// El puntero vale lo mismo en todos los nodos (todos mapean el espacio en base_address).
void *dsm_alloc(size_t size, const char *tag) {
    void *ptr = NULL;
    pthread_rwlock_rdlock(&state_lock);
    if (!running) last_error = DSM_ERR_STATE;
    else if (size == 0 || (tag && strlen(tag) > DSM_NAME_MAX)) last_error = DSM_ERR_ARG;
    else ptr = create(size, tag, 0);
    pthread_rwlock_unlock(&state_lock);
    return ptr;
}

// Asigna con un nombre único en el clúster, para que otro proceso la encuentre con dsm_attach.
// Sobrevive al proceso que la creó hasta que alguien llame a dsm_free.
void *dsm_alloc_named(const char *name, size_t size) {
    void *ptr = NULL;
    alloc_record_t r;
    pthread_rwlock_rdlock(&state_lock);
    if (!running) last_error = DSM_ERR_STATE;
    else if (size == 0 || !valid_name(name)) last_error = DSM_ERR_ARG;
    else if (alloc_find(name, &r) == 0) last_error = DSM_ERR_EXISTS;
    else ptr = create(size, name, ALLOC_NAMED);
    pthread_rwlock_unlock(&state_lock);
    return ptr;
}

// Busca una asignación con nombre creada por otro proceso y devuelve su dirección y tamaño
void *dsm_attach(const char *name, size_t *size) {
    void *ptr = NULL;
    alloc_record_t r;
    pthread_rwlock_rdlock(&state_lock);
    if (!running) last_error = DSM_ERR_STATE;
    else if (!valid_name(name)) last_error = DSM_ERR_ARG;
    else if (alloc_find(name, &r)) last_error = DSM_ERR_NOTFOUND;
    else if (alloc_attach(&r)) last_error = DSM_ERR_MEMORY;
    else {
        ptr = pager_app_addr(r.first_page);
        if (size) *size = (size_t)r.n_pages * config.page_size;
    }
    pthread_rwlock_unlock(&state_lock);
    return ptr;
}

// Libera la asignación que empieza en ptr. ptr debe ser el inicio de una página del espacio DSM.
int dsm_free(void *ptr) {
    pthread_rwlock_rdlock(&state_lock);
    int rc = DSM_OK;
    uintptr_t addr = (uintptr_t)ptr, base = (uintptr_t)config.base_address;
    if (!running) rc = DSM_ERR_STATE;
    else if (addr < base || (addr - base) % config.page_size) rc = DSM_ERR_ARG;
    else if ((addr - base) / config.page_size >= pager_page_count()) rc = DSM_ERR_ARG;
    else if (alloc_destroy((uint32_t)((addr - base) / config.page_size))) rc = DSM_ERR_ARG;
    pthread_rwlock_unlock(&state_lock);
    return rc;
}

/* ----- consultas ----- */

// Las consultas copian hasta max entradas en out y devuelven cuántas hay en total. Si el
// total es mayor que max, quien llama puede repetir con un arreglo más grande.

// Tabla de nodos: estado, dirección, puertos, memoria y páginas libres de cada uno
int dsm_get_nodes(dsm_node_info_t *out, int max) {
    pthread_rwlock_rdlock(&state_lock);
    int n = running ? config.node_count : DSM_ERR_STATE;
    for (int id = 0; id < n && id < max; id++) {
        node_info_t info;
        const node_config_t *nc = &config.nodes[id];
        node_info((uint16_t)id, &info);
        memset(&out[id], 0, sizeof out[id]);
        out[id].node = id;
        out[id].status = info.status;
        memcpy(out[id].host, nc->host, sizeof out[id].host);
        out[id].data_port = nc->data_port;
        out[id].hb_port = nc->hb_port;
        out[id].memory_bytes = nc->memory_bytes;
        out[id].first_page = info.first_page;
        out[id].n_pages = info.n_pages;
        out[id].free_pages = info.status == NODE_ONLINE ? info.free_pages : 0;
    }
    pthread_rwlock_unlock(&state_lock);
    return n;
}

// Nodo válido para una consulta: existe y está en línea
static int reachable(int node) {
    return node >= 0 && node < config.node_count && node_is_online((uint16_t)node);
}

// Páginas en uso según un nodo, o de todo el clúster con DSM_ALL_NODES
int dsm_get_page_table(int node, dsm_page_info_t *out, int max) {
    pthread_rwlock_rdlock(&state_lock);
    int n = 0;
    if (!running) n = DSM_ERR_STATE;
    else if (node != DSM_ALL_NODES) {
        n = reachable(node) ? stats_pages((uint16_t)node, out, max) : -1;
        if (n < 0) n = DSM_ERR_NODE;
    } else {
        // De cada nodo se toman solo las entradas de las páginas de las que es home: el home
        // tiene el directorio completo, y así cada página aparece una sola vez
        uint32_t total = pager_page_count();
        dsm_page_info_t *part = malloc(total * sizeof *part);
        for (int id = 0; part && id < config.node_count; id++) {
            int got = reachable(id) ? stats_pages((uint16_t)id, part, (int)total) : 0;
            for (int i = 0; i < got; i++) {
                if (part[i].home != id) continue;
                if (n < max) out[n] = part[i];
                n++;
            }
        }
        free(part);
    }
    pthread_rwlock_unlock(&state_lock);
    return n;
}

// Comparación para qsort: ordena por primera página
static int by_first_page(const void *a, const void *b) {
    const alloc_record_t *x = a, *y = b;
    return x->first_page < y->first_page ? -1 : x->first_page > y->first_page;
}

// Asignaciones con páginas en un nodo, o de todo el clúster con DSM_ALL_NODES, en orden de página
int dsm_get_allocs(int node, dsm_alloc_info_t *out, int max) {
    pthread_rwlock_rdlock(&state_lock);
    int n = 0;
    if (!running) n = DSM_ERR_STATE;
    else if (node != DSM_ALL_NODES && !reachable(node)) n = DSM_ERR_NODE;
    else {
        // Cada asignación tiene al menos una página: no puede haber más que páginas
        uint32_t cap = pager_page_count(), used = 0;
        alloc_record_t *recs = malloc(cap * sizeof *recs);
        for (int id = 0; recs && id < config.node_count; id++) {
            if (node != DSM_ALL_NODES ? id != node : !reachable(id)) continue;
            int got = stats_allocs((uint16_t)id, recs + used, (int)(cap - used));
            if (got > 0) used += (uint32_t)got < cap - used ? (uint32_t)got : cap - used;
        }
        if (recs) qsort(recs, used, sizeof *recs, by_first_page);
        // Una asignación que cruza nodos la informa cada home: tras ordenar quedan juntas
        for (uint32_t i = 0; i < used; i++) {
            if (i > 0 && recs[i].alloc_id == recs[i - 1].alloc_id && recs[i].first_page == recs[i - 1].first_page) continue;
            if (n < max) {
                memset(&out[n], 0, sizeof out[n]);
                out[n].id = recs[i].alloc_id;
                out[n].first_page = recs[i].first_page;
                out[n].n_pages = recs[i].n_pages;
                out[n].addr = pager_app_addr(recs[i].first_page);
                out[n].size = (size_t)recs[i].n_pages * config.page_size;
                out[n].pid = recs[i].pid;
                out[n].node = recs[i].node;
                out[n].named = (recs[i].flags & ALLOC_NAMED) != 0;
                memcpy(out[n].tag, recs[i].tag, sizeof out[n].tag);
            }
            n++;
        }
        free(recs);
    }
    pthread_rwlock_unlock(&state_lock);
    return n;
}

// Métricas de un nodo en línea
int dsm_get_stats(int node, dsm_stats_t *out) {
    pthread_rwlock_rdlock(&state_lock);
    int rc = DSM_OK;
    if (!running) rc = DSM_ERR_STATE;
    else if (!reachable(node) || stats_of((uint16_t)node, out)) rc = DSM_ERR_NODE;
    pthread_rwlock_unlock(&state_lock);
    return rc;
}

// Procesos del clúster: los nodos en línea con sus métricas actuales, y los que salieron de
// forma ordenada con sus métricas finales
int dsm_get_procs(dsm_stats_t *out, int max) {
    pthread_rwlock_rdlock(&state_lock);
    int n = running ? 0 : DSM_ERR_STATE;
    for (int id = 0; running && id < config.node_count; id++) {
        dsm_stats_t s;
        int have = reachable(id) ? stats_of((uint16_t)id, &s) == 0 : stats_finished((uint16_t)id, &s) == 0;
        if (!have) continue;
        if (n < max) out[n] = s;
        n++;
    }
    pthread_rwlock_unlock(&state_lock);
    return n;
}

int dsm_last_error(void) {
    return last_error;
}

// Texto en español de un código de error
const char *dsm_strerror(int code) {
    switch (code) {
        case DSM_OK:           return "sin error";
        case DSM_ERR_STATE:    return "el DSM no está iniciado, o ya lo estaba";
        case DSM_ERR_CONFIG:   return "el archivo de configuración no se pudo leer o no es válido";
        case DSM_ERR_NODE:     return "el nodo indicado no existe, o no hay un nodo libre para este equipo";
        case DSM_ERR_MEMORY:   return "no se pudo reservar el espacio DSM";
        case DSM_ERR_NETWORK:  return "no se pudieron abrir los puertos del nodo";
        case DSM_ERR_REJECTED: return "otro nodo rechazó la unión: su configuración es distinta";
        case DSM_ERR_THREADS:  return "no hay lugar para otro hilo, o el hilo ya tenía (o no tenía) lugar";
        case DSM_ERR_ARG:      return "argumento inválido";
        case DSM_ERR_NOMEM:    return "no hay un tramo contiguo libre de ese tamaño en los nodos en línea";
        case DSM_ERR_EXISTS:   return "ya existe una asignación con ese nombre";
        case DSM_ERR_NOTFOUND: return "ningún nodo en línea conoce una asignación con ese nombre";
        default:               return "código de error desconocido";
    }
}
