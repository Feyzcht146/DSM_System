// Programa del nodo DSM (dsm_system). Sin node_id solo valida e imprime la configuración; con
// node_id (o "auto") arranca ese nodo, se une al clúster y atiende una consola de texto.
// Es un nodo sin aplicación: aporta su memoria al clúster y sirve para inspeccionarlo.
// Las órdenes locales (nodes, pages, allocs) leen los módulos internos directamente; las
// globales (procs, stats, allocs global, pages global) usan la API pública, como una app.
#include <ctype.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "alloc.h"
#include "coherence.h"
#include "config.h"
#include "dsm.h"
#include "node.h"
#include "pager.h"

// Imprime la configuración cargada (modo sin node_id)
static void print_config(const dsm_config_t *c) {
    const char *levels[] = { "error", "warn", "info", "debug" };

    printf("page_size             = %u\n", c->page_size);
    printf("default_node_memory   = %lu\n", c->default_node_memory);
    printf("base_address          = 0x%lx\n", c->base_address);
    printf("port_min / port_max   = %u / %u\n", c->port_min, c->port_max);
    printf("heartbeat_interval_ms = %u\n", c->hb_interval_ms);
    printf("heartbeat_timeout_ms  = %u\n", c->hb_timeout_ms);
    printf("request_timeout_ms    = %u\n", c->request_timeout_ms);
    printf("request_retries       = %u\n", c->request_retries);
    printf("fault_threads         = %u\n", c->fault_threads);
    printf("service_threads       = %u\n", c->service_threads);
    printf("protocol_workers      = %u\n", c->protocol_workers);
    printf("max_threads           = %u\n", c->max_threads);
    printf("max_pending           = %u\n", c->max_pending);
    printf("max_payload           = %u\n", c->max_payload);
    printf("log_level             = %s\n", c->log_level <= LOG_DEBUG ? levels[c->log_level] : "?");
    printf("config_hash           = %016lx\n", c->config_hash);

    uint64_t total = 0;
    printf("\nnodos (%u):\n", c->node_count);
    for (int i = 0; i < c->node_count; i++) {
        const node_config_t *n = &c->nodes[i];
        printf("  %u  %-15s data=%u hb=%u mem=%lu\n",
               n->node_id, n->host, n->data_port, n->hb_port, n->memory_bytes);
        total += n->memory_bytes;
    }
    printf("memoria global = %lu bytes (0x%lx - 0x%lx)\n",
           total, c->base_address, c->base_address + total);
}

/* ----- consola del nodo ----- */

// La pone el manejador de SIGINT / SIGTERM. sig_atomic_t + volatile: el único tipo que se
// puede escribir con seguridad desde un manejador y leer desde el código normal.
static volatile sig_atomic_t stop_requested;

// Manejador de Ctrl+C y kill: solo marca la salida; el cierre lo hace el hilo principal
static void on_signal(int sig) {
    (void)sig;
    stop_requested = 1;
}

// Orden nodes: tabla de nodos y memoria disponible
static void cmd_nodes(const dsm_config_t *c) {
    static const char *status[] = { "desconocido", "en linea", "fuera de linea" };
    uint64_t free_pages = 0;
    for (uint16_t id = 0; id < c->node_count; id++) {
        node_info_t n;
        node_info(id, &n);
        printf("nodo %u  %-15s datos=%u hb=%u  %-14s memoria=%lu  paginas %u-%u",
               id, c->nodes[id].host, c->nodes[id].data_port, c->nodes[id].hb_port,
               status[n.status], (unsigned long)c->nodes[id].memory_bytes,
               n.first_page, n.first_page + n.n_pages - 1);
        // Las páginas libres de los demás nodos son las de su último latido
        if (n.status == NODE_ONLINE) {
            printf("  libres=%u", n.free_pages);
            free_pages += n.free_pages;
        }
        printf("%s\n", id == node_self() ? "  (este nodo)" : "");
    }
    uint16_t online;
    uint64_t bytes = node_online_memory(&online);
    printf("ok %u nodos en linea, memoria disponible %lu bytes, sin asignar %lu bytes\n",
           online, (unsigned long)bytes, (unsigned long)(free_pages * c->page_size));
}

// Lista las páginas que este nodo ha usado y, de las suyas, las que tienen copias
static void cmd_pages(void) {
    static const char local_names[] = { 'I', 'S', 'M' };
    static const char *dir_names[] = { "UNCACHED", "SHARED", "MODIFIED" };
    uint32_t listed = 0;
    for (uint32_t page = 0; page < pager_page_count(); page++) {
        local_page_t lp;
        dir_entry_t de;
        pager_page_info(page, &lp);
        int home_here = coherence_dir_info(page, &de) == 0;
        int used = lp.state != PAGE_INVALID || lp.read_faults || lp.write_faults || lp.invalidations;
        if (!used && !(home_here && (de.dir_state != DIR_UNCACHED || de.alloc_state == ALLOC_LOST))) continue;

        printf("pagina %u home=%u local=%c lecturas=%u escrituras=%u invalidaciones=%u",
               page, pager_page_home(page), local_names[lp.state],
               lp.read_faults, lp.write_faults, lp.invalidations);
        if (home_here) {
            printf(" dir=%s copias={", dir_names[de.dir_state]);
            for (uint16_t n = 0, first = 1; n < DSM_MAX_NODES; n++)
                if (de.copyset >> n & 1) { printf(first ? "%u" : ",%u", n); first = 0; }
            printf("}");
            if (de.dir_state == DIR_MODIFIED) printf(" owner=%u", de.owner);
            if (de.alloc_state == ALLOC_LOST) printf(" LOST");
            if (de.alloc_id) printf(" asignacion=%08x orden=%u pid=%d nodo=%u",
                                    de.alloc_id, de.alloc_order, de.alloc_pid, de.alloc_node);
        }
        printf("\n");
        listed++;
    }
    printf("ok %u paginas listadas\n", listed);
}

// Dirección de la página para leerla o escribirla, o NULL (con el error ya impreso).
// Revisa antes que el home esté en línea: un fallo sobre una página sin home no se puede
// resolver y el manejador abortaría el proceso.
static char *usable_page(uint32_t page) {
    if (page >= pager_page_count()) {
        printf("error la pagina %u no existe (hay %u)\n", page, pager_page_count());
        return NULL;
    }
    if (!node_is_online(pager_page_home(page))) {
        printf("error la pagina %u es del nodo %u, que no esta en linea\n", page, pager_page_home(page));
        return NULL;
    }
    return pager_app_addr(page);
}

// Orden read: lee la página como texto. Es un acceso normal a memoria: si la página no está
// en este nodo, el fallo de página la trae sola.
static void cmd_read(uint32_t page) {
    const char *p = usable_page(page);
    if (!p) return;
    char text[65];
    size_t n = 0;
    while (n < sizeof text - 1 && p[n]) {
        text[n] = isprint((unsigned char)p[n]) ? p[n] : '.';
        n++;
    }
    text[n] = '\0';
    printf("ok pagina %u: \"%s\"\n", page, text);
}

// Orden write: escribe un texto en la página (el fallo de escritura pide la página en M)
static void cmd_write(uint32_t page, const char *text, size_t page_size) {
    char *p = usable_page(page);
    if (!p) return;
    size_t len = strlen(text);
    if (len >= page_size) len = page_size - 1;
    memcpy(p, text, len);
    p[len] = '\0';
    printf("ok pagina %u escrita\n", page);
}

// Suma 1 varias veces a un contador de 32 bits dentro de la página: sirve para
// comprobar que no se pierden escrituras cuando varios nodos usan la misma página
static void cmd_counter(uint32_t page, uint32_t slot, uint32_t times, size_t page_size) {
    char *p = usable_page(page);
    if (!p) return;
    if (slot >= page_size / sizeof(uint32_t)) {
        printf("error el contador %u no cabe en la pagina\n", slot);
        return;
    }
    // volatile: obliga a leer y escribir la memoria en cada vuelta. Sin él, el compilador
    // podría sumar todo en un registro y escribir una sola vez, y la prueba no probaría nada.
    volatile uint32_t *counter = (volatile uint32_t *)(void *)p + slot;
    for (uint32_t i = 0; i < times; i++) (*counter)++;
    printf("ok pagina %u contador %u = %u\n", page, slot, *counter);
}

/* ----- asignaciones ----- */

// Imprime una asignación en una línea
static void print_alloc(const char *prefix, const alloc_record_t *r) {
    printf("%s %08x paginas %u-%u (%u) direccion %p pid=%d nodo=%u%s%s%s%s\n", prefix, r->alloc_id,
           r->first_page, r->first_page + r->n_pages - 1, r->n_pages, pager_app_addr(r->first_page),
           r->pid, r->node, r->tag[0] ? (r->flags & ALLOC_NAMED ? " nombre=" : " etiqueta=") : "", r->tag,
           r->flags & ALLOC_NAMED ? " (con nombre)" : "", r->flags & ALLOC_ATTACHED ? " (adjuntada)" : "");
}

// Primera página de una dirección devuelta por la API
static uint32_t page_of(const dsm_config_t *c, const void *ptr) {
    return (uint32_t)(((uintptr_t)ptr - (uintptr_t)c->base_address) / c->page_size);
}

// Orden alloc: asigna con nombre si se dio uno, si no con la etiqueta "consola"
static void cmd_alloc(const dsm_config_t *c, unsigned long bytes, const char *name) {
    char *ptr = name[0] ? dsm_alloc_named(name, bytes) : dsm_alloc(bytes, "consola");
    if (!ptr) {
        printf("error no se pudo asignar: %s\n", dsm_strerror(dsm_last_error()));
        return;
    }
    uint32_t first = page_of(c, ptr), pages = (uint32_t)((bytes + c->page_size - 1) / c->page_size);
    printf("ok asignacion paginas %u-%u (%u) direccion %p\n", first, first + pages - 1, pages, (void *)ptr);
}

// Orden attach: adjunta una asignación con nombre
static void cmd_attach(const dsm_config_t *c, const char *name) {
    size_t size = 0;
    char *ptr = dsm_attach(name, &size);
    if (!ptr) {
        printf("error no se pudo adjuntar '%s': %s\n", name, dsm_strerror(dsm_last_error()));
        return;
    }
    uint32_t first = page_of(c, ptr), pages = (uint32_t)(size / c->page_size);
    printf("ok asignacion '%s' paginas %u-%u (%u) direccion %p\n", name, first, first + pages - 1, pages, (void *)ptr);
}

// Orden free: libera la asignación que empieza en la página
static void cmd_free(uint32_t page) {
    int rc = page < pager_page_count() ? dsm_free(pager_app_addr(page)) : DSM_ERR_ARG;
    if (rc == DSM_OK) printf("ok asignacion de la pagina %u liberada\n", page);
    else printf("error no hay una asignacion de este proceso que empiece en la pagina %u\n", page);
}

// Asignaciones de este proceso y asignaciones con páginas en este nodo
static void cmd_allocs(void) {
    enum { MAX = 256 };
    static alloc_record_t list[MAX];
    uint32_t own = alloc_list_own(list, MAX);
    for (uint32_t i = 0; i < own && i < MAX; i++) print_alloc("propia", &list[i]);
    uint32_t home = alloc_list_home(list, MAX);
    for (uint32_t i = 0; i < home && i < MAX; i++) print_alloc("en este nodo", &list[i]);
    printf("ok %u asignaciones de este proceso, %u con paginas en este nodo, %u paginas libres aqui\n",
           own, home, coherence_free_pages());
}

/* ----- consultas globales: con la API pública, como las haría cualquier aplicación ----- */

// Imprime las métricas de un proceso en cuatro líneas
static void print_stats(const dsm_stats_t *s) {
    uint64_t faults = s->read_faults + s->write_faults;
    printf("nodo %d pid=%d programa=%s %s tiempo=%.1fs hilos=%u paginas: %u usadas, %u libres de %u\n",
           s->node, s->pid, s->program, s->state == DSM_PROC_RUNNING ? "en ejecucion" : "terminado",
           s->uptime_ms / 1000.0, s->threads, s->used_pages, s->free_pages, s->total_pages);
    printf("    fallos: %lu lectura, %lu escritura; latencia media %.1f us, maxima %.1f us\n",
           (unsigned long)s->read_faults, (unsigned long)s->write_faults,
           faults ? s->fault_ns_total / 1000.0 / faults : 0.0, s->fault_ns_max / 1000.0);
    printf("    paginas: %lu enviadas, %lu recibidas; invalidaciones: %lu enviadas, %lu recibidas\n",
           (unsigned long)s->pages_sent, (unsigned long)s->pages_received,
           (unsigned long)s->inval_sent, (unsigned long)s->inval_received);
    printf("    mensajes: %lu enviados (%lu bytes), %lu recibidos (%lu bytes)\n",
           (unsigned long)s->msgs_sent, (unsigned long)s->bytes_sent,
           (unsigned long)s->msgs_received, (unsigned long)s->bytes_received);
}

// Métricas de un nodo, o de todos los procesos del clúster (también los que ya salieron)
static void cmd_stats(int node) {
    static dsm_stats_t procs[DSM_MAX_NODES];
    int n = 1;
    if (node == DSM_ALL_NODES) n = dsm_get_procs(procs, DSM_MAX_NODES);
    else if (dsm_get_stats(node, &procs[0]) != DSM_OK) {
        printf("error el nodo %d no existe o no esta en linea\n", node);
        return;
    }
    for (int i = 0; i < n; i++) print_stats(&procs[i]);
    printf("ok %d procesos\n", n);
}

// Asignaciones de todo el clúster: qué proceso ocupa qué páginas
static void cmd_allocs_global(void) {
    enum { MAX = 512 };
    static dsm_alloc_info_t list[MAX];
    int n = dsm_get_allocs(DSM_ALL_NODES, list, MAX);
    uint64_t pages = 0;
    for (int i = 0; i < n && i < MAX; i++) {
        printf("asignacion %08x paginas %u-%u (%u) direccion %p pid=%d nodo=%d %s%s\n", list[i].id,
               list[i].first_page, list[i].first_page + list[i].n_pages - 1, list[i].n_pages,
               list[i].addr, list[i].pid, list[i].node, list[i].named ? "nombre=" : "etiqueta=", list[i].tag);
        pages += list[i].n_pages;
    }
    printf("ok %d asignaciones en el cluster, %lu paginas\n", n, (unsigned long)pages);
}

// Tabla de páginas de todo el clúster: una línea por página en uso, según su home
static void cmd_pages_global(void) {
    static const char local_names[] = { 'I', 'S', 'M' };
    static const char *dir_names[] = { "UNCACHED", "SHARED", "MODIFIED" };
    int max = (int)pager_page_count();
    dsm_page_info_t *list = malloc((size_t)max * sizeof *list);
    int n = list ? dsm_get_page_table(DSM_ALL_NODES, list, max) : 0;
    for (int i = 0; i < n && i < max; i++) {
        const dsm_page_info_t *p = &list[i];
        printf("pagina %u home=%d local=%c dir=%s copias={", p->page, p->home,
               local_names[p->local_state], dir_names[p->dir_state]);
        for (int node = 0, first = 1; node < DSM_MAX_NODES; node++)
            if (p->copyset >> node & 1) { printf(first ? "%d" : ",%d", node); first = 0; }
        printf("}");
        if (p->dir_state == DIR_MODIFIED) printf(" owner=%d", p->owner);
        if (p->alloc_id) printf(" asignacion=%08x orden=%u pid=%d nodo=%d",
                                p->alloc_id, p->alloc_order, p->alloc_pid, p->alloc_node);
        printf("\n");
    }
    free(list);
    printf("ok %d paginas en uso en el cluster\n", n);
}

// Orden help: lista de órdenes
static void cmd_help(void) {
    printf("nodes                      tabla de nodos y memoria disponible\n"
           "pages                      paginas usadas: estado local y directorio\n"
           "read <pagina>              lee el texto guardado en la pagina\n"
           "write <pagina> <texto>     escribe un texto en la pagina\n"
           "incr <pagina> <n> <veces>  suma 1 <veces> veces al contador n de la pagina\n"
           "get <pagina> <n>           muestra el contador n de la pagina\n"
           "alloc <bytes> [nombre]     asigna memoria DSM (con nombre: otros pueden adjuntarla)\n"
           "attach <nombre>            adjunta una asignacion con nombre creada en otro proceso\n"
           "free <pagina>              libera la asignacion que empieza en esa pagina\n"
           "allocs                     asignaciones de este proceso y de este nodo\n"
           "allocs global              asignaciones de todo el cluster: proceso, nodo y paginas\n"
           "pages global               paginas en uso en todo el cluster, con su asignacion y orden\n"
           "stats [nodo]               metricas de este nodo o de otro\n"
           "procs                      procesos del cluster (en ejecucion y terminados) y sus metricas\n"
           "quit                       sale del cluster (tambien Ctrl+D o Ctrl+C)\n"
           "ok\n");
}

// Lee órdenes de la entrada estándar hasta quit, Ctrl+D o Ctrl+C. Cada respuesta termina en
// una línea "ok ..." o "error ...", así las pruebas automáticas saben cuándo terminó.
static void console(const dsm_config_t *c) {
    char line[256];
    int tty = isatty(STDIN_FILENO);   // muestra el prompt solo si hay una persona escribiendo
    while (!stop_requested) {
        if (tty) {
            printf("dsm[%u]> ", node_self());
            fflush(stdout);
        }
        if (!fgets(line, sizeof line, stdin)) break;   // Ctrl+D, o Ctrl+C interrumpió la lectura
        line[strcspn(line, "\n")] = '\0';

        uint32_t page = 0, slot = 0, times = 0;
        unsigned long bytes = 0;
        char name[DSM_TAG_MAX] = "";
        int used = 0;
        if (line[0] == '\0') continue;
        if (strcmp(line, "quit") == 0) break;
        if (strcmp(line, "nodes") == 0) cmd_nodes(c);
        else if (strcmp(line, "pages") == 0) cmd_pages();
        else if (strcmp(line, "help") == 0) cmd_help();
        else if (strcmp(line, "allocs") == 0) cmd_allocs();
        else if (strcmp(line, "allocs global") == 0) cmd_allocs_global();
        else if (strcmp(line, "pages global") == 0) cmd_pages_global();
        else if (strcmp(line, "procs") == 0) cmd_stats(DSM_ALL_NODES);
        else if (strcmp(line, "stats") == 0) cmd_stats(node_self());
        else if (sscanf(line, "stats %u", &page) == 1) cmd_stats((int)page);
        else if (sscanf(line, "alloc %lu %31s", &bytes, name) >= 1) cmd_alloc(c, bytes, name);
        else if (sscanf(line, "attach %31s", name) == 1) cmd_attach(c, name);
        else if (sscanf(line, "free %u", &page) == 1) cmd_free(page);
        else if (sscanf(line, "read %u", &page) == 1) cmd_read(page);
        else if (sscanf(line, "write %u %n", &page, &used) == 1 && used > 0) cmd_write(page, line + used, c->page_size);
        else if (sscanf(line, "incr %u %u %u", &page, &slot, &times) == 3) cmd_counter(page, slot, times, c->page_size);
        else if (sscanf(line, "get %u %u", &page, &slot) == 2) cmd_counter(page, slot, 0, c->page_size);
        else printf("error comando desconocido (help muestra la lista)\n");
    }
}

// Arranca el nodo con la API del DSM y atiende la consola hasta quit, Ctrl+D o Ctrl+C
static int run_node(const char *path, const dsm_config_t *c, int id) {
    setvbuf(stdout, NULL, _IOLBF, 0);   // vacía stdout en cada línea, también hacia un pipe

    // Ctrl+C y kill piden una salida ordenada. Se instala antes de iniciar para que
    // también sirvan mientras el nodo se une; los hilos internos del DSM no las reciben.
    struct sigaction action;
    memset(&action, 0, sizeof action);
    action.sa_handler = on_signal;   // sin SA_RESTART: interrumpe la lectura de la consola
    sigemptyset(&action.sa_mask);
    sigaction(SIGINT, &action, NULL);
    sigaction(SIGTERM, &action, NULL);

    int rc = dsm_init(path, id);
    if (rc != DSM_OK) {
        fprintf(stderr, "%s: no se pudo iniciar el nodo: %s\n", path, dsm_strerror(rc));
        return 1;
    }
    console(c);
    dsm_shutdown();   // salida ordenada: avisa, detiene los hilos y libera la memoria
    return 0;
}

// Uso: dsm_system [dsm.conf] [node_id | auto]
int main(int argc, char **argv) {
    const char *path = argc > 1 ? argv[1] : "../dsm.conf";
    dsm_config_t config;

    if (dsm_config_load(path, &config) != 0)
        return 1;

    if (argc > 2) {
        // "auto": el primer nodo de dsm.conf que corresponde a este equipo y está libre
        if (strcmp(argv[2], "auto") == 0) return run_node(path, &config, DSM_NODE_AUTO);
        char *end;
        unsigned long id = strtoul(argv[2], &end, 10);
        if (*argv[2] == '\0' || *end != '\0' || id >= config.node_count) {
            fprintf(stderr, "%s: node_id '%s' no existe (hay %u nodos)\n", path, argv[2], config.node_count);
            return 1;
        }
        return run_node(path, &config, (int)id);
    }

    printf("Configuracion cargada correctamente: %s\n\n", path);
    print_config(&config);
    return 0;
}
