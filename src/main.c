// Programa del nodo DSM. Sin node_id solo valida e imprime la configuración; con
// node_id arranca ese nodo, se une al clúster y atiende una consola de texto.
#include <ctype.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "coherence.h"
#include "config.h"
#include "network.h"
#include "node.h"
#include "pager.h"

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

static volatile sig_atomic_t stop_requested;   // la pone el manejador de SIGINT / SIGTERM

static void on_signal(int sig) {
    (void)sig;
    stop_requested = 1;
}

static void cmd_nodes(const dsm_config_t *c) {
    static const char *status[] = { "desconocido", "en linea", "fuera de linea" };
    for (uint16_t id = 0; id < c->node_count; id++) {
        node_info_t n;
        node_info(id, &n);
        printf("nodo %u  %-15s datos=%u hb=%u  %-14s memoria=%lu  paginas %u-%u%s\n",
               id, c->nodes[id].host, c->nodes[id].data_port, c->nodes[id].hb_port,
               status[n.status], (unsigned long)c->nodes[id].memory_bytes,
               n.first_page, n.first_page + n.n_pages - 1, id == node_self() ? "  (este nodo)" : "");
    }
    uint16_t online;
    uint64_t bytes = node_online_memory(&online);
    printf("ok %u nodos en linea, memoria disponible %lu bytes\n", online, (unsigned long)bytes);
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
        }
        printf("\n");
        listed++;
    }
    printf("ok %u paginas listadas\n", listed);
}

// Dirección de la página para leerla o escribirla, o NULL (con el error ya impreso).
// Un fallo sobre una página cuyo nodo no está en línea no se puede resolver.
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
    volatile uint32_t *counter = (volatile uint32_t *)(void *)p + slot;
    for (uint32_t i = 0; i < times; i++) (*counter)++;
    printf("ok pagina %u contador %u = %u\n", page, slot, *counter);
}

static void cmd_help(void) {
    printf("nodes                      tabla de nodos y memoria disponible\n"
           "pages                      paginas usadas: estado local y directorio\n"
           "read <pagina>              lee el texto guardado en la pagina\n"
           "write <pagina> <texto>     escribe un texto en la pagina\n"
           "incr <pagina> <n> <veces>  suma 1 <veces> veces al contador n de la pagina\n"
           "get <pagina> <n>           muestra el contador n de la pagina\n"
           "quit                       sale del cluster (tambien Ctrl+D o Ctrl+C)\n"
           "ok\n");
}

static void console(const dsm_config_t *c) {
    char line[256];
    int tty = isatty(STDIN_FILENO);
    while (!stop_requested) {
        if (tty) {
            printf("dsm[%u]> ", node_self());
            fflush(stdout);
        }
        if (!fgets(line, sizeof line, stdin)) break;   // Ctrl+D, o Ctrl+C interrumpió la lectura
        line[strcspn(line, "\n")] = '\0';

        uint32_t page = 0, slot = 0, times = 0;
        int used = 0;
        if (line[0] == '\0') continue;
        if (strcmp(line, "quit") == 0) break;
        if (strcmp(line, "nodes") == 0) cmd_nodes(c);
        else if (strcmp(line, "pages") == 0) cmd_pages();
        else if (strcmp(line, "help") == 0) cmd_help();
        else if (sscanf(line, "read %u", &page) == 1) cmd_read(page);
        else if (sscanf(line, "write %u %n", &page, &used) == 1 && used > 0) cmd_write(page, line + used, c->page_size);
        else if (sscanf(line, "incr %u %u %u", &page, &slot, &times) == 3) cmd_counter(page, slot, times, c->page_size);
        else if (sscanf(line, "get %u %u", &page, &slot) == 2) cmd_counter(page, slot, 0, c->page_size);
        else printf("error comando desconocido (help muestra la lista)\n");
    }
}

static int run_node(const dsm_config_t *c, uint16_t id) {
    // Ctrl+C y kill piden una salida ordenada; solo las atiende este hilo (los internos
    // heredan la máscara que las bloquea)
    sigset_t stop_signals;
    sigemptyset(&stop_signals);
    sigaddset(&stop_signals, SIGINT);
    sigaddset(&stop_signals, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &stop_signals, NULL);

    setvbuf(stdout, NULL, _IOLBF, 0);
    dsm_log_setup(c->log_level, id);
    if (pager_init(c, id)) return 1;

    int started = node_init(c, id) == 0 && coherence_init(c, id) == 0 && net_init(c, id) == 0;
    int joined = started && node_join_cluster() == 0;

    struct sigaction action;
    memset(&action, 0, sizeof action);
    action.sa_handler = on_signal;   // sin SA_RESTART: interrumpe la lectura de la consola
    sigemptyset(&action.sa_mask);
    sigaction(SIGINT, &action, NULL);
    sigaction(SIGTERM, &action, NULL);
    pthread_sigmask(SIG_UNBLOCK, &stop_signals, NULL);

    if (joined) console(c);

    // Salida ordenada: avisar, detener los hilos y liberar la memoria
    if (started) {
        node_leave_cluster();
        net_shutdown();
    }
    coherence_shutdown();
    node_shutdown();
    pager_shutdown();
    dsm_log(LOG_INFO, "nodo detenido");
    return joined ? 0 : 1;
}

int main(int argc, char **argv) {
    const char *path = argc > 1 ? argv[1] : "../dsm.conf";
    dsm_config_t config;

    if (dsm_config_load(path, &config) != 0)
        return 1;

    if (argc > 2) {
        char *end;
        unsigned long id = strtoul(argv[2], &end, 10);
        if (*argv[2] == '\0' || *end != '\0' || id >= config.node_count) {
            fprintf(stderr, "%s: node_id '%s' no existe (hay %u nodos)\n", path, argv[2], config.node_count);
            return 1;
        }
        return run_node(&config, (uint16_t)id);
    }

    printf("Configuracion cargada correctamente: %s\n\n", path);
    print_config(&config);
    return 0;
}
