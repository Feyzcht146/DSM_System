// Consultas y estadísticas: contadores de este nodo y respuestas a las consultas de otros
// (proceso y métricas, páginas en uso, asignaciones). Cada proceso del DSM es un nodo, por
// eso "los procesos del clúster" son las métricas de cada nodo en línea.
#define _GNU_SOURCE   // program_invocation_short_name
#include <errno.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "alloc.h"
#include "coherence.h"
#include "network.h"
#include "node.h"
#include "pager.h"
#include "stats.h"

// Contadores de este nodo. Son atómicos: cualquier hilo los incrementa sin lock.
dsm_counters_t dsm_counters;

static dsm_config_t cfg;
static uint16_t self;

// Métricas finales del último proceso de cada nodo que salió de forma ordenada
static dsm_stats_t finished[DSM_MAX_NODES];
static uint8_t has_finished[DSM_MAX_NODES];
static pthread_mutex_t finished_lock = PTHREAD_MUTEX_INITIALIZER;

// Suma la latencia de un fallo y actualiza el máximo
void stats_fault(uint64_t ns) {
    atomic_fetch_add(&dsm_counters.fault_ns_total, ns);
    uint64_t max = atomic_load(&dsm_counters.fault_ns_max);
    while (ns > max && !atomic_compare_exchange_weak(&dsm_counters.fault_ns_max, &max, ns)) { }
}

/* ----- este nodo ----- */

// Métricas actuales de este nodo y su proceso
static void local_stats(dsm_stats_t *s) {
    uint32_t first, count;
    memset(s, 0, sizeof *s);
    pager_node_pages(self, &first, &count);
    s->node = self;
    s->pid = (int)getpid();
    s->state = DSM_PROC_RUNNING;
    strncpy(s->program, program_invocation_short_name, sizeof s->program - 1);
    s->uptime_ms = node_uptime_ms();
    s->total_pages = count;
    s->free_pages = coherence_free_pages();
    s->used_pages = count - s->free_pages;
    s->threads = pager_thread_count();
    s->read_faults    = atomic_load(&dsm_counters.read_faults);
    s->write_faults   = atomic_load(&dsm_counters.write_faults);
    s->pages_sent     = atomic_load(&dsm_counters.pages_sent);
    s->pages_received = atomic_load(&dsm_counters.pages_received);
    s->inval_sent     = atomic_load(&dsm_counters.inval_sent);
    s->inval_received = atomic_load(&dsm_counters.inval_received);
    s->msgs_sent      = atomic_load(&dsm_counters.msgs_sent);
    s->msgs_received  = atomic_load(&dsm_counters.msgs_received);
    s->bytes_sent     = atomic_load(&dsm_counters.bytes_sent);
    s->bytes_received = atomic_load(&dsm_counters.bytes_received);
    s->fault_ns_total = atomic_load(&dsm_counters.fault_ns_total);
    s->fault_ns_max   = atomic_load(&dsm_counters.fault_ns_max);
}

// Datos de una página en este nodo. Devuelve 0 si no está en uso aquí: sin copia local y,
// si es de este home, sin asignar y sin copias en otros nodos.
static int local_page(uint32_t page, dsm_page_info_t *p) {
    local_page_t lp;
    dir_entry_t de;
    memset(p, 0, sizeof *p);
    pager_page_info(page, &lp);
    int home_here = coherence_dir_info(page, &de) == 0;
    int in_use = lp.state != PAGE_INVALID ||
                 (home_here && (de.alloc_id != 0 || de.dir_state != DIR_UNCACHED));
    if (!in_use) return 0;
    p->page = page;
    p->node = self;
    p->home = pager_page_home(page);
    p->local_state = lp.state;
    if (home_here) {
        p->dir_state   = de.dir_state;
        p->owner       = de.owner;
        p->copyset     = de.copyset;
        p->alloc_id    = de.alloc_id;
        p->alloc_order = de.alloc_order;
        p->alloc_pid   = de.alloc_pid;
        p->alloc_node  = de.alloc_node;
    }
    return 1;
}

/* ----- formato en la red ----- */

// stats_wire (160 bytes): 12 contadores u64, 4 u32 de páginas e hilos, pid, estado,
// uptime u64 y el nombre del programa (32)
static void put_stats(uint8_t *b, const dsm_stats_t *s) {
    const uint64_t counters[12] = {
        s->read_faults, s->write_faults, s->pages_sent, s->pages_received, s->inval_sent,
        s->inval_received, s->msgs_sent, s->msgs_received, s->bytes_sent, s->bytes_received,
        s->fault_ns_total, s->fault_ns_max };
    for (int i = 0; i < 12; i++) net_put64(b + 8 * i, counters[i]);
    net_put32(b + 96, s->total_pages);
    net_put32(b + 100, s->free_pages);
    net_put32(b + 104, s->used_pages);
    net_put32(b + 108, s->threads);
    net_put32(b + 112, (uint32_t)s->pid);
    net_put32(b + 116, (uint32_t)s->state);
    net_put64(b + 120, s->uptime_ms);
    memcpy(b + 128, s->program, 32);
}

// Inverso de put_stats
static void get_stats(const uint8_t *b, uint16_t node, dsm_stats_t *s) {
    memset(s, 0, sizeof *s);
    s->node = node;
    s->read_faults    = net_get64(b);
    s->write_faults   = net_get64(b + 8);
    s->pages_sent     = net_get64(b + 16);
    s->pages_received = net_get64(b + 24);
    s->inval_sent     = net_get64(b + 32);
    s->inval_received = net_get64(b + 40);
    s->msgs_sent      = net_get64(b + 48);
    s->msgs_received  = net_get64(b + 56);
    s->bytes_sent     = net_get64(b + 64);
    s->bytes_received = net_get64(b + 72);
    s->fault_ns_total = net_get64(b + 80);
    s->fault_ns_max   = net_get64(b + 88);
    s->total_pages = net_get32(b + 96);
    s->free_pages  = net_get32(b + 100);
    s->used_pages  = net_get32(b + 104);
    s->threads     = net_get32(b + 108);
    s->pid         = (int)net_get32(b + 112);
    s->state       = (int)net_get32(b + 116);
    s->uptime_ms   = net_get64(b + 120);
    memcpy(s->program, b + 128, 32);
    s->program[sizeof s->program - 1] = '\0';
}

// page_entry_wire (32 bytes)
static void put_page(uint8_t *b, const dsm_page_info_t *p) {
    net_put32(b, p->page);
    net_put16(b + 4, (uint16_t)p->home);
    net_put16(b + 6, (uint16_t)p->owner);
    b[8] = (uint8_t)p->local_state;
    b[9] = (uint8_t)p->dir_state;
    net_put16(b + 10, (uint16_t)p->alloc_node);
    net_put64(b + 12, p->copyset);
    net_put32(b + 20, p->alloc_id);
    net_put32(b + 24, p->alloc_order);
    net_put32(b + 28, (uint32_t)p->alloc_pid);
}

// Inverso de put_page; node es quien respondió
static void get_page(const uint8_t *b, uint16_t node, dsm_page_info_t *p) {
    p->page        = net_get32(b);
    p->node        = node;
    p->home        = net_get16(b + 4);
    p->owner       = net_get16(b + 6);
    p->local_state = b[8];
    p->dir_state   = b[9];
    p->alloc_node  = net_get16(b + 10);
    p->copyset     = net_get64(b + 12);
    p->alloc_id    = net_get32(b + 20);
    p->alloc_order = net_get32(b + 24);
    p->alloc_pid   = (int)net_get32(b + 28);
}

/* ----- respuestas a otros nodos ----- */

// Responde las consultas de otro nodo: STATS_REQUEST, PAGETABLE_REQUEST y ALLOCS_REQUEST
void stats_handle(const work_item_t *item) {
    const msg_header_t *h = &item->header;
    msg_header_t reply = { .type = h->type, .flags = FLAG_IS_REPLY, .req_id = h->req_id };

    if (h->type == MSG_STATS_REQUEST) {
        uint8_t wire[DSM_STATS_LEN];
        dsm_stats_t s;
        local_stats(&s);
        put_stats(wire, &s);
        reply.payload_len = sizeof wire;
        node_send(item->from_node, &reply, wire);
        return;
    }

    // Respuestas por partes: cada respuesta lleva las entradas que quepan en max_payload y en
    // aux dice desde dónde seguir (0 = fin). Quien pregunta repite la consulta desde ahí.
    uint8_t *buf = malloc(cfg.max_payload);
    if (!buf) {
        reply.status = ERR_BUSY;
        node_send(item->from_node, &reply, NULL);
        return;
    }
    if (h->type == MSG_PAGETABLE_REQUEST) {
        uint32_t page = h->page, total = pager_page_count(), used = 0;
        for (; page < total && used + DSM_PAGE_ENTRY_LEN <= cfg.max_payload; page++) {
            dsm_page_info_t p;
            if (!local_page(page, &p)) continue;
            put_page(buf + used, &p);
            used += DSM_PAGE_ENTRY_LEN;
        }
        reply.payload_len = used;
        reply.aux = page < total ? page : 0;
    } else {
        uint32_t per_msg = cfg.max_payload / DSM_ALLOC_DESC_LEN, start = h->aux;
        alloc_record_t *recs = malloc((start + per_msg) * sizeof *recs);
        uint32_t n = recs ? alloc_list_home(recs, start + per_msg) : 0, sent = 0;
        for (uint32_t i = start; i < n && sent < per_msg; i++, sent++)
            alloc_put_desc(buf + sent * DSM_ALLOC_DESC_LEN, &recs[i]);
        free(recs);
        reply.payload_len = sent * DSM_ALLOC_DESC_LEN;
        reply.aux = start + sent < n ? start + sent : 0;
    }
    node_send(item->from_node, &reply, buf);
    free(buf);
}

/* ----- consultas ----- */

// Métricas de un nodo: locales o pedidas por la red
int stats_of(uint16_t node, dsm_stats_t *out) {
    if (node == self) {
        local_stats(out);
        return 0;
    }
    uint8_t wire[DSM_STATS_LEN];
    msg_header_t h = { .type = MSG_STATS_REQUEST };
    if (net_request(node, &h, NULL, wire, sizeof wire) != ST_OK) return -1;
    get_stats(wire, node, out);
    return 0;
}

// Páginas en uso según el nodo: copia hasta max y devuelve cuántas hay (-1 si falló la red)
int stats_pages(uint16_t node, dsm_page_info_t *out, int max) {
    int n = 0;
    if (node == self) {
        for (uint32_t page = 0; page < pager_page_count(); page++) {
            dsm_page_info_t p;
            if (!local_page(page, &p)) continue;
            if (n < max) out[n] = p;
            n++;
        }
        return n;
    }
    uint8_t *buf = malloc(cfg.max_payload);
    if (!buf) return -1;
    uint32_t next = 0;
    do {
        msg_header_t h = { .type = MSG_PAGETABLE_REQUEST, .page = next };
        if (net_request(node, &h, NULL, buf, cfg.max_payload) != ST_OK) {
            free(buf);
            return -1;
        }
        for (uint32_t off = 0; off + DSM_PAGE_ENTRY_LEN <= h.payload_len; off += DSM_PAGE_ENTRY_LEN, n++)
            if (n < max) get_page(buf + off, node, &out[n]);
        next = h.aux;
    } while (next != 0);
    free(buf);
    return n;
}

// Asignaciones que tienen páginas en el home node: copia hasta max y devuelve cuántas hay
int stats_allocs(uint16_t node, alloc_record_t *out, int max) {
    if (node == self) return (int)alloc_list_home(out, max > 0 ? (uint32_t)max : 0);
    uint8_t *buf = malloc(cfg.max_payload);
    if (!buf) return -1;
    int n = 0;
    uint32_t next = 0;
    do {
        msg_header_t h = { .type = MSG_ALLOCS_REQUEST, .aux = next };
        if (net_request(node, &h, NULL, buf, cfg.max_payload) != ST_OK) {
            free(buf);
            return -1;
        }
        for (uint32_t off = 0; off + DSM_ALLOC_DESC_LEN <= h.payload_len; off += DSM_ALLOC_DESC_LEN, n++)
            if (n < max) alloc_get_desc(buf + off, &out[n]);
        next = h.aux;
    } while (next != 0);
    free(buf);
    return n;
}

// Guarda las métricas finales de un nodo que está saliendo (se llama al recibir su LEAVE,
// cuando todavía responde). Así procs lo muestra como terminado.
void stats_remember(uint16_t node) {
    dsm_stats_t s;
    if (node >= DSM_MAX_NODES || stats_of(node, &s)) return;
    s.state = DSM_PROC_FINISHED;
    pthread_mutex_lock(&finished_lock);
    finished[node] = s;
    has_finished[node] = 1;
    pthread_mutex_unlock(&finished_lock);
}

// Métricas finales guardadas del último proceso que salió del nodo. -1 si no hay.
int stats_finished(uint16_t node, dsm_stats_t *out) {
    pthread_mutex_lock(&finished_lock);
    int has = node < DSM_MAX_NODES && has_finished[node];
    if (has) *out = finished[node];
    pthread_mutex_unlock(&finished_lock);
    return has ? 0 : -1;
}

void stats_init(const dsm_config_t *c, uint16_t self_id) {
    cfg = *c;
    self = self_id;
    memset(&dsm_counters, 0, sizeof dsm_counters);
    memset(has_finished, 0, sizeof has_finished);
}
