// Nodos del DSM: tabla de nodos, unión y salida del clúster, envío por nodo y heartbeat.
#include <pthread.h>
#include <stdatomic.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "coherence.h"
#include "config.h"
#include "network.h"
#include "node.h"
#include "pager.h"

static dsm_config_t cfg;
static uint16_t self;

// Tabla de nodos: una entrada por nodo configurado. table_lock protege status, last_seen,
// missed_hb y free_pages; el send_lock de cada nodo protege sus sockets de salida.
static node_info_t nodes[DSM_MAX_NODES];
static pthread_rwlock_t table_lock = PTHREAD_RWLOCK_INITIALIZER;

static pthread_t hb_thread;
static int hb_started;
static atomic_int stopping;
static struct timespec started;     // para el uptime que viaja en el heartbeat

static uint64_t ms_since(const struct timespec *t) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    int64_t ms = (int64_t)(now.tv_sec - t->tv_sec) * 1000 + (now.tv_nsec - t->tv_nsec) / 1000000;
    return ms > 0 ? (uint64_t)ms : 0;
}

static int connect_timeout(void) {
    return cfg.request_timeout_ms < 1000 ? (int)cfg.request_timeout_ms : 1000;
}

static void stamp(msg_header_t *h, uint16_t dst) {
    h->dsm_id = DSM_PROTOCOL_ID;
    h->version = DSM_PROTOCOL_VERSION;
    h->src = self;
    h->dst = dst;
}

// Descripción de este nodo que viaja en JOIN y JOIN_ACK (node_desc, 96 bytes)
static void fill_node_desc(uint8_t *b) {
    const node_config_t *me = &cfg.nodes[self];
    memset(b, 0, DSM_NODE_DESC_LEN);
    net_put16(b, self);
    net_put16(b + 2, me->data_port);
    net_put16(b + 4, me->hb_port);
    net_put64(b + 8, me->memory_bytes);
    net_put64(b + 16, cfg.config_hash);
    net_put32(b + 24, nodes[self].first_page);
    net_put32(b + 28, nodes[self].n_pages);
    memcpy(b + 32, me->host, DSM_HOST_MAX);
}

static void close_out_sockets(node_info_t *n) {
    pthread_mutex_lock(&n->send_lock);
    if (n->data_out_fd >= 0) close(n->data_out_fd);
    if (n->hb_out_fd >= 0) close(n->hb_out_fd);
    n->data_out_fd = n->hb_out_fd = -1;
    pthread_mutex_unlock(&n->send_lock);
}

/* ----- consultas ----- */

uint16_t node_self(void) {
    return self;
}

int node_is_online(uint16_t id) {
    if (id >= cfg.node_count) return 0;
    pthread_rwlock_rdlock(&table_lock);
    int online = nodes[id].status == NODE_ONLINE;
    pthread_rwlock_unlock(&table_lock);
    return online;
}

int node_info(uint16_t id, node_info_t *out) {
    if (id >= cfg.node_count) return -1;
    const node_info_t *n = &nodes[id];
    pthread_rwlock_rdlock(&table_lock);
    out->last_seen   = n->last_seen;
    out->first_page  = n->first_page;
    out->n_pages     = n->n_pages;
    out->free_pages  = n->free_pages;
    out->node_id     = n->node_id;
    out->status      = n->status;
    out->missed_hb   = n->missed_hb;
    pthread_rwlock_unlock(&table_lock);
    out->data_out_fd = out->hb_out_fd = -1;   // los sockets son del send_lock: no se copian
    return 0;
}

uint64_t node_online_memory(uint16_t *online) {
    uint64_t bytes = 0;
    uint16_t count = 0;
    pthread_rwlock_rdlock(&table_lock);
    for (uint16_t i = 0; i < cfg.node_count; i++) {
        if (nodes[i].status != NODE_ONLINE) continue;
        bytes += cfg.nodes[i].memory_bytes;
        count++;
    }
    pthread_rwlock_unlock(&table_lock);
    if (online) *online = count;
    return bytes;
}

static void log_membership(int level, uint16_t id, const char *what, const char *reason) {
    uint16_t online;
    uint64_t bytes = node_online_memory(&online);
    dsm_log(level, "nodo %u %s%s%s; memoria disponible: %lu bytes en %u nodos",
            id, what, reason ? ": " : "", reason ? reason : "", (unsigned long)bytes, online);
}

/* ----- recepción ----- */

void node_seen(uint16_t id) {
    pthread_rwlock_wrlock(&table_lock);
    clock_gettime(CLOCK_MONOTONIC, &nodes[id].last_seen);
    nodes[id].missed_hb = 0;
    pthread_rwlock_unlock(&table_lock);
}

void node_heartbeat(uint16_t id, uint32_t free_pages) {
    pthread_rwlock_wrlock(&table_lock);
    nodes[id].free_pages = free_pages;
    pthread_rwlock_unlock(&table_lock);
}

/* ----- envío ----- */

int node_send(uint16_t dst, const msg_header_t *h, const void *payload) {
    msg_header_t out = *h;
    stamp(&out, dst);
    if (dst == self) {
        net_deliver(&out, payload);
        return 0;
    }
    if (dst >= cfg.node_count) return -1;

    // La unión es lo único que se envía a un nodo que todavía no está en línea
    int joining = h->type == MSG_JOIN || h->type == MSG_JOIN_ACK;
    if (!joining && !node_is_online(dst)) return -1;

    node_info_t *n = &nodes[dst];
    pthread_mutex_lock(&n->send_lock);   // un mensaje a la vez por socket: no se mezclan
    int fd = h->type == MSG_HEARTBEAT ? n->hb_out_fd : n->data_out_fd;
    int rc = fd < 0 ? -1 : net_send_fd(fd, &out, payload);
    pthread_mutex_unlock(&n->send_lock);

    if (rc && !joining) node_down(dst, "falló el envío");
    return rc;
}

/* ----- cambios de estado ----- */

// Abre los sockets de salida que falten y marca el nodo en línea. 0 o -1.
static int set_online(uint16_t id) {
    node_info_t *n = &nodes[id];
    const node_config_t *nc = &cfg.nodes[id];

    pthread_mutex_lock(&n->send_lock);
    if (n->data_out_fd < 0) n->data_out_fd = net_connect(nc->host, nc->data_port, connect_timeout());
    if (n->hb_out_fd < 0) n->hb_out_fd = net_connect(nc->host, nc->hb_port, connect_timeout());
    int connected = n->data_out_fd >= 0 && n->hb_out_fd >= 0;
    pthread_mutex_unlock(&n->send_lock);
    if (!connected) {
        close_out_sockets(n);
        dsm_log(LOG_WARN, "no se pudo conectar con el nodo %u (%s)", id, nc->host);
        return -1;
    }

    pthread_rwlock_wrlock(&table_lock);
    int was_online = n->status == NODE_ONLINE;
    n->status = NODE_ONLINE;
    n->missed_hb = 0;
    n->free_pages = n->n_pages;
    clock_gettime(CLOCK_MONOTONIC, &n->last_seen);
    pthread_rwlock_unlock(&table_lock);
    if (!was_online) log_membership(LOG_INFO, id, "en línea", nc->host);
    return 0;
}

void node_down(uint16_t id, const char *reason) {
    if (id >= cfg.node_count || id == self) return;
    node_info_t *n = &nodes[id];

    pthread_rwlock_wrlock(&table_lock);
    int was_online = n->status == NODE_ONLINE;
    if (was_online) n->status = NODE_OFFLINE;
    pthread_rwlock_unlock(&table_lock);
    if (!was_online) return;

    close_out_sockets(n);        // el otro nodo verá cerrada la conexión y hará lo mismo
    net_fail_pending(id);        // quienes esperaban su respuesta terminan con ERR_NODE_DOWN
    if (stopping) return;        // este nodo está saliendo: su directorio ya no importa
    coherence_node_down(id);     // sale de las copias; sus páginas dejan de estar disponibles
    log_membership(LOG_INFO, id, "fuera de línea", reason);
}

/* ----- unión y salida ----- */

// Intenta unirse al nodo id. 0 = en línea, 1 = no disponible, -1 = rechazó la unión.
static int try_join(uint16_t id) {
    node_info_t *n = &nodes[id];
    if (node_is_online(id)) return 0;

    int fd = net_connect(cfg.nodes[id].host, cfg.nodes[id].data_port, connect_timeout());
    if (fd < 0) return 1;
    pthread_mutex_lock(&n->send_lock);
    if (n->data_out_fd < 0) n->data_out_fd = fd;
    else close(fd);   // el otro nodo se unió a la vez y ya hay conexión: se usa esa
    pthread_mutex_unlock(&n->send_lock);

    uint8_t desc[DSM_NODE_DESC_LEN], reply[DSM_NODE_DESC_LEN];
    fill_node_desc(desc);
    msg_header_t h = { .type = MSG_JOIN, .payload_len = sizeof desc };
    int rc = net_request(id, &h, desc, reply, sizeof reply);
    if (rc == ST_OK && set_online(id) == 0) return 0;

    if (!node_is_online(id)) close_out_sockets(n);
    if (rc == ERR_CONFIG_MISMATCH) {
        dsm_log(LOG_ERROR, "el nodo %u rechazó la unión: su dsm.conf es distinto", id);
        return -1;
    }
    return 1;
}

static void handle_join(const work_item_t *item) {
    uint16_t id = item->from_node;
    uint8_t desc[DSM_NODE_DESC_LEN];
    fill_node_desc(desc);
    msg_header_t reply = { .type = MSG_JOIN_ACK, .flags = FLAG_IS_REPLY,
                           .req_id = item->header.req_id, .payload_len = sizeof desc };

    // Todos los nodos deben usar el mismo dsm.conf: se compara su hash
    int same = item->header.payload_len == DSM_NODE_DESC_LEN &&
               net_get16(item->payload) == id &&
               net_get64(item->payload + 16) == cfg.config_hash;
    if (!same) {
        dsm_log(LOG_WARN, "se rechaza la unión del nodo %u: su dsm.conf es distinto", id);
        reply.status = ERR_CONFIG_MISMATCH;
        stamp(&reply, id);
        // No queda en línea: la respuesta va por una conexión que se cierra enseguida
        int fd = net_connect(cfg.nodes[id].host, cfg.nodes[id].data_port, connect_timeout());
        if (fd >= 0) {
            net_send_fd(fd, &reply, desc);
            close(fd);
        }
        return;
    }
    if (set_online(id) == 0) node_send(id, &reply, desc);
}

static void handle_leave(const work_item_t *item) {
    uint16_t id = item->from_node;
    coherence_recover_from(id);   // antes de que se vaya: devuelve lo que modificó
    msg_header_t reply = { .type = MSG_LEAVE, .flags = FLAG_IS_REPLY, .req_id = item->header.req_id };
    node_send(id, &reply, NULL);
    node_down(id, "salida ordenada");
}

void node_handle(const work_item_t *item) {
    if (item->header.type == MSG_LEAVE) {
        handle_leave(item);
    } else if (item->from_node == self) {   // orden local del hilo de heartbeat
        try_join(item->header.dst);
    } else {
        handle_join(item);
    }
}

/* ----- heartbeat ----- */

static void *heartbeat(void *arg) {
    (void)arg;
    uint16_t retry = 0;
    while (!stopping) {
        uint8_t hb[DSM_HB_PAYLOAD_LEN];
        net_put32(hb, nodes[self].n_pages);   // páginas libres: todas, mientras no haya asignador
        net_put32(hb + 4, 0);
        net_put64(hb + 8, ms_since(&started));

        for (uint16_t id = 0; id < cfg.node_count; id++) {
            if (id == self) continue;
            pthread_rwlock_wrlock(&table_lock);
            int online = nodes[id].status == NODE_ONLINE;
            uint64_t silent = online ? ms_since(&nodes[id].last_seen) : 0;
            if (online) nodes[id].missed_hb = (uint8_t)(silent / cfg.hb_interval_ms);
            pthread_rwlock_unlock(&table_lock);
            if (!online) continue;

            // Cualquier mensaje cuenta como señal de vida, no solo los latidos
            if (silent > cfg.hb_timeout_ms) {
                node_down(id, "sin mensajes durante heartbeat_timeout_ms");
                continue;
            }
            msg_header_t h = { .type = MSG_HEARTBEAT, .payload_len = sizeof hb };
            node_send(id, &h, hb);
        }
        coherence_tick();

        // Reintenta la unión con un nodo que no esté en línea por vuelta
        for (uint16_t k = 0; k < cfg.node_count; k++) {
            retry = (uint16_t)((retry + 1) % cfg.node_count);
            if (retry == self || node_is_online(retry)) continue;
            net_push_join(retry);
            break;
        }

        for (uint32_t waited = 0; waited < cfg.hb_interval_ms && !stopping; waited += 50)
            usleep(50 * 1000);   // pausas cortas para terminar pronto al salir
    }
    return NULL;
}

int node_join_cluster(void) {
    clock_gettime(CLOCK_MONOTONIC, &started);
    int rejected = 0;
    for (uint16_t id = 0; id < cfg.node_count; id++)
        if (id != self && try_join(id) < 0) rejected = 1;
    if (rejected) return -1;

    if (pthread_create(&hb_thread, NULL, heartbeat, NULL)) {
        dsm_log(LOG_ERROR, "no se pudo crear el hilo de heartbeat");
        return -1;
    }
    hb_started = 1;
    log_membership(LOG_INFO, self, "listo", cfg.nodes[self].host);
    return 0;
}

void node_leave_cluster(void) {
    stopping = 1;
    if (hb_started) pthread_join(hb_thread, NULL);
    hb_started = 0;

    for (uint16_t id = 0; id < cfg.node_count; id++) {
        if (id == self || !node_is_online(id)) continue;
        msg_header_t h = { .type = MSG_LEAVE };
        net_request(id, &h, NULL, NULL, 0);   // el otro nodo responde tras recuperar sus páginas
        node_down(id, "este nodo sale");
    }
}

/* ----- inicio y liberación ----- */

int node_init(const dsm_config_t *c, uint16_t self_id) {
    cfg = *c;
    self = self_id;
    stopping = 0;
    for (uint16_t id = 0; id < c->node_count; id++) {
        node_info_t *n = &nodes[id];
        memset(n, 0, sizeof *n);
        pthread_mutex_init(&n->send_lock, NULL);
        n->node_id = id;
        n->status = id == self ? NODE_ONLINE : NODE_UNKNOWN;
        n->data_out_fd = n->hb_out_fd = -1;
        if (pager_node_pages(id, &n->first_page, &n->n_pages)) return -1;
        n->free_pages = n->n_pages;
    }
    return 0;
}

void node_shutdown(void) {
    for (uint16_t id = 0; id < cfg.node_count; id++) {
        close_out_sockets(&nodes[id]);
        pthread_mutex_destroy(&nodes[id].send_lock);
    }
}
