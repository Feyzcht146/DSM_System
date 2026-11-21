// Coherencia MSI: este nodo es el home de las páginas que aporta y lleva su directorio
// (quién tiene copia y quién la tiene en M). Toda solicitud sobre una página va a su home,
// que atiende una a la vez por página (busy) y deja las demás en una cola FIFO.
#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "coherence.h"
#include "config.h"
#include "network.h"
#include "node.h"
#include "pager.h"
#include "stats.h"

static dsm_config_t cfg;
static uint16_t self;
static int ready;

// Directorio: una entrada por página home, cada una con su lock. Nunca se mantiene un lock
// mientras se espera la red; la exclusión durante una transacción la da el flag busy.
static dir_entry_t* dir;
static uint32_t home_first, home_count;   // primera página home y cuántas son

// Asignación de las páginas home: una reserva a la vez, para que dos no tomen la misma página
static pthread_mutex_t alloc_lock = PTHREAD_MUTEX_INITIALIZER;
static atomic_uint free_count;   // páginas home sin asignar

// Entrada del directorio de la página, o NULL si este nodo no es su home
static dir_entry_t* entry_of(uint32_t page) {
    if (page < home_first || page - home_first >= home_count) return NULL;
    return &dir[page - home_first];
}

// Bit del nodo en copyset (bit n = el nodo n tiene copia)
static uint64_t bit(uint16_t node) {
    return 1ULL << node;
}

// Momento límite para recibir el PAGE_ACK: el tiempo de todos los reintentos de una solicitud.
// Si vence, coherence_tick da al nodo por caído.
static struct timespec transaction_deadline(void) {
    struct timespec t;
    uint64_t ms = (uint64_t)cfg.request_timeout_ms * (cfg.request_retries + 1u);
    clock_gettime(CLOCK_MONOTONIC, &t);
    t.tv_sec += (time_t)(ms / 1000);
    t.tv_nsec += (long)(ms % 1000) * 1000000L;
    if (t.tv_nsec >= 1000000000L) {
        t.tv_sec++;
        t.tv_nsec -= 1000000000L;
    }
    return t;
}

// Quita la copia de node del directorio. Se llama con el lock de la entrada tomado.
static void remove_copy(dir_entry_t* e, uint16_t node) {
    e->copyset &= ~bit(node);
    if (e->copyset == 0) e->dir_state = DIR_UNCACHED;   // el dato válido queda en el home
}

// Termina la transacción en curso: si hay solicitudes en cola, busy sigue tomado y pasa a la
// siguiente (la atiende un hilo de servicio); si no, busy se libera.
static void release_busy(uint32_t page) {
    dir_entry_t* e = entry_of(page);
    pthread_mutex_lock(&e->lock);
    e->transac_req_id = 0;
    if (e->q_len > 0) net_push_ready(page);
    else e->busy = 0;
    pthread_mutex_unlock(&e->lock);
}

// El solicitante no llegó a instalar la página: quita su copia del directorio y libera busy.
// Solo actúa si la transacción en curso sigue siendo la de ese req_id.
static void abort_transaction(uint32_t page, uint16_t node, uint32_t req_id) {
    dir_entry_t* e = entry_of(page);
    pthread_mutex_lock(&e->lock);
    int mine = e->busy && e->transac_req_id == req_id && e->transac_node == node;
    if (mine) remove_copy(e, node);
    pthread_mutex_unlock(&e->lock);
    if (mine) release_busy(page);
}

// ----- acciones del home sobre las copias de otros nodos -----

// Trae al home la versión que owner tiene en M. owner queda en S si el acceso pedido es
// lectura, o en I si es escritura. Devuelve -1 si la versión se perdió.
static int fetch_from_owner(uint32_t page, uint16_t owner, uint8_t access) {
    if (owner == self) {   // el dato ya está en el memfd de este nodo
        pager_downgrade(page, access == ACCESS_WRITE ? PAGE_INVALID : PAGE_SHARED);
        return 0;
    }
    // La respuesta se copia directo en sys_view: el contenido queda en el memfd del home
    msg_header_t h = { .type = MSG_OWNER_FETCH, .page = page, .access = access };
    if (net_request(owner, &h, NULL, pager_sys_addr(page), cfg.page_size) != ST_OK) return -1;
    atomic_fetch_add(&dsm_counters.pages_received, 1);
    return 0;
}

// Invalida la copia de node y espera su INVALIDATE_ACK (net_request bloquea hasta la respuesta)
static void invalidate_copy(uint32_t page, uint16_t node) {
    if (node == self) {
        pager_downgrade(page, PAGE_INVALID);
        return;
    }
    msg_header_t h = { .type = MSG_INVALIDATE, .page = page };
    atomic_fetch_add(&dsm_counters.inval_sent, 1);
    net_request(node, &h, NULL, NULL, 0);   // si falla, el nodo cayó y ya no tiene copia
}

/* ----- transacción en el home ----- */

// Atiende la solicitud de node sobre una página de este home. Se llama con busy tomado para
// esa página; busy se libera al llegar PAGE_ACK (o si la transacción se aborta).
static void serve_request(uint32_t page, uint16_t node, uint8_t access, uint32_t req_id) {
    dir_entry_t *e = entry_of(page);
    // Copia el estado y suelta el lock: los pasos 1 y 2 esperan respuestas de la red.
    // Nadie más cambia la entrada mientras tanto porque busy está tomado.
    pthread_mutex_lock(&e->lock);
    uint8_t  state = e->dir_state;
    uint16_t owner = e->owner;
    uint64_t copies = e->copyset;
    pthread_mutex_unlock(&e->lock);

    // 1. Si otro nodo tiene la página en M, su versión vuelve primero al home.
    // Si se pide lectura, el owner queda con copia S; si se pide escritura, queda sin copia.
    if (state == DIR_MODIFIED && owner != node) {
        int lost = fetch_from_owner(page, owner, access);
        copies = (access == ACCESS_READ && !lost) ? bit(owner) : 0;
        pthread_mutex_lock(&e->lock);
        e->copyset = copies;
        e->dir_state = copies ? DIR_SHARED : DIR_UNCACHED;
        if (lost) e->alloc_state = ALLOC_LOST;
        pthread_mutex_unlock(&e->lock);
        if (lost) dsm_log(LOG_WARN, "página %u: se perdió la versión que tenía el nodo %u", page, owner);
    }

    // 2. Antes de conceder escritura invalida todas las demás copias y espera la confirmación
    // de cada una: así nunca hay un nodo en M mientras otro todavía lee una copia vieja.
    if (access == ACCESS_WRITE)
        for (uint16_t n = 0; n < cfg.node_count; n++)
            if (n != node && (copies & bit(n))) invalidate_copy(page, n);

    // 3. Actualiza el directorio y deja registrada la transacción hasta que llegue el PAGE_ACK
    pthread_mutex_lock(&e->lock);
    int has_copy = (e->copyset & bit(node)) != 0;   // S -> M: ya tiene los datos, solo falta el permiso
    if (access == ACCESS_WRITE) {
        e->copyset = bit(node);
        e->owner = node;
        e->dir_state = DIR_MODIFIED;
    } else {
        e->copyset |= bit(node);
        e->dir_state = DIR_SHARED;
    }
    e->transac_req_id = req_id;
    e->transac_node = node;
    e->transac_access = access;
    e->transac_deadline = transaction_deadline();
    pthread_mutex_unlock(&e->lock);

    // 4. Responde PAGE_DATA: el permiso concedido y, si el nodo no tiene copia, el contenido
    msg_header_t reply = {
        .type = MSG_PAGE_DATA, .flags = FLAG_IS_REPLY, .req_id = req_id, .page = page,
        .access = access, .state = access == ACCESS_WRITE ? PAGE_MODIFIED : PAGE_SHARED,
    };
    if (!has_copy) {
        reply.flags |= FLAG_HAS_DATA;
        reply.payload_len = cfg.page_size;
    }
    dsm_log(LOG_DEBUG, "página %u: %s concedida al nodo %u (%s datos)", page,
            access == ACCESS_WRITE ? "escritura" : "lectura", node, has_copy ? "sin" : "con");
    if (!has_copy && node != self) atomic_fetch_add(&dsm_counters.pages_sent, 1);
    if (node_send(node, &reply, has_copy ? NULL : pager_sys_addr(page)))
        abort_transaction(page, node, req_id);
}

// Atiende la primera solicitud de la cola de la página (release_busy la dejó lista).
// Busy ya está tomado desde la transacción anterior.
void coherence_serve_ready(uint32_t page) {
    dir_entry_t *e = entry_of(page);
    pthread_mutex_lock(&e->lock);
    if (e->q_len == 0) {   // la solicitud que esperaba era de un nodo que cayó
        e->busy = 0;
        pthread_mutex_unlock(&e->lock);
        return;
    }
    dir_waiter_t w = e->queue[e->q_head];
    e->q_head = (uint8_t)((e->q_head + 1) % DSM_MAX_NODES);
    e->q_len--;
    pthread_mutex_unlock(&e->lock);
    serve_request(page, w.node, w.access, w.req_id);
}

/* ----- mensajes recibidos ----- */

// PAGE_REQUEST: si la página está libre la atiende ya; si está ocupada, encola la solicitud
static void handle_page_request(const msg_header_t *h, uint16_t node) {
    dir_entry_t *e = entry_of(h->page);
    // Página que no es de este home o acceso inválido: responde error de protocolo
    if (!e || (h->access != ACCESS_READ && h->access != ACCESS_WRITE)) {
        msg_header_t reply = { .type = MSG_PAGE_DATA, .flags = FLAG_IS_REPLY, .req_id = h->req_id,
                               .page = h->page, .status = ERR_PROTOCOL };
        node_send(node, &reply, NULL);
        return;
    }

    pthread_mutex_lock(&e->lock);
    int busy = e->busy;
    if (!busy) {
        e->busy = 1;
    } else {
        // Página ocupada: la solicitud espera su turno en la cola. Cada nodo tiene a lo sumo una
        // solicitud por página (el paginador agrupa los fallos), así la cola nunca pasa de
        // DSM_MAX_NODES; una repetida indica un error y se descarta.
        int repeated = 0;
        for (uint8_t k = 0; k < e->q_len; k++)
            if (e->queue[(e->q_head + k) % DSM_MAX_NODES].node == node) repeated = 1;
        if (repeated || e->q_len == DSM_MAX_NODES) {
            dsm_log(LOG_ERROR, "página %u: se descarta una solicitud repetida del nodo %u", h->page, node);
        } else {
            dir_waiter_t w = { .req_id = h->req_id, .node = node, .access = h->access };
            e->queue[(e->q_head + e->q_len) % DSM_MAX_NODES] = w;
            e->q_len++;
        }
    }
    pthread_mutex_unlock(&e->lock);
    if (!busy) serve_request(h->page, node, h->access, h->req_id);
}

// PAGE_ACK: el solicitante instaló la página (o no pudo). Cierra la transacción.
static void handle_page_ack(const msg_header_t *h, uint16_t node) {
    dir_entry_t *e = entry_of(h->page);
    if (!e) return;
    pthread_mutex_lock(&e->lock);
    int mine = e->busy && e->transac_req_id == h->req_id && e->transac_node == node;
    if (mine && h->status != ST_OK) remove_copy(e, node);
    pthread_mutex_unlock(&e->lock);
    if (mine) release_busy(h->page);
}

// INVALIDATE: el home ordena borrar la copia local; responde INVALIDATE_ACK
static void handle_invalidate(const msg_header_t *h, uint16_t node) {
    atomic_fetch_add(&dsm_counters.inval_received, 1);
    pager_downgrade(h->page, PAGE_INVALID);
    msg_header_t reply = { .type = MSG_INVALIDATE_ACK, .flags = FLAG_IS_REPLY,
                           .req_id = h->req_id, .page = h->page };
    node_send(node, &reply, NULL);
}

// OWNER_FETCH: el home pide la versión que este nodo tiene en M. Responde con los datos.
static void handle_owner_fetch(const msg_header_t *h, uint16_t node) {
    uint8_t keep = h->access == ACCESS_WRITE ? PAGE_INVALID : PAGE_SHARED;
    // Primero quita el permiso de escritura y después envía: así ningún hilo local cambia la
    // página mientras se copia, y lo que llega al home es la versión final.
    int old = pager_downgrade(h->page, keep);
    msg_header_t reply = { .type = MSG_PAGE_DATA, .flags = FLAG_IS_REPLY, .req_id = h->req_id,
                           .page = h->page, .state = keep };
    if (old != PAGE_MODIFIED) {   // este nodo ya no tenía la página en M
        reply.status = ERR_NOT_OWNER;
        node_send(node, &reply, NULL);
        return;
    }
    reply.flags |= FLAG_HAS_DATA;
    reply.payload_len = cfg.page_size;
    atomic_fetch_add(&dsm_counters.pages_sent, 1);
    node_send(node, &reply, pager_sys_addr(h->page));
}

// Punto de entrada de los mensajes de coherencia; lo llama el hilo de servicio de la red
void coherence_handle(const work_item_t *item) {
    const msg_header_t *h = &item->header;
    uint16_t node = item->from_node;
    if (h->type == MSG_PAGE_REQUEST) {
        handle_page_request(h, node);
    } else if (h->type == MSG_PAGE_ACK) {
        handle_page_ack(h, node);
    } else if (pager_page_home(h->page) != node) {
        // INVALIDATE y OWNER_FETCH solo los puede ordenar el home de la página
        dsm_log(LOG_ERROR, "el nodo %u ordenó sobre la página %u sin ser su home", node, h->page);
    } else if (h->type == MSG_INVALIDATE) {
        handle_invalidate(h, node);
    } else if (h->type == MSG_OWNER_FETCH) {
        handle_owner_fetch(h, node);
    }
}

/* ----- lado del solicitante: lo usa el servicio de fallos ----- */

// Pide la página a su home y bloquea hasta recibir PAGE_DATA. Los datos llegan directo a
// sys_view. Devuelve ST_OK, un error del home (> 0) o -1 si no hay red.
// Si el home es este mismo nodo, la solicitud no usa socket (node_send la entrega con
// net_deliver), pero entra a la misma cola que las remotas: así el home serializa todas las
// solicitudes de una página, sean locales o remotas.
int coherence_acquire(uint32_t page, uint8_t access, uint32_t *req_id) {
    *req_id = 0;
    if (!ready) return -1;   // sin red: el paginador resuelve solo sus propias páginas
    msg_header_t h = { .type = MSG_PAGE_REQUEST, .page = page, .access = access };
    int rc = net_request(pager_page_home(page), &h, NULL, pager_sys_addr(page), cfg.page_size);
    // payload_len > 0: la respuesta trajo el contenido de la página, no solo el permiso
    if (rc == ST_OK && h.payload_len > 0 && pager_page_home(page) != self)
        atomic_fetch_add(&dsm_counters.pages_received, 1);
    *req_id = h.req_id;
    return rc;
}

// Envía PAGE_ACK al home: la página ya está instalada (ok = 1) o no se pudo (ok = 0)
void coherence_confirm(uint32_t page, uint32_t req_id, int ok) {
    if (!ready || req_id == 0) return;
    msg_header_t h = { .type = MSG_PAGE_ACK, .page = page, .req_id = req_id,
                       .status = ok ? ST_OK : ERR_PROTOCOL };
    node_send(pager_page_home(page), &h, NULL);
}

/* ----- salida y caída de nodos ----- */

// Salida ordenada de node (LEAVE): trae al home las páginas que node tenía en M, para que
// sus datos no se pierdan cuando se vaya.
void coherence_recover_from(uint16_t node) {
    if (!ready) return;
    uint32_t recovered = 0;
    for (uint32_t i = 0; i < home_count; i++) {
        dir_entry_t *e = &dir[i];
        uint32_t page = home_first + i;
        int taken = 0;
        // Toma busy de la página, esperando hasta 2 s a que termine la transacción en curso
        for (int tries = 0; tries < 2000 && !taken; tries++) {
            pthread_mutex_lock(&e->lock);
            int owned = e->dir_state == DIR_MODIFIED && e->owner == node;
            if (owned && !e->busy) e->busy = taken = 1;
            pthread_mutex_unlock(&e->lock);
            if (!owned) break;
            if (!taken) usleep(1000);
        }
        if (!taken) continue;

        int lost = fetch_from_owner(page, node, ACCESS_WRITE);
        pthread_mutex_lock(&e->lock);
        e->copyset = 0;
        e->dir_state = DIR_UNCACHED;
        if (lost) e->alloc_state = ALLOC_LOST;
        pthread_mutex_unlock(&e->lock);
        release_busy(page);
        if (!lost) recovered++;
    }
    if (recovered)
        dsm_log(LOG_INFO, "se recuperaron %u páginas que el nodo %u tenía modificadas", recovered, node);
}

// Caída de node (sin LEAVE): borra sus copias del directorio y descarta sus solicitudes.
// Las páginas que solo node tenía en M quedan LOST: su único dato válido se perdió.
void coherence_node_down(uint16_t node) {
    if (!ready) return;
    uint32_t lost = 0;
    for (uint32_t i = 0; i < home_count; i++) {
        dir_entry_t *e = &dir[i];
        pthread_mutex_lock(&e->lock);
        if (e->dir_state == DIR_MODIFIED && e->owner == node) {
            e->alloc_state = ALLOC_LOST;   // la única copia válida estaba en ese nodo
            lost++;
        }
        remove_copy(e, node);

        // Sus solicitudes en cola ya no tienen a quién responder: compacta la cola sin ellas
        uint8_t kept = 0;
        for (uint8_t k = 0; k < e->q_len; k++) {
            dir_waiter_t w = e->queue[(e->q_head + k) % DSM_MAX_NODES];
            if (w.node != node) e->queue[(e->q_head + kept++) % DSM_MAX_NODES] = w;
        }
        e->q_len = kept;

        // Si la transacción en curso esperaba el ACK de ese nodo, nunca llegará: se cierra
        int waiting_ack = e->busy && e->transac_req_id != 0 && e->transac_node == node;
        pthread_mutex_unlock(&e->lock);
        if (waiting_ack) release_busy(home_first + i);
    }
    if (lost)
        dsm_log(LOG_WARN, "%u páginas quedaron LOST: el nodo %u tenía su única copia válida", lost, node);

    // Las copias locales de las páginas de ese nodo ya no sirven: su directorio no existe
    uint32_t first, count;
    if (pager_node_pages(node, &first, &count) == 0) pager_drop_range(first, count);
}

// Revisión periódica (la llama el heartbeat): cierra las transacciones cuyo PAGE_ACK venció.
// Si el que no confirmó es otro nodo, se le da por caído.
void coherence_tick(void) {
    if (!ready) return;
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    for (uint32_t i = 0; i < home_count; i++) {
        dir_entry_t *e = &dir[i];
        pthread_mutex_lock(&e->lock);
        int expired = e->busy && e->transac_req_id != 0 &&
                      (now.tv_sec > e->transac_deadline.tv_sec ||
                       (now.tv_sec == e->transac_deadline.tv_sec && now.tv_nsec > e->transac_deadline.tv_nsec));
        uint16_t node = e->transac_node;
        uint32_t req_id = e->transac_req_id;
        pthread_mutex_unlock(&e->lock);
        if (!expired) continue;

        dsm_log(LOG_WARN, "página %u: el nodo %u no confirmó a tiempo", home_first + i, node);
        if (node == self) abort_transaction(home_first + i, node, req_id);
        else node_down(node, "no confirmó una página");
    }
}

/* ----- asignación de las páginas de este home ----- */

// Páginas de este home que no pertenecen a ninguna asignación
uint32_t coherence_free_pages(void) {
    return ready ? atomic_load(&free_count) : 0;
}

// Llena un bitmap con las páginas home libres (bit i = página home_first + i libre).
// Lo envía ALLOC_MAP para que otro nodo busque un tramo libre.
void coherence_free_map(uint8_t *map) {
    memset(map, 0, (home_count + 7) / 8);
    for (uint32_t i = 0; ready && i < home_count; i++) {
        pthread_mutex_lock(&dir[i].lock);
        if (dir[i].alloc_id == 0) map[i / 8] |= (uint8_t)(1u << (i % 8));
        pthread_mutex_unlock(&dir[i].lock);
    }
}

// Reserva [first, first+count) para la asignación alloc_id, todo o nada: si alguna página ya
// está ocupada no reserva ninguna y devuelve -1. No espera: quien pide reintenta con otro tramo.
// order es la posición de la primera página dentro de la asignación.
int coherence_reserve(uint32_t first, uint32_t count, uint32_t alloc_id, uint32_t order,
                      int32_t pid, uint16_t node) {
    dir_entry_t *e = ready ? entry_of(first) : NULL;
    if (!e || count == 0 || count > home_count - (first - home_first)) return -1;

    pthread_mutex_lock(&alloc_lock);
    int all_free = 1;
    for (uint32_t i = 0; i < count && all_free; i++) {
        pthread_mutex_lock(&e[i].lock);
        if (e[i].alloc_id != 0) all_free = 0;
        pthread_mutex_unlock(&e[i].lock);
    }
    // Siguen libres entre los dos ciclos: solo una reserva con alloc_lock puede ocupar páginas
    for (uint32_t i = 0; i < count && all_free; i++) {
        pthread_mutex_lock(&e[i].lock);
        e[i].alloc_id    = alloc_id;
        e[i].alloc_order = order + i;
        e[i].alloc_pid   = pid;
        e[i].alloc_node  = node;
        e[i].alloc_state = ALLOC_ALLOCATED;
        pthread_mutex_unlock(&e[i].lock);
    }
    if (all_free) atomic_fetch_sub(&free_count, count);
    pthread_mutex_unlock(&alloc_lock);
    return all_free ? 0 : -1;
}

// Deja una página de este home sin copias en ningún nodo y con su contenido en cero
static void reset_page(uint32_t page) {
    dir_entry_t *e = entry_of(page);
    int taken = 0;
    // Toma busy, esperando hasta 2 s a que termine la transacción en curso
    for (int tries = 0; tries < 2000 && !taken; tries++) {
        pthread_mutex_lock(&e->lock);
        if (!e->busy) e->busy = taken = 1;
        pthread_mutex_unlock(&e->lock);
        if (!taken) usleep(1000);
    }
    if (!taken) {
        dsm_log(LOG_WARN, "página %u: sigue ocupada y se libera sin limpiar su contenido", page);
        return;
    }
    pthread_mutex_lock(&e->lock);
    uint64_t copies = e->copyset;
    pthread_mutex_unlock(&e->lock);
    for (uint16_t n = 0; n < cfg.node_count; n++)
        if (copies & bit(n)) invalidate_copy(page, n);
    pager_drop_range(page, 1);   // devuelve su memoria: al volver a usarse empieza en cero

    pthread_mutex_lock(&e->lock);
    e->copyset = 0;
    e->dir_state = DIR_UNCACHED;
    pthread_mutex_unlock(&e->lock);
    release_busy(page);
}

// Libera las páginas del rango que pertenecen a alloc_id: invalida sus copias, borra su
// contenido y las marca libres. Las páginas de otra asignación no se tocan.
void coherence_release(uint32_t alloc_id, uint32_t first, uint32_t count) {
    for (uint32_t page = first; ready && page < first + count; page++) {
        dir_entry_t *e = entry_of(page);
        if (!e) continue;
        pthread_mutex_lock(&e->lock);
        int mine = e->alloc_id == alloc_id;
        pthread_mutex_unlock(&e->lock);
        if (!mine) continue;

        reset_page(page);
        pthread_mutex_lock(&e->lock);
        e->alloc_id = e->alloc_order = 0;
        e->alloc_pid = 0;
        e->alloc_node = 0;
        e->alloc_state = ALLOC_FREE;
        pthread_mutex_unlock(&e->lock);
        atomic_fetch_add(&free_count, 1);
    }
}

/* ----- consultas, inicio y liberación ----- */

// Copia la entrada del directorio bajo su lock (sin copiar el mutex ni la cola)
int coherence_dir_info(uint32_t page, dir_entry_t *out) {
    dir_entry_t *e = ready ? entry_of(page) : NULL;
    if (!e) return -1;
    pthread_mutex_lock(&e->lock);
    out->copyset     = e->copyset;
    out->owner       = e->owner;
    out->dir_state   = e->dir_state;
    out->busy        = e->busy;
    out->alloc_state = e->alloc_state;
    out->alloc_id    = e->alloc_id;
    out->alloc_order = e->alloc_order;
    out->alloc_pid   = e->alloc_pid;
    out->alloc_node  = e->alloc_node;
    out->q_len       = e->q_len;
    pthread_mutex_unlock(&e->lock);
    return 0;
}

// Crea el directorio de las páginas home de este nodo; todas empiezan libres y sin copias
int coherence_init(const dsm_config_t *c, uint16_t self_id) {
    cfg = *c;
    self = self_id;
    if (pager_node_pages(self, &home_first, &home_count)) return -1;
    dir = calloc(home_count, sizeof *dir);   // calloc: DIR_UNCACHED, sin copias, libres
    if (!dir) {
        dsm_log(LOG_ERROR, "no hay memoria para el directorio de %u páginas", home_count);
        return -1;
    }
    for (uint32_t i = 0; i < home_count; i++) pthread_mutex_init(&dir[i].lock, NULL);
    atomic_store(&free_count, home_count);
    ready = 1;
    return 0;
}

void coherence_shutdown(void) {
    if (!ready) return;
    ready = 0;
    for (uint32_t i = 0; i < home_count; i++) pthread_mutex_destroy(&dir[i].lock);
    free(dir);
    dir = NULL;
}
