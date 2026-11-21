// Red del DSM: sockets TCP, formato de los mensajes, hilo listener, colas de trabajo con sus
// hilos (workers y servicio de coherencia) y solicitudes que esperan respuesta.
#define _GNU_SOURCE   // sem_clockwait, accept4
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <pthread.h>
#include <semaphore.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "alloc.h"
#include "coherence.h"
#include "config.h"
#include "network.h"
#include "node.h"
#include "pager.h"
#include "stats.h"

// 2 sockets de escucha (datos y heartbeat) más hasta 4 conexiones entrantes por nodo
enum { LISTENERS = 2, MAX_CONNS = LISTENERS + 4 * DSM_MAX_NODES };

static dsm_config_t cfg;
static uint16_t self;

// Sockets que vigila el listener: [0] escucha de datos, [1] escucha de heartbeat y luego
// las conexiones entrantes. Solo los toca el hilo listener.
static struct pollfd conns[MAX_CONNS];
static int conn_node[MAX_CONNS];    // nodo que envía por esa conexión; -1 = aún no se sabe
static int n_conns;

static pending_req_t *pending;      // max_pending solicitudes en espera de respuesta
static pthread_mutex_t pending_lock = PTHREAD_MUTEX_INITIALIZER;
static atomic_uint last_req_id;     // último req_id usado; cada solicitud toma el siguiente

static work_queue_t q_request;      // PAGE_REQUEST, JOIN, LEAVE, alloc, consultas: pueden esperar a otros nodos
static work_queue_t q_service;      // INVALIDATE, OWNER_FETCH, PAGE_ACK: nunca esperan la red

static pthread_t listener_thread, worker_threads[UINT8_MAX], service_threads[UINT8_MAX];
static int listener_started, n_workers, n_service;
static atomic_int stopping;

/* ----- formato de los mensajes: enteros en orden de red, campo a campo ----- */

void net_put16(uint8_t *p, uint16_t v) { p[0] = (uint8_t)(v >> 8); p[1] = (uint8_t)v; }
void net_put32(uint8_t *p, uint32_t v) { net_put16(p, (uint16_t)(v >> 16)); net_put16(p + 2, (uint16_t)v); }
void net_put64(uint8_t *p, uint64_t v) { net_put32(p, (uint32_t)(v >> 32)); net_put32(p + 4, (uint32_t)v); }
uint16_t net_get16(const uint8_t *p) { return (uint16_t)(p[0] << 8 | p[1]); }
uint32_t net_get32(const uint8_t *p) { return (uint32_t)net_get16(p) << 16 | net_get16(p + 2); }
uint64_t net_get64(const uint8_t *p) { return (uint64_t)net_get32(p) << 32 | net_get32(p + 4); }

// Escribe el encabezado en los DSM_HEADER_LEN bytes que viajan por la red
static void encode_header(const msg_header_t *h, uint8_t *b) {
    net_put32(b, h->dsm_id);
    b[4] = h->version;
    b[5] = h->type;
    net_put16(b + 6, h->flags);
    net_put16(b + 8, h->src);
    net_put16(b + 10, h->dst);
    net_put32(b + 12, h->req_id);
    net_put32(b + 16, h->page);
    net_put16(b + 20, h->status);
    b[22] = h->state;
    b[23] = h->access;
    net_put32(b + 24, h->payload_len);
    net_put32(b + 28, h->aux);
}

// Inverso de encode_header
static void decode_header(const uint8_t *b, msg_header_t *h) {
    h->dsm_id      = net_get32(b);
    h->version     = b[4];
    h->type        = b[5];
    h->flags       = net_get16(b + 6);
    h->src         = net_get16(b + 8);
    h->dst         = net_get16(b + 10);
    h->req_id      = net_get32(b + 12);
    h->page        = net_get32(b + 16);
    h->status      = net_get16(b + 20);
    h->state       = b[22];
    h->access      = b[23];
    h->payload_len = net_get32(b + 24);
    h->aux         = net_get32(b + 28);
}

// Valida el encabezado recibido: protocolo, tipo, origen, destino y tamaños. No se confía en
// lo que llega por la red; un mensaje inválido cierra la conexión.
static int valid_header(const msg_header_t *h) {
    if (h->dsm_id != DSM_PROTOCOL_ID || h->version != DSM_PROTOCOL_VERSION) return 0;
    if (h->type == 0 || h->type >= MSG_TYPE_COUNT) return 0;
    if (h->src >= cfg.node_count || h->src == self || h->dst != self) return 0;
    if (h->payload_len > cfg.max_payload || h->page >= pager_page_count()) return 0;
    return 1;
}

/* ----- envío y recepción completos: TCP es un flujo de bytes, no de mensajes ----- */

// Envía len bytes completos. -1 si la conexión falló.
static int send_all(int fd, const void *buf, size_t len) {
    const char *p = buf;
    while (len > 0) {
        ssize_t n = send(fd, p, len, MSG_NOSIGNAL);   // sin SIGPIPE si el otro lado cerró
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return -1;
        p += n;
        len -= (size_t)n;
    }
    return 0;
}

// Recibe len bytes completos. -1 si la conexión se cerró o falló.
static int recv_all(int fd, void *buf, size_t len) {
    char *p = buf;
    while (len > 0) {
        ssize_t n = recv(fd, p, len, 0);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return -1;   // 0 = el otro lado cerró la conexión
        p += n;
        len -= (size_t)n;
    }
    return 0;
}

// Lee y descarta len bytes: la carga de un mensaje que nadie espera. Sin esto, esos bytes se
// leerían como si fueran el encabezado del mensaje siguiente.
static int discard(int fd, uint32_t len) {
    char tmp[4096];
    while (len > 0) {
        uint32_t chunk = len < sizeof tmp ? len : (uint32_t)sizeof tmp;
        if (recv_all(fd, tmp, chunk)) return -1;
        len -= chunk;
    }
    return 0;
}

// Envía encabezado y carga por el socket fd y cuenta el mensaje en las métricas
int net_send_fd(int fd, const msg_header_t *h, const void *payload) {
    atomic_fetch_add(&dsm_counters.msgs_sent, 1);
    atomic_fetch_add(&dsm_counters.bytes_sent, DSM_HEADER_LEN + h->payload_len);
    uint8_t raw[DSM_HEADER_LEN];
    encode_header(h, raw);
    if (send_all(fd, raw, sizeof raw)) return -1;
    if (h->payload_len && send_all(fd, payload, h->payload_len)) return -1;
    return 0;
}

// Opciones de cada conexión: sin retraso de Nagle y con límite de espera al enviar y recibir
static void set_socket_options(int fd) {
    int on = 1;
    struct timeval tv = { cfg.request_timeout_ms / 1000, (cfg.request_timeout_ms % 1000) * 1000 };
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof on);    // envía los mensajes cortos al instante
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof tv);     // un nodo trabado no bloquea
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof tv);     // para siempre a este
}

// Abre una conexión TCP a host:port esperando a lo sumo timeout_ms. Devuelve el socket o -1.
int net_connect(const char *host, uint16_t port, int timeout_ms) {
    char service[8];
    struct addrinfo hints, *res;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    snprintf(service, sizeof service, "%u", port);
    if (getaddrinfo(host, service, &hints, &res)) return -1;

    int fd = socket(res->ai_family, res->ai_socktype | SOCK_CLOEXEC, res->ai_protocol);
    if (fd < 0) {
        freeaddrinfo(res);
        return -1;
    }
    // connect normal puede esperar más de un minuto a un equipo apagado. En modo no bloqueante
    // retorna EINPROGRESS al instante y poll limita la espera a timeout_ms.
    int fl = fcntl(fd, F_GETFL);
    fcntl(fd, F_SETFL, fl | O_NONBLOCK);
    int rc = connect(fd, res->ai_addr, res->ai_addrlen);
    freeaddrinfo(res);
    if (rc < 0 && errno == EINPROGRESS) {
        struct pollfd pfd = { .fd = fd, .events = POLLOUT };
        int err = 0;
        socklen_t len = sizeof err;
        rc = -1;
        if (poll(&pfd, 1, timeout_ms) == 1 &&
            getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) == 0 && err == 0) rc = 0;
    }
    if (rc < 0) {
        close(fd);
        return -1;
    }
    fcntl(fd, F_SETFL, fl);
    set_socket_options(fd);
    return fd;
}

// 1 si este equipo puede escuchar en host:port (la IP es suya y el puerto está libre).
// dsm_init la usa para elegir solo qué línea node de la configuración le corresponde.
int net_can_listen(const char *host, uint16_t port) {
    char service[8];
    struct addrinfo hints, *res;
    memset(&hints, 0, sizeof hints);
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    snprintf(service, sizeof service, "%u", port);
    if (getaddrinfo(host, service, &hints, &res)) return 0;

    int on = 1, ok = 0;
    int fd = socket(res->ai_family, res->ai_socktype | SOCK_CLOEXEC, res->ai_protocol);
    if (fd >= 0) {
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof on);
        // bind falla si la dirección no es de este equipo o si otro proceso ya escucha ahí
        ok = bind(fd, res->ai_addr, res->ai_addrlen) == 0 && listen(fd, 1) == 0;
        close(fd);
    }
    freeaddrinfo(res);
    return ok;
}

// Abre un socket que escucha en el puerto, en todas las interfaces del equipo
static int open_listener(uint16_t port) {
    int on = 1;
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof addr);
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(port);

    int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) return -1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof on);
    if (bind(fd, (struct sockaddr *)&addr, sizeof addr) || listen(fd, DSM_MAX_NODES)) {
        dsm_log(LOG_ERROR, "no se pudo escuchar en el puerto %u: %s (¿ya corre este nodo?)",
                port, strerror(errno));
        close(fd);
        return -1;
    }
    return fd;
}

/* ----- colas de trabajo (productor-consumidor) ----- */

// Crea una cola con cap solicitudes y ready_cap páginas listas
static int queue_init(work_queue_t *q, uint32_t cap, uint32_t ready_cap) {
    memset(q, 0, sizeof *q);
    q->ring = calloc(cap, sizeof *q->ring);
    q->ready_pages = calloc(ready_cap, sizeof *q->ready_pages);
    if (!q->ring || !q->ready_pages) return -1;
    q->cap = cap;
    q->ready_cap = ready_cap;
    pthread_mutex_init(&q->lock, NULL);
    sem_init(&q->free_slots, 0, cap);
    sem_init(&q->items, 0, 0);
    return 0;
}

static void queue_destroy(work_queue_t *q) {
    if (q->cap) {
        pthread_mutex_destroy(&q->lock);
        sem_destroy(&q->free_slots);
        sem_destroy(&q->items);
    }
    free(q->ring);
    free(q->ready_pages);
    memset(q, 0, sizeof *q);
}

// Mete una solicitud en la cola. Si está llena no espera: descarta la solicitud, porque el
// listener no se puede bloquear (dejaría de leer todos los sockets).
static void queue_push(work_queue_t *q, const work_item_t *item) {
    if (sem_trywait(&q->free_slots)) {
        dsm_log(LOG_ERROR, "cola de trabajo llena: se descarta un mensaje del nodo %u", item->from_node);
        return;
    }
    pthread_mutex_lock(&q->lock);
    q->ring[(q->head + q->len) % q->cap] = *item;
    q->len++;
    pthread_mutex_unlock(&q->lock);
    sem_post(&q->items);
}

// Espera trabajo. Devuelve 1 si es una página lista (en *page), 0 si es una solicitud
// (en *item) y -1 si hay que terminar.
static int queue_pop(work_queue_t *q, work_item_t *item, uint32_t *page) {
    while (sem_wait(&q->items) && errno == EINTR) { }
    if (stopping) return -1;

    int is_page = 0;
    pthread_mutex_lock(&q->lock);
    if (q->ready_len > 0) {   // primero las páginas listas: siguen con busy tomado y bloquean a otros
        *page = q->ready_pages[q->ready_head];
        q->ready_head = (q->ready_head + 1) % q->ready_cap;
        q->ready_len--;
        is_page = 1;
    } else {
        *item = q->ring[q->head];
        q->head = (q->head + 1) % q->cap;
        q->len--;
    }
    pthread_mutex_unlock(&q->lock);
    if (!is_page) sem_post(&q->free_slots);
    return is_page;
}

// Avisa que la página tiene la siguiente solicitud lista para atender (ver release_busy)
void net_push_ready(uint32_t page) {
    pthread_mutex_lock(&q_request.lock);
    q_request.ready_pages[(q_request.ready_head + q_request.ready_len) % q_request.ready_cap] = page;
    q_request.ready_len++;
    pthread_mutex_unlock(&q_request.lock);
    sem_post(&q_request.items);
}

// Encola una orden local de unirse a dst; la ejecuta un worker, no el hilo que la pide
void net_push_join(uint16_t dst) {
    work_item_t item;
    memset(&item, 0, sizeof item);
    item.header.type = MSG_JOIN;
    item.header.src = self;   // JOIN propio: orden local de unirse a dst
    item.header.dst = dst;
    item.from_node = self;
    queue_push(&q_request, &item);
}

// Encola una orden local de liberar las asignaciones sin nombre que hizo node (salió o cayó)
void net_push_release(uint16_t node) {
    work_item_t item;
    memset(&item, 0, sizeof item);
    item.header.type = MSG_ALLOC_RELEASE;
    item.header.src = self;   // orden local: liberar lo que asignó node
    item.header.aux = node;
    item.from_node = self;
    queue_push(&q_request, &item);
}

// Hilo worker: saca trabajo de su cola y lo pasa al módulo según el tipo de mensaje.
// Los tipos están numerados por grupo, por eso alcanza con comparar con el primero de cada uno.
static void *worker(void *arg) {
    work_queue_t *q = arg;
    work_item_t item;
    uint32_t page = 0;
    for (;;) {
        int kind = queue_pop(q, &item, &page);
        if (kind < 0) break;
        if (kind == 1) coherence_serve_ready(page);
        else if (item.header.type == MSG_JOIN || item.header.type == MSG_LEAVE) node_handle(&item);
        else if (item.header.type >= MSG_STATS_REQUEST) stats_handle(&item);
        else if (item.header.type >= MSG_ALLOC_MAP) alloc_handle(&item);
        else coherence_handle(&item);
    }
    return NULL;
}

/* ----- solicitudes pendientes: enlazan cada respuesta con quien la espera ----- */

// Hora actual + ms, en el reloj monotónico (no cambia si alguien ajusta la hora del sistema)
static struct timespec time_after_ms(uint32_t ms) {
    struct timespec t;
    clock_gettime(CLOCK_MONOTONIC, &t);
    t.tv_sec += ms / 1000;
    t.tv_nsec += (long)(ms % 1000) * 1000000L;
    if (t.tv_nsec >= 1000000000L) {
        t.tv_sec++;
        t.tv_nsec -= 1000000000L;
    }
    return t;
}

// Entrega una respuesta a quien la espera. La carga viene del socket fd, o de payload
// si fd es -1 (respuesta local). Devuelve -1 solo si falló la lectura del socket.
static int deliver_reply(const msg_header_t *h, int fd, const void *payload) {
    pending_req_t *p = NULL;
    pthread_mutex_lock(&pending_lock);
    for (uint16_t i = 0; i < cfg.max_pending && !p; i++)
        if (pending[i].in_use && pending[i].state == REQ_SENT &&
            pending[i].req_id == h->req_id && pending[i].peer == h->src) p = &pending[i];
    if (p) p->state = REQ_RECEIVING;   // quien espera no puede liberar el slot durante la copia
    pthread_mutex_unlock(&pending_lock);

    int rc = 0, status = h->status;
    if (!p || h->payload_len > p->buf_len) {
        // Respuesta tardía (ya nadie la espera) o más larga que el destino: se descarta
        if (p) status = ERR_PROTOCOL;
        if (fd >= 0) rc = discard(fd, h->payload_len);
    } else if (h->payload_len > 0) {
        if (fd >= 0) rc = recv_all(fd, p->buf, h->payload_len);
        else if (payload != p->buf) memcpy(p->buf, payload, h->payload_len);
    }
    if (!p) return rc;
    if (rc) status = ERR_NODE_DOWN;

    pthread_mutex_lock(&pending_lock);
    p->status = (int16_t)status;
    p->recv_len = rc ? 0 : h->payload_len;
    p->aux = h->aux;
    p->state = REQ_ANSWERED;
    pthread_mutex_unlock(&pending_lock);
    sem_post(&p->done);
    return rc;
}

// Envía una solicitud a dst y bloquea hasta la respuesta o hasta agotar las esperas.
// La carga de la respuesta queda en buf; en h quedan req_id, payload_len y aux de la respuesta.
// Devuelve el status de la respuesta, ERR_TIMEOUT, ERR_NODE_DOWN o ERR_BUSY.
int net_request(uint16_t dst, msg_header_t *h, const void *payload, void *buf, uint32_t buf_len) {
    pending_req_t *p = NULL;
    pthread_mutex_lock(&pending_lock);
    for (uint16_t i = 0; i < cfg.max_pending && !p; i++)
        if (!pending[i].in_use) p = &pending[i];
    if (p) {
        uint32_t id = atomic_fetch_add(&last_req_id, 1) + 1;
        if (id == 0) id = atomic_fetch_add(&last_req_id, 1) + 1;   // 0 significa "ninguna"
        p->in_use = 1;
        p->state = REQ_SENT;
        p->req_id = h->req_id = id;
        p->peer = dst;
        p->buf = buf;
        p->buf_len = buf_len;
        p->access = h->access;
        p->status = ST_OK;
    }
    pthread_mutex_unlock(&pending_lock);
    if (!p) {
        dsm_log(LOG_ERROR, "no hay espacio para otra solicitud pendiente (max_pending=%u)", cfg.max_pending);
        return ERR_BUSY;
    }

    h->flags &= (uint16_t)~FLAG_IS_REPLY;
    int answered = 0;
    if (node_send(dst, h, payload) == 0) {
        // TCP no pierde mensajes: un "reintento" es seguir esperando, sin reenviar.
        // La unión espera una sola vez: el hilo de heartbeat la reintenta después.
        int waits = h->type == MSG_JOIN ? 1 : cfg.request_retries + 1;
        for (int attempt = 0; attempt < waits && !answered; attempt++) {
            struct timespec until = time_after_ms(cfg.request_timeout_ms);
            int rc;
            do rc = sem_clockwait(&p->done, CLOCK_MONOTONIC, &until); while (rc && errno == EINTR);
            if (rc == 0) answered = 1;
            else dsm_log(LOG_WARN, "sin respuesta del nodo %u al mensaje %u (página %u), espera %d de %d",
                         dst, h->type, h->page, attempt + 1, waits);
        }
    } else {
        pthread_mutex_lock(&pending_lock);
        p->in_use = 0;
        pthread_mutex_unlock(&pending_lock);
        return ERR_NODE_DOWN;
    }

    pthread_mutex_lock(&pending_lock);
    // La respuesta llegó justo al vencer la espera y el listener la está copiando: hay que
    // esperar a que termine, porque está escribiendo en buf
    if (!answered && p->state != REQ_SENT) {
        pthread_mutex_unlock(&pending_lock);
        while (sem_wait(&p->done) && errno == EINTR) { }
        pthread_mutex_lock(&pending_lock);
        answered = 1;
    }
    int status = answered ? p->status : ERR_TIMEOUT;
    if (answered) {   // largo de la carga y argumento de la respuesta, para quien los necesite
        h->payload_len = p->recv_len;
        h->aux = p->aux;
    }
    p->in_use = 0;
    pthread_mutex_unlock(&pending_lock);
    return status;
}

// Despierta con ERR_NODE_DOWN a todos los que esperan respuesta de peer (que cayó)
void net_fail_pending(uint16_t peer) {
    pthread_mutex_lock(&pending_lock);
    for (uint16_t i = 0; i < cfg.max_pending; i++) {
        pending_req_t *p = &pending[i];
        if (!p->in_use || p->peer != peer || p->state != REQ_SENT) continue;
        p->status = ERR_NODE_DOWN;
        p->state = REQ_ANSWERED;
        sem_post(&p->done);
    }
    pthread_mutex_unlock(&pending_lock);
}

/* ----- reparto de los mensajes recibidos ----- */

// Cola que atiende cada tipo de solicitud; NULL si el tipo solo existe como respuesta
static work_queue_t *queue_for(uint8_t type) {
    switch (type) {
        case MSG_JOIN: case MSG_LEAVE: case MSG_PAGE_REQUEST: return &q_request;
        case MSG_ALLOC_MAP: case MSG_ALLOC_RESERVE: case MSG_ALLOC_RELEASE: case MSG_NAME_LOOKUP:
        case MSG_STATS_REQUEST: case MSG_PAGETABLE_REQUEST: case MSG_ALLOCS_REQUEST:
            return &q_request;
        case MSG_PAGE_ACK: case MSG_OWNER_FETCH: case MSG_INVALIDATE: return &q_service;
        default: return NULL;   // los demás tipos solo existen como respuesta
    }
}

// Entrega un mensaje de este nodo a sí mismo, sin socket: lo mismo que haría el listener
void net_deliver(const msg_header_t *h, const void *payload) {
    if (h->flags & FLAG_IS_REPLY) {
        deliver_reply(h, -1, payload);
        return;
    }
    work_queue_t *q = queue_for(h->type);
    if (!q || h->payload_len > DSM_WORK_PAYLOAD) return;
    work_item_t item;
    memset(&item, 0, sizeof item);
    item.header = *h;
    item.from_node = h->src;
    if (h->payload_len) memcpy(item.payload, payload, h->payload_len);
    queue_push(q, &item);
}

/* ----- hilo listener: lee todas las conexiones entrantes; nunca envía ----- */

// Acepta una conexión entrante y la agrega a las que vigila poll
static void accept_conn(int listen_fd) {
    int fd = accept4(listen_fd, NULL, NULL, SOCK_CLOEXEC);
    if (fd < 0) return;
    if (n_conns == MAX_CONNS) {
        dsm_log(LOG_WARN, "demasiadas conexiones entrantes: se rechaza una");
        close(fd);
        return;
    }
    set_socket_options(fd);
    conns[n_conns].fd = fd;
    conns[n_conns].events = POLLIN;
    conns[n_conns].revents = 0;
    conn_node[n_conns] = -1;
    n_conns++;
}

// Lee un mensaje completo de la conexión i. Devuelve -1 si hay que cerrarla.
static int read_message(int i) {
    int fd = conns[i].fd;
    uint8_t raw[DSM_HEADER_LEN];
    msg_header_t h;
    if (recv_all(fd, raw, sizeof raw)) return -1;
    decode_header(raw, &h);
    if (!valid_header(&h)) {
        dsm_log(LOG_ERROR, "mensaje inválido (tipo %u, origen %u, %u bytes): se cierra la conexión",
                h.type, h.src, h.payload_len);
        conn_node[i] = -1;
        return -1;
    }
    conn_node[i] = h.src;
    atomic_fetch_add(&dsm_counters.msgs_received, 1);
    atomic_fetch_add(&dsm_counters.bytes_received, DSM_HEADER_LEN + h.payload_len);

    // Solo la unión puede venir de un nodo que aún no está en línea
    int joining = h.type == MSG_JOIN || h.type == MSG_JOIN_ACK;
    if (!joining && !node_is_online(h.src)) {
        dsm_log(LOG_DEBUG, "mensaje tipo %u del nodo %u, que aquí no está en línea: se cierra su conexión",
                h.type, h.src);
        conn_node[i] = -1;
        return -1;
    }
    if (!joining) node_seen(h.src);   // cualquier mensaje cuenta como señal de vida

    if (h.flags & FLAG_IS_REPLY) return deliver_reply(&h, fd, NULL);

    if (h.type == MSG_HEARTBEAT) {   // el listener lo atiende directo: es corto y no responde
        uint8_t hb[DSM_HB_PAYLOAD_LEN];
        if (h.payload_len != sizeof hb || recv_all(fd, hb, sizeof hb)) return -1;
        node_heartbeat(h.src, net_get32(hb));
        return 0;
    }

    work_queue_t *q = queue_for(h.type);
    work_item_t item;
    memset(&item, 0, sizeof item);
    item.header = h;
    item.from_node = h.src;
    if (!q || h.payload_len > sizeof item.payload) {
        dsm_log(LOG_ERROR, "solicitud inválida del nodo %u (tipo %u): se cierra la conexión", h.src, h.type);
        return -1;
    }
    if (h.payload_len && recv_all(fd, item.payload, h.payload_len)) return -1;
    queue_push(q, &item);
    return 0;
}

// Hilo listener: un solo hilo vigila todos los sockets con poll y lee lo que llegue.
static void *listener(void *arg) {
    (void)arg;
    while (!stopping) {
        if (poll(conns, (nfds_t)n_conns, 100) <= 0) continue;   // 100 ms: revisa stopping
        // Hacia atrás: al cerrar una conexión, su lugar lo toma la última, ya revisada
        for (int i = n_conns - 1; i >= 0; i--) {
            if (!conns[i].revents) continue;
            if (i < LISTENERS) {
                accept_conn(conns[i].fd);
            } else if (read_message(i)) {
                int node = conn_node[i];
                close(conns[i].fd);
                conns[i] = conns[n_conns - 1];
                conn_node[i] = conn_node[n_conns - 1];
                n_conns--;
                // Una conexión cerrada significa que el proceso del otro nodo terminó
                if (node >= 0) node_down((uint16_t)node, "cerró la conexión");
            }
        }
    }
    return NULL;
}

/* ----- inicio y liberación ----- */

// Detiene los hilos y cierra todo. Los sem_post despiertan a los workers dormidos para que
// vean stopping y terminen.
void net_shutdown(void) {
    stopping = 1;
    for (int i = 0; i < n_workers; i++) sem_post(&q_request.items);
    for (int i = 0; i < n_service; i++) sem_post(&q_service.items);
    for (int i = 0; i < n_workers; i++) pthread_join(worker_threads[i], NULL);
    for (int i = 0; i < n_service; i++) pthread_join(service_threads[i], NULL);
    if (listener_started) pthread_join(listener_thread, NULL);
    n_workers = n_service = listener_started = 0;

    for (int i = 0; i < n_conns; i++) close(conns[i].fd);
    n_conns = 0;
    queue_destroy(&q_request);
    queue_destroy(&q_service);
    if (pending)
        for (uint16_t i = 0; i < cfg.max_pending; i++) sem_destroy(&pending[i].done);
    free(pending);
    pending = NULL;
    stopping = 0;
}

// Abre los puertos de datos y heartbeat del nodo, crea las colas y arranca los hilos
int net_init(const dsm_config_t *c, uint16_t self_id) {
    cfg = *c;
    self = self_id;
    stopping = 0;

    int data_fd = open_listener(c->nodes[self].data_port);
    int hb_fd = data_fd < 0 ? -1 : open_listener(c->nodes[self].hb_port);
    if (hb_fd < 0) {
        if (data_fd >= 0) close(data_fd);
        return -1;
    }
    conns[0].fd = data_fd;
    conns[1].fd = hb_fd;
    conns[0].events = conns[1].events = POLLIN;
    conn_node[0] = conn_node[1] = -1;
    n_conns = LISTENERS;

    pending = calloc(c->max_pending, sizeof *pending);
    if (!pending) goto fail;
    for (uint16_t i = 0; i < c->max_pending; i++) sem_init(&pending[i].done, 0, 0);

    // Capacidad holgada: cada nodo puede tener a lo sumo max_pending solicitudes en vuelo
    uint32_t cap = (uint32_t)c->max_pending * c->node_count;
    uint32_t first, home_pages;
    if (pager_node_pages(self, &first, &home_pages)) goto fail;
    if (queue_init(&q_request, cap, home_pages) || queue_init(&q_service, cap, 1)) goto fail;

    for (int i = 0; i < c->protocol_workers; i++) {
        if (pthread_create(&worker_threads[i], NULL, worker, &q_request)) goto fail;
        n_workers++;
    }
    for (int i = 0; i < c->service_threads; i++) {
        if (pthread_create(&service_threads[i], NULL, worker, &q_service)) goto fail;
        n_service++;
    }
    if (pthread_create(&listener_thread, NULL, listener, NULL)) goto fail;
    listener_started = 1;
    return 0;

fail:
    dsm_log(LOG_ERROR, "no se pudo iniciar la red");
    net_shutdown();
    return -1;
}
