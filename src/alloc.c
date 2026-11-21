// Asignación de memoria del DSM (dsm_alloc y dsm_free). Cada home marca en su directorio a
// qué asignación pertenece cada una de sus páginas, y guarda un registro por cada asignación
// que toca su rango. Quien asigna pide el mapa de páginas libres a los nodos en línea, elige
// un tramo contiguo y lo reserva en cada home; si otro se adelanta, deshace y repite.
#include <pthread.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "alloc.h"
#include "coherence.h"
#include "config.h"
#include "network.h"
#include "node.h"
#include "pager.h"

enum { ATTEMPTS = 8 };   // veces que se repite una asignación si otro nodo gana la reserva

static dsm_config_t cfg;
static uint16_t self;
static int ready;
static uint32_t home_first, home_count;

// Asignaciones con páginas en este home; a lo sumo una por página
static alloc_record_t *home_recs;
static uint32_t n_home;
static pthread_mutex_t home_lock = PTHREAD_MUTEX_INITIALIZER;

// Asignaciones que este proceso creó o adjuntó; crece según haga falta
static alloc_record_t *own_recs;
static uint32_t n_own, cap_own;
static pthread_mutex_t own_lock = PTHREAD_MUTEX_INITIALIZER;

// alloc_id = node_id en los 8 bits altos + contador de 24 bits: es único en todo el clúster
// sin que los nodos tengan que coordinarse. 0 significa "página sin asignar".
static atomic_uint next_id;

// Turno para asignar: los hilos de este proceso asignan de uno en uno y no compiten entre
// sí por las mismas páginas. Solo lo toman hilos de la aplicación, nunca los que atienden
// la red, así que esperar una respuesta con él tomado no puede bloquear a otro nodo.
static pthread_mutex_t create_lock = PTHREAD_MUTEX_INITIALIZER;

/* ----- formato en la red ----- */

// alloc_desc (56 bytes): la asignación, o el tramo de ella que toca a un home
static void put_desc(uint8_t *b, const alloc_record_t *r, uint32_t first, uint32_t count) {
    net_put32(b, r->alloc_id);
    net_put32(b + 4, first);
    net_put32(b + 8, count);
    net_put32(b + 12, first - r->first_page);   // posición del tramo dentro de la asignación
    net_put32(b + 16, (uint32_t)r->pid);
    net_put16(b + 20, r->node);
    net_put16(b + 22, r->flags);
    memcpy(b + 24, r->tag, DSM_TAG_MAX);
}

// Inverso de put_desc. Deja en first_page y n_pages el tramo que viajó.
static void get_desc(const uint8_t *b, alloc_record_t *r) {
    r->alloc_id   = net_get32(b);
    r->first_page = net_get32(b + 4);
    r->n_pages    = net_get32(b + 8);
    r->pid        = (int32_t)net_get32(b + 16);
    r->node       = net_get16(b + 20);
    r->flags      = net_get16(b + 22);
    memcpy(r->tag, b + 24, DSM_TAG_MAX);
    r->tag[DSM_TAG_MAX - 1] = '\0';
}

// Versiones públicas para stats.c: siempre la asignación completa
void alloc_put_desc(uint8_t *b, const alloc_record_t *r) {
    put_desc(b, r, r->first_page, r->n_pages);
}

void alloc_get_desc(const uint8_t *b, alloc_record_t *r) {
    get_desc(b, r);
}

// Tramo de la asignación r que cae en las páginas del nodo node: la intersección de los dos
// rangos. Devuelve 1 y el tramo en first/count, o 0 si la asignación no toca ese nodo.
static int segment(const alloc_record_t *r, uint16_t node, uint32_t *first, uint32_t *count) {
    uint32_t node_first, node_count;
    if (pager_node_pages(node, &node_first, &node_count)) return 0;
    uint32_t begin = r->first_page > node_first ? r->first_page : node_first;
    uint32_t end = r->first_page + r->n_pages;
    if (end > node_first + node_count) end = node_first + node_count;
    if (begin >= end) return 0;
    *first = begin;
    *count = end - begin;
    return 1;
}

/* ----- lado del home ----- */

// Reserva en este home el tramo de r y anota el registro. -1 si alguna página ya estaba ocupada.
// home_recs tiene lugar para home_count registros: cada página es de a lo sumo una asignación.
static int home_reserve(const alloc_record_t *r, uint32_t first, uint32_t count) {
    if (coherence_reserve(first, count, r->alloc_id, first - r->first_page, r->pid, r->node)) return -1;
    pthread_mutex_lock(&home_lock);
    home_recs[n_home] = *r;
    home_recs[n_home].flags &= (uint16_t)~ALLOC_ATTACHED;
    n_home++;
    pthread_mutex_unlock(&home_lock);
    return 0;
}

// Libera en este home el tramo de la asignación y borra su registro (el último ocupa su lugar)
static void home_release(uint32_t alloc_id, uint32_t first, uint32_t count) {
    coherence_release(alloc_id, first, count);   // sin locks: invalida copias en otros nodos
    pthread_mutex_lock(&home_lock);
    for (uint32_t i = 0; i < n_home; i++) {
        if (home_recs[i].alloc_id != alloc_id) continue;
        home_recs[i] = home_recs[--n_home];
        break;
    }
    pthread_mutex_unlock(&home_lock);
}

// Un nodo dejó el clúster: libera en este home las asignaciones sin nombre de su proceso.
// Las que tienen nombre se conservan, porque otro proceso puede adjuntarlas después.
// Busca una por vuelta y suelta home_lock antes de liberar, porque liberar usa la red.
static void release_node(uint16_t node) {
    for (;;) {
        alloc_record_t r;
        int found = 0;
        pthread_mutex_lock(&home_lock);
        for (uint32_t i = 0; i < n_home && !found; i++) {
            if (home_recs[i].node != node || (home_recs[i].flags & ALLOC_NAMED)) continue;
            r = home_recs[i];
            found = 1;
        }
        pthread_mutex_unlock(&home_lock);
        if (!found) return;

        uint32_t first, count;
        segment(&r, self, &first, &count);
        home_release(r.alloc_id, first, count);
        dsm_log(LOG_INFO, "se liberaron %u páginas que había asignado el nodo %u", count, node);
    }
}

// Busca en la lista una asignación con ese nombre. 1 si la encontró.
static int find_named(const alloc_record_t *list, uint32_t n, const char *name, alloc_record_t *out) {
    for (uint32_t i = 0; i < n; i++) {
        if (!(list[i].flags & ALLOC_NAMED) || strcmp(list[i].tag, name) != 0) continue;
        *out = list[i];
        return 1;
    }
    return 0;
}

// Atiende los mensajes de asignación que llegan a este home:
// ALLOC_MAP (envía el bitmap de libres), ALLOC_RESERVE (reserva un tramo), ALLOC_RELEASE
// (libera un tramo) y NAME_LOOKUP (busca una asignación por nombre).
void alloc_handle(const work_item_t *item) {
    const msg_header_t *h = &item->header;
    uint16_t node = item->from_node;
    if (!ready) return;
    if (node == self) {   // orden local (node_down): liberar lo que asignó el nodo h->aux
        release_node((uint16_t)h->aux);
        return;
    }
    msg_header_t reply = { .type = h->type, .flags = FLAG_IS_REPLY, .req_id = h->req_id, .page = h->page };

    if (h->type == MSG_ALLOC_MAP) {
        uint32_t len = (home_count + 7) / 8;
        uint8_t *map = malloc(len);
        if (!map) reply.status = ERR_BUSY;
        else {
            coherence_free_map(map);
            reply.payload_len = len;
        }
        node_send(node, &reply, map);
        free(map);
    } else if (h->type == MSG_ALLOC_RESERVE) {
        // La cabecera lleva la asignación completa (page, aux) y la carga, el tramo de este home
        alloc_record_t seg, whole;
        reply.status = ERR_PROTOCOL;
        if (h->payload_len == DSM_ALLOC_DESC_LEN) {
            get_desc(item->payload, &seg);
            whole = seg;
            whole.first_page = h->page;
            whole.n_pages = h->aux;
            reply.status = home_reserve(&whole, seg.first_page, seg.n_pages) ? ERR_BUSY : ST_OK;
        }
        node_send(node, &reply, NULL);
    } else if (h->type == MSG_ALLOC_RELEASE) {
        if (h->payload_len == DSM_ALLOC_RELEASE_LEN)
            home_release(net_get32(item->payload), net_get32(item->payload + 4), net_get32(item->payload + 8));
        node_send(node, &reply, NULL);
    } else if (h->type == MSG_NAME_LOOKUP) {
        char name[DSM_TAG_MAX];
        uint8_t desc[DSM_ALLOC_DESC_LEN];
        alloc_record_t r;
        memcpy(name, item->payload, sizeof name);
        name[sizeof name - 1] = '\0';
        pthread_mutex_lock(&home_lock);
        int found = h->payload_len == DSM_TAG_MAX && find_named(home_recs, n_home, name, &r);
        pthread_mutex_unlock(&home_lock);
        if (found) {
            put_desc(desc, &r, r.first_page, r.n_pages);
            reply.payload_len = sizeof desc;
        } else {
            reply.status = ERR_NOT_FOUND;
        }
        node_send(node, &reply, desc);
    }
}

/* ----- lado de quien asigna ----- */

// Marca en is_free (un byte por página global) las páginas libres del nodo. Si el nodo no
// responde, sus páginas quedan en 0 (no disponibles).
static void load_free_pages(uint16_t node, uint8_t *is_free) {
    uint32_t first, count;
    if (pager_node_pages(node, &first, &count)) return;
    uint32_t len = (count + 7) / 8;
    uint8_t *map = calloc(1, len);
    if (!map) return;

    int ok = 1;
    if (node == self) coherence_free_map(map);
    else {
        msg_header_t h = { .type = MSG_ALLOC_MAP };
        ok = node_is_online(node) && net_request(node, &h, NULL, map, len) == ST_OK;
    }
    for (uint32_t i = 0; ok && i < count; i++)
        is_free[first + i] = (map[i / 8] >> (i % 8)) & 1;
    free(map);
}

// First-fit: primer tramo de n páginas libres seguidas dentro de [begin, end). Cuenta las
// libres consecutivas y se reinicia en cada ocupada. Devuelve 1 y el inicio, o 0 si no hay.
static int find_run(const uint8_t *is_free, uint32_t begin, uint32_t end, uint32_t n, uint32_t *first) {
    uint32_t run = 0;
    for (uint32_t p = begin; p < end; p++) {
        run = is_free[p] ? run + 1 : 0;
        if (run == n) {
            *first = p + 1 - n;
            return 1;
        }
    }
    return 0;
}

// Libera la asignación en los homes con id menor que limit (todos si limit = node_count)
static void release_homes(const alloc_record_t *r, uint16_t limit) {
    for (uint16_t node = 0; node < limit; node++) {
        uint32_t first, count;
        if (!segment(r, node, &first, &count)) continue;
        if (node == self) {
            home_release(r->alloc_id, first, count);
            continue;
        }
        uint8_t rel[DSM_ALLOC_RELEASE_LEN];
        net_put32(rel, r->alloc_id);
        net_put32(rel + 4, first);
        net_put32(rel + 8, count);
        msg_header_t h = { .type = MSG_ALLOC_RELEASE, .page = first, .payload_len = sizeof rel };
        net_request(node, &h, rel, NULL, 0);   // si el nodo cayó, sus páginas ya no existen
    }
}

// Reserva la asignación en cada home que toca, en orden de nodo. Las reservas no esperan:
// si un home ya dio alguna página a otro, deshace lo reservado en los homes anteriores. 0 o -1.
static int reserve_homes(const alloc_record_t *r) {
    for (uint16_t node = 0; node < cfg.node_count; node++) {
        uint32_t first, count;
        if (!segment(r, node, &first, &count)) continue;
        int rc;
        if (node == self) {
            rc = home_reserve(r, first, count);
        } else {
            uint8_t desc[DSM_ALLOC_DESC_LEN];
            put_desc(desc, r, first, count);
            msg_header_t h = { .type = MSG_ALLOC_RESERVE, .page = r->first_page, .aux = r->n_pages,
                               .payload_len = sizeof desc };
            rc = net_request(node, &h, desc, NULL, 0) == ST_OK ? 0 : -1;
        }
        if (rc) {
            release_homes(r, node);
            return -1;
        }
    }
    return 0;
}

// Anota la asignación en la lista de este proceso; duplica la capacidad cuando se llena
static int add_own(const alloc_record_t *r) {
    pthread_mutex_lock(&own_lock);
    if (n_own == cap_own) {
        uint32_t cap = cap_own ? cap_own * 2 : 16;
        alloc_record_t *grown = realloc(own_recs, cap * sizeof *grown);
        if (!grown) {
            pthread_mutex_unlock(&own_lock);
            return -1;
        }
        own_recs = grown;
        cap_own = cap;
    }
    own_recs[n_own++] = *r;
    pthread_mutex_unlock(&own_lock);
    return 0;
}

// Crea una asignación de size bytes (redondeado a páginas enteras). Pasos de cada intento:
// 1) junta el mapa de libres de todos los nodos, 2) busca un tramo (primero en este nodo,
// luego en todo el espacio), 3) lo reserva en cada home. Devuelve 0 y el registro en *out.
int alloc_create(uint64_t size, const char *tag, uint16_t flags, alloc_record_t *out) {
    uint32_t total = pager_page_count();
    uint64_t pages = (size + cfg.page_size - 1) / cfg.page_size;
    if (!ready || pages == 0 || pages > total) return -1;

    alloc_record_t r;
    memset(&r, 0, sizeof r);
    r.n_pages = (uint32_t)pages;
    r.pid = (int32_t)getpid();
    r.node = self;
    r.flags = flags;
    if (tag) strncpy(r.tag, tag, DSM_TAG_MAX - 1);

    uint8_t *is_free = malloc(total);
    if (!is_free) return -1;
    int done = 0;
    pthread_mutex_lock(&create_lock);
    for (int attempt = 0; attempt < ATTEMPTS && !done; attempt++) {
        // Otro nodo ganó la reserva. La pausa crece con el id del nodo: si dos nodos chocan,
        // no reintentan al mismo tiempo y no vuelven a chocar.
        if (attempt > 0) usleep(1000 * (useconds_t)(self + 1) * (useconds_t)attempt);
        memset(is_free, 0, total);   // las páginas de un nodo fuera de línea no se pueden usar
        for (uint16_t node = 0; node < cfg.node_count; node++) load_free_pages(node, is_free);

        // Primero en este nodo; si no cabe, en cualquier tramo contiguo del espacio global
        if (!find_run(is_free, home_first, home_first + home_count, r.n_pages, &r.first_page) &&
            !find_run(is_free, 0, total, r.n_pages, &r.first_page)) break;

        uint32_t counter = atomic_fetch_add(&next_id, 1) & 0xFFFFFF;
        if (counter == 0) counter = atomic_fetch_add(&next_id, 1) & 0xFFFFFF;   // 0 = página libre
        r.alloc_id = (uint32_t)self << 24 | counter;
        done = reserve_homes(&r) == 0;
    }
    pthread_mutex_unlock(&create_lock);
    free(is_free);
    if (!done) return -1;

    if (add_own(&r)) {
        release_homes(&r, cfg.node_count);
        return -1;
    }
    *out = r;
    return 0;
}

// Libera la asignación de este proceso que empieza en first_page, en todos sus homes
int alloc_destroy(uint32_t first_page) {
    alloc_record_t r;
    int found = 0;
    pthread_mutex_lock(&own_lock);
    for (uint32_t i = 0; i < n_own && !found; i++) {
        if (own_recs[i].first_page != first_page) continue;
        r = own_recs[i];
        own_recs[i] = own_recs[--n_own];
        found = 1;
    }
    pthread_mutex_unlock(&own_lock);
    if (!found) return -1;
    release_homes(&r, cfg.node_count);
    return 0;
}

// Busca una asignación con nombre: primero en este home y luego preguntando a cada nodo
int alloc_find(const char *name, alloc_record_t *out) {
    if (!ready) return -1;
    // Responden los homes, que saben si sigue existiendo; lo anotado en este proceso
    // puede ser de una asignación que otro ya liberó
    pthread_mutex_lock(&home_lock);
    int found = find_named(home_recs, n_home, name, out);
    pthread_mutex_unlock(&home_lock);
    if (found) return 0;

    char wire[DSM_TAG_MAX];
    memset(wire, 0, sizeof wire);
    strncpy(wire, name, sizeof wire - 1);
    for (uint16_t node = 0; node < cfg.node_count; node++) {
        if (node == self || !node_is_online(node)) continue;
        uint8_t desc[DSM_ALLOC_DESC_LEN];
        msg_header_t h = { .type = MSG_NAME_LOOKUP, .payload_len = sizeof wire };
        if (net_request(node, &h, wire, desc, sizeof desc) != ST_OK) continue;
        get_desc(desc, out);
        return 0;
    }
    return -1;
}

// Anota en este proceso una asignación ajena (dsm_attach). ALLOC_ATTACHED evita que se
// libere cuando este proceso termina: no es suya.
int alloc_attach(const alloc_record_t *rec) {
    // Si este proceso ya la tiene (la creó o la adjuntó antes) no se anota otra vez
    int known = 0;
    pthread_mutex_lock(&own_lock);
    for (uint32_t i = 0; i < n_own; i++)
        if (own_recs[i].first_page == rec->first_page) known = 1;
    pthread_mutex_unlock(&own_lock);
    if (known) return 0;

    alloc_record_t r = *rec;
    r.flags |= ALLOC_ATTACHED;
    return add_own(&r);
}

// Al salir el proceso: libera sus asignaciones sin nombre y que creó él mismo
void alloc_release_own(void) {
    for (;;) {
        uint32_t first = 0;
        int found = 0;
        pthread_mutex_lock(&own_lock);
        for (uint32_t i = 0; i < n_own && !found; i++) {
            if (own_recs[i].flags & (ALLOC_NAMED | ALLOC_ATTACHED)) continue;
            first = own_recs[i].first_page;
            found = 1;
        }
        pthread_mutex_unlock(&own_lock);
        if (!found) return;
        alloc_destroy(first);
    }
}

/* ----- consultas, inicio y liberación ----- */

// Copian hasta max registros y devuelven cuántos hay en total (puede ser más que max)
uint32_t alloc_list_home(alloc_record_t *out, uint32_t max) {
    pthread_mutex_lock(&home_lock);
    uint32_t n = n_home;
    for (uint32_t i = 0; i < n && i < max; i++) out[i] = home_recs[i];
    pthread_mutex_unlock(&home_lock);
    return n;
}

uint32_t alloc_list_own(alloc_record_t *out, uint32_t max) {
    pthread_mutex_lock(&own_lock);
    uint32_t n = n_own;
    for (uint32_t i = 0; i < n && i < max; i++) out[i] = own_recs[i];
    pthread_mutex_unlock(&own_lock);
    return n;
}

int alloc_init(const dsm_config_t *c, uint16_t self_id) {
    cfg = *c;
    self = self_id;
    if (pager_node_pages(self, &home_first, &home_count)) return -1;
    home_recs = calloc(home_count, sizeof *home_recs);
    if (!home_recs) {
        dsm_log(LOG_ERROR, "no hay memoria para las asignaciones de %u páginas", home_count);
        return -1;
    }
    n_home = n_own = cap_own = 0;
    own_recs = NULL;
    // Arranca en un valor distinto cada vez: no repite los id de una ejecución anterior
    atomic_store(&next_id, (unsigned)time(NULL));
    ready = 1;
    return 0;
}

void alloc_shutdown(void) {
    if (!ready) return;
    ready = 0;
    free(home_recs);
    free(own_recs);
    home_recs = own_recs = NULL;
    n_home = n_own = cap_own = 0;
}
