#ifndef ESTRUCTURAS_H
#define ESTRUCTURAS_H

#include <pthread.h>
#include <semaphore.h>
#include <stdint.h>
#include <time.h>

#define DSM_MAX_NODES   50
#define DSM_HOST_MAX    64 // max len para IPs o hostnames
#define DSM_MAX_THREADS 64 // hilos de aplicación por nodo: un bit por hilo en waiters_mask

#define DSM_PROTOCOL_ID      0x54454344u // firma de los mensajes: "TECD"
#define DSM_PROTOCOL_VERSION 1
#define DSM_HEADER_LEN       32 // bytes de la cabecera en la red
#define DSM_WORK_PAYLOAD     96 // mayor carga fija de una solicitud (node_desc)
#define DSM_NODE_DESC_LEN    96
#define DSM_HB_PAYLOAD_LEN   16

enum { LOG_ERROR = 0, LOG_WARN, LOG_INFO, LOG_DEBUG };

// Estado MSI de la copia local de una página; fija su permiso en la vista de aplicación
enum {
    PAGE_INVALID = 0,   // PROT_NONE
    PAGE_SHARED,        // PROT_READ
    PAGE_MODIFIED       // PROT_READ | PROT_WRITE
};

// Tipo de acceso que provocó un fallo; UNKNOWN si no se puede informar debidamente
enum { ACCESS_UNKNOWN = 0, ACCESS_READ = 1, ACCESS_WRITE = 2 };

enum { NODE_UNKNOWN = 0, NODE_ONLINE, NODE_OFFLINE };   // estado en la tabla de nodos
enum { DIR_UNCACHED = 0, DIR_SHARED, DIR_MODIFIED };    // estado de una página en su home
enum { ALLOC_FREE = 0, ALLOC_RESERVED, ALLOC_ALLOCATED, ALLOC_LOST };
enum { REQ_SENT = 1, REQ_RECEIVING, REQ_ANSWERED };     // vida de una solicitud pendiente

// Tipos de mensaje
enum {
    MSG_JOIN = 1,        // unión al clúster: lleva node_desc con el hash de la configuración
    MSG_JOIN_ACK,        // respuesta a JOIN
    MSG_LEAVE,           // salida ordenada; su respuesta es LEAVE con FLAG_IS_REPLY
    MSG_HEARTBEAT,
    MSG_PAGE_REQUEST,    // solicitante -> home
    MSG_PAGE_DATA,       // respuesta a PAGE_REQUEST y a OWNER_FETCH
    MSG_PAGE_ACK,        // solicitante -> home: la página quedó instalada
    MSG_OWNER_FETCH,     // home -> owner: devolver la página modificada
    MSG_INVALIDATE,      // home -> nodo con copia
    MSG_INVALIDATE_ACK,
    MSG_TYPE_COUNT
};

enum { FLAG_HAS_DATA = 1, FLAG_MORE = 2, FLAG_IS_REPLY = 4 };   // msg_header_t.flags

// msg_header_t.status
enum {
    ST_OK = 0,
    ERR_CONFIG_MISMATCH,   // el hash de dsm.conf no coincide
    ERR_NODE_DOWN,         // el nodo destino no está en línea
    ERR_TIMEOUT,           // no llegó la respuesta a tiempo
    ERR_BUSY,              // no hay espacio para otra solicitud
    ERR_PROTOCOL,          // mensaje inválido
    ERR_NOT_OWNER          // se pidió la página a un nodo que no la tiene en M
};

// ----- configuración -----

typedef struct {
    uint16_t node_id;
    char     host[DSM_HOST_MAX];
    uint16_t data_port;
    uint16_t hb_port; // hb = heartbeat
    uint64_t memory_bytes;
} node_config_t;

typedef struct {
    uint64_t default_node_memory;
    uint64_t base_address;
    uint64_t config_hash;
    uint32_t page_size;
    uint32_t hb_interval_ms;
    uint32_t hb_timeout_ms;
    uint32_t request_timeout_ms;
    uint32_t max_payload;
    uint16_t port_min, port_max;
    uint16_t max_threads, max_pending, node_count;
    uint8_t  request_retries, fault_threads, service_threads, protocol_workers;
    uint8_t  log_level;
    node_config_t nodes[DSM_MAX_NODES];
} dsm_config_t;

// ----- paginador -----

typedef struct {                    // copia local de una página; una por página global
    pthread_mutex_t lock;
    uint64_t waiters_mask;          // bit i = el hilo del slot i espera esta página
    uint8_t  state;                 // estado MSI local
    uint8_t  pending;               // 1 si hay un fallo en curso (no es un estado MSI)
    uint8_t  pending_access;        // acceso pedido en el fallo en curso (READ o WRITE)
    uint8_t  flags;                 // bit 0 = LOST
    uint32_t read_faults;
    uint32_t write_faults;
    uint32_t invalidations;         // invalidaciones recibidas
} local_page_t;

typedef struct {                    // solicitud en espera en la cola de una página
    uint32_t req_id;
    uint16_t node;                  // nodo solicitante
    uint8_t  access;
    uint8_t  pad;
} dir_waiter_t;

typedef struct {                    // entrada del directorio; una por página que aporta el nodo
    pthread_mutex_t lock;
    uint64_t copyset;               // bit i = el nodo i tiene copia (S o M)
    uint32_t alloc_id;              // identificar asignaciones (0 = libnre)
    uint32_t alloc_order;
    int32_t  alloc_pid;
    uint32_t transac_req_id;        // transacción que espera PAGE_ACK (0 = ninguna)
    struct timespec transac_deadline;
    uint16_t owner;                 // nodo con la copia M; válido si dir_state = DIR_MODIFIED
    uint16_t alloc_node;
    uint16_t transac_node;
    uint8_t  dir_state;             // DIR_UNCACHED / DIR_SHARED / DIR_MODIFIED
    uint8_t  busy;                  // 1 mientras hay una transacción en curso
    uint8_t  alloc_state;           // ALLOC_LOST: su última versión se perdió con un nodo caído
    uint8_t  q_head, q_len;         // cola circular
    uint8_t  transac_access;
    dir_waiter_t queue[DSM_MAX_NODES];   // FIFO; a lo sumo una solicitud por nodo
} dir_entry_t;

typedef struct {                    // hilo de aplicación registrado
    int       reply_r, reply_w;     // pipe por el que el servicio de fallos lo despierta
    pthread_t tid;
    uint8_t   in_use;
} thread_slot_t;

typedef struct {                    // aviso del manejador al servicio de fallos
    uint32_t page;                  // página que provocó el fallo
    uint8_t  access;                // ACCESS_READ / ACCESS_WRITE / ACCESS_UNKNOWN
    uint16_t slot;                  // slot del hilo que falló
    uint64_t addr;                  // si_addr, para diagnóstico
} fault_record_t;

// ----- red -----

typedef struct {                    // cabecera de un mensaje (en la red va campo a campo)
    uint32_t dsm_id;                // DSM_PROTOCOL_ID
    uint8_t  version;
    uint8_t  type;                  // MSG_*
    uint16_t flags;                 // FLAG_*
    uint16_t src, dst;              // nodo emisor y destinatario
    uint32_t req_id;                // enlaza la respuesta con su solicitud
    uint32_t page;
    uint16_t status;                // ST_OK o ERR_*
    uint8_t  state;                 // estado MSI concedido
    uint8_t  access;
    uint32_t payload_len;
    uint32_t aux;
} msg_header_t;

typedef struct {                    // un nodo del clúster
    pthread_mutex_t send_lock;      // serializa los envíos a este nodo
    struct timespec last_seen;      // último mensaje recibido (CLOCK_MONOTONIC)
    uint32_t first_page, n_pages;   // páginas globales que aporta
    uint32_t free_pages;            // según su último heartbeat
    uint16_t node_id;
    uint8_t  status;                // NODE_UNKNOWN / NODE_ONLINE / NODE_OFFLINE
    uint8_t  missed_hb;             // latidos consecutivos perdidos
    int      data_out_fd;           // socket saliente de datos
    int      hb_out_fd;             // socket saliente de heartbeat
} node_info_t;

typedef struct {                    // solicitud enviada que espera respuesta
    sem_t    done;                  // el listener hace sem_post al llegar la respuesta
    void    *buf;                   // destino de la carga recibida
    uint32_t req_id;
    uint32_t buf_len;
    uint32_t recv_len;
    uint32_t aux;
    int16_t  status;
    uint16_t peer;                  // nodo al que se envió
    uint8_t  in_use;
    uint8_t  state;                 // REQ_*
    uint8_t  access;
    uint8_t  flags;
} pending_req_t;

typedef struct {                    // elemento de una cola de trabajo
    msg_header_t header;            // cabecera ya decodificada
    uint8_t  payload[DSM_WORK_PAYLOAD];
    uint16_t from_node;
} work_item_t;

typedef struct {                    // cola acotada productor-consumidor
    pthread_mutex_t lock;
    sem_t free_slots;               // huecos libres
    sem_t items;                    // elementos disponibles (solicitudes + páginas listas)
    work_item_t *ring;
    uint32_t *ready_pages;          // páginas cuyo busy pasó a la siguiente solicitud de su cola
    uint32_t cap, head, len;
    uint32_t ready_cap, ready_head, ready_len;
} work_queue_t;

#endif
