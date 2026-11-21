// API del DSM: lo único que incluyen las aplicaciones.
// El proceso que llama a dsm_init pasa a ser un nodo del clúster descrito en dsm.conf.
#ifndef DSM_H
#define DSM_H

#include <stddef.h>
#include <stdint.h>

// Resultado de las funciones que devuelven int: DSM_OK o un error (negativo)
enum {
    DSM_OK           =  0,
    DSM_ERR_STATE    = -1,   // el DSM no está iniciado, o ya lo estaba
    DSM_ERR_CONFIG   = -2,   // dsm.conf no se pudo leer o no es válido
    DSM_ERR_NODE     = -3,   // el node_id no existe, o no hay un nodo libre para este equipo
    DSM_ERR_MEMORY   = -4,   // no se pudo reservar el espacio DSM
    DSM_ERR_NETWORK  = -5,   // no se pudieron abrir los puertos del nodo
    DSM_ERR_REJECTED = -6,   // otro nodo rechazó la unión: su dsm.conf es distinto
    DSM_ERR_THREADS  = -7,   // no hay lugar para otro hilo (max_threads), o el hilo ya tenía
    DSM_ERR_ARG      = -8,   // argumento inválido (tamaño 0, nombre vacío o largo, puntero ajeno)
    DSM_ERR_NOMEM    = -9,   // no hay un tramo contiguo libre de ese tamaño en los nodos en línea
    DSM_ERR_EXISTS   = -10,  // ya existe una asignación con ese nombre
    DSM_ERR_NOTFOUND = -11   // ningún nodo en línea conoce una asignación con ese nombre
};

enum { DSM_NAME_MAX = 31 };  // caracteres de una etiqueta o un nombre

// node_id para que dsm_init elija el nodo: el que indique la variable de entorno DSM_NODE
// o, si no existe, el primer nodo de dsm.conf cuyo host es este equipo y está libre
enum { DSM_NODE_AUTO = -1 };

// Inicia este proceso como el nodo node_id del clúster de config_path: reserva el espacio
// DSM, abre sus puertos y se une a los nodos que estén en línea. No espera a los demás.
// Errores: DSM_ERR_STATE, DSM_ERR_CONFIG, DSM_ERR_NODE, DSM_ERR_MEMORY, DSM_ERR_NETWORK,
// DSM_ERR_REJECTED. Si falla no deja nada reservado.
int dsm_init(const char *config_path, int node_id);

// Sale del clúster de forma ordenada y libera el espacio DSM. Antes, los demás hilos
// deben dejar de usar memoria DSM. Error: DSM_ERR_STATE si no estaba iniciado.
int dsm_shutdown(void);

// Nodo que es este proceso, o DSM_ERR_STATE si el DSM no está iniciado.
int dsm_node_id(void);

// Los hilos creados con pthread_create pueden usar memoria DSM sin registrarse: cada uno
// ocupa uno de los max_threads lugares en su primer fallo de página. Estas funciones son
// opcionales: register reserva el lugar por adelantado y unregister lo devuelve antes de
// que el hilo termine. Errores: DSM_ERR_STATE, DSM_ERR_THREADS.
int dsm_thread_register(void);
int dsm_thread_unregister(void);

// Reserva size bytes de memoria DSM, redondeados a páginas completas, y devuelve su
// dirección, que es la misma en todos los nodos. Usa primero las páginas de este nodo y,
// si no alcanzan, un tramo contiguo que puede cruzar a otros nodos en línea. La memoria
// empieza en cero. tag es una etiqueta opcional (puede ser NULL) que se ve en las
// consultas. Devuelve NULL si falla; el motivo queda en dsm_last_error():
// DSM_ERR_STATE, DSM_ERR_ARG, DSM_ERR_NOMEM.
void *dsm_alloc(size_t size, const char *tag);

// Como dsm_alloc, con un nombre que otros procesos usan en dsm_attach. No se libera sola
// cuando termina quien la creó. Errores: los de dsm_alloc y DSM_ERR_EXISTS.
void *dsm_alloc_named(const char *name, size_t size);

// Dirección de la asignación llamada name, creada por cualquier proceso del clúster.
// Si size no es NULL deja ahí su tamaño en bytes. Devuelve NULL si falla; errores:
// DSM_ERR_STATE, DSM_ERR_ARG, DSM_ERR_NOTFOUND.
void *dsm_attach(const char *name, size_t *size);

// Libera una asignación de este proceso (creada o adjuntada): sus páginas quedan libres
// en sus nodos y se invalidan las copias que hubiera. ptr es la dirección que devolvió
// dsm_alloc, dsm_alloc_named o dsm_attach. Errores: DSM_ERR_STATE, DSM_ERR_ARG.
// Al salir (dsm_shutdown o caída del nodo) las asignaciones sin nombre se liberan solas.
int dsm_free(void *ptr);

/* ----- consultas: sirven desde cualquier nodo, sobre un nodo o sobre todo el clúster ----- */

enum { DSM_ALL_NODES = -1 };                                      // en lugar de un node_id
enum { DSM_NODE_UNKNOWN = 0, DSM_NODE_ONLINE, DSM_NODE_OFFLINE }; // dsm_node_info_t.status
enum { DSM_PROC_RUNNING = 1, DSM_PROC_FINISHED };                 // dsm_stats_t.state

typedef struct {                // un nodo de dsm.conf
    int      node, status;
    char     host[64];
    uint16_t data_port, hb_port;
    uint64_t memory_bytes;
    uint32_t first_page, n_pages;   // páginas que aporta
    uint32_t free_pages;            // sin asignar (de otro nodo: las de su último latido)
} dsm_node_info_t;

typedef struct {                // una página, vista por el nodo que responde
    uint32_t page;
    int      node, home;            // nodo que responde y nodo que aporta la página
    int      local_state;           // copia de ese nodo: 0 = I, 1 = S, 2 = M
    int      dir_state;             // si responde su home: 0 = UNCACHED, 1 = SHARED, 2 = MODIFIED
    int      owner;                 // nodo con la copia M (si dir_state es MODIFIED)
    uint64_t copyset;               // bit i = el nodo i tiene copia
    uint32_t alloc_id;              // asignación a la que pertenece; 0 = libre
    uint32_t alloc_order;           // posición de la página dentro de su asignación
    int      alloc_pid, alloc_node; // proceso que la asignó y su nodo
} dsm_page_info_t;

typedef struct {                // una asignación
    uint32_t id, first_page, n_pages;
    void    *addr;
    size_t   size;
    int      pid, node;             // proceso que la creó y su nodo
    int      named;                 // 1 si tiene nombre
    char     tag[32];               // etiqueta o nombre
} dsm_alloc_info_t;

typedef struct {                // un proceso (cada proceso es un nodo) y sus métricas
    int      node, pid, state;      // DSM_PROC_RUNNING, o DSM_PROC_FINISHED si ya salió
    char     program[32];
    uint64_t uptime_ms;
    uint32_t total_pages, free_pages, used_pages;   // de las páginas que aporta
    uint32_t threads;               // hilos de aplicación que usan el DSM
    uint64_t read_faults, write_faults;
    uint64_t pages_sent, pages_received;
    uint64_t inval_sent, inval_received;
    uint64_t msgs_sent, msgs_received, bytes_sent, bytes_received;
    uint64_t fault_ns_total, fault_ns_max;          // latencia de resolución de fallos
} dsm_stats_t;

// Las que llenan un arreglo copian hasta max elementos y devuelven cuántos hay en total
// (puede ser más que max), o un error negativo. node puede ser DSM_ALL_NODES.
// Errores: DSM_ERR_STATE, DSM_ERR_NODE (el nodo no existe o no está en línea).

// Tabla de nodos de dsm.conf con su estado.
int dsm_get_nodes(dsm_node_info_t *out, int max);

// Páginas en uso de un nodo: las que tienen copia ahí y las suyas asignadas o con copias.
// Con DSM_ALL_NODES, una entrada por página, la que da su home.
int dsm_get_page_table(int node, dsm_page_info_t *out, int max);

// Asignaciones con páginas en un nodo; con DSM_ALL_NODES, todas, sin repetir.
// Ordenadas por primera página.
int dsm_get_allocs(int node, dsm_alloc_info_t *out, int max);

// Proceso y métricas de un nodo en línea. Devuelve DSM_OK o un error.
int dsm_get_stats(int node, dsm_stats_t *out);

// Procesos del clúster: los de los nodos en línea y el último de cada nodo que salió
// de forma ordenada (con las métricas que tenía al salir).
int dsm_get_procs(dsm_stats_t *out, int max);

// Código del último error de este hilo en una función que devuelve puntero.
int dsm_last_error(void);

// Texto que describe un código de error; nunca devuelve NULL.
const char *dsm_strerror(int code);

#endif
