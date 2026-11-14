#ifndef NODE_H
#define NODE_H

#include "estructuras.h"

// Prepara la tabla de nodos: este nodo en línea y los demás desconocidos.
int  node_init(const dsm_config_t *c, uint16_t self);
void node_shutdown(void);

// Envía JOIN a cada nodo configurado y lanza el hilo de heartbeat. Devuelve -1 si
// algún nodo rechaza la unión porque su dsm.conf es distinto.
int node_join_cluster(void);

// Salida ordenada: avisa con LEAVE a los nodos en línea y espera a que cada uno
// recupere las páginas suyas que este nodo tenía modificadas.
void node_leave_cluster(void);

// Envía un mensaje a dst (o lo entrega localmente si dst es este nodo). 0 o -1.
int node_send(uint16_t dst, const msg_header_t *h, const void *payload);

uint16_t node_self(void);
int  node_is_online(uint16_t id);
void node_seen(uint16_t id);                           // llegó un mensaje de id
void node_heartbeat(uint16_t id, uint32_t free_pages); // llegó un latido de id
void node_down(uint16_t id, const char *reason);       // pasa a fuera de línea y limpia su estado
void node_handle(const work_item_t *item);             // atiende JOIN y LEAVE

// Consultas
int      node_info(uint16_t id, node_info_t *out);     // copia de la entrada, sin su lock
uint64_t node_online_memory(uint16_t *online);         // bytes que aportan los nodos en línea

#endif
