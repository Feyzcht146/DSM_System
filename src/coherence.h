#ifndef COHERENCE_H
#define COHERENCE_H

#include "estructuras.h"

// Crea el directorio de las páginas que aporta este nodo (todas DIR_UNCACHED).
int  coherence_init(const dsm_config_t* c, uint16_t self);
void coherence_shutdown(void);

// Las usa el servicio de fallos del paginador, acquire pide la página a su home con el
// acceso indicado y espera el contenido (queda en la vista de sistema);
// confirm avisa al home que la página ya quedó instalada, acquire devuelve ST_OK o ERR_*.
int  coherence_acquire(uint32_t page, uint8_t access, uint32_t* req_id);
void coherence_confirm(uint32_t page, uint32_t req_id, int ok);

void coherence_handle(const work_item_t* item);   // PAGE_REQUEST, PAGE_ACK, INVALIDATE, OWNER_FETCH
void coherence_serve_ready(uint32_t page);        // atiende la siguiente solicitud de la cola de page

// Un nodo sale: recover trae de vuelta las páginas de este home que node tenía en M (salida ordenada)
// y node_down quita a node de las copias y cancela lo que dependía de él.
void coherence_recover_from(uint16_t node);
void coherence_node_down(uint16_t node);

void coherence_tick(void); // vence las transacciones sin confirmar

int coherence_dir_info(uint32_t page, dir_entry_t* out); // copia de la entrada, sin su lock

#endif
