// Interfaz de las métricas y consultas (stats.c)
#ifndef STATS_H
#define STATS_H

#include "dsm.h"
#include "estructuras.h"

// Contadores de este nodo; cada módulo suma en ellos con atomic_fetch_add.
extern dsm_counters_t dsm_counters;

void stats_init(const dsm_config_t* c, uint16_t self);
void stats_fault(uint64_t ns);                 // latencia de un fallo de página resuelto
void stats_handle(const work_item_t* item);    // STATS_REQUEST, PAGETABLE_REQUEST, ALLOCS_REQUEST

// Consultas a un nodo (este u otro en línea). stats_of devuelve 0 o -1; las otras copian
// hasta max elementos y devuelven cuántos hay, o -1 si el nodo no respondió.
int stats_of(uint16_t node, dsm_stats_t* out);
int stats_pages(uint16_t node, dsm_page_info_t* out, int max);
int stats_allocs(uint16_t node, alloc_record_t* out, int max);

// Un nodo avisó que sale: se guardan sus métricas finales. stats_finished las devuelve.
void stats_remember(uint16_t node);
int  stats_finished(uint16_t node, dsm_stats_t* out);

#endif
