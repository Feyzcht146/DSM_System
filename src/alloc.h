// Interfaz del asignador distribuido (alloc.c): crear, liberar y buscar asignaciones
#ifndef ALLOC_H
#define ALLOC_H

#include "estructuras.h"

// Asignación de memoria del espacio global. Cada nodo (home) lleva la asignación de sus
// propias páginas; quien asigna busca un tramo contiguo libre y lo reserva home por home.
int  alloc_init(const dsm_config_t* c, uint16_t self);
void alloc_shutdown(void);

// Reserva las páginas contiguas que hagan falta para size bytes (pueden cruzar nodos) y
// deja la asignación en out. flags: 0 o ALLOC_NAMED. Devuelve 0, o -1 si no hay un tramo libre.
int alloc_create(uint64_t size, const char* tag, uint16_t flags, alloc_record_t* out);

// Libera la asignación de este proceso que empieza en first_page, en todos sus homes.
// Devuelve 0, o -1 si este proceso no tiene una asignación que empiece ahí.
int alloc_destroy(uint32_t first_page);

// Busca una asignación por nombre: entre las que tienen páginas en este home y, si no
// está, preguntando a los nodos en línea. Devuelve 0 y la deja en out, o -1.
int alloc_find(const char* name, alloc_record_t* out);

// Anota en este proceso una asignación creada por otro (dsm_attach), si no la tenía. 0 o -1.
int alloc_attach(const alloc_record_t* rec);

// Al salir: libera las asignaciones sin nombre que creó este proceso.
void alloc_release_own(void);

void alloc_handle(const work_item_t* item);   // ALLOC_MAP, ALLOC_RESERVE, ALLOC_RELEASE, NAME_LOOKUP

// Una asignación completa en el formato de red alloc_desc (DSM_ALLOC_DESC_LEN bytes).
void alloc_put_desc(uint8_t* b, const alloc_record_t* r);
void alloc_get_desc(const uint8_t* b, alloc_record_t* r);

// Consultas: copian hasta max asignaciones en out y devuelven cuántas hay.
uint32_t alloc_list_home(alloc_record_t* out, uint32_t max);   // con páginas en este home
uint32_t alloc_list_own(alloc_record_t* out, uint32_t max);    // creadas o adjuntadas por este proceso

#endif
