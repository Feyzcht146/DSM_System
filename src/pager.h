#ifndef PAGER_H
#define PAGER_H

#include "estructuras.h"

// Reserva el espacio global con todas las páginas en PAGE_INVALID, instala el manejador
// de SIGSEGV, lanza los hilos de servicio de fallos y registra al hilo que llama.
// c debe venir de dsm_config_load; self es el id de este nodo.
// Devuelve 0, o -1 con el motivo en stderr (no deja nada reservado).
int pager_init(const dsm_config_t* c, uint16_t self);

// Detiene los hilos de fallos, restaura la acción previa de SIGSEGV y libera el espacio.
// Antes, los demás hilos deben dejar de usar memoria DSM y darse de baja.
void pager_shutdown(void);

// Un hilo debe registrarse antes de tocar memoria DSM: el manejador usa su pipe de
// respuesta. Devuelven 0, o -1 si no hay slots libres o el hilo ya estaba (o no estaba).
int pager_thread_register(void);
int pager_thread_unregister(void);

// Las usa la coherencia cuando el home de una página lo ordena.
// downgrade baja la copia local a state (PAGE_SHARED o PAGE_INVALID) si tenía más
// permiso y devuelve el estado anterior; drop_range invalida las copias locales de un
// rango de páginas y libera su memoria (el nodo que las aporta salió).
int  pager_downgrade(uint32_t page, uint8_t state);
void pager_drop_range(uint32_t first, uint32_t count);

// Consultas. Las que devuelven puntero dan NULL si page está fuera del espacio.
uint32_t pager_page_count(void);
void* pager_app_addr(uint32_t page);   // vista de aplicación (permisos MSI)
void* pager_sys_addr(uint32_t page);   // vista de sistema, solo para hilos internos
int pager_node_pages(uint16_t node, uint32_t* first, uint32_t* count);
uint16_t pager_page_home(uint32_t page);   // nodo que aporta la página
int pager_page_info(uint32_t page, local_page_t* out);   // copia de la entrada, sin su lock

#endif
