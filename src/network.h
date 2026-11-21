// Interfaz de la red (network.c): solicitudes con respuesta, colas y formato de mensajes
#ifndef NETWORK_H
#define NETWORK_H

#include "estructuras.h"

// Abre los puertos de escucha de este nodo y lanza el listener, los workers de
// solicitudes y los hilos de servicio de coherencia. Devuelve 0, o -1 con el motivo.
int  net_init(const dsm_config_t *c, uint16_t self);
void net_shutdown(void);

// Conecta por TCP a host:port esperando a lo sumo timeout_ms. Devuelve el socket o -1.
int net_connect(const char *host, uint16_t port, int timeout_ms);

// 1 si este equipo puede escuchar en host:port: la dirección es suya y el puerto está libre.
int net_can_listen(const char *host, uint16_t port);

// Escribe un mensaje completo (cabecera + payload_len bytes) en fd. 0 o -1.
int net_send_fd(int fd, const msg_header_t *h, const void *payload);

// Envía una solicitud a dst y espera su respuesta. Asigna h->req_id. La carga de la
// respuesta (hasta buf_len bytes) se copia en buf, y su largo y su aux quedan en h.
// Devuelve ST_OK o un código ERR_*.
int net_request(uint16_t dst, msg_header_t *h, const void *payload, void *buf, uint32_t buf_len);

// Entrega un mensaje cuyo destino es este mismo nodo, sin pasar por la red.
void net_deliver(const msg_header_t *h, const void *payload);

// Termina con ERR_NODE_DOWN las solicitudes que esperaban respuesta de peer.
void net_fail_pending(uint16_t peer);

// Avisa a los workers que el busy de page pasó a la siguiente solicitud de su cola.
void net_push_ready(uint32_t page);

// Encola una orden local para que un worker intente unirse al nodo dst.
void net_push_join(uint16_t dst);

// Encola una orden local para que un worker libere las asignaciones del nodo que salió.
void net_push_release(uint16_t node);

// Enteros en orden de red dentro de un buffer
void     net_put16(uint8_t *p, uint16_t v);
void     net_put32(uint8_t *p, uint32_t v);
void     net_put64(uint8_t *p, uint64_t v);
uint16_t net_get16(const uint8_t *p);
uint32_t net_get32(const uint8_t *p);
uint64_t net_get64(const uint8_t *p);

#endif
