// Interfaz de la configuración (config.c): carga de dsm.conf y registro de eventos
#ifndef CONFIG_H
#define CONFIG_H

#include "estructuras.h"

int dsm_config_load(const char* path, dsm_config_t* c);

// Registro de eventos en stderr. Solo se escriben los mensajes cuyo nivel (LOG_*) no supere el configurado
void dsm_log_setup(uint8_t level, uint16_t node);
void dsm_log(int level, const char* fmt, ...) __attribute__((format(printf, 2, 3)));

#endif
