#pragma once

#include "ghost_status.h"
#include <semaphore.h>

typedef int (*sem_op_type)(sem_t*);
int sem_wait(sem_t *sem);