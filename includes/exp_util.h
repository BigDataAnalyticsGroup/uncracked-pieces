/*
 * Copyright Information Systems Group, Saarland University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 * exp_util.h
 *
 * Information Systems Group
 * Saarland University
 * 2012 - 2013
 */

#ifndef EXP_UTIL_H
#define	EXP_UTIL_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <algorithm>
#include "timer.h"
#include "utils.h"
#include "table.h"

struct setup {
	IndexEntry** index_data;
	internal_int_t *queries;
	internal_int_t *queriesL;
	internal_int_t *queriesH;
	internal_int_t numberOfProjectedAttributes;
	table_t table;
};

typedef struct setup *SETUP;

void initialize();
void terminate();
SETUP get_setup(internal_int_t data_size, internal_int_t wkld_size);
void free_setup(SETUP s);
void prepare_data(SETUP s, internal_int_t size, internal_int_t maxKey, bool covering);

void prepare_queries3(SETUP s, internal_int_t maxKey);
void prepare_queries3_with_projection(SETUP s, internal_int_t maxKey, internal_int_t numberOfProjectedAttributes, float selectivity);

#endif	/* EXP_UTIL_H */

