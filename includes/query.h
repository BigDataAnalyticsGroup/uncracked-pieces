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
 * query.h
 *
 * Information Systems Group
 * Saarland University
 * 2012 - 2013
 */

#ifndef QUERY_H_
#define QUERY_H_

#include <cstdlib>
#include <algorithm>
#include "experiment.h"
#include "bptree_wrapper.h"
#include "bulkBPTree.h"


// querying (query.c)

typedef enum query_type	{FILTER, SEARCH, Q_AVL, AVL_FILTER, VIEW, Q_BPTREE, Q_BPTREE_BULK, Q_ART, Q_HYBRID} QUERY_TYPE;


internal_int_t scanQuery3Mod(IndexEntry *c, table_t* table, internal_int_t key, internal_int_t numberOfProjectedAttributes, internal_int_t cover_type, internal_int_t from, internal_int_t to);
internal_int_t query3(void *T, IndexEntry *data, table_t *table, internal_int_t keyL, internal_int_t keyH, internal_int_t numberOfProjectedAttributes, internal_int_t cover_type, sideways_data_t sideways_data, internal_int_t limit, QUERY_TYPE type, Result r, internal_int_t*& rowIdToPartition);
internal_int_t queryTable(IndexEntry* currentEnrty, table_t* table, internal_int_t numberOfProjectedAttributes, internal_int_t cover_type);

#endif /* QUERY_H_ */
