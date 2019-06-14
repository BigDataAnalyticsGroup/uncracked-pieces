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
 * index.h
 *
 * Information Systems Group
 * Saarland University
 * 2012 - 2013
 */

#ifndef INDEX_H_
#define INDEX_H_

#include "avltree.h"
#include "bulkBPTree.h"
#include "arttree.h"
#include "experiment.h"

void *build_avl_tree(IndexEntry *c, internal_int_t n);
void *build_art_tree(IndexEntry *c, internal_int_t n);

// does nothing. returns null.
void *build_binary_tree(IndexEntry *c, internal_int_t n);
void *build_bptree(IndexEntry *c, internal_int_t n);

void *fullIndex(IndexEntry *c, internal_int_t n, void (*sort)(IndexEntry *c, internal_int_t n), void *(*index)(IndexEntry *c, internal_int_t n), Result r);

typedef enum index_type	{AVL=0, BINARY, BPTREE, BPTREE_BULK, ART} INDEX_TYPE;
extern void *(*INDEX[])(IndexEntry *c, internal_int_t n);



#endif /* INDEX_H_ */
