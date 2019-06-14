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
 * binary_search.h
 *
 * Information Systems Group
 * Saarland University
 * 2012 - 2013
 */

#ifndef BINARY_SEARCH_H
#define	BINARY_SEARCH_H


void *build_binary_tree(IndexEntry *c, internal_int_t n);

internal_int_t binary_search(IndexEntry *c, internal_int_t key, internal_int_t lower, internal_int_t upper, bool* foundKey);
internal_int_t binary_search_lt(IndexEntry *c, internal_int_t key, internal_int_t start, internal_int_t end);
internal_int_t binary_search_gte(IndexEntry *c, internal_int_t key, internal_int_t start, internal_int_t end);


#endif	/* BINARY_SEARCH_H */

