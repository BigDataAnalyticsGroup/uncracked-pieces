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
 * sorting.h
 *
 * Information Systems Group
 * Saarland University
 * 2012 - 2013
 */

#ifndef SORTING_H_
#define SORTING_H_

// pure sorting algorithms
void quick_sort(IndexEntry *c, internal_int_t n);
void insertion_sort(IndexEntry *c, internal_int_t n);
void merge_sort(IndexEntry *c, internal_int_t n);
void inplace_radixsort(IndexEntry *c, internal_int_t n);


// hybrid sorting algorithms
void hybrid_quicksort(IndexEntry *c, internal_int_t n);
void hybrid_radixsort_insert(IndexEntry *c, internal_int_t n);
void hybrid_radixsort_quick(IndexEntry *c, internal_int_t n);
void hybrid_radixsort_quickinsert(IndexEntry *c, internal_int_t n);

typedef enum sort_type	{QUICK=0, INSERT, MERGE, RADIX, QUICK_INSERT, RADIX_INSERT, RADIX_QUICK, RADIX_QUICK_INSERT} SORT_TYPE;
extern void (*SORT[])(IndexEntry *c, internal_int_t n);


void merge(internal_int_t *c, internal_int_t lo, internal_int_t m, internal_int_t hi);
void merge_index(IndexEntry *c, internal_int_t lo, internal_int_t m, internal_int_t hi);
void merge_index_chunk(IndexEntry *c, internal_int_t lo, internal_int_t m, internal_int_t hi);
//AvlTree fullIndex(internal_int_t *c, internal_int_t n, void (*sort)(internal_int_t *c, internal_int_t n), Result r);


// sorting utilities
bool testSorted(internal_int_t *data, internal_int_t n);
void inverse(internal_int_t *c,internal_int_t n);

#endif /* SORTING_H_ */
