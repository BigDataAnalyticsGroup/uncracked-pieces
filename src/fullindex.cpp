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
 * fullindex.cpp
 *
 * Information Systems Group
 * Saarland University
 * 2012 - 2013
 */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "../includes/avltree.h"
#include "../includes/cracking.h"
#include "../includes/sorting.h"
#include "../includes/index.h"
#include "../includes/binary_search.h"
#include "../includes/bulkBPTree.h"

unsigned internal_int_t INSERT_SORT_LEVEL;
unsigned internal_int_t QUICK_SORT_LEVEL;

void (*SORT[])(IndexEntry *c, internal_int_t n) = {&quick_sort, &insertion_sort, &merge_sort, &inplace_radixsort, &hybrid_quicksort,
								 &hybrid_radixsort_insert, &hybrid_radixsort_quick, &hybrid_radixsort_quickinsert};


void *(*INDEX[])(IndexEntry *c, internal_int_t n) = {&build_avl_tree, &build_binary_tree, &build_bptree, &build_bptree_bulk, &build_art_tree};

// recursive pure quick sort (implement non-recursive as well?)
void quick_sort(IndexEntry *c, internal_int_t n) {
	internal_int_t x1 = 0, x2 = n-1;
	IndexEntry med = c[(x1 + x2) / 2];
	while (x1 <= x2) {
		if (c[x1] < med)
			x1++;
		else{
			while (c[x2] > med && x2 >= x1)
				x2--;

			if (x1 <= x2) {
				exchange(c, x1, x2);
				x1++;
				x2--;
			}
		}
	}

#ifndef SINGLE_ITER
	/* recursion */
	if (0 < x2)
		quick_sort(c, x2+1);
	if (x1 < n-1)
		quick_sort(&c[x1], n-x1);
#endif
}


// insertion sort
void insertion_sort(IndexEntry *c, internal_int_t n) {
	internal_int_t k;
	for (k = 1; k < n; k++) {
		IndexEntry key = c[k];
		internal_int_t i = k-1;
		while ((i >= 0) && (key < c[i])) {
			c[i+1] = c[i];
			i--;
		}
		c[i+1] = key;
	}
}

/**
 * Merge two partitions:
 * 		(1)	[ lo , m ]
 * 		(2) [ m+1 , hi ]
 *
 */

void merge(IndexEntry *c, internal_int_t lo, internal_int_t m, internal_int_t hi){
	internal_int_t i, j, k;
	IndexEntry *b;
	b = (IndexEntry*) malloc((m-lo+1)*INDEX_ENTRY_SIZE);

	i=0; j=lo;
	// copy first half of array a to auxiliary array b
	while (j<=m)
		b[i++]=c[j++];

	i=0; k=lo;
	// copy back next-greatest element at each time
	while (k<j && j<=hi)
		if (b[i]<=c[j])
			c[k++]=b[i++];
		else
			c[k++]=c[j++];

	// copy back remaining elements of first half (if any)
	while (k<j)
		c[k++]=b[i++];
}


// merge sort
void merge_sort(IndexEntry *c, internal_int_t n){
	if (0 < n-1){
		internal_int_t m = (n-1)/2;
		merge_sort(c, m+1);
		merge_sort(&c[m+1], n-m-1);
		merge(c, 0, m, n-1);
	}
}


// in-place pure radix sort
void do_inplace_radixsort(IndexEntry *c, unsigned internal_int_t n, internal_int_t shift) {
	unsigned internal_int_t last[256],ptr[256],cnt[256];
	unsigned internal_int_t i,j,k,sorted,remain;
	IndexEntry temp,swap;

	memset(cnt, 0, 256*sizeof(unsigned internal_int_t)); // Zero counters
	switch (shift) { // Count occurrences
	case 0: 	for(i=0;i<n;i++) cnt[c[i].m_key & 0xFF]++; break;
	case 8: 	for(i=0;i<n;i++) cnt[(c[i].m_key >> 8) & 0xFF]++; break;
	case 16: 	for(i=0;i<n;i++) cnt[(c[i].m_key >> 16) & 0xFF]++; break;
	case 24: 	for(i=0;i<n;i++) cnt[c[i].m_key >> 24]++; break;
	/*
	 * 	Note that even though our radix sort implementations work on 8 byte keys, only 4 bytes are actually processed,
	 * 	as our key domain is never lager than [0, 2^32-1]. In a system implementation, this would be handled before sorting by inspecting
	 * 	the max value of the column in the statistics.
	 */
	}
	sorted = (cnt[0]==n);	// Accumulate counters into pointers
	ptr[0] = 0;
	last[0] = cnt[0];
	for(i=1; i<256; i++){
		last[i] = (ptr[i]=last[i-1]) + cnt[i];
		sorted |= (cnt[i]==n);
	}

	if(!sorted){	// Go through all swaps
		i = 255;
		remain = n;
		while(remain > 0){
			while(ptr[i] == last[i])
				i--;	// Find uncompleted value range
			j = ptr[i];	// Grab first element in cycle
			swap = c[j];
			k = (swap.m_key >> shift) & 0xFF;
			if(i != k){	// Swap into correct range until cycle completed
				do{
					temp = c[ptr[k]];
					c[ptr[k]++] = swap;
					k = ((swap=temp).m_key >> shift) & 0xFF;
					remain--;
				}while(i != k);
				c[j] = swap;	// Place last element in cycle
			}
			ptr[k]++;
			remain--;
		}
	}
	if(shift > 0){	// Sort on next digit
		shift -= 8;
		for(i=0; i<256; i++)
			do_inplace_radixsort(&c[last[i]-cnt[i]], cnt[i], shift);
	}
}

void inplace_radixsort(IndexEntry *c, internal_int_t n){
	do_inplace_radixsort(c, n, 24);
}


// hybrid quick sort: switches to insertion sort after a threshold
void hybrid_quicksort(IndexEntry *c, internal_int_t n) {
	internal_int_t x1 = 0, x2 = n-1;
	IndexEntry med = c[(x1 + x2) / 2];
	while (x1 <= x2) {
		if (c[x1] < med)
			x1++;
		else{
			while (c[x2] > med && x2 >= x1)
				x2--;

			if (x1 <= x2) {
				exchange(c, x1, x2);
				x1++;
				x2--;
			}
		}
	}

	/* recursion */
	if (0 < x2){
		if(x2+1 < (internal_int_t)(INSERT_SORT_LEVEL))
			insertion_sort(c, x2+1);
		else
			hybrid_quicksort(c, x2+1);
	}
	if (x1 < n-1){
		if(n-x1 < (internal_int_t)(INSERT_SORT_LEVEL))
			insertion_sort(&c[x1],n-x1);
		else
			hybrid_quicksort(&c[x1], n-x1);
	}
}


// hybrid radix sort: switches to insertion sort after a threshold
void do_hybrid_radixsort_insert(IndexEntry *c, unsigned internal_int_t n, internal_int_t shift) {
	unsigned internal_int_t last[256],ptr[256],cnt[256];
	unsigned internal_int_t i,j,k,sorted,remain;
	IndexEntry temp,swap;

	memset(cnt, 0, 256*sizeof(unsigned internal_int_t)); // Zero counters
	switch (shift) { // Count occurrences
	case 0: 	for(i=0;i<n;i++) cnt[c[i].m_key & 0xFF]++; break;
	case 8: 	for(i=0;i<n;i++) cnt[(c[i].m_key >> 8) & 0xFF]++; break;
	case 16: 	for(i=0;i<n;i++) cnt[(c[i].m_key >> 16) & 0xFF]++; break;
	case 24: 	for(i=0;i<n;i++) cnt[c[i].m_key >> 24]++; break;
	/*
	 * 	Note that even though our radix sort implementations work on 8 byte keys, only 4 bytes are actually processed,
	 * 	as our key domain is never lager than [0, 2^32-1]. In a system implementation, this would be handled before sorting by inspecting
	 * 	the max value of the column in the statistics.
	 */
	}
	sorted = (cnt[0]==n);	// Accumulate counters into pointers
	ptr[0] = 0;
	last[0] = cnt[0];
	for(i=1; i<256; i++){
		last[i] = (ptr[i]=last[i-1]) + cnt[i];
		sorted |= (cnt[i]==n);
	}
	if(!sorted){	// Go through all swaps
		i = 255;
		remain = n;
		while(remain > 0){
			while(ptr[i] == last[i])
				i--;	// Find uncompleted value range
			j = ptr[i];	// Grab first element in cycle
			swap = c[j];
			k = (swap.m_key >> shift) & 0xFF;
			if(i != k){	// Swap into correct range until cycle completed
				do{
					temp = c[ptr[k]];
					c[ptr[k]++] = swap;
					k = ((swap=temp).m_key >> shift) & 0xFF;
					remain--;
				}while(i != k);
				c[j] = swap;	// Place last element in cycle
			}
			ptr[k]++;
			remain--;
		}
	}
	if(shift > 0){	// Sort on next digit
		shift -= 8;
		for(i=0; i<256; i++){
			if (cnt[i] > INSERT_SORT_LEVEL)
				do_hybrid_radixsort_insert(&c[last[i]-cnt[i]], cnt[i], shift);
			else if(cnt[i] > 1)
				insertion_sort(&c[last[i]-cnt[i]], cnt[i]);
		}
	}
}

void hybrid_radixsort_insert(IndexEntry *c, internal_int_t n){
	do_hybrid_radixsort_insert(c, n, 24);
}

// hybrid radix sort: switches to quick sort after a threshold
void do_hybrid_radixsort_quick(IndexEntry *c, unsigned internal_int_t n, internal_int_t shift) {
	unsigned internal_int_t last[256],ptr[256],cnt[256];
	unsigned internal_int_t i,j,k,sorted,remain;
	IndexEntry temp,swap;

	memset(cnt, 0, 256*sizeof(unsigned internal_int_t)); // Zero counters
	switch (shift) { // Count occurrences
	case 0: 	for(i=0;i<n;i++) cnt[c[i].m_key & 0xFF]++; break;
	case 8: 	for(i=0;i<n;i++) cnt[(c[i].m_key >> 8) & 0xFF]++; break;
	case 16: 	for(i=0;i<n;i++) cnt[(c[i].m_key >> 16) & 0xFF]++; break;
	case 24: 	for(i=0;i<n;i++) cnt[c[i].m_key >> 24]++; break;
	/*
	 * 	Note that even though our radix sort implementations work on 8 byte keys, only 4 bytes are actually processed,
	 * 	as our key domain is never lager than [0, 2^32-1]. In a system implementation, this would be handled before sorting by inspecting
	 * 	the max value of the column in the statistics.
	 */
	}
	sorted = (cnt[0]==n);	// Accumulate counters into pointers
	ptr[0] = 0;
	last[0] = cnt[0];
	for(i=1; i<256; i++){
		last[i] = (ptr[i]=last[i-1]) + cnt[i];
		sorted |= (cnt[i]==n);
	}
	if(!sorted){	// Go through all swaps
		i = 255;
		remain = n;
		while(remain > 0){
			while(ptr[i] == last[i])
				i--;	// Find uncompleted value range
			j = ptr[i];	// Grab first element in cycle
			swap = c[j];
			k = (swap.m_key >> shift) & 0xFF;
			if(i != k){	// Swap into correct range until cycle completed
				do{
					temp = c[ptr[k]];
					c[ptr[k]++] = swap;
					k = ((swap=temp).m_key >> shift) & 0xFF;
					remain--;
				}while(i != k);
				c[j] = swap;	// Place last element in cycle
			}
			ptr[k]++;
			remain--;
		}
	}
	if(shift > 0){	// Sort on next digit
		shift -= 8;
		for(i=0; i<256; i++){
			if (cnt[i] > QUICK_SORT_LEVEL)
				do_hybrid_radixsort_quick(&c[last[i]-cnt[i]], cnt[i], shift);
			else if(cnt[i] > 1)
				quick_sort(&c[last[i]-cnt[i]], cnt[i]);
		}
	}
}

void hybrid_radixsort_quick(IndexEntry *c, internal_int_t n) {
	do_hybrid_radixsort_quick(c, n, 24);
}

// hybrid radix sort: switches to quick sort after a threshold, which internally again switches to insertion sort after a threshold
void do_hybrid_radixsort_quickinsert(IndexEntry *c, unsigned internal_int_t n, internal_int_t shift) {
	unsigned internal_int_t last[256],ptr[256],cnt[256];
	unsigned internal_int_t i,j,k,sorted,remain;
	IndexEntry temp,swap;

	memset(cnt, 0, 256*sizeof(unsigned internal_int_t)); // Zero counters
	switch (shift) { // Count occurrences
	case 0: 	for(i=0;i<n;i++) cnt[c[i].m_key & 0xFF]++; break;
	case 8: 	for(i=0;i<n;i++) cnt[(c[i].m_key >> 8) & 0xFF]++; break;
	case 16: 	for(i=0;i<n;i++) cnt[(c[i].m_key >> 16) & 0xFF]++; break;
	case 24: 	for(i=0;i<n;i++) cnt[c[i].m_key >> 24]++; break;
	/*
	 * 	Note that even though our radix sort implementations work on 8 byte keys, only 4 bytes are actually processed,
	 * 	as our key domain is never lager than [0, 2^32-1]. In a system implementation, this would be handled before sorting by inspecting
	 * 	the max value of the column in the statistics.
	 */
	}
	sorted = (cnt[0]==n);	// Accumulate counters into pointers
	ptr[0] = 0;
	last[0] = cnt[0];
	for(i=1; i<256; i++){
		last[i] = (ptr[i]=last[i-1]) + cnt[i];
		sorted |= (cnt[i]==n);
	}
	if(!sorted){	// Go through all swaps
		i = 255;
		remain = n;
		while(remain > 0){
			while(ptr[i] == last[i])
				i--;	// Find uncompleted value range
			j = ptr[i];	// Grab first element in cycle
			swap = c[j];
			k = (swap.m_key >> shift) & 0xFF;
			if(i != k){	// Swap into correct range until cycle completed
				do{
					temp = c[ptr[k]];
					c[ptr[k]++] = swap;
					k = ((swap=temp).m_key >> shift) & 0xFF;
					remain--;
				}while(i != k);
				c[j] = swap;	// Place last element in cycle
			}
			ptr[k]++;
			remain--;
		}
	}
	if(shift > 0){	// Sort on next digit
		shift -= 8;
		for(i=0; i<256; i++){
			if (cnt[i] > QUICK_SORT_LEVEL)
				do_hybrid_radixsort_quickinsert(&c[last[i]-cnt[i]], cnt[i], shift);
			else if(cnt[i] > 1)
				hybrid_quicksort(&c[last[i]-cnt[i]], cnt[i]);
		}
	}
}

void hybrid_radixsort_quickinsert(IndexEntry *c, internal_int_t n) {
	do_hybrid_radixsort_quickinsert(c, n, 24);
}

bool testSorted(internal_int_t *data, internal_int_t n){
	internal_int_t prev = data[0], i = 1;
	bool sorted = true;
	for (; i<n; i++) {
	  if (data[i] < prev) {
	    sorted = false;
	    break;
	  }
	  prev = data[i];
	}
	return sorted;
}

bool testIndex(internal_int_t *data, internal_int_t size, AvlTree T){
	internal_int_t i=0;
	bool idxOK = true;
	for (; i<size; i++) {
		IntPair p = FindNeighborsLT(data[i], T, size);
		if(p->second >=0 && data[p->second] >= data[i])
			idxOK = false;
	}
	return idxOK;
}

void inverse(internal_int_t *c,internal_int_t n){
   internal_int_t i,temp;
   internal_int_t j = n-1;
   for(i=0;i<(n/2);i++){
      temp = c[i];
      c[i] = c[j];
      c[j] = temp;
      j--;
   }
}

void *build_avl_tree(IndexEntry *c, internal_int_t n){
	AvlTree T = MakeEmpty(NULL);
	IndexEntry previous=c[0];
	internal_int_t i=0, previous_i = -1;
	for(;i<n;i++){
		if(c[i]!=previous){
			T = Insert(previous_i, previous.m_key, T);

			previous = c[i];
			previous_i = i-1;
		}
	}
	T = Insert(previous_i, previous.m_key, T);
	//assert(testIndex(c, n, T));
	printAVLTree(T);

	return T;
}

void *build_art_tree(IndexEntry *c, internal_int_t n){
	ARTTree tree=NULL;
	//IndexEntry previous=c[0];
	internal_int_t i=0; //previous_i = 0;
	uint8_t key[8];
	for(;i<n;i++){
		loadKey(c[i].m_key,key);
		insert(tree,&tree,key,0,i+1,8);

//		if(c[i]!=previous){
//			loadKey(previous.m_key,key);
//			insert(tree,&tree,key,0,previous_i,8);
//
//			previous = c[i];
//			previous_i = i;
//		}
	}
//	loadKey(previous.m_key,key);
//	insert(tree,&tree,key,0,previous_i,8);
	return tree;
}

//AvlTree fullIndex(internal_int_t *c, internal_int_t n, void (*sort)(internal_int_t *c, internal_int_t n), Result r){
void *fullIndex(IndexEntry *c, internal_int_t n, void (*sort)(IndexEntry *c, internal_int_t n), void *(*index)(IndexEntry *c, internal_int_t n), Result r){
	INSERT_SORT_LEVEL = 64;
	QUICK_SORT_LEVEL = 256;

	t_on(r->c_do);
	sort(c, n);
	t_off(r->c_do); t_clear(r->c_do);

	printIndexArray(c, n);
	//assert(testSorted(c,n));

	t_on(r->c_iu);
	void *I = index(c, n);
	t_off(r->c_iu); t_clear(r->c_iu);

	return I;
}


// FUNCTIONS FOR IndexEntry

void merge_index(IndexEntry *c, internal_int_t lo, internal_int_t m, internal_int_t hi){
	internal_int_t i, j, k;
	IndexEntry *b;
	b = (IndexEntry*) malloc((m-lo+1)*INDEX_ENTRY_SIZE);

	// copy first half of array a to auxiliary array b
	i=0; j=lo;
	while (j<=m)
		b[i++]=c[j++];

	i=0; k=lo;
	// copy back next-greatest element at each time
	while (k<j && j<=hi)
		if (b[i]<=c[j])
			c[k++]=b[i++];
		else
			c[k++]=c[j++];

	// copy back remaining elements of first half (if any)
	while (k<j)
		c[k++]=b[i++];

	free(b);
}

void merge_index_chunk(IndexEntry *c, internal_int_t lo, internal_int_t m, internal_int_t hi){
	internal_int_t offset = binary_search_gte(c, c[m+1].m_key, lo, m);
	if(offset<=m){
		IndexEntry *b = (IndexEntry*) malloc((hi-m)*INDEX_ENTRY_SIZE);
		memcpy(b, &c[m+1], (hi-m)*INDEX_ENTRY_SIZE);
		memmove(&c[offset+hi-m], &c[offset], (m-offset+1)*INDEX_ENTRY_SIZE);
		memcpy(&c[offset], b, (hi-m)*INDEX_ENTRY_SIZE);
	}
}
