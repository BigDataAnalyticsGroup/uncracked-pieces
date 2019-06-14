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
 * crack_intwo.cpp
 *
 * Information Systems Group
 * Saarland University
 * 2012 - 2013
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../includes/avltree.h"
#include "../includes/cracking.h"
#include "../includes/sorting.h"
#include "../includes/gheap/gheap.h"

extern internal_int_t swapCount;
extern internal_int_t compCount;
extern INDEX_RUNS* index_runs;


void exchange(IndexEntry*& c, internal_int_t x1, internal_int_t x2){
	IndexEntry tmp = *(c+x1);
	*(c+x1) = *(c+x2);
	*(c+x2) = tmp;
	swapCount++;
}

/**
 *
 * Standard Crack-in-Two Algorithm
 *
 * inc = false
 *
 * return - +ve offset, or -1 if no keys found
 * it holds that: for all o <= offset => c[o] < med
 */
internal_int_t crackInTwoItemWise(IndexEntry*& c, internal_int_t posL, internal_int_t posH, internal_int_t med){
	//debug(INFO, "Key=""%lld"",",med);

	internal_int_t x1 = posL, x2 = posH;
	while (x1 <= x2) {					// the element at x1=x2 was not inspected in the original cracking algorithm --(BUG 1)
		if(countComp() && c[x1] < med)
			x1++;
		else {
			while (x2 >= x1 && (countComp() && c[x2] >= med))	// the element at x1=x2 was not inspected in the original cracking algorithm --(BUG 1)
												// we should first check the positions (x2 >= x1) and then check the values (c[x2] >= med) --(BUG 1.1)
				x2--;

			if(x1 < x2){				// this was not checked in the original cracking algorithm --(BUG 2)
				exchange(c, x1,x2);
				x1++;
				x2--;
			}
		}
	}

	// check if all elements have been inspected
	if(x1 < x2)
		printError("Not all elements were inspected!");

	x1--;

	//debug(INFO, "Pivot=""%lld""",x1);

	return x1;
}


IntPair crackInTwoMDD1R(IndexEntry*& c, internal_int_t posL, internal_int_t posH, IndexEntry*& view, internal_int_t& view_size, internal_int_t low, internal_int_t high){

	internal_int_t L = posL; internal_int_t R = posH;
	internal_int_t a = low; internal_int_t b = high;

	view = (IndexEntry*)malloc((R-L+1)*sizeof(IndexEntry));		// initialize view to the maximum possible size
	internal_int_t size = 0;

	internal_int_t x = c[L + randomNumber(R-L+1) - 1].m_key;
	while(L <= R){
		while(L<=R && c[L]<x){
			if(c[L]>=a && c[L]<b)
				view[size++] = c[L];
			L=L+1;
		}
		while(L<=R && c[R]>=x){
			if(c[R]>=a && c[R]<b)
				view[size++] = c[R];
			R=R-1;
		}
		if(L<R)
			exchange(c, L, R);
	}

	view_size = size;

	// add crack on X at position L
	IntPair p = (IntPair) malloc(sizeof(struct int_pair));
	p->first = x;
	p->second = L-1;
	return p;
}

int less_comparer(const void *const ctx, const void *const a,
    const void *const b)
{
  uintptr_t must_invert = (uintptr_t)ctx;
  return (must_invert ? (((IndexEntry *)b)->m_key < ((IndexEntry *)a)->m_key): (((IndexEntry *)a)->m_key < ((IndexEntry *)b)->m_key));
}

void item_mover(void *const dst, const void *const src)
{
  *((IndexEntry *)dst) = *((IndexEntry *)src);
}

internal_int_t crackInTwoItemWiseBuffered(IndexEntry*& c, internal_int_t posL, internal_int_t posH, internal_int_t med){
	//debug(INFO, "Key=""%lld"",",med);

	typedef struct gheap_ctx gheap;
	IndexEntry* heaps[2];
	gheap ctx_v[2];
	for(internal_int_t i = 0; i < 2; ++i) {
		heaps[i] = (IndexEntry*) malloc(INDEX_ENTRY_SIZE * HEAP_SIZE);

		ctx_v[i].fanout = 4;
		ctx_v[i].page_chunks = 1;
		ctx_v[i].item_size = INDEX_ENTRY_SIZE;
		ctx_v[i].less_comparer = &less_comparer;
		ctx_v[i].less_comparer_ctx = (void*) i;
		ctx_v[i].item_mover = &item_mover;
	}
	internal_int_t currentHeapSize = 0;


	//Heap maxHeap(true, HEAP_SIZE);
	std::queue<internal_int_t> maxQueue;
	//Heap minHeap(false, HEAP_SIZE);
	std::queue<internal_int_t> minQueue;

	internal_int_t x1 = posL, x2 = posH;
	while (x1 <= x2) {					// the element at x1=x2 was not inspected in the original cracking algorithm --(BUG 1)
		if(c[x1] < med) {
			x1++;
		}
		else {
			while (x1 <= x2 && c[x2] >= med) {	// the element at x1=x2 was not inspected in the original cracking algorithm --(BUG 1)
				x2--;							// we should first check the positions (x2 >= x1) and then check the values (c[x2] >= med) --(BUG 1.1)
			}

			if(x1 < x2){				// this was not checked in the original cracking algorithm --(BUG 2)
				if(currentHeapSize == HEAP_SIZE /*maxHeap.isFull() || minHeap.isFull()*/) {
					// the heaps were full, so we could not add the seen elements
					// therefore, we swap the top element on the heaps and add the current values then for the next round
					c[minQueue.front()] = heaps[0][0]; /* *maxHeap.getTopElement(); */
					c[maxQueue.front()] = heaps[1][0]; /* *minHeap.getTopElement(); */

					gheap_pop_heap(&ctx_v[0], heaps[0], currentHeapSize);
					gheap_pop_heap(&ctx_v[1], heaps[1], currentHeapSize);
					--currentHeapSize;

					//maxHeap.removeTopElement();
					//minHeap.removeTopElement();
					maxQueue.pop();
					minQueue.pop();
				}

				heaps[0][currentHeapSize] = c[x1];
				gheap_push_heap(&ctx_v[0], heaps[0], currentHeapSize + 1);
				//maxHeap.addElement(c[x1]);
				maxQueue.push(x1);
				heaps[1][currentHeapSize] = c[x2];
				gheap_push_heap(&ctx_v[1], heaps[1], currentHeapSize + 1);
				//minHeap.addElement(c[x2]);
				minQueue.push(x2);
				swapCount++;

				++currentHeapSize;

				x1++;
				x2--;
			}
		}
	}
	// there might be elements left in the heaps, so swap them as well
	while(/*maxHeap.getNumberOfElements()*/ currentHeapSize > 0) {
		c[minQueue.front()] = heaps[0][0]; /* *maxHeap.getTopElement(); */
		c[maxQueue.front()] = heaps[1][0]; /* *minHeap.getTopElement(); */

		gheap_pop_heap(&ctx_v[0], heaps[0], currentHeapSize);
		gheap_pop_heap(&ctx_v[1], heaps[1], currentHeapSize);
		--currentHeapSize;

//		maxHeap.removeTopElement();
//		minHeap.removeTopElement();
		maxQueue.pop();
		minQueue.pop();
	}

	free(heaps[0]);
	free(heaps[1]);

	// check if all elements have been inspected
	if(x1 < x2)
		printError("Not all elements were inspected!");

	x1--;

	//debug(INFO, "Pivot=""%lld""",x1);


	return x1;
}

// hybrid radix sort: switches to insertion sort after a threshold
template <typename T>
void do_hybrid_radixpartitioning_insert(T *c, internal_int_t n, internal_int_t bitToConsider, internal_int_t bitsLeft) {
	internal_int_t last[2],ptr[2],cnt[2];
	internal_int_t i,j,k,sorted,remain;
	T temp,swap;

	memset(cnt, 0, 2 * INT_SIZE); // Zero counters
	// count occurences
	for(i=0;i<n;i++) {
		cnt[(c[i].m_key >> bitToConsider) & 0x1]++;
	}

	sorted = (cnt[0]==n);	// Accumulate counters into pointers
	ptr[0] = 0;
	last[0] = cnt[0];
	for(i=1; i<2; i++){
		last[i] = (ptr[i]=last[i-1]) + cnt[i];
		sorted |= (cnt[i]==n);
	}
	if(!sorted){	// Go through all swaps
		i = 1;
		remain = n;
		while(remain > 0){
			while(ptr[i] == last[i])
				i--;	// Find uncompleted value range
			j = ptr[i];	// Grab first element in cycle
			swap = c[j];
			k = (swap.m_key >> bitToConsider) & 0x1;
			if(i != k){	// Swap into correct range until cycle completed
				do{
					temp = c[ptr[k]];
					c[ptr[k]++] = swap;
					k = ((swap=temp).m_key >> bitToConsider) & 0x1;
					remain--;
				}while(i != k && remain > 0);
				c[j] = swap;	// Place last element in cycle
			}
			ptr[k]++;
			remain--;
		}
	}

	if(bitsLeft > 1 && bitToConsider > 0){	// Sort on next digit
		--bitToConsider;
		--bitsLeft;
		for(i=0; i<2; i++){
			do_hybrid_radixpartitioning_insert(&c[last[i]-cnt[i]], cnt[i], bitToConsider, bitsLeft);
		}
	}
}



/**
 *
 * Hybrid Crack-in-Two Algorithm (Crack-Sort variant)
 *
 */

// split the input into index_runs
void create_index_runs(IndexEntry **c, internal_int_t run_size){
	internal_int_t i;
	internal_int_t partitions_struct_size = sizeof(struct partitions_index);
	index_runs = (INDEX_RUNS*) malloc(NUMBER_OF_INDEXES * sizeof(INDEX_RUNS));
	for(i = 0; i < NUMBER_OF_INDEXES; ++i) {

		index_runs[i] = (INDEX_RUNS) malloc(partitions_struct_size);

		index_runs[i]->firstTime = true;
		index_runs[i]->size = DATA_SIZE / run_size;
		index_runs[i]->c = (IndexEntry**) malloc(index_runs[i]->size * sizeof(IndexEntry*));
		index_runs[i]->sizes = (internal_int_t*) malloc(index_runs[i]->size * sizeof(internal_int_t));
		index_runs[i]->indexes = (AvlTree*) malloc(index_runs[i]->size * sizeof(AvlTree));

//		index_runs[i]->final_index = MakeEmpty(NULL);
//		index_runs[i]->next = (internal_int_t*) malloc(DATA_SIZE * sizeof(internal_int_t));
//		index_runs[i]->next[0] = DATA_SIZE;
//		index_runs[i]->floor = (internal_int_t*) malloc(DATA_SIZE * sizeof(internal_int_t));
//		index_runs[i]->ceil = (internal_int_t*) malloc(DATA_SIZE * sizeof(internal_int_t));
//		index_runs[i]->hole = (bool*) malloc(DATA_SIZE * sizeof(bool));
//		index_runs[i]->f = (IndexEntry**) malloc(DATA_SIZE * sizeof(IndexEntry*));
//		index_runs[i]->final_part_sizes = (internal_int_t*) malloc(DATA_SIZE * sizeof(internal_int_t));

		index_runs[i]->radix_offsets = (internal_int_t*) malloc(index_runs[i]->size * sizeof(internal_int_t));
		index_runs[i]->radix_minVals = (internal_int_t*) malloc(index_runs[i]->size * sizeof(internal_int_t));
		index_runs[i]->radix_maxVals = (internal_int_t*) malloc(index_runs[i]->size * sizeof(internal_int_t));
		index_runs[i]->radix_msb = (internal_int_t*) malloc(index_runs[i]->size * sizeof(internal_int_t));

		internal_int_t pos=0, j=0;
		debug(INFO, "Number of index_runs: ""%lld"", data size: ""%lld""",index_runs[i]->size, DATA_SIZE);
		for (; j < index_runs[i]->size; j++){
		   index_runs[i]->c[j] = (IndexEntry*) malloc(run_size*INDEX_ENTRY_SIZE);
		   memcpy(index_runs[i]->c[j], &c[i][pos], run_size*INDEX_ENTRY_SIZE);
		   pos += run_size;
		   index_runs[i]->sizes[j] = run_size;
		   index_runs[i]->indexes[j] = MakeEmpty(NULL);
		}

		final_partition_t emptyPartition;
		emptyPartition.data = NULL;
		emptyPartition.hole = true;
		emptyPartition.num_elements = 0;

		index_runs[i]->final_index = new final_run_t;
		index_runs[i]->final_index->insert(std::make_pair(0, emptyPartition));

		index_runs[i]->final_size = 1;
		index_runs[i]->final = c[i];
	}
}

void reset_index_runs() {
	if(index_runs) {
		internal_int_t i,j;

		for(i = 0; i < NUMBER_OF_INDEXES; ++i) {
			for(j = 0; j < index_runs[i]->size; ++j) {
				free(index_runs[i]->c[j]);
			}
			free(index_runs[i]->c);
			free(index_runs[i]);
			index_runs[i] = NULL;
		}
		free(index_runs);
		index_runs = NULL;
	}
}

void crackInTwoHybrid(IndexEntry*& c, internal_int_t key, Result rs, sideways_data_t sideways_data){
	internal_int_t runIndex = std::max((int) (sideways_data.coveredAttribute - 1), 0);

	INDEX_RUNS current_runs = index_runs[runIndex];

	internal_int_t final_size = current_runs->final_size;

	internal_int_t i=0;
	for (; i < current_runs->size; i++){
		// crack the initial partition
		debug(INFO, "Initial Partition: ""%lld"", Run Size: ""%lld""",i,current_runs->sizes[i]);
		printIndexArray(current_runs->c[i], current_runs->sizes[i]);
		internal_int_t pivot = crackInTwoItemWise(current_runs->c[i], 0, current_runs->sizes[i]-1, key);

		// copy into final partition
		printIndexArray(current_runs->c[i], current_runs->sizes[i]);
		debug(INFO, "Final Partition Size: ""%lld"", Additional Entries Copied: ""%lld""",current_runs->final_size,(pivot+1));
		memcpy(&current_runs->final[current_runs->final_size], current_runs->c[i], (pivot+1)*INDEX_ENTRY_SIZE);
		current_runs->final_size += pivot+1;

		// remove from the initial partition
		current_runs->sizes[i] -= pivot+1;
		debug(INFO, "New Run Size: ""%lld""",current_runs->sizes[i]);
		memmove(current_runs->c[i], &current_runs->c[i][pivot+1], (current_runs->sizes[i])*INDEX_ENTRY_SIZE);
		debug(INFO, "%s", "NEW RUN:");
		printIndexArray(current_runs->c[i], current_runs->sizes[i]);
	}

	// sort newly added elements in the final partition
	//printf("previous_final_size=""%lld"", current_final_size=""%lld""\n",final_size,index_runs->final_size);
	if(current_runs->final_size - final_size > 0){
		debug(INFO, "%s", "FINAL RUN: ");
		printIndexArray(current_runs->final, current_runs->final_size);
		SORT[RADIX_INSERT](&current_runs->final[final_size], current_runs->final_size - final_size);
		debug(INFO, "%s", "FINAL RUN: ");
		printIndexArray(current_runs->final, current_runs->final_size);
		if(final_size > 0)
			merge_index(current_runs->final, 0, final_size-1, current_runs->final_size-1);
		debug(INFO, "%s", "FINAL RUN: ");
		printIndexArray(current_runs->final, current_runs->final_size);

	}
}
