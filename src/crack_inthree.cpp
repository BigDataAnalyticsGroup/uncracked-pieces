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
 * cracking_inthree.cpp
 *
 * Information Systems Group
 * Saarland University
 * 2012 - 2013
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include "../includes/cracking.h"
#include "../includes/sorting.h"
#include "../includes/utils.h"
#include "../includes/binary_search.h"

void* malloc_huge(size_t size) {
   void* p = mmap(NULL, size, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS|MAP_NORESERVE, -1, 0);
   if(!p) {
	   printf("Out of memory\n");
	   exit(1);
   }

   #ifndef __MACH__
   	   madvise(p, size, MADV_HUGEPAGE);
   	   madvise(p, size, MADV_SEQUENTIAL);
   #endif

   return p;
}

internal_int_t swapCount;
internal_int_t compCount;

extern INDEX_RUNS* index_runs;

AvlTree (*CRACK_IN_THREE[])(IndexEntry*& c, internal_int_t dataSize, AvlTree T, internal_int_t lowKey, internal_int_t highKey, Result r, sideways_data_t sideways_data, COVERING_TYPE cover_type, bptree_t*& partitionBeginToIndex, internal_int_t*& rowIdToPartition)
																																				= {&crackInThree_standard,
																																					&crackInThree_stochastic,
																																					&crackInThree_hybrid_cs,
																																					&crackInThree_hybrid_rs,
																																					&crackInThree_hybrid_ss,
																																					&crackInThree_partitioned_standard,
																																					&crackInThree_buffered};


/**
 * incL = incH = false
 *
 *
 *
 */
IntPair crackInThreeItemWise(IndexEntry *c, internal_int_t posL, internal_int_t posH, internal_int_t low, internal_int_t high){

	internal_int_t x1 = posL, x2 = posH;

	while(x2 > x1 && c[x2] >= high)
		x2--;

	internal_int_t x3 = x2;
	while(x3 > x1 && c[x3] >= low){
		if(c[x3]>=high){
			exchange(c, x2, x3);
			x2--;
		}
		x3--;
	}

	while(x1<=x3){
		if(c[x1] < low)
			x1++;
		else{
			exchange(c, x1, x3);
			while(x3 > x1 && c[x3] >= low){
				if(c[x3]>=high){
					exchange(c, x2, x3);
					x2--;
				}
				x3--;
			}
		}
	}

	IntPair p = (IntPair) malloc(sizeof(struct int_pair));
	p->first = x3;
	p->second = x2;
	return p;
}

IntPair crackInThreeItemWiseImproved(IndexEntry *c, internal_int_t posL, internal_int_t posH, internal_int_t low, internal_int_t high){
	internal_int_t x1 = posL, x2 = posH;

	while(x2 > x1 && c[x2] >= high)
		x2--;

	internal_int_t x3 = x2;
	while(x3 > x1 && (countComp() && c[x3] >= low)){
		if(countComp() && c[x3]>=high){
			exchange(c, x2, x3);
			x2--;
		}
		x3--;
	}

	while(x1<=x3){
		if(countComp() && c[x1] < low)
			x1++;
		else{
			exchange(c, x1, x3);
			if(countComp() && c[x3]>=high){
				exchange(c, x2, x3);
				x2--;
			}
			x3--;
		}
	}

	IntPair p = (IntPair) malloc(sizeof(struct int_pair));
	p->first = x3;
	p->second = x2;
	return p;
}

void crackIntoPartition(INDEX_RUNS current_runs, final_partition_t& newPart, internal_int_t newLower, internal_int_t newUpper, Result r) {
	// crack 'newLower' to 'newUpper' in all initial partitions
	IntPair *pivot_pairs = (IntPair*) malloc(current_runs->size*sizeof(IntPair));
	internal_int_t k, num_elements = 0, pos=0;
	for (k=0; k < current_runs->size; k++){
		t_on(r->c_il);
		IntPair p1 = FindNeighborsLT(newLower, current_runs->indexes[k], current_runs->sizes[k]-1);
		IntPair p2 = FindNeighborsLT(newUpper, current_runs->indexes[k], current_runs->sizes[k]-1);
		t_extendLap(r->c_il);

		t_on(r->c_do);
		pivot_pairs[k] = crackInThreeItemWiseImproved(current_runs->c[k], p1->first, p2->second, newLower, newUpper);		// should always be crack-in-three
		t_extendLap(r->c_do);
		free(p1);
		free(p2);

		num_elements += pivot_pairs[k]->second-pivot_pairs[k]->first;

		t_on(r->c_iu);
		current_runs->indexes[k] = Insert(pivot_pairs[k]->first, newLower, current_runs->indexes[k]);
		current_runs->indexes[k] = Insert(pivot_pairs[k]->second, newUpper, current_runs->indexes[k]);
		t_extendLap(r->c_iu);
	}
	// collect the final values in a single array
	IndexEntry *new_partition = (IndexEntry*)malloc(num_elements*sizeof(IndexEntry));
	for(k=0; k < current_runs->size; k++){
		internal_int_t new_elements = pivot_pairs[k]->second-pivot_pairs[k]->first;
		if(new_elements > 0){
			memcpy(&new_partition[pos], &current_runs->c[k][pivot_pairs[k]->first+1], new_elements*INDEX_ENTRY_SIZE);
			//printIndexArray(new_partition, pos);
			pos += new_elements;
		}
	}

	for(k=0; k < current_runs->size; k++){
		free(pivot_pairs[k]);
	}
	free(pivot_pairs);
	// sort the array
	//printIndexArray(new_partition, num_elements);
	SORT[RADIX_INSERT](new_partition, num_elements);
	newPart.data = new_partition;
	newPart.num_elements = num_elements;
}

void sortIntoPartition(INDEX_RUNS current_runs, final_partition_t& newPart, internal_int_t newLower, internal_int_t newUpper, Result r) {
	// crack 'newLower' to 'newUpper' in all initial partitions
	if(current_runs->firstTime){
		current_runs->firstTime = false;
		for (internal_int_t i=0; i < current_runs->size; i++) {
			SORT[RADIX_INSERT](current_runs->c[i], current_runs->sizes[i]);
		}
	}

	IntPair *pivot_pairs = (IntPair*) malloc(current_runs->size*sizeof(IntPair));
	internal_int_t k, num_elements = 0, pos=0;
	for (k=0; k < current_runs->size; k++){
		pivot_pairs[k] = (IntPair) malloc(sizeof(struct int_pair));
		pivot_pairs[k]->first = binary_search_gte(current_runs->c[k], newLower, 0, current_runs->sizes[k]-1);
		pivot_pairs[k]->second = binary_search_lt(current_runs->c[k], newUpper, 0, current_runs->sizes[k]-1);
		num_elements += pivot_pairs[k]->second-pivot_pairs[k]->first+1;
	}
	// collect the final values in a single array
	IndexEntry *new_partition = (IndexEntry*)malloc(num_elements*sizeof(IndexEntry));
	for(k=0; k < current_runs->size; k++){
		internal_int_t new_elements = pivot_pairs[k]->second-pivot_pairs[k]->first+1;
		if(new_elements > 0){
			memcpy(&new_partition[pos], &current_runs->c[k][pivot_pairs[k]->first], new_elements*INDEX_ENTRY_SIZE);
			printIndexArray(new_partition, pos);
			pos += new_elements;
		}
	}
	for(k=0; k < current_runs->size; k++){
		free(pivot_pairs[k]);
	}
	free(pivot_pairs);
	// sort the array
	//printIndexArray(new_partition, num_elements);
	SORT[RADIX_INSERT](new_partition, num_elements);
	newPart.data = new_partition;
	newPart.num_elements = num_elements;
}

void radixIntoPartition(INDEX_RUNS current_runs, final_partition_t& newPart, internal_int_t newLower, internal_int_t newUpper, Result r) {
	if(current_runs->firstTime){
		current_runs->firstTime = false;

		// radix-partition each run
		internal_int_t last[2],ptr[2],cnt[2];
		internal_int_t i,j,k,sorted,remain, p;
		IndexEntry temp,swap;

		for (p=0; p < current_runs->size; p++){

			printIndexArray(current_runs->c[p], current_runs->sizes[p]);

			// scan to find the minimum, maximum element
			internal_int_t min=LLONG_MAX,max=0,max_msb;
			for(i=0;i<current_runs->sizes[p];i++){
				if(current_runs->c[p][i].m_key > max)
					max = current_runs->c[p][i].m_key;
				if(current_runs->c[p][i].m_key < min)
					min = current_runs->c[p][i].m_key;
			}
			current_runs->radix_minVals[p] = min;
			current_runs->radix_maxVals[p] = max;
			debug(INFO,"min=""%lld"", max=""%lld""",min,max,max);

			max=max-min, max_msb=-1;
			for(; max>0; max = max >> 1) max_msb++;
			current_runs->radix_msb[p] = max_msb;
			debug(INFO,"msb=""%lld""",max_msb);

			// create the two buckets
			cnt[0] = 0; cnt[1] = 0;
			for(i=0, max=0;i<current_runs->sizes[p];i++)
				cnt[((current_runs->c[p][i].m_key - min) >> max_msb) & 0x01]++;
			sorted = (cnt[0]==current_runs->sizes[p]) | (cnt[1]==current_runs->sizes[p]);	// check if sorted
			current_runs->radix_offsets[p] = cnt[0];
			debug(INFO,"radix offset=""%lld""",cnt[0]);

			// Accumulate counters into pointers
			ptr[0]=0; last[0]=cnt[0]; ptr[1]=last[0]; last[1]=last[0]+cnt[1];

			if(!sorted){	// Go through all swaps
				i = 1;
				remain = current_runs->sizes[p];
				while(remain > 0){
					while(ptr[i] == last[i])
						i--;	// Find uncompleted value range
					j = ptr[i];	// Grab first element in cycle
					swap = current_runs->c[p][j];
					k = ((swap.m_key-min) >> max_msb) & 0x01;
					if(i != k){	// Swap into correct range until cycle completed
						do{
							temp = current_runs->c[p][ptr[k]];
							current_runs->c[p][ptr[k]++] = swap;
							k = (((swap=temp).m_key-min) >> max_msb) & 0x01;
							remain--;
						}while(i != k);
						current_runs->c[p][j] = swap;	// Place last element in cycle
					}
					ptr[k]++;
					remain--;
				}
			}

			printIndexArray(current_runs->c[p], current_runs->sizes[p]);
		}
	}

	IntPair *pivot_pairs = (IntPair*) malloc(current_runs->size*sizeof(IntPair));
	internal_int_t k, num_elements = 0, pos=0;
	for (k=0; k < current_runs->size; k++){
		if((newLower < current_runs->radix_minVals[k] && newUpper < current_runs->radix_minVals[k]) ||
				(newLower > current_runs->radix_maxVals[k] && newUpper > current_runs->radix_maxVals[k])) {
			pivot_pairs[k] = (IntPair) malloc(sizeof(struct int_pair));
			pivot_pairs[k]->first = 0;
			pivot_pairs[k]->second = 0;
			continue;
		}

		IntPair crackerIndexP1 = FindNeighborsLT(newLower, current_runs->indexes[k], current_runs->sizes[k]-1);
		IntPair crackerIndexP2 = FindNeighborsLT(newUpper, current_runs->indexes[k], current_runs->sizes[k]-1);

		// crack the initial partition
		debug(INFO,"Initial Partition: ""%lld"", Run Size: ""%lld""",k ,current_runs->sizes[k]);
		printIndexArray(current_runs->c[k], current_runs->sizes[k]);
		internal_int_t p1 = newLower < current_runs->radix_minVals[k] ? 0 : ((newLower-current_runs->radix_minVals[k]) >> current_runs->radix_msb[k]) & 0x01;
		internal_int_t p2 = newUpper > current_runs->radix_maxVals[k] ? 1 : ((newUpper-current_runs->radix_minVals[k]) >> current_runs->radix_msb[k]) & 0x01;
		debug(INFO, "radix partition 1: ""%lld"", radix partition 2: ""%lld""",p1,p2);

		if(p1==0 && p2==0) {
			pivot_pairs[k] = crackInThreeItemWiseImproved(current_runs->c[k],
														std::max((internal_int_t) 0, crackerIndexP1->first),
														std::min(current_runs->radix_offsets[k]-1, crackerIndexP2->second),
														newLower, newUpper);
		}
		else if(p1==1 && p2==1) {
			pivot_pairs[k] = crackInThreeItemWiseImproved(current_runs->c[k],
														std::max(current_runs->radix_offsets[k], crackerIndexP1->first),
														std::min(current_runs->sizes[k]-1, crackerIndexP2->second),
														newLower, newUpper);
		}
		else {
			pivot_pairs[k] = crackInThreeItemWiseImproved(current_runs->c[k],
														std::max((internal_int_t) 0, crackerIndexP1->first),
														std::min(current_runs->sizes[k]-1, crackerIndexP2->second),
														newLower, newUpper);
		}

		current_runs->indexes[k] = Insert(pivot_pairs[k]->first, newLower, current_runs->indexes[k]);
		current_runs->indexes[k] = Insert(pivot_pairs[k]->second, newUpper, current_runs->indexes[k]);

		num_elements += pivot_pairs[k]->second-pivot_pairs[k]->first;
	}

	// collect the final values in a single array
	IndexEntry *new_partition = (IndexEntry*)malloc(num_elements*sizeof(IndexEntry));
	for(k=0; k < current_runs->size; k++){
		internal_int_t new_elements = pivot_pairs[k]->second-pivot_pairs[k]->first;
		if(new_elements > 0){
			memcpy(&new_partition[pos], &current_runs->c[k][pivot_pairs[k]->first+1], new_elements*INDEX_ENTRY_SIZE);
			//printIndexArray(new_partition, pos);
			pos += new_elements;
		}
	}
	for(k=0; k < current_runs->size; k++){
		free(pivot_pairs[k]);
	}
	free(pivot_pairs);
	// sort the array
	//printIndexArray(new_partition, num_elements);
	SORT[RADIX_INSERT](new_partition, num_elements);
	newPart.data = new_partition;
	newPart.num_elements = num_elements;
}

void crackInThreeHybridSort(CRACK_TYPE t, IndexEntry *c, internal_int_t low, internal_int_t high, Result rs, sideways_data_t sideways_data){
	debug(INFO, "KeyL=""%lld"", KeyH=""%lld""",low,high);

	if(low >= high) {
		return;
	}

	internal_int_t runIndex = std::max((int)(sideways_data.coveredAttribute - 1), 0);
	INDEX_RUNS current_runs = index_runs[runIndex];

	t_on(rs->c_il);
	final_run_t::iterator firstPart = current_runs->final_index->upper_bound(low);
	--firstPart;
	final_run_t::iterator secondPart = current_runs->final_index->upper_bound(high);
	--secondPart;
	t_extendLap(rs->c_il);

	bool seenAllParts = false;
	std::vector<std::pair<internal_int_t, final_partition_t> > partitionChanges;

	while(!seenAllParts) {
		internal_int_t key = firstPart->first;
		final_partition_t& partition = firstPart->second;

		if(partition.hole) {
			// create new partition
			final_partition_t newPart;
			newPart.hole = false;

			final_run_t::iterator followingPart = firstPart;
			followingPart++;

			// this hole is affected by the query, so find the borders
			internal_int_t newLower = std::max(key, low);
			internal_int_t newUpper = std::min(followingPart == current_runs->final_index->end() ? DATA_SIZE + 1 : followingPart->first, high);

			// copy the data from the initial partitions to the new final one
			switch(t) {
				case HYBRID_CS: crackIntoPartition(current_runs, newPart, newLower, newUpper, rs); break;
				case HYBRID_SS: sortIntoPartition(current_runs, newPart, newLower, newUpper, rs); break;
				case HYBRID_RS: radixIntoPartition(current_runs, newPart, newLower, newUpper, rs); break;
				default: printError("Only Hybrid crack types are allowed.");
			}
			partitionChanges.push_back(std::make_pair(newLower, newPart));

			// create new holes (at most 2)
			if(newLower > key) {
				final_partition_t leftHole = getHole();
				partitionChanges.push_back(std::make_pair(key, leftHole));
			}

			final_partition_t rightHole = getHole();
			if(followingPart == current_runs->final_index->end()) {
				if(newUpper < DATA_SIZE + 1) {
					partitionChanges.push_back(std::make_pair(newUpper, rightHole));
				}
			}
			else {
				if(newUpper < followingPart->first) {
					partitionChanges.push_back(std::make_pair(newUpper, rightHole));
				}
			}
		}

		if(firstPart == secondPart) {
			seenAllParts = true;
		}
		else {
			++firstPart;
		}
	}

	// apply changes and remove old entries
	for(internal_int_t i = 0; i < (internal_int_t) partitionChanges.size(); ++i) {
		current_runs->final_index->erase(partitionChanges[i].first);
		current_runs->final_index->insert(partitionChanges[i]);
	}

	rs->sc = swapCount;
	rs->t_sc += rs->sc;
}


// hybrid radix sort (HRS)
void crackInThreeHybridRadixSort(IndexEntry *c, internal_int_t low, internal_int_t high, Result rs, sideways_data_t sideways_data){
	internal_int_t runIndex = std::max((int)(sideways_data.coveredAttribute - 1), 0);
	INDEX_RUNS current_runs = index_runs[runIndex];

	if(current_runs->firstTime){
		current_runs->firstTime = false;

		// radix-partition each run
		internal_int_t last[2],ptr[2],cnt[2];
		internal_int_t i,j,k,sorted,remain, p;
		IndexEntry temp,swap;

		for (p=0; p < current_runs->size; p++){

			printIndexArray(current_runs->c[p], current_runs->sizes[p]);

			// scan to find the minimum, maximum element
			internal_int_t min=LLONG_MAX,max=0,max_msb;
			for(i=0;i<current_runs->sizes[p];i++){
				if(current_runs->c[p][i].m_key > max)
					max = current_runs->c[p][i].m_key;
				if(current_runs->c[p][i].m_key < min)
					min = current_runs->c[p][i].m_key;
			}
			current_runs->radix_minVals[p] = min;
			current_runs->radix_maxVals[p] = max;
			debug(INFO,"min=""%lld"", max=""%lld""",min,max,max);

			max=max-min, max_msb=-1;
			for(; max>0; max = max >> 1) max_msb++;
			current_runs->radix_msb[p] = max_msb;
			debug(INFO,"msb=""%lld""",max_msb);

			// create the two buckets
			cnt[0] = 0; cnt[1] = 0;
			for(i=0, max=0;i<current_runs->sizes[p];i++)
				cnt[((current_runs->c[p][i].m_key - min) >> max_msb) & 0x01]++;
			sorted = (cnt[0]==current_runs->sizes[p]) | (cnt[1]==current_runs->sizes[p]);	// check if sorted
			current_runs->radix_offsets[p] = cnt[0];
			debug(INFO,"radix offset=""%lld""",cnt[0]);

			// Accumulate counters into pointers
			ptr[0]=0; last[0]=cnt[0]; ptr[1]=last[0]; last[1]=last[0]+cnt[1];

			if(!sorted){	// Go through all swaps
				i = 1;
				remain = current_runs->sizes[p];
				while(remain > 0){
					while(ptr[i] == last[i])
						i--;	// Find uncompleted value range
					j = ptr[i];	// Grab first element in cycle
					swap = current_runs->c[p][j];
					k = ((swap.m_key-min) >> max_msb) & 0x01;
					if(i != k){	// Swap into correct range until cycle completed
						do{
							temp = current_runs->c[p][ptr[k]];
							current_runs->c[p][ptr[k]++] = swap;
							k = (((swap=temp).m_key-min) >> max_msb) & 0x01;
							remain--;
						}while(i != k);
						current_runs->c[p][j] = swap;	// Place last element in cycle
					}
					ptr[k]++;
					remain--;
				}
			}

			printIndexArray(current_runs->c[p], current_runs->sizes[p]);
		}
	}


	internal_int_t final_size = current_runs->final_size;

	internal_int_t i=0, from, to;
	for (; i < current_runs->size; i++){
		if((low < current_runs->radix_minVals[i] && high < current_runs->radix_minVals[i]) ||
				(low > current_runs->radix_maxVals[i] && high > current_runs->radix_maxVals[i]))
			continue;

		// crack the initial partition
		debug(INFO,"Initial Partition: ""%lld"", Run Size: ""%lld""",i,current_runs->sizes[i]);
		printIndexArray(current_runs->c[i], current_runs->sizes[i]);
		internal_int_t p1 = low < current_runs->radix_minVals[i] ? 0 : ((low-current_runs->radix_minVals[i]) >> current_runs->radix_msb[i]) & 0x01;
		internal_int_t p2 = high > current_runs->radix_maxVals[i] ? 1 : ((high-current_runs->radix_minVals[i]) >> current_runs->radix_msb[i]) & 0x01;
		debug(INFO, "radix partition 1: ""%lld"", radix partition 2: ""%lld""",p1,p2);
		IntPair pivots;
		if(p1==0 && p2==0)
			pivots = crackInThreeItemWiseImproved(current_runs->c[i], 0, current_runs->radix_offsets[i]-1, low, high);
		else if(p1==1 && p2==1)
			pivots = crackInThreeItemWiseImproved(current_runs->c[i], current_runs->radix_offsets[i], current_runs->sizes[i]-1, low, high);
		else
			pivots = crackInThreeItemWiseImproved(current_runs->c[i], 0, current_runs->sizes[i]-1, low, high);

		// copy into final partition
		printIndexArray(current_runs->c[i], current_runs->sizes[i]);
		from = pivots->first+1; to = pivots->second;
		debug(INFO, "Final Partition Size: ""%lld"", Additional Entries Copied: ""%lld""",current_runs->final_size,(to-from+1));
		memcpy(&current_runs->final[current_runs->final_size], &current_runs->c[i][from], (to-from+1)*INDEX_ENTRY_SIZE);
		current_runs->final_size += to-from+1;

		// remove from the initial partition
		memmove(&current_runs->c[i][from], &current_runs->c[i][to+1], (current_runs->sizes[i]-to-1)*INDEX_ENTRY_SIZE);
		current_runs->sizes[i] -= to-from+1;
		debug(INFO, "from=""%lld"", to=""%lld"", New Run Size: ""%lld""",from, to, current_runs->sizes[i]);
		if(p1==0 && p2==0)
			current_runs->radix_offsets[i] -= to-from+1;		// both low and high keys are in the "0" partition
		else if(p1!=p2)
			current_runs->radix_offsets[i] = from;			// shift "1" offset to the low key position

		free(pivots);
	}

	// sort newly added elements in the final partition
	//printf("previous_final_size=""%lld"", current_final_size=""%lld""\n",final_size,index_runs->final_size);
	debug(INFO, "Size of final partition = ""%lld""", current_runs->final_size);
	if(current_runs->final_size - final_size > 0){
		debug(INFO, "%s", "FINAL RUN: ");
		printIndexArray(current_runs->final, current_runs->final_size);
		SORT[RADIX_INSERT](&current_runs->final[final_size], current_runs->final_size - final_size);
		debug(INFO, "%s", "FINAL RUN: ");
		printIndexArray(current_runs->final, current_runs->final_size);
		if(final_size > 0)
			merge_index_chunk(current_runs->final, 0, final_size-1, current_runs->final_size-1);
		debug(INFO, "%s", "FINAL RUN: ");
		printIndexArray(current_runs->final, current_runs->final_size);
	}

	rs->sc = swapCount;
	rs->t_sc += rs->sc;
}

typedef struct {
	internal_int_t threadId;
	IndexEntry* c;
	IndexEntry* partitioned_c;
	internal_int_t numberOfBytes;
	internal_int_t rangePerPartition;
	internal_int_t numberOfPartitions;
	internal_int_t* currentPartitionEntries;
	internal_int_t** entriesPerPartitionPerThread;
	internal_int_t* rowIdToPartition;
	COVERING_TYPE cover_type;
	pthread_mutex_t* partitionMutexes;
	internal_int_t lowerBound;
	internal_int_t upperBound;
} PartitioningData;


bool partitioned = false;

void partitionIndex(IndexEntry*& c,
						AvlTree &T,
						internal_int_t numberOfPartitions,
						internal_int_t*& partitionBeginIndex,
						bptree_t*& partitionBeginToIndex,
						internal_int_t& rangePerPartition,
						internal_int_t*& rowIdToPartition,
						Result r,
						COVERING_TYPE cover_type,
						CRACK_TYPE t) {
	// precondition: the cracker index is empty when this function is called, we build the index from scratch

	// count the number of values that will be in each partition to compute the starting pointers
	internal_int_t i;
	internal_int_t valueRange = DATA_SIZE / DUP_FACTOR;
	rangePerPartition = valueRange / numberOfPartitions;

	if(valueRange % numberOfPartitions != 0) {
		printError("Please make sure that (DATA_SIZE / DUP_FACTOR) % NUMBER_OF_PARTITIONS == 0");
	}

	if(!partitioned) {
		internal_int_t entriesPerPartition[numberOfPartitions];
		memset(entriesPerPartition, 0, numberOfPartitions * INT_SIZE);

		// count the entries per partition
		internal_int_t index;

		for(i = 0; i < DATA_SIZE; ++i) {
			index = getPartitionIndex(c[i].m_key, rangePerPartition, numberOfPartitions);
			++entriesPerPartition[index];
		}

		// calculate the pointers
		partitionBeginIndex = (internal_int_t*) malloc(numberOfPartitions * INT_SIZE);
		// build a tree that provides the reverse direction of the partitionBeginIndex:
		// a mapping from begin of partition to the partition index that will be necessary
		// in the decompression phase
		partitionBeginToIndex = new bptree_t;
		partitionBeginIndex[0] = 0;
		partitionBeginToIndex->insert(std::make_pair(partitionBeginIndex[0], 0));
		debug(INFO, "Size of partition %lld: %lld", 0, entriesPerPartition[0]);
		for(i = 1; i < numberOfPartitions; ++i) {
			debug(INFO, "Size of partition %lld: %lld", i, entriesPerPartition[i]);
			partitionBeginIndex[i] = partitionBeginIndex[i-1] + entriesPerPartition[i-1];
			partitionBeginToIndex->insert(std::make_pair(partitionBeginIndex[i], i));
		}

		internal_int_t currentPartitionEntries[numberOfPartitions];
		memcpy(currentPartitionEntries, partitionBeginIndex, numberOfPartitions * INT_SIZE);
		rowIdToPartition = (internal_int_t*) malloc(DATA_SIZE * INT_SIZE);

		IndexEntry* partitioned_c = (IndexEntry*) malloc_huge(DATA_SIZE * INDEX_ENTRY_SIZE);


		// fill the partitions with the elements
		for(i = 0; i < DATA_SIZE; ++i) {
			index = getPartitionIndex(c[i].m_key, rangePerPartition, numberOfPartitions);
			internal_int_t& currentPartitionEntry = currentPartitionEntries[index];

			rowIdToPartition[i] = currentPartitionEntry;
			partitioned_c[currentPartitionEntry] = c[i];
			if(cover_type == NEARBY_CLUSTERING_EXACT) {
				partitioned_c[currentPartitionEntry].m_rowId = currentPartitionEntry;
			}
			currentPartitionEntry++;
		}

		munmap(c, DATA_SIZE * INDEX_ENTRY_SIZE);
		c = partitioned_c;

		if(t==PARTITIONED_STANDARD) {
			// update the cracker index by adding an entry for each partition boundary
			printIndexArray(c, DATA_SIZE);
			for(i = 1; i < numberOfPartitions; ++i) {
				debug(INFO, "partitionBeginIndex: %lld, Key: %lld", partitionBeginIndex[i], rangePerPartition * i);
				T = Insert(partitionBeginIndex[i] - 1, rangePerPartition * i, T);
			}
			printAVLTree(T);
		}
	}
}

sortedPartitions_t sortedPartitions;

AvlTree crackInThree(IndexEntry*& c, internal_int_t dataSize, AvlTree T, internal_int_t lowKey, internal_int_t highKey, CRACK_TYPE t, Result r, sideways_data_t sideways_data, COVERING_TYPE cover_type, bptree_t*& partitionBeginToIndex, internal_int_t*& rowIdToPartition){
	internal_int_t rangePerPartition = 0;

	static internal_int_t* partitionBeginIndex;
	if(t==PARTITIONED_STANDARD) {
		t_on(r->c_ip);
		partitionIndex(c, T, NUMBER_OF_PARTITIONS, partitionBeginIndex, partitionBeginToIndex, rangePerPartition, rowIdToPartition, r, cover_type, t);
		partitioned = true;
		t_off(r->c_ip);
	}

	IntPair p1,p2;

	t_on(r->c_il);
	p1 = FindNeighborsLT(lowKey, T, dataSize-1);
	p2 = FindNeighborsLT(highKey, T, dataSize-1);
	t_off(r->c_il);

	IntPair pivot_pair = NULL ; swapCount = 0; compCount = 0;

	// stop at certain partition size
	#ifdef STOP_CRACKING_AT_THRESHOLD
		if(p1->second - p1->first < STOP_CRACKING_AT_THRESHOLD || p2->second - p2->first < STOP_CRACKING_AT_THRESHOLD) {
			#ifdef SORT_AFTER_CRACKING_STOP
				if(sortedPartitions.find(std::make_pair(p1->first, p1->second)) == sortedPartitions.end()) {
					//printf("Sorting partition (%lld, %lld)\n", p1->first, p1->second);
					hybrid_quicksort(c + p1->first, p1->second - p1->first + 1);
					sortedPartitions.insert(std::make_pair(p1->first, p1->second));
				}
				else {
					printf("Already sorted (%lld, %lld)\n", p1->first, p1->second);
				}
				if(!(p1->first==p2->first && p1->second==p2->second)) {
					if(sortedPartitions.find(std::make_pair(p2->first, p2->second)) == sortedPartitions.end()) {
						//printf("Sorting partition (%lld, %lld)\n", p2->first, p2->second);
						hybrid_quicksort(c + p2->first, p2->second - p2->first + 1);
						sortedPartitions.insert(std::make_pair(p2->first, p2->second));
					}
				}
				else {
					printf("Already sorted (%lld, %lld)\n", p1->first, p1->second);
				}
			#endif
			r->cracked = false;
			return T;
		}
	#endif

	if(p1->first==p2->first && p1->second==p2->second){

		// crack in three
		t_on(r->c_do);

		if(t==STOCHASTIC){
			pivot_pair = crackInTwoMDD1R(c, p1->first, p1->second, r->qo->view1, r->qo->view_size1, lowKey, highKey);
			printIndexArray(r->qo->view1, r->qo->view_size1);
			printIndexArray(c, dataSize-1);
			lowKey = pivot_pair->first;
			highKey = pivot_pair->first;
			pivot_pair->first = pivot_pair->second;
		}
		else if(t==HYBRID_CS || t==HYBRID_RS || t==HYBRID_SS){
			t_off(r->c_do);
			t_reset(r->c_do);
			t_reset(r->c_il);
			crackInThreeHybridSort(t, c, lowKey, highKey, r, sideways_data);
			free(p1);
			free(p2);
			return NULL;
		}
		else if(t==STANDARD || t==PARTITIONED_STANDARD) {		// other types to be supported
			pivot_pair = crackInThreeItemWise(c, p1->first, p1->second, lowKey, highKey);
		}
		else if(t==BUFFERED) {
			pivot_pair = (IntPair) malloc(sizeof(struct int_pair));
			pivot_pair->first = crackInTwoItemWiseBuffered(c, p1->first, p1->second, lowKey);
			pivot_pair->second = crackInTwoItemWiseBuffered(c, pivot_pair->first, p2->second, highKey);
		}

		t_off(r->c_do);
	}
	else{
		// crack in two
		pivot_pair = (IntPair) malloc(sizeof(struct int_pair));

		t_on(r->c_do);
		if(t==STOCHASTIC){
			IntPair pivot_pair1 = crackInTwoMDD1R(c, p1->first, p1->second, r->qo->view1, r->qo->view_size1, lowKey, highKey);

			r->qo->middlePart = &c[p1->second+1];
			internal_int_t size2 = p2->first-p1->second-1;
			r->qo->middlePart_size = size2;

			IntPair pivot_pair2 = crackInTwoMDD1R(c, p2->first, p2->second, r->qo->view2, r->qo->view_size2, lowKey, highKey);

			pivot_pair->first = pivot_pair1->second;
			lowKey = pivot_pair1->first;
			pivot_pair->second = pivot_pair2->second;
			highKey = pivot_pair2->first;

			free(pivot_pair1);
			free(pivot_pair2);
		}
		else if(t==HYBRID_CS){						// crack in two for RS and SS not implemented
			t_off(r->c_do);
			t_reset(r->c_do);
			t_reset(r->c_il);
			crackInTwoHybrid(c, lowKey, r, sideways_data);
			crackInTwoHybrid(c, highKey, r, sideways_data);
			if(pivot_pair) {
				free(pivot_pair);
				pivot_pair = NULL;
			}
			free(p1);
			free(p2);
			return NULL;
		}
		else if(t==STANDARD || t==PARTITIONED_STANDARD){
			pivot_pair->first = crackInTwoItemWise(c, p1->first, p1->second, lowKey);
			pivot_pair->second = crackInTwoItemWise(c, pivot_pair->first, p2->second, highKey);
		}
		else if(t==BUFFERED) {
			pivot_pair->first = crackInTwoItemWiseBuffered(c, p1->first, p1->second, lowKey);
			pivot_pair->second = crackInTwoItemWiseBuffered(c, pivot_pair->first, p2->second, highKey);
		}
		t_off(r->c_do);
	}

	t_on(r->c_iu);
	T = Insert(pivot_pair->first, lowKey, T);
	T = Insert(pivot_pair->second, highKey, T);
	t_off(r->c_iu);

	free(p1);
	free(p2);
	if(pivot_pair) {
		free(pivot_pair);
		pivot_pair = NULL;
	}

	printIndexArray(c, dataSize);
	printAVLTree(T);

	r->sc = swapCount;
	r->t_sc += r->sc;

	r->cc = compCount;
	r->t_cc += r->cc;
	return T;
}

AvlTree crackInThree_standard(IndexEntry*& c, internal_int_t dataSize, AvlTree T, internal_int_t lowKey, internal_int_t highKey, Result r, sideways_data_t sideways_data, COVERING_TYPE cover_type, bptree_t*& partitionBeginToIndex, internal_int_t*& rowIdToPartition){
	return crackInThree(c, dataSize, T, lowKey, highKey, STANDARD, r, sideways_data, cover_type, partitionBeginToIndex, rowIdToPartition);
}

AvlTree crackInThree_stochastic(IndexEntry*& c, internal_int_t dataSize, AvlTree T, internal_int_t lowKey, internal_int_t highKey, Result r, sideways_data_t sideways_data, COVERING_TYPE cover_type, bptree_t*& partitionBeginToIndex, internal_int_t*& rowIdToPartition){
	return crackInThree(c, dataSize, T, lowKey, highKey, STOCHASTIC, r, sideways_data, cover_type, partitionBeginToIndex, rowIdToPartition);
}

AvlTree crackInThree_hybrid_cs(IndexEntry*& c, internal_int_t dataSize, AvlTree T, internal_int_t lowKey, internal_int_t highKey, Result r, sideways_data_t sideways_data, COVERING_TYPE cover_type, bptree_t*& partitionBeginToIndex, internal_int_t*& rowIdToPartition){
	return crackInThree(c, dataSize, T, lowKey, highKey, HYBRID_CS, r, sideways_data, cover_type, partitionBeginToIndex, rowIdToPartition);
}

AvlTree crackInThree_hybrid_rs(IndexEntry*& c, internal_int_t dataSize, AvlTree T, internal_int_t lowKey, internal_int_t highKey, Result r, sideways_data_t sideways_data, COVERING_TYPE cover_type, bptree_t*& partitionBeginToIndex, internal_int_t*& rowIdToPartition){
	return crackInThree(c, dataSize, T, lowKey, highKey, HYBRID_RS, r, sideways_data, cover_type, partitionBeginToIndex, rowIdToPartition);
}

AvlTree crackInThree_hybrid_ss(IndexEntry*& c, internal_int_t dataSize, AvlTree T, internal_int_t lowKey, internal_int_t highKey, Result r, sideways_data_t sideways_data, COVERING_TYPE cover_type, bptree_t*& partitionBeginToIndex, internal_int_t*& rowIdToPartition){
	return crackInThree(c, dataSize, T, lowKey, highKey, HYBRID_SS, r, sideways_data, cover_type, partitionBeginToIndex, rowIdToPartition);
}

AvlTree crackInThree_partitioned_standard(IndexEntry*& c, internal_int_t dataSize, AvlTree T, internal_int_t lowKey, internal_int_t highKey, Result r, sideways_data_t sideways_data, COVERING_TYPE cover_type, bptree_t*& partitionBeginToIndex, internal_int_t*& rowIdToPartition){
	return crackInThree(c, dataSize, T, lowKey, highKey, PARTITIONED_STANDARD, r, sideways_data, cover_type, partitionBeginToIndex, rowIdToPartition);
}

AvlTree crackInThree_buffered(IndexEntry*& c, internal_int_t dataSize, AvlTree T, internal_int_t lowKey, internal_int_t highKey, Result r, sideways_data_t sideways_data, COVERING_TYPE cover_type, bptree_t*& partitionBeginToIndex, internal_int_t*& rowIdToPartition){
	return crackInThree(c, dataSize, T, lowKey, highKey, BUFFERED, r, sideways_data, cover_type, partitionBeginToIndex, rowIdToPartition);
}

