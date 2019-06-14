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
 * cracking.h
 *
 * Information Systems Group
 * Saarland University
 * 2012 - 2013
 */

#ifndef _Cracking_H
#define _Cracking_H

//#define DEBUG	// should be replaced with compiler option -dDEBUG

#include <sys/mman.h>
#include <map>
#include <pthread.h>
#include <assert.h>
#include <queue>
#include <list>
#include <set>
#include <algorithm>
#include <math.h>
#include "bptree_wrapper.h"
#include "avltree.h"
#include "experiment.h"
#include "utils.h"
#include "print.h"

#ifndef DEBUG	// for asserts!
#ifndef NDEBUG
#define NDEBUG	// should be replaced with compiler option -dNDEBUG
#endif
#endif

typedef enum crack_type	{STANDARD=0, STOCHASTIC, HYBRID_CS, HYBRID_RS, HYBRID_SS, PARTITIONED_STANDARD, BUFFERED} CRACK_TYPE;
//struct pair {
//	internal_int_t first;
//	internal_int_t second;
//};
//typedef struct pair *Pair;

void setSeed(internal_int_t fixedSeed);
void exchange(IndexEntry*& c, internal_int_t x1, internal_int_t x2);


typedef struct {
	IndexEntry* data;
	internal_int_t num_elements;
	bool hole;
} final_partition_t;

inline final_partition_t getHole() {
	final_partition_t hole;
	hole.data = NULL;
	hole.hole = true;
	hole.num_elements = 0;
	return hole;
}

typedef std::map<internal_int_t, final_partition_t> final_run_t;

inline internal_int_t getPartitionIndex(internal_int_t key, internal_int_t rangePerPartition, internal_int_t numberOfPartitions) {
	internal_int_t index = key / rangePerPartition;
	if(index >= numberOfPartitions) {
		index = numberOfPartitions - 1;
	}
	return index;
}

struct partitions_index{
	internal_int_t size;
	IndexEntry **c;
	internal_int_t *sizes;

	AvlTree *indexes;
	final_run_t* final_index;

	bool firstTime;

	internal_int_t *radix_offsets;
	internal_int_t *radix_minVals;
	internal_int_t *radix_maxVals;
	internal_int_t *radix_msb;

	internal_int_t final_size;
	IndexEntry *final;
};

typedef struct partitions_index *INDEX_RUNS;

void create_index_runs(IndexEntry **c, internal_int_t run_size);
void reset_index_runs();

#ifdef COUNT_COMP
	#define countComp() ++compCount
#else
	#define countComp() true
#endif

typedef std::set<std::pair<internal_int_t, internal_int_t> > sortedPartitions_t;
extern sortedPartitions_t sortedPartitions;

internal_int_t crackInTwoItemWise(IndexEntry*& c, internal_int_t posL, internal_int_t posH, internal_int_t med);
internal_int_t crackInTwoItemWiseBuffered(IndexEntry*& c, internal_int_t posL, internal_int_t posH, internal_int_t med);
IntPair crackInTwoMDD1R(IndexEntry*& c, internal_int_t posL, internal_int_t posH, IndexEntry*& view, internal_int_t& view_size, internal_int_t low, internal_int_t high);
void crackInTwoHybrid(IndexEntry*& c, internal_int_t key, Result rs, sideways_data_t sideways_data);

AvlTree crackInThree_standard(IndexEntry*& c, internal_int_t dataSize, AvlTree T, internal_int_t lowKey, internal_int_t highKey, Result r, sideways_data_t sideways_data, COVERING_TYPE cover_type, bptree_t*& partitionBeginToIndex, internal_int_t*& rowIdToPartition);
AvlTree crackInThree_stochastic(IndexEntry*& c, internal_int_t dataSize, AvlTree T, internal_int_t lowKey, internal_int_t highKey, Result r, sideways_data_t sideways_data, COVERING_TYPE cover_type, bptree_t*& partitionBeginToIndex, internal_int_t*& rowIdToPartition);
AvlTree crackInThree_hybrid_cs(IndexEntry*& c, internal_int_t dataSize, AvlTree T, internal_int_t lowKey, internal_int_t highKey, Result r, sideways_data_t sideways_data, COVERING_TYPE cover_type, bptree_t*& partitionBeginToIndex, internal_int_t*& rowIdToPartition);
AvlTree crackInThree_hybrid_rs(IndexEntry*& c, internal_int_t dataSize,  AvlTree T, internal_int_t lowKey, internal_int_t highKey, Result r, sideways_data_t sideways_data, COVERING_TYPE cover_type, bptree_t*& partitionBeginToIndex, internal_int_t*& rowIdToPartition);
AvlTree crackInThree_hybrid_ss(IndexEntry*& c, internal_int_t dataSize, AvlTree T, internal_int_t lowKey, internal_int_t highKey, Result r, sideways_data_t sideways_data, COVERING_TYPE cover_type, bptree_t*& partitionBeginToIndex, internal_int_t*& rowIdToPartition);
AvlTree crackInThree_partitioned_standard(IndexEntry*& c,internal_int_t dataSize,  AvlTree T, internal_int_t lowKey, internal_int_t highKey, Result r, sideways_data_t sideways_data, COVERING_TYPE cover_type, bptree_t*& partitionBeginToIndex, internal_int_t*& rowIdToPartition);
AvlTree crackInThree_buffered(IndexEntry*& c, internal_int_t dataSize, AvlTree T, internal_int_t lowKey, internal_int_t highKey, Result r, sideways_data_t sideways_data, COVERING_TYPE cover_type, bptree_t*& partitionBeginToIndex, internal_int_t*& rowIdToPartition);
extern AvlTree (*CRACK_IN_THREE[])(IndexEntry*& c, internal_int_t dataSize, AvlTree T, internal_int_t lowKey, internal_int_t highKey, Result r, sideways_data_t sideways_data, COVERING_TYPE cover_type, bptree_t*& partitionBeginToIndex, internal_int_t*& rowIdToPartition);

internal_int_t getSeed(internal_int_t fixedSeed);

#endif /* _Cracking_H */
