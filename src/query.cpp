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
 * query.cpp
 *
 * Information Systems Group
 * Saarland University
 * 2012 - 2013
 */

#include <xmmintrin.h>
#include <stdio.h>
#include "../includes/avltree.h"
#include "../includes/query.h"
#include "../includes/arttree.h"
#include "../includes/cracking.h"
#include "../includes/binary_search.h"

extern INDEX_RUNS* index_runs;

// QUERY TABLE

inline internal_int_t queryTable(IndexEntry* currentEntry, table_t* table, internal_int_t numberOfProjectedAttributes) {
	internal_int_t j=1, sum = 0;
	for(;j<numberOfProjectedAttributes;++j) {
			sum += table->cells[j][currentEntry->m_rowId];
	}
	return sum;
}

inline internal_int_t queryTable_Cover(IndexEntry* currentEntry, table_t* table, internal_int_t numberOfProjectedAttributes) {
	internal_int_t j=1, sum = 0;
	for(;j<numberOfProjectedAttributes;++j) {
		if(j <= NUMBER_OF_COVERED_ATTRIBUTES) {
			// the cell must be already covered, so read it from the index
			sum += currentEntry->m_covering[j];
		}
		else {
			sum += table->cells[j][currentEntry->m_rowId];
		}
	}
	return sum;
}

inline internal_int_t queryTable_CoverSideways(IndexEntry* currentEntry, table_t* table, internal_int_t numberOfProjectedAttributes, sideways_data_t sideways_data) {
	internal_int_t j, sum = 0;

	if(sideways_data.coveredAttribute < numberOfProjectedAttributes) {
		// the cell must be already covered, so read it from the index
		sum += currentEntry->m_covering[1];
		if(sideways_data.coveredAttribute == NUMBER_OF_COVERED_ATTRIBUTES) {
			// this was the last covering part, which should also handle the querying of the uncovered
			// data from the source table
			for(j=NUMBER_OF_COVERED_ATTRIBUTES + 1; j < numberOfProjectedAttributes; ++j) {
				sum += table->cells[j][currentEntry->m_rowId];
			}
		}
	}

	return sum;
}

// SCAN

typedef struct {
	IndexEntry *c;
	table_t* table;
	internal_int_t numberOfProjectedAttributes;
	internal_int_t lowerBound;
	internal_int_t upperBound;
	internal_int_t* localSum;
} ScanData;

void* scanThread(void* data) {
	ScanData* scanData = (ScanData*) data;

	IndexEntry* c = scanData->c;
	table_t* table = scanData->table;
	const internal_int_t numberOfProjectedAttributes = scanData->numberOfProjectedAttributes;
	const internal_int_t lowerBound = scanData->lowerBound;
	const internal_int_t upperBound = scanData->upperBound;
	internal_int_t* localSum = scanData->localSum;

	for(internal_int_t i=lowerBound;i<upperBound;i++) {
		*localSum += c[i].m_key;
		*localSum += queryTable(c + i, table, numberOfProjectedAttributes);
	}

	return NULL;
}

internal_int_t scanQuery(IndexEntry *c, table_t* table, internal_int_t numberOfProjectedAttributes, internal_int_t from, internal_int_t to, Result r){
	internal_int_t i=from, sum=0;

	t_on(r->q_da);
	for(i=from;i<=to;i++) {
		sum += c[i].m_key;
		sum += queryTable(c + i, table, numberOfProjectedAttributes);
	}
	t_off(r->q_da);

	return sum;
}



internal_int_t scanQuery_CoverDirect(IndexEntry *c, table_t* table, internal_int_t numberOfProjectedAttributes, internal_int_t from, internal_int_t to){
	internal_int_t i=from, sum=0;
	for(;i<=to;i++) {
		sum += c[i].m_key;
		// the covering data is already copied, so we don't have to do any preparations here
		sum += queryTable_Cover(c + i, table, numberOfProjectedAttributes);
	}
	return sum;
}

internal_int_t scanQuery_CoverDirectSideways(IndexEntry *c, table_t* table, internal_int_t numberOfProjectedAttributes, sideways_data_t sideways_data, internal_int_t from, internal_int_t to){
	internal_int_t i=from, sum=0;
	if(sideways_data.coveredAttribute == 1) {
		// this index is the first part of the covering, so take the key data as well into account
		for(;i<=to;i++) {
			sum += c[i].m_key;
			// the covering data is already copied, so we don't have to do any preparations here
			sum += queryTable_CoverSideways(c + i, table, numberOfProjectedAttributes, sideways_data);
		}
	}
	else {
		for(;i<=to;i++) {
			// the covering data is already copied, so we don't have to do any preparations here
			sum += queryTable_CoverSideways(c + i, table, numberOfProjectedAttributes, sideways_data);
		}
	}

	return sum;
}

// FILTER3

internal_int_t filterQuery3(IndexEntry *c, table_t* table, internal_int_t keyL, internal_int_t keyH, internal_int_t numberOfProjectedAttributes, internal_int_t from, internal_int_t to){
	internal_int_t i=from, sum=0;
	for(;i<=to;i++){
		if(c[i] >= keyL && c[i] < keyH)	 { // filter
			sum += c[i].m_key;
			debug(INFO, "Filtered Key: %lld, RowId: %lld", c[i].m_key, c[i].m_rowId);
			sum += queryTable(c + i, table, numberOfProjectedAttributes);
		}
	}
	return sum;
}

internal_int_t filterQuery3_CoverDirect(IndexEntry *c, table_t* table, internal_int_t keyL, internal_int_t keyH, internal_int_t numberOfProjectedAttributes, internal_int_t from, internal_int_t to){
	internal_int_t i=from, sum=0;
	for(;i<=to;i++){
		if(c[i] >= keyL && c[i] < keyH) {	// filter
			sum += c[i].m_key;
			sum += queryTable_Cover(c + i, table, numberOfProjectedAttributes);
		}
	}
	return sum;
}

internal_int_t filterQuery3_CoverDirectSideways(IndexEntry *c, table_t* table, internal_int_t keyL, internal_int_t keyH, internal_int_t numberOfProjectedAttributes, sideways_data_t sideways_data, internal_int_t from, internal_int_t to){
	internal_int_t i=from, sum=0;
	if(sideways_data.coveredAttribute == 1) {
		for(;i<=to;i++){
			if(c[i] >= keyL && c[i] < keyH) {	// filter
				sum += c[i].m_key;
				sum += queryTable_CoverSideways(c + i, table, numberOfProjectedAttributes, sideways_data);
			}
		}
	}
	else {
		for(;i<=to;i++){
			if(c[i] >= keyL && c[i] < keyH) {	// filter
				sum += queryTable_CoverSideways(c + i, table, numberOfProjectedAttributes, sideways_data);
			}
		}
	}
	return sum;
}

internal_int_t scan_distinguish_covering_mode(IndexEntry *data,
									table_t* table,
									internal_int_t numberOfProjectedAttributes,
									internal_int_t cover_type,
									sideways_data_t sideways_data,
									Result r,
									internal_int_t offset1,
									internal_int_t offset2,
									internal_int_t*& rowIdToPartition) {
	internal_int_t i,j,sum = 0;

	switch(cover_type) {
		case NO_COVER: {
//			t_on(r->q_da);
			sum = scanQuery(data, table, numberOfProjectedAttributes, offset1, offset2, r);
//			t_off(r->q_da);
		} break;
		case COVER_DIRECT: {
			if(sideways_data.sidewaysEnabled) {
				t_on(r->q_da);
				if(data[0].m_covering[0] == 0) {
					for(j = 0; j < DATA_SIZE; ++j) {
						data[j].m_covering[1] = table->cells[sideways_data.coveredAttribute][data[j].m_rowId];
						data[j].m_covering[0] = 1;
					}
				}
				t_off(r->q_da);

				if(sideways_data.coveredAttribute < numberOfProjectedAttributes ||
						(sideways_data.coveredAttribute == numberOfProjectedAttributes && numberOfProjectedAttributes == 1)) {
					t_on(r->q_da);
					sum = scanQuery_CoverDirectSideways(data, table, numberOfProjectedAttributes, sideways_data, offset1, offset2);
					t_off(r->q_da);
				}
			}
			else {
				t_on(r->q_da);
				// copy the covering data at once for all covered attributes
				if(data[0].m_covering[0] == 0) {
					for(i = 1; i <= NUMBER_OF_COVERED_ATTRIBUTES; ++i) {
						for(j = 0; j < DATA_SIZE; ++j) {
							data[j].m_covering[i] = table->cells[i][data[j].m_rowId];
							data[j].m_covering[0] = 1;
						}
					}
				}

				sum = scanQuery_CoverDirect(data, table, numberOfProjectedAttributes, offset1, offset2);
				t_off(r->q_da);
			}
		} break;
		case NEARBY_CLUSTERING_EXACT: {
			t_on(r->q_da);
			if(rowIdToPartition != NULL) {
				// nearby clustering
				if(cover_type==NEARBY_CLUSTERING_EXACT && !table->nearbyClustered) {
					printf("Clustering Table ... ");
					// partition the table in the same way as the index has been partitioned
					internal_int_t i,j;
					for(i = 0; i < table->numCols; ++i) {
						// allocate a new column
						internal_int_t* partitionedCol = (internal_int_t*) malloc(DATA_SIZE * INT_SIZE);
						// partition data based on index partitioning
						for(j = 0; j < DATA_SIZE; ++j) {
							partitionedCol[rowIdToPartition[j]] = table->cells[i][j];
						}
						free(table->cells[i]);
						table->cells[i] = partitionedCol;
					}
					table->nearbyClustered = true;
					printf("Finished\n");
					debug(INFO, "%s", "ClusteredTable");
				}
			}

			sum = scanQuery(data, table, numberOfProjectedAttributes, offset1, offset2, r);
			t_off(r->q_da);
		} break;
		default:
			printError("Unknown covering option passed."); break;
	}

	return sum;
}

internal_int_t filter3_distinguish_covering_mode(IndexEntry *data,
									table_t* table,
									internal_int_t keyL,
									internal_int_t keyH,
									internal_int_t numberOfProjectedAttributes,
									internal_int_t cover_type,
									sideways_data_t sideways_data,
									Result r,
									internal_int_t offset1,
									internal_int_t offset2,
									internal_int_t*& rowIdToPartition) {
	internal_int_t i,j,sum = 0;

	switch(cover_type) {
		case NO_COVER: {
			t_on(r->q_pf);
			sum = filterQuery3(data, table, keyL, keyH, numberOfProjectedAttributes, offset1, offset2);
			t_off(r->q_pf);
		} break;
		case COVER_DIRECT: {
			if(sideways_data.sidewaysEnabled) {
				t_on(r->q_da);
				if(data[0].m_covering[0] == 0) {
					// copy the covering data at once
					for(j = 0; j < DATA_SIZE; ++j) {
						data[j].m_covering[1] = table->cells[sideways_data.coveredAttribute][data[j].m_rowId];
						data[j].m_covering[0] = 1;
					}

				}
				t_off(r->q_da);

				if(sideways_data.coveredAttribute < numberOfProjectedAttributes ||
						(sideways_data.coveredAttribute == numberOfProjectedAttributes && numberOfProjectedAttributes == 1)) {
					t_on(r->q_pf);
					sum = filterQuery3_CoverDirectSideways(data, table, keyL, keyH, numberOfProjectedAttributes, sideways_data, offset1, offset2);
					t_off(r->q_pf);
				}
			}
			else {
				t_on(r->q_da);
				// copy the covering data at once for all covered attributes
				if(data[0].m_covering[0] == 0) {
					for(i = 1; i <= NUMBER_OF_COVERED_ATTRIBUTES; ++i) {
						for(j = 0; j < DATA_SIZE; ++j) {
							data[j].m_covering[i] = table->cells[i][data[j].m_rowId];
							data[j].m_covering[0] = 1;
						}
						free(table->cells[i]);
					}
				}
				t_off(r->q_da);

				t_on(r->q_pf);
				sum = filterQuery3_CoverDirect(data, table, keyL, keyH, numberOfProjectedAttributes, offset1, offset2);
				t_off(r->q_pf);
			}
		} break;
		case NEARBY_CLUSTERING_EXACT: {
			t_on(r->q_pf);
			if(rowIdToPartition != NULL) {
				// nearby clustering
				if(cover_type==NEARBY_CLUSTERING_EXACT && !table->nearbyClustered) {
					// partition the table in the same way as the index has been partitioned
					internal_int_t i,j;
					for(i = 0; i < table->numCols; ++i) {
						// allocate a new column
						internal_int_t* partitionedCol = (internal_int_t*) malloc(DATA_SIZE * INT_SIZE);
						// partition data based on index partitioning
						for(j = 0; j < DATA_SIZE; ++j) {
							partitionedCol[rowIdToPartition[j]] = table->cells[i][j];
						}
						free(table->cells[i]);
						table->cells[i] = partitionedCol;
					}
					table->nearbyClustered = true;
				}
			}

			sum = filterQuery3(data, table, keyL, keyH, numberOfProjectedAttributes, offset1, offset2);
			t_off(r->q_pf);
		} break;
		default:
			printError("Unknown covering option passed."); break;
	}

	return sum;
}


internal_int_t query3(void *T, IndexEntry *data, table_t* table, internal_int_t keyL, internal_int_t keyH, internal_int_t numberOfProjectedAttributes, internal_int_t cover_type, sideways_data_t sideways_data, internal_int_t limit, QUERY_TYPE type, Result r, internal_int_t*& rowIdToPartition){

	// find the offsets
	internal_int_t intermediateSum = 0;
	internal_int_t offset1=0, offset2=0;

	if(type==SEARCH){
		t_on(r->q_il);
		offset1 = binary_search_gte(data, keyL, 0, limit);
		offset2 = binary_search_lt(data, keyH, 0, limit);
		t_off(r->q_il);
	}
	else if(type==Q_AVL || type==AVL_FILTER){
		if(r->cracked) {
			t_on(r->q_il);
			IntPair p1 = FindNeighborsGTE(keyL, (AvlTree)T, limit);
			IntPair p2 = FindNeighborsLT(keyH, (AvlTree)T, limit);
			t_off(r->q_il);
			offset1 = p1->first;
			offset2 = p2->second;
			free(p1);
			free(p2);
		}
	}
	else if(type==Q_BPTREE){
		t_on(r->q_il);
		IntPair p1 = bptreeSearchGTE((BPTree)T, keyL);
		IntPair p2 = bptreeSearchLT((BPTree)T, keyH);
		t_off(r->q_il);
		offset1 = p1->first;
		offset2 = p2->second;
		free(p1);
		free(p2);
	}
	else if(type==Q_BPTREE_BULK){
		t_on(r->q_il);
		offset1 = ((BulkBPTree*)T)->gte(keyL);
		offset2 = ((BulkBPTree*)T)->lt(keyH);
		t_off(r->q_il);
	}
	else if(type==Q_ART){
		t_on(r->q_il);
		uint8_t key[8];
		loadKey(keyL,key);
		offset1 = getLeafValue(lookupPessimistic((ARTTree)T,key,8,0,8))-1;
		loadKey(keyH,key);
		uintptr_t pos = getLeafValue(lookupPessimistic((ARTTree)T,key,8,0,8));
		// workaround for 100% selectivity
		offset2 = pos == 0 ? limit : pos-2;
		t_off(r->q_il);
	}
	else if(type==Q_HYBRID) {
		if(keyL >= keyH) {
			intermediateSum = 0;
		}
		else {
			// lookup start and end partitions for scan
			internal_int_t runIndex = std::max((int)(sideways_data.coveredAttribute - 1), 0);
			INDEX_RUNS current_runs = index_runs[runIndex];

			t_on(r->q_il);
			final_run_t::iterator firstPart = current_runs->final_index->upper_bound(keyL);
			--firstPart;

			final_run_t::iterator secondPart = current_runs->final_index->upper_bound(keyH);
			--secondPart;

			// query first partition
			// perform binary search as there might be non-qualifying entries in this partition if it was created in a previous query
			offset1 = binary_search_gte(firstPart->second.data, keyL, 0, firstPart->second.num_elements);
			offset2 = firstPart->second.num_elements - 1;
			t_extendLap(r->q_il);
			// scan the first partition
			intermediateSum += scan_distinguish_covering_mode(firstPart->second.data, table, numberOfProjectedAttributes, cover_type, sideways_data, r, offset1, offset2, rowIdToPartition);

			// scan the middle partitions
			if(firstPart != secondPart) {
				final_run_t::iterator currentPart = firstPart;
				currentPart++;
				while(currentPart != secondPart) {
					offset1 = 0;
					offset2 = currentPart->second.num_elements - 1;
					// scan the first partition
					intermediateSum += scan_distinguish_covering_mode(currentPart->second.data, table, numberOfProjectedAttributes, cover_type, sideways_data, r, offset1, offset2, rowIdToPartition);

					// find next partition
					currentPart++;
				}

				// query last partition
				if(keyH > currentPart->first) {
					t_on(r->q_il);
					offset1 = 0;
					offset2 = binary_search_lt(currentPart->second.data, keyH, 0, currentPart->second.num_elements);
					t_extendLap(r->q_il);
					// scan the last partition
					intermediateSum += scan_distinguish_covering_mode(currentPart->second.data, table, numberOfProjectedAttributes, cover_type, sideways_data, r, offset1, offset2, rowIdToPartition);
				}
			}
		}
	}
	else{
		offset1 = 0;
		offset2 = limit;
	}
	debug(INFO,"Query offset1=""%lld"", offset2=""%lld""", offset1, offset2);

//	printf("QOffset:""%lld""\n",offset2);

	internal_int_t sum = 0;

	if(r->cracked) {
		if(type==FILTER){
			t_on(r->q_pf);
			sum = filterQuery3(data, table, keyL, keyH, numberOfProjectedAttributes, offset1, offset2);
			t_off(r->q_pf);
		}
		else if(type == AVL_FILTER) {
			sum = filter3_distinguish_covering_mode(data, table, keyL, keyH, numberOfProjectedAttributes, cover_type, sideways_data, r, offset1, offset2, rowIdToPartition);
		}
		else if(type == Q_HYBRID) {
			sum = intermediateSum;
		}
		else{
			sum = scan_distinguish_covering_mode(data, table, numberOfProjectedAttributes, cover_type, sideways_data, r, offset1, offset2, rowIdToPartition);
		}
	}
	else {
		if(type == Q_AVL) {
			IntPair uncrackedLowerPart = FindNeighborsLT(keyL, (AvlTree)T, limit);
			IntPair uncrackedUpperPart = FindNeighborsLT(keyH, (AvlTree)T, limit);

			#ifndef SORT_AFTER_CRACKING_STOP
			// handle cracking that was stopped by threshold
			// apply postfiltering to the border partitions and a simple scan to the inner partitions

			if(uncrackedLowerPart->first==uncrackedUpperPart->first && uncrackedLowerPart->second==uncrackedUpperPart->second) {
				sum += filter3_distinguish_covering_mode(data, table, keyL, keyH, numberOfProjectedAttributes, cover_type, sideways_data, r, uncrackedLowerPart->first, uncrackedLowerPart->second, rowIdToPartition);
			}
			else {
				sum += filter3_distinguish_covering_mode(data, table, keyL, keyH, numberOfProjectedAttributes, cover_type, sideways_data, r, uncrackedLowerPart->first, uncrackedLowerPart->second, rowIdToPartition);
				sum += scan_distinguish_covering_mode(data, table, numberOfProjectedAttributes, cover_type, sideways_data, r, uncrackedLowerPart->second+1, uncrackedUpperPart->first-1, rowIdToPartition);
				sum += filter3_distinguish_covering_mode(data, table, keyL, keyH, numberOfProjectedAttributes, cover_type, sideways_data, r, uncrackedUpperPart->first, uncrackedUpperPart->second, rowIdToPartition);
			}
			#else
				offset1 = binary_search_gte(data, keyL, uncrackedLowerPart->first, uncrackedLowerPart->second);
				offset2 = binary_search_lt(data, keyH, uncrackedUpperPart->first, uncrackedUpperPart->second);
				sum += scan_distinguish_covering_mode(data, table, numberOfProjectedAttributes, cover_type, sideways_data, r, offset1, offset2, rowIdToPartition);
			#endif

			free(uncrackedLowerPart);
			free(uncrackedUpperPart);
		}
		else {
			printError("Query type not supported for stopping at threshold.");
		}
	}

	return sum;
}
