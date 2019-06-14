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
 * utils.h
 *
 * Information Systems Group
 * Saarland University
 * 2012 - 2013
 */

#ifndef UTILS_H_
#define UTILS_H_

#include <string.h>
 #include <sys/mman.h>
#include <stdlib.h>
#include <stdio.h>

#define COVERING
//#define SIDEWAYS
#define NUMBER_OF_COVERED_ATTRIBUTES 2

#ifdef SIDEWAYS
#define NUMBER_OF_INDEXES NUMBER_OF_COVERED_ATTRIBUTES
#else
#define NUMBER_OF_INDEXES 1
#endif


#define internal_int_t long long int

class IndexEntry {
public:
	internal_int_t m_key;
	internal_int_t m_rowId;

	// add one additional column for the status information
	internal_int_t m_covering[
								#ifdef SIDEWAYS
	                          		2				// for sideways, we have only the status column and one covered column
								#else
									#ifdef COVERING
	                          			1+NUMBER_OF_COVERED_ATTRIBUTES
									#else
	                          			0
									#endif
								#endif
	                         ];

	IndexEntry()
	: m_key(-1)
	, m_rowId(-1)
	{}

	IndexEntry(internal_int_t key, internal_int_t rowId)
	: m_key(key)
	, m_rowId(rowId)
	{}

	bool operator>(internal_int_t& other) const { return m_key > other; }
	bool operator>=(internal_int_t& other) const { return m_key >= other; }
	bool operator<(internal_int_t& other) const { return m_key < other; }
	bool operator<=(internal_int_t& other) const { return m_key <= other; }
	bool operator!=(internal_int_t& other) const { return m_key != other; }
	bool operator==(internal_int_t& other) const { return m_key == other; }

	bool operator>(const IndexEntry& other) const { return m_key > other.m_key; }
	bool operator>=(const IndexEntry& other) const { return m_key >= other.m_key; }
	bool operator<(const IndexEntry& other) const { return m_key < other.m_key; }
	bool operator<=(const IndexEntry& other) const { return m_key <= other.m_key; }
	bool operator!=(const IndexEntry& other) const { return m_key != other.m_key; }

};

typedef struct {
	bool sidewaysEnabled;
	internal_int_t coveredAttribute;		// stores which of the attributes should be handle by a certain part
} sideways_data_t;

#define PRINTF_INT_TYPE_OPTION "%lld"

#define INT_SIZE sizeof(internal_int_t)
#define INDEX_ENTRY_SIZE sizeof(IndexEntry)

#ifndef MAP_ANONYMOUS
# define MAP_ANONYMOUS MAP_ANON
#endif

void* malloc_huge(size_t size);

struct int_pair
{
	internal_int_t first;
	internal_int_t second;
};
typedef struct int_pair *IntPair;
#ifndef __cplusplus
typedef enum { false = 0, true = 1 } bool;
#endif

// data and query generator (datagen.c)
internal_int_t randomNumber(internal_int_t max);
void generateRandomIndexData(IndexEntry *c, internal_int_t n, internal_int_t max, bool covering);
void generateUniqueIndexData(IndexEntry *c, internal_int_t n, internal_int_t max, bool covering);

void generate_random_queries_3(internal_int_t num_queries, internal_int_t *qL, internal_int_t *qH, internal_int_t max_value, float selectivity);
void generate_random_queries_3(internal_int_t lower_bound_query, internal_int_t upper_bound_query, internal_int_t *qL, internal_int_t *qH, internal_int_t max_value, float selectivity);

void generate_sequential_queries_3(internal_int_t num_queries, internal_int_t *qL, internal_int_t *qH, internal_int_t max_value, float selectivity);
void generate_sequential_queries_3(internal_int_t lower_bound_query, internal_int_t upper_bound_query, internal_int_t *qL, internal_int_t *qH, internal_int_t max_value, float selectivity);

void generate_skewed_queries_3(internal_int_t num_queries, internal_int_t *qL, internal_int_t *qH, internal_int_t max_value, float selectivity);
void generate_skewed_queries_3(internal_int_t lower_bound_query, internal_int_t upper_bound_query, internal_int_t *qL, internal_int_t *qH, internal_int_t max_value, float selectivity);

int zipf(double alpha, int n);

#endif /* UTILS_H_ */
