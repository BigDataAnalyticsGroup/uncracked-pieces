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
 * datagen.cpp
 *
 * Information Systems Group
 * Saarland University
 * 2012 - 2013
 */

#include <stdlib.h>
#include <time.h>
#include "../includes/cracking.h"

internal_int_t randomNumber(internal_int_t max){
	return 1 + (internal_int_t)( max * (double)rand() / ( RAND_MAX + 1.0 ) );
}

internal_int_t getSeed(internal_int_t fixedSeed) {
#ifdef FIXED_SEED
	return fixedSeed;
#else
	return (unsigned int) time(NULL);
#endif
}

void setSeed(internal_int_t fixedSeed) {
	srand (getSeed(fixedSeed));
}

// 1. random data
void generateRandomIndexData(IndexEntry *c, internal_int_t n, internal_int_t max, bool covering){
	internal_int_t i;
	setSeed(DATA_SEED);
	for(i=0;i<n;i++) {
		c[i].m_key = randomNumber(max);
		c[i].m_rowId = i;
		if(covering) {
			c[i].m_covering[0] = 0;
		}
		debug(INFO, "Key: %lld", c[i].m_key);
	}
}

void generateUniqueIndexData(IndexEntry *c, internal_int_t n, internal_int_t max, bool covering){
	internal_int_t *data = (internal_int_t*)malloc(n*sizeof(internal_int_t));
	internal_int_t i;
	for(i=0;i<n;i++)
		data[i] = i+1;
	setSeed(DATA_SEED);
	std::random_shuffle(data,data+n);

	for(i=0;i<n;i++) {
		c[i].m_key = data[i];
		c[i].m_rowId = i;
		if(covering) {
			c[i].m_covering[0] = 0;
		}
		debug(INFO, "Key: %lld", c[i].m_key);
	}

	free(data);
}



void populate_low_and_hi(internal_int_t *lKey, float *deltaKey, internal_int_t max, float selectivity){
	setSeed(QUERY_SEED);

    if(selectivity > 100 || selectivity < 0) // Wrong number, pick the entire data set.
        selectivity = 1;
    if(selectivity > 0)  // Treat as percentage from 1% to 100%
        selectivity = selectivity / 100;
    
    // Here selectivity is (0 .. 1)
    *lKey = (internal_int_t)((1 - selectivity) * max);
    *deltaKey = (internal_int_t)(selectivity * max);
}

void generate_random_queries_3(internal_int_t num_queries, internal_int_t *qL, internal_int_t *qH, internal_int_t max_value, float selectivity) {
	generate_random_queries_3(0, num_queries, qL, qH, max_value, selectivity);
}

void generate_random_queries_3(internal_int_t lower_bound_query, internal_int_t upper_bound_query, internal_int_t *qL, internal_int_t *qH, internal_int_t max_value, float selectivity) {

    internal_int_t lKey = 0;
    float deltaK = 0.0;

    populate_low_and_hi(&lKey, &deltaK, max_value, selectivity);

    internal_int_t i = lower_bound_query;
	for(; i < upper_bound_query; ++i){
		qL[i] = randomNumber(lKey);
		qH[i] = qL[i] + deltaK;
		debug(INFO, "(%lld,%lld)", qL[i], qH[i]);
	}
}


void generate_sequential_queries_3(internal_int_t num_queries, internal_int_t *qL, internal_int_t *qH, internal_int_t max_value, float selectivity) {
	generate_sequential_queries_3(0, num_queries, qL, qH, max_value, selectivity);
}

void generate_sequential_queries_3(internal_int_t lower_bound_query, internal_int_t upper_bound_query, internal_int_t *qL, internal_int_t *qH, internal_int_t max_value, float selectivity) {

    internal_int_t lKey = 0;
    float deltaK = 0.0;

    populate_low_and_hi(&lKey, &deltaK, max_value, selectivity);
    lKey = 0; // reset lKey which is also affected by the populate function

    internal_int_t i = lower_bound_query;
	for(; i < upper_bound_query; ++i){
		// check if query is still in the domain
		if(lKey + deltaK > max_value) {
			// out of bounds
			lKey = randomNumber(DATA_SIZE / 10000); // variation of 0.01% to avoid using exactly the same queries in the next round
		}
		qL[i] = lKey;
		qH[i] = qL[i] + deltaK;

		// increment for next round, but use an overlapping of a half
		lKey += deltaK / 2;
		debug(INFO, "(%lld,%lld)", qL[i], qH[i]);
	}
}


inline int zipf(double alpha, int n)
{
  static int first = true;      // Static first time flag
  static double c = 0;          // Normalization constant
  double z;                     // Uniform random number (0 < z < 1)
  double sum_prob;              // Sum of probabilities
  double zipf_value = 0.0;      // Computed exponential value to be returned
  int    i;                     // Loop counter

  // Compute normalization constant on first call only
  if (first == true)
  {
    for (i=1; i<=n; i++)
      c = c + (1.0 / pow((double) i, alpha));
    c = 1.0 / c;
    first = false;
  }

  // Pull a uniform random number (0 < z < 1)
  do
  {
    z = static_cast<float>(rand()) / static_cast<float>(RAND_MAX);
  }
  while ((z == 0) || (z == 1));

  // Map z to the value
  sum_prob = 0;
  for (i=1; i<=n; i++)
  {
    sum_prob = sum_prob + c / pow((double) i, alpha);
    if (sum_prob >= z)
    {
      zipf_value = i;
      break;
    }
  }

  // Assert that zipf_value is between 1 and N
  assert((zipf_value >=1) && (zipf_value <= n));

  return zipf_value;
}

void generate_skewed_queries_3(internal_int_t num_queries, internal_int_t *qL, internal_int_t *qH, internal_int_t max_value, float selectivity) {
	generate_skewed_queries_3(0, num_queries, qL, qH, max_value, selectivity);
}

void generate_skewed_queries_3(internal_int_t lower_bound_query, internal_int_t upper_bound_query, internal_int_t *qL, internal_int_t *qH, internal_int_t max_value, float selectivity) {
    internal_int_t lKey = 0;
    float deltaK = 0.0;

    populate_low_and_hi(&lKey, &deltaK, max_value, selectivity);

    // the focus should be in the center of the dataset
    internal_int_t hotspot = DATA_SIZE / DUP_FACTOR / 2;

	// compute zipf distribution
	typedef std::map<internal_int_t, internal_int_t> result_t;
	typedef result_t::iterator result_iterator_t;

	result_t result;
	for(size_t i = 0; i < NUM_QUERIES; ++i) {
		internal_int_t nextValue = (internal_int_t) zipf(ZIPF_ALPHA, upper_bound_query - lower_bound_query);
		result_iterator_t it = result.find(nextValue);
		if(it != result.end()) {
			++it->second;
		}
		else {
			result.insert(std::make_pair(nextValue, 1));
		}
	}

    for(result_iterator_t it = result.begin(); it != result.end(); ++it) {
    	debug(INFO, "First: %lld", it->first);
    	debug(INFO, "Second: %lld", it->second);
    }

    internal_int_t zoneSize = hotspot / result.size();

    typedef std::vector<std::pair<internal_int_t, internal_int_t> > queries_t;
    queries_t queries;

    internal_int_t zone = 0;
    for(result_iterator_t it = result.begin(); it != result.end(); ++it) {
    	for(internal_int_t i = 0; i < it->second; ++i) {
        	internal_int_t direction = rand() % 2 == 0 ? 1 : -1;

        	internal_int_t zoneBegin = hotspot + (zone * zoneSize * direction);
        	internal_int_t zoneEnd = zoneBegin + (zoneSize * direction);

        	if(direction == -1) {
        		internal_int_t tmp = zoneBegin;
        		zoneBegin = zoneEnd;
        		zoneEnd = tmp;
        	}

        	internal_int_t predicate = rand() % (zoneEnd - zoneBegin + 1) + zoneBegin;

        	queries.push_back(std::make_pair(predicate, predicate + deltaK));
    	}
    	++zone;
    }

    std::random_shuffle(queries.begin(), queries.end());

    for(unsigned internal_int_t i = 0; i < queries.size(); ++i) {
    	qL[i] = queries[i].first;
    	qH[i] = queries[i].second;
    	debug(INFO, "(%lld,%lld)", qL[i], qH[i]);
    }
}
