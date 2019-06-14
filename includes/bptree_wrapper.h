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
 * bptree_wrapper.h
 *
 * Information Systems Group
 * Saarland University
 * 2012 - 2013
 */

#ifndef BPTREE_WRAPPER_HPP
#define	BPTREE_WRAPPER_HPP
//
//#include <stx/btree_multimap.h>
//#include <stx/btree_multiset.h>
//
#include <map>
#include "utils.h"
#include "experiment.h"
//
///** Generates specific traits for a B+ tree used as a map. */
//template <typename _Key, typename _Data>
//struct bptree_configuration_traits
//{
//    /// If true, the tree will self verify it's invariants after each insert()
//    /// or erase(). The header must have been compiled with BTREE_DEBUG defined.
//    static const bool   selfverify = false;
//
//    /// If true, the tree will print out debug information and a tree dump
//    /// during insert() or erase() operation. The header must have been
//    /// compiled with BTREE_DEBUG defined and key_type must be std::ostream
//    /// printable.
//    static const bool   debug = false;
//
//    /// Number of slots in each leaf of the tree.
//    static const int    leafslots = BTREE_MAX( 8, BPTREE_ELEMENTSPERNODE / (sizeof(_Key) + sizeof(_Data)) );
//
//    /// Number of slots in each inner node of the tree.
//    static const int    innerslots = BTREE_MAX( 8, BPTREE_ELEMENTSPERNODE / (sizeof(_Key) + sizeof(void*)) );
//};
//
//typedef stx::btree_multimap<internal_int_t, internal_int_t, std::less<internal_int_t>, bptree_configuration_traits<internal_int_t, internal_int_t> > bptree_t;
typedef std::map<internal_int_t, internal_int_t> bptree_t;
typedef bptree_t* BPTree;
typedef bptree_t::iterator BPTreeIterator;
//
//template <typename _Key>
//struct btree_set_configuration_traits
//{
//    /// If true, the tree will self verify it's invariants after each insert()
//    /// or erase(). The header must have been compiled with BTREE_DEBUG defined.
//    static const bool   selfverify = false;
//
//    /// If true, the tree will print out debug information and a tree dump
//    /// during insert() or erase() operation. The header must have been
//    /// compiled with BTREE_DEBUG defined and key_type must be std::ostream
//    /// printable.
//    static const bool   debug = false;
//
//    /// Number of slots in each leaf of the tree. Estimated so that each node
//    /// has a size of about 256 bytes.
//    static const int    leafslots = BTREE_MAX( 8, BPTREE_ELEMENTSPERNODE / (sizeof(_Key)) );
//
//    /// Number of slots in each inner node of the tree. Estimated so that each node
//    /// has a size of about 256 bytes.
//    static const int    innerslots = BTREE_MAX( 8, BPTREE_ELEMENTSPERNODE / (sizeof(_Key) + sizeof(void*)) );
//};
//
//typedef stx::btree_multiset<internal_int_t, std::less<internal_int_t>, btree_set_configuration_traits<internal_int_t> > bpset_t;
//
IntPair bptreeSearchLT(BPTree t, internal_int_t key);
IntPair bptreeSearchGTE(BPTree t, internal_int_t key);
//
#endif	/* BPTREE_WRAPPER_HPP */
