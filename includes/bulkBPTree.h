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
 * bulkBPTree.h
 *
 * Information Systems Group
 * Saarland University
 * 2012 - 2013
 */

#ifndef BULKBPTREE_HPP_
#define BULKBPTREE_HPP_

#include <vector>
#include "utils.h"
#include "print.h"
#include "binary_search.h"

typedef internal_int_t colKey_t;
typedef internal_int_t rowId_t;

typedef enum _NodeType {
	InnerNodeType,
	LeafNodeType
} NodeType;

class InnerNode;
class LeafNode;

class BPNode {

protected:

	const NodeType m_nodeType;

	internal_int_t m_currentNumberOfEntries;
	colKey_t* m_keys;

	internal_int_t m_currentNumberOfNodes;
	BPNode** m_pointers;

	BPNode(NodeType nodeType)
	: m_nodeType(nodeType)
	, m_currentNumberOfEntries(0)
	, m_keys(NULL)
	, m_currentNumberOfNodes(0)
	, m_pointers(NULL)
	, m_fatherNode(NULL)
	{}

	~BPNode() {
		if(m_keys) {
			delete[] m_keys;
			m_keys = NULL;
		}
		if(m_pointers) {

		}
	}

public:

	static internal_int_t m_maxNumberOfEntries;

	BPNode* m_fatherNode;

	bool isFull();
	void addKey(const colKey_t& key);

	const internal_int_t& getKey(const internal_int_t position);
	void removeKey(const internal_int_t position);

	void addPointer(BPNode* const node);
	BPNode* const getPointer(const internal_int_t position);
	void removePointer(const internal_int_t position);

	const internal_int_t numberOfKeys();

	BPNode* split(BPNode*& root);
	const LeafNode* lookup(const colKey_t& key);

	const NodeType& getNodeType() const {
		return m_nodeType;
	}
};

class InnerNode : public BPNode {

public:

	InnerNode() : BPNode(InnerNodeType) {}
};

class LeafNode : public BPNode {

private:

	//std::vector<rowId_t> m_rowIds;
	LeafNode* m_previous;
	LeafNode* m_next;
	bool m_isOverflowNode;
	IndexEntry* m_currentOffset;

public:

	LeafNode(IndexEntry* currentOffset)
	: BPNode(LeafNodeType)
	, m_previous(NULL)
	, m_next(NULL)
	, m_isOverflowNode(false)
	, m_currentOffset(currentOffset)
	{}

	LeafNode(IndexEntry* currentOffset, bool overflowNode)
	: BPNode(LeafNodeType)
	, m_previous(NULL)
	, m_next(NULL)
	, m_isOverflowNode(overflowNode)
	, m_currentOffset(currentOffset)
	{}

	void addRowId(const rowId_t& rowId);
	void addKey(const colKey_t& key);
	rowId_t getRowId(const colKey_t& key) const;
	rowId_t getGTE(const colKey_t& key) const;
	rowId_t getLT(const colKey_t& key) const;
	void setNext(LeafNode* next);
	void setPrevious(LeafNode* previous);
	LeafNode* getPrevious();
	LeafNode* getNext();
	bool isOverflowNode() { return m_isOverflowNode; }
	void setAsOverflowNode() { m_isOverflowNode = true; }
	void printNode(IndexEntry* data, internal_int_t& currentId);
};



class BulkBPTree {

private:

	BPNode* m_root;
	LeafNode* m_currentLeaf;

public:

	typedef std::pair<colKey_t, rowId_t> keyValuePair_t;

public:

	BulkBPTree(IndexEntry* data, internal_int_t size);

	rowId_t lookup(const colKey_t& key);
	rowId_t gte(const colKey_t& key);
	rowId_t lt(const colKey_t& key);

	void printIndex(IndexEntry* c, internal_int_t n);
	void printLeaves(IndexEntry* c);
};

void *build_bptree_bulk(IndexEntry *c, internal_int_t n);


#endif /* BULKBPTREE_HPP_ */
