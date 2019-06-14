/*
  Adaptive Radix Tree
  Viktor Leis, 2012
  leis@in.tum.de
 */

#ifndef ARTTREE_H_
#define ARTTREE_H_

#include <stdint.h>

// The maximum prefix length for compressed paths stored in the
// header, if the path is longer it is loaded from the database on
// demand
static const unsigned maxPrefixLength=9;

// Shared header of all inner nodes
struct Node {
   // length of the compressed path (prefix)
   uint32_t prefixLength;
   // number of non-null children
   uint16_t count;
   // node type
   int8_t type;
   // compressed path (prefix)
   uint8_t prefix[maxPrefixLength];

   Node(int8_t type) : prefixLength(0),count(0),type(type) {}
};

typedef struct Node *ARTTree;

void loadKey(uintptr_t tid,uint8_t key[]);
void insert(Node* node,Node** nodeRef,uint8_t key[],unsigned depth,uintptr_t value,unsigned maxKeyLength);
Node* lookup(Node* node,uint8_t key[],unsigned keyLength,unsigned depth,unsigned maxKeyLength);
Node* lookupPessimistic(Node* node,uint8_t key[],unsigned keyLength,unsigned depth,unsigned maxKeyLength);
inline uintptr_t getLeafValue(Node* node) {
	return reinterpret_cast<uintptr_t>(node)>>1;
}

#endif /* ARTTREE_H_ */
