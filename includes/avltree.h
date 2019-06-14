/*
 * Copyright Mark Allen Weiss
 * Source Code from "Data Structures and Algorithm Analysis in C (Second Edition)"
 * http://users.cis.fiu.edu/~weiss/dsaa_c2e/
 *
 * Code extended/modified by Information Systems Group
 */

#ifndef _AvlTree_H
#define _AvlTree_H

#include "experiment.h"
#include "utils.h"

typedef internal_int_t ElementType;
struct AvlNode;

typedef struct AvlNode *Position;
typedef struct AvlNode *AvlTree;

//struct OffsetPair {
//	internal_int_t first;
//	internal_int_t second;
//};
//typedef struct OffsetPair *Pair;

AvlTree MakeEmpty( AvlTree T );
//Position Find( ElementType X, AvlTree T );
internal_int_t FindLT( ElementType X, AvlTree T );
internal_int_t FindLTE( ElementType X, AvlTree T, ElementType limit );
Position FindMin( AvlTree T );
Position FindMax( AvlTree T );
AvlTree Insert( internal_int_t offset, ElementType X, AvlTree T );
AvlTree Delete( ElementType X, AvlTree T );
ElementType Retrieve( Position P );


IntPair FindNeighborsLT( ElementType X, AvlTree T, ElementType limit );
IntPair FindNeighborsLT_Limit( ElementType X, AvlTree T, ElementType limit, IntPair* limits );
IntPair FindNeighborsGTE( ElementType X, AvlTree T, ElementType limit );
void Print( AvlTree T );

#endif  /* _AvlTree_H */
/* END */
