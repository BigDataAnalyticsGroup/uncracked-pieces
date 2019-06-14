/*
 * Copyright Mark Allen Weiss
 * Source Code from "Data Structures and Algorithm Analysis in C (Second Edition)"
 * http://users.cis.fiu.edu/~weiss/dsaa_c2e/
 *
 * Code extended/modified by Information Systems Group
 */

#include "../includes/avltree.h"
#include <stdlib.h>
#include "../includes/fatal.h"
#include "../includes/utils.h"

struct AvlNode
{
	ElementType Element;
	internal_int_t offset;

	AvlTree  Left;
	AvlTree  Right;
	internal_int_t      Height;
};

AvlTree
MakeEmpty( AvlTree T )
{
	if( T != NULL )
	{
		MakeEmpty( T->Left );
		MakeEmpty( T->Right );
		free( T );
	}
	return NULL;
}

internal_int_t
FindLT( ElementType X, AvlTree T )
{
	if( T == NULL )
		return -1;
	if( X < T->Element )
		return FindLT( X, T->Left );
	else
		if( X > T->Element )
			return FindLT( X, T->Right );
		else
			return T->offset;
}

internal_int_t
FindLTE( ElementType X, AvlTree T, ElementType limit )
{
	if(T) {
		if( X < T->Element )
			if(!T->Left) {
				return 0;
			}
			else {
				return FindLTE( X, T->Left, limit );
			}
		else
			if( X > T->Element) {
				if(!T->Right) {
					return T->offset;
				}
				else {
					return FindLTE( X, T->Right, limit );
				}
			}
			else
				return T->offset;
	}

	return limit;
}

IntPair createOffsetPair(Position first, Position second, ElementType limit){
	IntPair op = (IntPair) malloc(sizeof(struct int_pair));
	if (first && second){
		op->first = first->offset+1;
		op->second = second->offset;
	}
	else if (first){
		op->first = first->offset+1;
		op->second = limit;
	}
	else if (second){
		op->first = 0;
		op->second = second->offset;
	}
	else{
		op->first = 0;
		op->second = limit;
	}
	return op;
}

IntPair
FindNeighborsLT( ElementType X, AvlTree T, ElementType limit )
{
	Position first = 0, second = 0;
	//if( T == NULL )
	//	return NULL;

	while (T != NULL) {
		if( X < T->Element ){
			second = T;
			T = T->Left;
		}
		else if( X > T->Element ){
			first = T;
			T = T->Right;
		}
		else {
			second = T;
			if(T->Left != NULL) {
				first = FindMax(T->Left);
			}
			break;
		}
	}

	return createOffsetPair(first, second, limit);
}

IntPair
FindNeighborsLT_Limit( ElementType X, AvlTree T, ElementType limit, IntPair* limits)
{
	Position first = 0, second = 0;
	//if( T == NULL )
	//	return NULL;
	
	bool initFirstBorder = false;
	internal_int_t firstBorderElement = 0;
	bool initSecondBorder = false;
	internal_int_t secondBorderElement = 0;

	while (T != NULL) {
		if( X < T->Element ){
			second = T;
			secondBorderElement = T->Element;
			initSecondBorder = true;
			T = T->Left;
		}
		else if( X > T->Element ){
			first = T;
			firstBorderElement = T->Element;
			initFirstBorder = true;
			T = T->Right;
//			if(T == NULL) {
//				secondBorderElement = limit / DUP_FACTOR;
//				initSecondBorder = true;
//			}
		}
		else {
			second = T;
			secondBorderElement = T->Element;
			initSecondBorder = true;
			if(T->Left != NULL) {
				first = FindMax(T->Left);
				firstBorderElement =  first->Element;
				initFirstBorder = true;
			}
			break;
		}
	}
	
	if(!initFirstBorder) {
		firstBorderElement = 0;
	}
	if(!initSecondBorder) {
		secondBorderElement = limit / DUP_FACTOR;
	}

	if(limits) {
		(*limits)->first = firstBorderElement;
		(*limits)->second = secondBorderElement;
	}
//	printf("firstBorderElement = %lld\n", firstBorderElement);
//	printf("secondBorderElement = %lld\n", secondBorderElement);

	return createOffsetPair(first, second, limit);
}

IntPair
FindNeighborsGTE( ElementType X, AvlTree T, ElementType limit )
{
	Position first = 0, second = 0;
	//if( T == NULL )
	//	return NULL;

	while (T != NULL) {
		if( X < T->Element ){
			second = T;
			T = T->Left;
		}
		else if( X > T->Element ){
			first = T;
			T = T->Right;
		}
		else {
			first = T;		// this is the only difference from FindNeighborsLT !
			break;
		}
	}

	return createOffsetPair(first, second, limit);
}

Position
FindMin( AvlTree T )
{
	if( T == NULL )
		return NULL;
	else
		if( T->Left == NULL )
			return T;
		else
			return FindMin( T->Left );
}

Position
FindMax( AvlTree T )
{
	if( T != NULL )
		while( T->Right != NULL )
			T = T->Right;
	
	return T;
}

/* START: fig4_36.txt */
static internal_int_t
Height( Position P )
{
	if( P == NULL )
		return -1;
	else
		return P->Height;
}
/* END */

static ElementType
Max( ElementType Lhs, ElementType Rhs )
{
	return Lhs > Rhs ? Lhs : Rhs;
}

/* START: fig4_39.txt */
/* This function can be called only if K2 has a left child */
/* Perform a rotate between a node (K2) and its left child */
/* Update heights, then return new root */

static Position
SingleRotateWithLeft( Position K2 )
{
	Position K1;
	
	K1 = K2->Left;
	K2->Left = K1->Right;
	K1->Right = K2;
	
	K2->Height = Max( Height( K2->Left ), Height( K2->Right ) ) + 1;
	K1->Height = Max( Height( K1->Left ), K2->Height ) + 1;
	
	return K1;  /* New root */
}
/* END */

/* This function can be called only if K1 has a right child */
/* Perform a rotate between a node (K1) and its right child */
/* Update heights, then return new root */

static Position
SingleRotateWithRight( Position K1 )
{
	Position K2;
	
	K2 = K1->Right;
	K1->Right = K2->Left;
	K2->Left = K1;
	
	K1->Height = Max( Height( K1->Left ), Height( K1->Right ) ) + 1;
	K2->Height = Max( Height( K2->Right ), K1->Height ) + 1;
	
	return K2;  /* New root */
}

/* START: fig4_41.txt */
/* This function can be called only if K3 has a left */
/* child and K3's left child has a right child */
/* Do the left-right double rotation */
/* Update heights, then return new root */

static Position
DoubleRotateWithLeft( Position K3 )
{
	/* Rotate between K1 and K2 */
	K3->Left = SingleRotateWithRight( K3->Left );
	
	/* Rotate between K3 and K2 */
	return SingleRotateWithLeft( K3 );
}
/* END */

/* This function can be called only if K1 has a right */
/* child and K1's right child has a left child */
/* Do the right-left double rotation */
/* Update heights, then return new root */

static Position
DoubleRotateWithRight( Position K1 )
{
	/* Rotate between K3 and K2 */
	K1->Right = SingleRotateWithLeft( K1->Right );
	
	/* Rotate between K1 and K2 */
	return SingleRotateWithRight( K1 );
}


/* START: fig4_37.txt */
AvlTree
Insert( internal_int_t offset, ElementType X, AvlTree T )
{
	if( T == NULL )
	{
		/* Create and return a one-node tree */
		T = (AvlTree) malloc( sizeof( struct AvlNode ) );
		if( T == NULL )
			FatalError( "Out of space!!!" );
		else
		{
			T->Element = X; T->Height = 0;
			T->Left = T->Right = NULL;
			T->offset = offset;
		}
	}
	else
		if( X < T->Element )
		{
			T->Left = Insert( offset, X, T->Left );
			if( Height( T->Left ) - Height( T->Right ) == 2 ){
				if( X < T->Left->Element )
					T = SingleRotateWithLeft( T );
				else
					T = DoubleRotateWithLeft( T );
			}
		}
		else
            if( X > T->Element )
            {
                T->Right = Insert( offset, X, T->Right );
                if( Height( T->Right ) - Height( T->Left ) == 2 ){
                    if( X > T->Right->Element )
                        T = SingleRotateWithRight( T );
                    else
                        T = DoubleRotateWithRight( T );
                }
            }
	/* Else X is in the tree already; we'll do nothing */
	
	T->Height = Max( Height( T->Left ), Height( T->Right ) ) + 1;
	return T;
}
/* END */

AvlTree
Delete( ElementType X, AvlTree T )
{
	printf( "Sorry; Delete is unimplemented; %lld remains\n", (long long int) X );
	return T;
}

ElementType
Retrieve( Position P )
{
	return P->Element;
}

void Print( AvlTree T ){

	if(T==NULL)
		return;

	printf("(%lld,%lld) ",(long long int) T->Element, (long long int) T->offset);
	Print(T->Right);
	Print(T->Left);
	printf("\n");
}
