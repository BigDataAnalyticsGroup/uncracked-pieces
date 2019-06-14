///////////////////////////////////////////////////////////
//							 //
//        The Uncracked Pieces in Database Cracking      // 
// Felix Martin Schuhknecht, Alekh Jindal, Jens Dittrich //
//   Proceedings of the VLDB Endowment, Vol. 7, No. 2    // 
//							 //
//			Used Code Base			 // 
//		   Version December 18, 2013 		 //
//		   Information Systems Group		 //
//		      Saarland University		 //
///////////////////////////////////////////////////////////


///////////////////////////////////////////////////////////
//			Compilation			 //
///////////////////////////////////////////////////////////

The code base consists of the two folders “includes” and “src”. The folder “includes” contains all header files while “src” contains all source files of the project. 

To build the project with g++, the following command can be used (tested with g++ 4.7 and 4.8), which creates an executable of the name “Cracking”:

g++ -O3 -Wall src/ART.cpp src/avltree.cpp src/binary_search.cpp src/bptree_wrapper.cpp src/bulkBPTree.cpp src/crack_inthree.cpp src/crack_intwo.cpp src/cracking_main.cpp src/datagen.cpp src/exp_util.cpp src/experiment.cpp src/fullindex.cpp src/print.cpp src/query.cpp src/signals.cpp src/table.cpp src/timer.cpp -o Cracking && echo $?

On linux machines, add -D__STDC_LIMIT_MACROS and -lrt to the command, i.e.
g++ -O3 -Wall -D__STDC_LIMIT_MACROS -lrt src/ART.cpp src/avltree.cpp src/binary_search.cpp src/bptree_wrapper.cpp src/bulkBPTree.cpp src/crack_inthree.cpp src/crack_intwo.cpp src/cracking_main.cpp src/datagen.cpp src/exp_util.cpp src/experiment.cpp src/fullindex.cpp src/print.cpp src/query.cpp src/signals.cpp src/table.cpp src/timer.cpp -o Cracking && echo $?

///////////////////////////////////////////////////////////
//	             Experimental Setup			 //
///////////////////////////////////////////////////////////

The setup of an experiment is done at three places:

1. includes/experiment.h:
In this file several macros must be set to define input and behavior of the tested methods. E.g. the data size, selectivity, and number of applied repetitions can be set there. There are two different configurations available, one for DEBUG and one for RELEASE mode. If the RELEASE configuration is chosen, no debug information is printed.  

2. src/cracking_main.cpp:
This file contains the main function and is the starting point of the experimental execution. In this file, the methods to test are chosen. We distinct three method categories here: cracking methods, indexing methods, and sorting methods. 
For example, if we set
	crk_types[] = {EXP_CRACK_STD, EXP_CRACK_BUFFERED};
	num_crk = 2;
, then the program would execute standard cracking and buffered swapping and output the results.
The index and sorting methods are always combined with each other.
For example, if we set
	idx_types[] = {EXP_SORT_BIN, EXP_SORT_AVL};
	num_idx = 2;

	srt_types[] = {QUICK, RADIX};
	num_srt = 2;
, then the program would execute four tests: Sorting with Quicksort and querying with binary search, sorting with Quicksort and querying using an AVL tree, sorting with radix and querying with binary search, and sorting with radix and querying using an AVL tree.
Further, the number of projected attributes can be set here as well (1 by default, e.g. only index access).
If more than one attribute should be projected, a base table access will be performed, if covering_types[] = {NO_COVER}. In combination with standard cracking, also COVER_DIRECT is allowed. covering_types[] = NEARBY_CLUSTERING_EXACT is allowed only in combination with crk_types[] = {EXP_CRACK_PARTITIONED_STANDARD}. 
If covering is used, please also check the settings in utils.h, see next point.
Sideways cracking is an exception from the rule and is issued by calling run_cracking3_sideways() instead of run_cracking3(). Set COVER_DIRECT as well in combination with sideways cracking.  

3. includes/utils.h:
If Covering is chosen in cracking_main.cpp, make sure that #define COVERING is set in this file. If sideways cracking is executed, #define SIDEWAYS must be set.


///////////////////////////////////////////////////////////
//	             Experimental Run			 //
///////////////////////////////////////////////////////////

After setup and compilation, the program can be executed. This triggers the execution of all specified methods one after another. If multiple repetitions are set, these repetitions are executed one after another and the average runtime is calculated. If the program is executed in RELEASE mode, all output in form of runtimes will be stored in the file output.txt next to the binary.


///////////////////////////////////////////////////////////
//	             Experimental Output		 //
///////////////////////////////////////////////////////////

One line of the output.txt corresponds to a single query. At the end of the file, the total time over all queries is printed. A line contains the following information:

Name | QueryTime | IndexLookupTime | DataAccessTime | PostFilteringTime | IndexingTime | IndexLookupTime | DataOrganizationTime | IndexUpdateTime | IndexPartitioningTime | SwapCount | NumberOfProjectedAttributes | CoveringType | NumberOfCoveredAttributes

Depending on the tested methods, some times might remain 0. Overall, QueryTime + IndexingTime gives the total execution time of the query. The times IndexLookupTime, DataAccessTime, and PostFilteringTime are sub measurements in QueryTime, while IndexLookupTime, DataOrganizationTime, IndexUpdateTime, and IndexPartitioningTime are sub measurements in IndexingTime. 













