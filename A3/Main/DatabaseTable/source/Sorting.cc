
#ifndef SORT_C
#define SORT_C

#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableRecIterator.h"
#include "MyDB_TableRecIteratorAlt.h"
#include "MyDB_PageListIteratorAlt.h"
#include "MyDB_TableReaderWriter.h"
#include "Sorting.h"

#include <queue>

using namespace std;

void mergeIntoFile (MyDB_TableReaderWriter &table_rw, vector <MyDB_RecordIteratorAltPtr> &iters, function <bool ()> comparator, MyDB_RecordPtr record1, MyDB_RecordPtr record2) {
	auto iter_comparator = [comparator, record1, record2](MyDB_RecordIteratorAltPtr iter1, MyDB_RecordIteratorAltPtr iter2) {
		iter1->getCurrent(record1);
		iter2->getCurrent(record2);

		// comparator return true if record1 < record2, but the STL
		// priority queue defaults to a max heap and we want a min heap.
		return !comparator();
	};

	priority_queue<MyDB_RecordIteratorAltPtr, vector<MyDB_RecordIteratorAltPtr>, function< bool (MyDB_RecordIteratorAltPtr, MyDB_RecordIteratorAltPtr)>> pq(iters.begin(), iters.end());

	while (pq.size()) {
		MyDB_RecordIteratorAltPtr iter = pq.top();
		pq.pop();
		
		MyDB_RecordPtr record = table_rw.getEmptyRecord();
		iter->getCurrent(record);
		table_rw.append(record);

		// Push iterator back if it has more.
		if (iter->advance())
			pq.push(iter);
	}
}

// helper function.  Gets two iterators, leftIter and rightIter.  It is assumed that these are iterators over
// sorted lists of records.  This function then merges all of those records into a list of anonymous pages,
// and returns the list of anonymous pages to the caller.  The resulting list of anonymous pages is sorted.
// Comparisons are performed using comparator, lhs, rhs
vector <MyDB_PageReaderWriter> mergeIntoList (
	MyDB_BufferManagerPtr parent, 
	MyDB_RecordIteratorAltPtr leftIter, 
	MyDB_RecordIteratorAltPtr rightIter, 
	function <bool ()> comparator, 
	MyDB_RecordPtr lhs, 
	MyDB_RecordPtr rhs
) {
	// define the comparator

	// Create anonymous pages to be used
	vector<MyDB_PageReaderWriter> anonPages;
	
	anonPages.push_back(MyDB_PageReaderWriter(*parent));

	leftIter->getCurrent(lhs);
	rightIter->getCurrent(rhs);

	bool leftAdvance;
	while (true) {
		// compare the left and right iterators
		if (comparator) {
			addRecord(lhs, anonPages, parent);
			leftAdvance = true;

		} else {
			addRecord(rhs, anonPages, parent);
			leftAdvance = false;
		}

		// advance to the next record if valid
		if (leftAdvance) {
			if (!advanceRecord(leftIter, lhs)) {
				break;
			}
		} else {
			if (!advanceRecord(rightIter, rhs)) {
					break;
			}
		}
	}

	// add the remainder of the records from the leftover iterator

	// means that there right iterator still has records to be added
	if (leftAdvance) {
		addRecord(rhs, anonPages, parent);
		while (advanceRecord(rightIter, rhs)) {
			addRecord(rhs, anonPages, parent);
		}
	} else { // means that there left iterator still has records to be added
		addRecord(lhs, anonPages, parent);
		while (advanceRecord(leftIter, lhs)) {
			addRecord(lhs, anonPages, parent);
		}
	}

	return anonPages; 
} 

void addRecord(MyDB_RecordPtr record, vector<MyDB_PageReaderWriter> &anonPages, MyDB_BufferManagerPtr parent) {
	// check if we can append it
	if (!anonPages.back().append(record)) {
		anonPages.push_back(MyDB_PageReaderWriter(*parent));
		anonPages.back().append(record);
	}
}

bool advanceRecord(MyDB_RecordIteratorAltPtr iterator, MyDB_RecordPtr record) {
	// check if can advance this next record
	if (iterator->advance()) {
		iterator->getCurrent(record);
		return true;
	} 

	return false;
}
	
// performs a TPMMS of the table sortMe.  The results are written to sortIntoMe.  The run 
// size for the first phase of the TPMMS is given by runSize.  Comarisons are performed 
// using comparator, lhs, rhs
void sort (
	int runSize,
	MyDB_TableReaderWriter &sortMe, 
	MyDB_TableReaderWriter &sortIntoMe, 
	function <bool ()> comparator, 
	MyDB_RecordPtr lhs, 
	MyDB_RecordPtr rhs) {
		MyDB_BufferManagerPtr bufferMgr = sortIntoMe.getBufferMgr();
		
		int numberRuns = sortMe.getNumPages() / runSize;
		vector<vector<MyDB_PageReaderWriter>> sortedRuns;

		// go through each run of pages
		for (int r = 0; r < numberRuns; r++) {
			vector<vector<MyDB_PageReaderWriter>> sortedPages;
			// load a run of pages into RAM, sorting each page 
			for (int i = r*runSize; i < runSize; i++)
			{
				MyDB_PageReaderWriter pageRW = sortMe[i];			

				MyDB_PageReaderWriterPtr sortedPage = pageRW.sort(comparator, lhs, rhs);
				
				// add this to the list of vectors
				vector<MyDB_PageReaderWriter> sortedPageVector;
				sortedPageVector.push_back(*sortedPage);
				sortedPages.push_back(sortedPageVector);
			}
			vector<vector<MyDB_PageReaderWriter>> sortedRun;
			// after sorting the pages in the run, sort the entire run by merging the adjacent pairs of lists
			while (sortedRun.size() != 1) {
				sortedRun.clear();
				for (int i = 0; i < 10; i+=2) {
					// create the page list iterators
					MyDB_RecordIteratorAltPtr leftIter = getIteratorAlt(sortedPages[i]);
					MyDB_RecordIteratorAltPtr rightIter = getIteratorAlt(sortedPages[i+1]);

					// merge the two lists into one sorted list
					vector<MyDB_PageReaderWriter> mergedSortedList = mergeIntoList(bufferMgr, leftIter, rightIter, comparator, lhs, rhs);
					sortedRun.push_back(mergedSortedList);
				}
				// change the sorted pages list to the ones we just added
				sortedPages = sortedRun;
			}
			// at this point, the only thing in sortedRun is one list
			sortedRuns.push_back(sortedRun[0]);
		}

		// create a list of iterators for each list
		vector <MyDB_RecordIteratorAltPtr> mergeUs;
		for (int i = 0; i < sortedRuns.size(); i ++) {
			mergeUs.push_back(getIteratorAlt(sortedRuns[i]));
		}

		mergeIntoFile(sortIntoMe, mergeUs, comparator, lhs, rhs);
} 

#endif
