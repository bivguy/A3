
#ifndef SORT_C
#define SORT_C

#include "MyDB_PageReaderWriter.h"
#include "MyDB_TableRecIterator.h"
#include "MyDB_TableRecIteratorAlt.h"
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
	
	anonPages.push_back(MyDB_PageReaderWriter(parent->getPage()));

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
		anonPages.push_back(MyDB_PageReaderWriter(parent->getPage()));
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
	
void sort (int, MyDB_TableReaderWriter &, MyDB_TableReaderWriter &, function <bool ()>, MyDB_RecordPtr, MyDB_RecordPtr) {} 

#endif
