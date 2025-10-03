
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

vector <MyDB_PageReaderWriter> mergeIntoList (
	MyDB_BufferManagerPtr parent, 
	MyDB_RecordIteratorAltPtr leftIter, 
	MyDB_RecordIteratorAltPtr rightIter, 
	function <bool ()> comparator, 
	MyDB_RecordPtr lhs, 
	MyDB_RecordPtr rhs
) {
	// Create anonymous pages to be used
	vector<
	parent->getPage();



	return vector <MyDB_PageReaderWriter> (); 
} 
	
void sort (int, MyDB_TableReaderWriter &, MyDB_TableReaderWriter &, function <bool ()>, MyDB_RecordPtr, MyDB_RecordPtr) {} 

#endif
