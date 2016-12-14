#ifndef BTREEINDEX_H
#define BTREEINDEX_H

#include "Bruinbase.h"
#include "PageFile.h"
#include "RecordFile.h"
#include "BTreeNode.h"
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <vector>

using namespace std;

typedef struct {
  PageId pid;
  int eid;
} IndexCursor;

class BTreeIndex {
  PageFile pf;
  vector<PageId> path;
  PageId rootPid;
  int alturaArbol;
 public:
  BTreeIndex();
  RC open(const std::string& indexname, char mode);
  RC close();
  RC insertKey(int seq);
  RC insert(int key, const RecordId& rid);
  RC insertInParent(vector<PageId> &path, int siblingKey);
  RC locate(int searchKey, IndexCursor& cursor, bool isTracking = false);
  RC readForward(IndexCursor& cursor, int& key, RecordId& rid);

};
#endif
