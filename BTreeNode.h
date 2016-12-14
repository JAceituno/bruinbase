/*
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 5/28/2008
 *
 * @author Zengwen Yuan
 * @author Chris Buonocore
 * @date 11/15/2015
 */

#ifndef BTREENODE_H
#define BTREENODE_H

#include "RecordFile.h"
#include "PageFile.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define P_SIZE 1024

#define RID_SIZE 8
#define PID_SIZE 4
#define K_SIZE 4

#define L_OFFSET 12
#define NL_OFFSET 8

#define N_PTR 86
#define N_KEY 85

#define NONE -1
#define EC -100


class BTLeafNode {
  public:
    BTLeafNode();
    RC insert(int key, const RecordId& rid);
    RC insertAndSplit(int key, const RecordId& rid, BTLeafNode& sibling, int& siblingKey);
    RC locate(int searchKey, int& eid);
    RC readEntry(int eid, int& key, RecordId& rid);
    PageId getNextNodePtr();
    RC setNextNodePtr(PageId pid);
    int getKeyCount();
    RC read(PageId pid, const PageFile& pf);
    RC write(PageId pid, PageFile& pf);

   
    void shiftKeysRight(int pos) {
        char *loc = buffer + pos * L_OFFSET;

        char* temp = (char*) malloc(P_SIZE);
        memset(temp, NONE, P_SIZE);

        memcpy(temp, loc, (getKeyCount() - pos) * L_OFFSET);
        loc += L_OFFSET;
        memcpy(loc, temp, (getKeyCount() - pos) * L_OFFSET);

        free(temp);
    }

    void printKeys();

    void initBuffer(char *buf, size_t size) {
        memset(buffer, NONE, P_SIZE);
        memcpy(buffer, buf, size);
    }

  private:
    char buffer[PageFile::PAGE_SIZE];
}; 


class BTNonLeafNode {
  public:
    BTNonLeafNode();
    RC insert(int key, PageId pid);
    RC insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey);
    RC locateChildPtr(int searchKey, PageId& pid);
    RC initializeRoot(PageId pid1, int key, PageId pid2);
    int getKeyCount();
    RC read(PageId pid, const PageFile& pf);
    RC write(PageId pid, PageFile& pf);
    RC locate(int searchKey, int &eid);
    RC readEntry(int eid, int& key, PageId& pid);


    void shiftKeysRight(int pos) {
        char *loc = buffer + pos * NL_OFFSET;

        char* temp = (char*) malloc(P_SIZE);
        memset(temp, NONE, P_SIZE);

        memcpy(temp, loc, (getKeyCount() + 1 - pos) * NL_OFFSET);
        loc += NL_OFFSET;
        memcpy(loc, temp, (getKeyCount() + 1 - pos) * NL_OFFSET);
        free(temp);
    }

    void printKeys();

    void initBuffer(char *buf, size_t size) {
        memset(buffer, NONE, P_SIZE);
        memcpy(buffer, buf, size);
    }

  private:
    char buffer[PageFile::PAGE_SIZE];   
    
}; 

#endif /* BTREENODE_H */
