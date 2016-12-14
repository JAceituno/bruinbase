#include "BTreeNode.h"

using namespace std;


BTLeafNode::BTLeafNode()
{
	memset(buffer, NONE, PageFile::PAGE_SIZE);
}

void BTLeafNode::printKeys()
{
	int key;
	RecordId rid;
	printf("=== Leaf Node: [rid(pid, sid), key] ===\n");
	for(int i = 0; i < getKeyCount(); i++) {
		readEntry(i, key, rid);
		printf("[(%d, %d), %d]\t", rid.pid, rid.sid, key);
	}
	printf("\n");
}


RC BTLeafNode::read(PageId pid, const PageFile& pf)
{
	return pf.read(pid,buffer);
}
    

RC BTLeafNode::write(PageId pid, PageFile& pf)
{
	return pf.write(pid,buffer);
}


int BTLeafNode::getKeyCount()
{
	char* kstart = buffer;
	char* end = buffer + (N_KEY * L_OFFSET) - K_SIZE;
	int curRid = -1;
	int i = 0;

	while (kstart <= end) {
		curRid = *((int *) kstart);

		if(curRid == NONE) {
			return i;
		}
		
		kstart += L_OFFSET;
		i++;
	}

	return i;
}


RC BTLeafNode::insert(int key, const RecordId& rid)
{
	int pos;
	// encuentra la llave
	BTLeafNode::locate(key, pos);

	// find copy location
	char* loc = buffer + pos * L_OFFSET;

	// mueve un espacio a la derecha
	BTLeafNode::shiftKeysRight(pos);

	memcpy(loc, &rid, RID_SIZE);
	loc += RID_SIZE;
	memcpy(loc, &key, K_SIZE);

	return 0;
}


RC BTLeafNode::insertAndSplit(int key, const RecordId& rid, 
                              BTLeafNode& sibling, int& siblingKey)
{

	int mid_key = N_KEY / 2;
	int pos;
	BTLeafNode::locate(key, pos);	
	int num_copy = N_KEY - mid_key;
	if (pos > mid_key) {
		num_copy--;
	}

	char* sib_start = buffer + ((N_KEY - num_copy) * L_OFFSET);
	sibling.initBuffer(sib_start, (num_copy * L_OFFSET));
	memset(sib_start, NONE, (num_copy * L_OFFSET));

	if (pos > mid_key) {
		// inserta en el hermano
		sibling.insert(key,rid);
	} else {
		// inserta en el nodo
		insert(key, rid);
	}

	RecordId siblingRid;
	sibling.readEntry(0, siblingKey, siblingRid);

	return 0;
}


RC BTLeafNode::locate(int searchKey, int& eid)
{
	char* kstart = buffer + RID_SIZE;
	int curKey;
	int i = 0;

	for (int iter = 0; iter < getKeyCount(); iter++) {
		curKey=*((int *) kstart);

		if (searchKey == curKey) {
			eid = i;
			return 0;
		}
		
		if (searchKey < curKey) {
			eid = i;
			return RC_NO_SUCH_RECORD;
		}

		kstart += L_OFFSET;
		i++;
	}

	eid = i;
	return EC;
}


RC BTLeafNode::readEntry(int eid, int& key, RecordId& rid)
{
	char* entryStart = buffer + (eid * L_OFFSET);
	char* end = buffer + P_SIZE;

	if (buffer > end)
		return RC_INVALID_CURSOR;
	memcpy(&rid, entryStart, RID_SIZE);
	entryStart += RID_SIZE;
	memcpy(&key, entryStart, K_SIZE);

	return 0;
}


PageId BTLeafNode::getNextNodePtr()
{

	return *((PageId *) (buffer + L_OFFSET * N_KEY));
}


RC BTLeafNode::setNextNodePtr(PageId pid)
{
	*((PageId *) (buffer + L_OFFSET * N_KEY)) = pid;
	return 0;
}


BTNonLeafNode::BTNonLeafNode()
{
	memset(buffer, NONE, PageFile::PAGE_SIZE);
}

void BTNonLeafNode::printKeys() {
	int key;
	PageId pid;
	printf("=== Non-leaf Node: [pid, key] ===\n");
	for(int i = 0; i < BTNonLeafNode::getKeyCount(); i++) {
		BTNonLeafNode::readEntry(i, key, pid);
			printf("[%d, %d]\t", pid, key);
	}
	printf("\n");

}


RC BTNonLeafNode::read(PageId pid, const PageFile& pf)
{
	return pf.read(pid,buffer);
}


RC BTNonLeafNode::write(PageId pid, PageFile& pf)
{
	return pf.write(pid,buffer);
}


int BTNonLeafNode::getKeyCount()
{
	char* kstart = buffer + NL_OFFSET;
	char* end = buffer + (N_KEY * NL_OFFSET) + PID_SIZE;
	int curPid;
	int i = 0;

	while (kstart <= end) {
		curPid = *((int *) kstart);

		if(curPid == NONE) {
			return i;
		}
		
		kstart += NL_OFFSET;
		i++;
	}

	return i;
}


RC BTNonLeafNode::insert(int key, PageId pid)
{
	int pos;
	// encuentra la posición de la llave
	BTNonLeafNode::locate(key, pos);

	// encuentra la posición donde va a copiar
	char* loc = buffer + PID_SIZE + pos * NL_OFFSET;

	BTNonLeafNode::shiftKeysRight(pos);
	memcpy(loc, &key, K_SIZE);
	loc += K_SIZE;
	memcpy(loc, &pid, PID_SIZE);

	return 0;
}


RC BTNonLeafNode::insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey)
{
	insert(key, pid);
	int mid_key = (N_KEY + 1) / 2;
	char* loc = buffer + PID_SIZE + (mid_key * NL_OFFSET);
	memcpy(&midKey, loc, K_SIZE);
	char* sib_start = loc + K_SIZE;
	int num_copy = N_KEY - mid_key;
	sibling.initBuffer(sib_start, ((num_copy * NL_OFFSET) + PID_SIZE));
	memset(loc, NONE, ((num_copy + 1) * NL_OFFSET));

	return 0;
}


RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
{
	int idx;
	locate(searchKey, idx);

	if (idx >= getKeyCount()) {
		idx = getKeyCount() - 1;
	}

	char* keyLoc = buffer + PID_SIZE + (idx * NL_OFFSET);

	int curKey = *((int *) keyLoc);

	if (searchKey >= curKey) {
		pid = *((PageId *) (keyLoc + K_SIZE));
	}
	else {
		pid = *((PageId *) (keyLoc - PID_SIZE));
	}
	return 0;
}


RC BTNonLeafNode::locate(int searchKey, int &eid) {
	char* kstart = buffer + PID_SIZE;
	int curKey;
	int i = 0;

	for(int iter = 0; iter < getKeyCount(); iter++) {
		curKey = *((int *) kstart);

		if (searchKey == curKey) {
			eid = i;
			return 0;
		}
		
		if (searchKey < curKey) {
			eid = i;
			return RC_NO_SUCH_RECORD;
		}

		kstart += NL_OFFSET;
		i++;
	}

	eid = i;

	return EC;
}


RC BTNonLeafNode::readEntry(int eid, int& key, PageId& pid)
{
	char* entryStart;
	entryStart = buffer + (eid * NL_OFFSET);

	memcpy(&pid, entryStart, PID_SIZE);
	entryStart += PID_SIZE;
	memcpy(&key, entryStart, K_SIZE);

	return 0;
}


RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{
	char* start = buffer;
	memset(buffer, NONE, P_SIZE);
	memcpy(start, &pid1, PID_SIZE);
	start += PID_SIZE;
	memcpy(start, &key, K_SIZE);
	start += K_SIZE;
	memcpy(start, &pid2, PID_SIZE);

	return 0;
}