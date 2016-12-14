#include "BTreeIndex.h"
#include "BTreeNode.h"

using namespace std;

BTreeIndex::BTreeIndex(){
    rootPid = -1;
    alturaArbol = 0;
}

RC BTreeIndex::open(const string& indexname, char mode){
    RC retorno = 0;
    retorno = pf.open(indexname, mode);
    if (retorno != 0) {
        return retorno;
    } else {
        char *buffer = (char *) malloc(P_SIZE * sizeof(char));
        memset(buffer, 0, P_SIZE);
        PageId indexEndPid = pf.endPid();
        if (indexEndPid == 0) {
            rootPid = -1;
            alturaArbol = 0;
            memcpy(buffer, &rootPid, PID_SIZE);
            memcpy((buffer + PID_SIZE), &alturaArbol, sizeof(int));
            retorno = pf.write(0, buffer);
            if (retorno != 0) {
                free(buffer);
                return retorno;
            }
        } else {
            retorno = pf.read(0, buffer);
            if (retorno != 0) {
                free(buffer);
                return retorno;
            }
            memcpy(&rootPid, buffer, PID_SIZE);
            memcpy(&alturaArbol, buffer + PID_SIZE, sizeof(int));
        }
        free(buffer);
    }
    return retorno;
}

RC BTreeIndex::insert(int llave, const RecordId& rid) {
    RC retorno = 0;
    path.erase(path.begin(), path.end());
    if (alturaArbol == 0) {
        BTLeafNode root;
        retorno = root.insert(llave, rid);
        if (retorno != 0) {
            return retorno;
        }
        rootPid = pf.endPid();
        rootPid = rootPid > 0 ? rootPid : 1;
        alturaArbol++;
        retorno = root.write(rootPid, pf);
        if (retorno != 0) {
            return retorno;
        }
        return retorno;
    }
    IndexCursor targetIdx;
    locate(llave, targetIdx, true);
    PageId targetPid = targetIdx.pid;
    BTLeafNode targetLeafNode;
    targetLeafNode.read(targetPid, pf);
    if (targetLeafNode.getKeyCount() < N_KEY) {
        targetLeafNode.insert(llave, rid);
        retorno = targetLeafNode.write(targetPid, pf);
        if (retorno != 0) {
            return retorno;
        }
    } else {
        PageId siblingPid = pf.endPid();
        BTLeafNode siblingLeafNode;
        int siblingKey;
        path.push_back(targetPid);
        path.push_back(siblingPid);
        targetLeafNode.insertAndSplit(llave, rid, siblingLeafNode, siblingKey);
        siblingLeafNode.setNextNodePtr(targetLeafNode.getNextNodePtr());
        targetLeafNode.setNextNodePtr(siblingPid);
        retorno = siblingLeafNode.write(siblingPid, pf);
        if (retorno != 0) {
            return retorno;
        }
        retorno = targetLeafNode.write(targetPid, pf);
        if (retorno != 0) {
            return retorno;
        }

        if (alturaArbol == 1) {
            PageId newRootPid = pf.endPid();
            BTNonLeafNode newRootNode;
            newRootNode.initializeRoot(targetPid, siblingKey, siblingPid);
            retorno = newRootNode.write(newRootPid, pf);
            if (retorno != 0) {
                return retorno;
            }
            rootPid = newRootPid;
            alturaArbol++;
        } else {
            retorno = insertInParent(path, siblingKey);
        }
    }
    return retorno;
}

RC BTreeIndex::close(){
    RC retorno = 0;
    char *buffer = (char *) malloc(P_SIZE * sizeof(char));
    memset(buffer, 0, P_SIZE);
    memcpy(buffer, &rootPid, PID_SIZE);
    memcpy((buffer + PID_SIZE), &alturaArbol, sizeof(int));
    retorno = pf.write(0, buffer);
    if (retorno != 0) {
        free(buffer);
        return RC_FILE_WRITE_FAILED;
    }
    retorno = pf.close();
    if (retorno != 0) {
        free(buffer);
        return RC_FILE_CLOSE_FAILED;
    }
    free(buffer);
    return retorno;
}

RC BTreeIndex::locate(int searchKey, IndexCursor& offset, bool isTracking){
    RC retorno = 0;
    path.erase(path.begin(), path.end());
    int currentLevel = 1;
    assert(alturaArbol >= 0);
    PageId currentPid = rootPid;
    BTNonLeafNode currentNonLeafNode;
    while (currentLevel < alturaArbol) {
        retorno = currentNonLeafNode.read(currentPid, pf);
        if (isTracking) {
            path.push_back(currentPid);
        }
        retorno = currentNonLeafNode.locateChildPtr(searchKey, currentPid);
        if (retorno != 0) {
            return retorno;
        }
        currentLevel++;
    }
    BTLeafNode currentNode;
    retorno = currentNode.read(currentPid, pf);
    if (retorno != 0) {
        return retorno;
    }
    offset.pid = currentPid;
    retorno = currentNode.locate(searchKey, offset.eid);
    if (retorno != 0) {
        return retorno;
    }
    return retorno;
}

RC BTreeIndex::readForward(IndexCursor& offset, int& llave, RecordId& rid){
    RC retorno = 0;
    PageId currentPid = offset.pid;
    int currentEid = offset.eid;
    BTLeafNode currentNode;
    retorno = currentNode.read(offset.pid, pf);
    if (retorno != 0) {
        return retorno;
    }
    retorno = currentNode.readEntry(currentEid, llave, rid);
    if (retorno != 0) {
        return retorno;
    }
    if (currentEid == currentNode.getKeyCount() - 1) {
        offset.eid = 0;
        offset.pid = currentNode.getNextNodePtr();
    } else {
        offset.eid = ++currentEid;
        offset.pid = currentPid;
    }
    return retorno;
}

RC BTreeIndex::insertInParent(vector<PageId> &path, int siblingKey) {
    RC retorno = 0;
    PageId siblingPid = path.back();
    path.pop_back();
    PageId currentPid = path.back();
    path.pop_back();
    PageId parentPid = path.back();
    path.pop_back();
    if (parentPid > 0 && parentPid != rootPid) {
        BTNonLeafNode parentNode;
        retorno = parentNode.read(parentPid, pf);
        if (retorno != 0) {
            return retorno;
        }
        if (parentNode.getKeyCount() < N_KEY) {
            parentNode.insert(siblingKey, siblingPid);
            retorno = parentNode.write(parentPid, pf);
            if (retorno != 0) {
                return retorno;
            }
        } else {
            PageId newSiblingPid = pf.endPid();
            BTNonLeafNode newSiblingNode;
            int newSiblingKey;
            parentNode.insertAndSplit(siblingKey, siblingPid, newSiblingNode, newSiblingKey);
            retorno = newSiblingNode.write(newSiblingPid, pf);
            if (retorno != 0) {
                return retorno;
            }
            retorno = parentNode.write(parentPid, pf);
            if (retorno != 0) {
                return retorno;
            }
            path.push_back(parentPid);
            path.push_back(newSiblingPid);
            retorno = insertInParent(path, newSiblingKey);
        }
    } else if (parentPid > 0 && parentPid == rootPid) {
        BTNonLeafNode parentNode;
        retorno = parentNode.read(parentPid, pf);
        if (retorno != 0) {
            return retorno;
        }
        if (parentNode.getKeyCount() < N_KEY) {
            parentNode.insert(siblingKey, siblingPid);
            retorno = parentNode.write(parentPid, pf);
            if (retorno != 0) {
                return retorno;
            }
        } else {
            PageId newSiblingPid = pf.endPid();
            BTNonLeafNode newSiblingNode;
            int newSiblingKey;
            parentNode.insertAndSplit(siblingKey, siblingPid, newSiblingNode, newSiblingKey);

            retorno = newSiblingNode.write(newSiblingPid, pf);
            if (retorno != 0) {
                return retorno;
            }
            retorno = parentNode.write(parentPid, pf);
            if (retorno != 0) {
                return retorno;
            }

            PageId newRootPid = pf.endPid();
            BTNonLeafNode rootNode;
            rootNode.initializeRoot(parentPid, newSiblingKey, newSiblingPid);

            retorno = rootNode.write(newRootPid, pf);
            if (retorno != 0) {
                return retorno;
            }
            rootPid = newRootPid;
            alturaArbol++;
        }

    } else if (currentPid == rootPid) {
        BTNonLeafNode currentNode;
        currentNode.read(currentPid, pf);
        if (currentNode.getKeyCount() >= N_KEY) {
            PageId newRootPid = pf.endPid();
            BTNonLeafNode rootNode;
            rootNode.initializeRoot(currentPid, siblingKey, siblingPid);
            retorno = rootNode.write(newRootPid, pf);
            if (retorno != 0) {
                return retorno;
            }
            rootPid = newRootPid;
            alturaArbol++;
        } else {
            currentNode.insert(siblingKey, siblingPid);
            retorno = currentNode.write(currentPid, pf);
            if (retorno != 0) {
                return retorno;
            }
        }
        return retorno;
    }
    return retorno;
}

RC BTreeIndex::insertKey(int s) {
    int llave;
    RC retorno = 0;
    IndexCursor offset;
    RecordId rid;
    rid.pid = s;
    rid.sid = rid.pid;
    llave = s * 100;
    retorno = insert(llave, rid);
    return retorno;
}
