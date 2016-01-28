/*
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */

#include <iostream>
#include <cstdlib>
#include <cstdio>
#include <cstring> 
#include "BTreeIndex.h"
#include "BTreeNode.h"

using namespace std;

/*
 * BTreeIndex constructor
 */
BTreeIndex::BTreeIndex()
{
    //endPid returns pid+1 we set to -1: -1+1=0
    rootPid = 0;
    //no root yet so height=0
    treeHeight=0;
}

/*
 * Open the index file in read or write mode.
 * Under 'w' mode, the index file should be created if it does not exist.
 * @param indexname[IN] the name of the index file
 * @param mode[IN] 'r' for read, 'w' for write
 * @return error code. 0 if no error
 */
RC BTreeIndex::open(const string& indexname, char mode)
{
    //buffer to store stuff

    char buffer[PageFile::PAGE_SIZE];
    memset(buffer, '\0', PageFile::PAGE_SIZE);
    
    //error variable
    RC rc=pf.open(indexname,mode);
    //check for error
    if(rc)
        return rc;
 
    //first
    if(pf.endPid()<=0)
    {
        rootPid=0;
        treeHeight=0;
    }
    else//rest
    {
        rc=pf.read(0, buffer);

        //check for error
        if(rc)
            return rc;

        memcpy(&treeHeight,buffer+PageFile::PAGE_SIZE-sizeof(int),sizeof(int));
    }

    //if we got here then success
    return 0;
}

/*
 * Close the index file.
 * @return error code. 0 if no error
 */
RC BTreeIndex::close()
{
    RC rc=pf.close();
    
    return rc;
}

/*
 * Insert (key, RecordId) pair to the index.
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @return error code. 0 if no error
 */
RC BTreeIndex::insert(int key, const RecordId& rid)
{
    RC rc;
    //if no root, make one
    if(treeHeight==0)
    {
        //create a root
        //root is leaf node at first
        BTLeafNode newTreeRoot;

        //insert the key and rid into root
        newTreeRoot.insert(key,rid);

        //set rootPid
        rootPid=pf.endPid();

        //root has height of 1
        treeHeight=1;
        memcpy(newTreeRoot.getBuffer()+PageFile::PAGE_SIZE-sizeof(int),&treeHeight,sizeof(int));

        //write data to disk
        newTreeRoot.write(rootPid,pf);
        
        //success
        return 0;

    }
    else//else root exists, insert recursively
    {
        //variables to pass data into on root overflow
        int pKey;
        PageId pPid;

        //attempt to insert recursively
        rc=insertRecursively(key,rid,1,rootPid,pKey,pPid);
        //failure
        if(rc && rc!=1000)
            return rc;

        //success
        if(!rc)
            return rc;

        //overflow at node level
        if(rc==1000)
        {
            if(treeHeight==1)//root is leaf
            {
                BTLeafNode newLeaf;
                newLeaf.read(rootPid,pf);
                PageId newLeafPid=pf.endPid();
                newLeaf.setNextNodePtr(pPid);
                newLeaf.write(newLeafPid,pf);
                
                BTNonLeafNode rootNode;
                rootNode.initializeRoot(newLeafPid,pKey,pPid);

                //increment treeHeight
                treeHeight+=1;
                memcpy(rootNode.getBuffer()+PageFile::PAGE_SIZE-sizeof(int),&treeHeight,sizeof(int));

                //write data to disk
                rootNode.write(rootPid,pf);

            }
            else//root is nonleaf 
            {
                BTNonLeafNode newNode;
                newNode.read(rootPid,pf);
                PageId newNodePid=pf.endPid();
                newNode.write(newNodePid,pf);
                
                BTNonLeafNode rootNode;
                rootNode.initializeRoot(newNodePid,pKey,pPid);

                //increment treeHeight
                treeHeight+=1;
                memcpy(rootNode.getBuffer()+PageFile::PAGE_SIZE-sizeof(int),&treeHeight,sizeof(int));
                //write data to disk
                rootNode.write(rootPid,pf);
            }

            //success
            return 0;
        }

    }

    //
    return -1;
}


/*
 * Insert (key, RecordId) pair while handling overflows
 * Traverses B+ tree starting at root
 * Parent nodes will call insertRecursively on their children
 * Duty of the parent node level call to insert into parent after
 * being notified by error code of recursive call
 * Leaf level will insert key, rid
 * @param key[IN] the key for the value inserted into the index
 * @param rid[IN] the RecordId for the record being inserted into the index
 * @param currHeight[IN] the current height we are traversing, root=1
 * @param pid[IN] the pid of the current node
 * @param pKey[IN] on overflow, variable used to pass key to insert to parent
 * @param pPid[IN] on overflow, variable used to pass pid to insert to parent
 * @return error code. 0 if no error
 */
RC BTreeIndex::insertRecursively(int key, const RecordId& rid, int currHeight, PageId pid, int& pKey, PageId& pPid)
{
    RC rc;
    BTLeafNode currLeaf;
    int sibKey;
    PageId sibPid;
    BTNonLeafNode nonLeaf;
    PageId nextPid;
    //the current node is leaf node
    if(currHeight==treeHeight)
    {
        BTLeafNode sibNode;
        //obtain current leaf node data
        currLeaf.read(pid,pf);

        //insertion attempt
        rc=currLeaf.insert(key,rid);
        if(!rc)
        {
            //success: write and return
            if(treeHeight==1)
                memcpy(currLeaf.getBuffer()+PageFile::PAGE_SIZE-sizeof(int),&treeHeight,sizeof(int));
            currLeaf.write(pid,pf);
            return rc;
        }
        //failure: overflow
        //insertAndSplit since overflow
         if(treeHeight==1){
            int tempnum=0; 
            memcpy(currLeaf.getBuffer()+PageFile::PAGE_SIZE-sizeof(int),&tempnum,sizeof(int));
            }
        rc=currLeaf.insertAndSplit(key,rid,sibNode,sibKey);

        //return on failure
        if(rc)
            return rc;

        //successful insertAndSplit
        //insertion into parent node will be done by caller

        //set nextNodePtrs
        //insertAndSplit automatically transfers the next node ptr to the sibling node

        sibPid=pf.endPid();

        //set current leaf node's next leaf pid
        currLeaf.setNextNodePtr(sibPid);
       
        //update current leaf data
        currLeaf.write(pid,pf);

        //update sibling leaf data
        sibNode.write(sibPid,pf);

        //transfer over to p variables
        pKey=sibKey;
        pPid=sibPid;
        
        //successful but need to tell caller to insert into parent
        return 1000;

    }
    else//nonleaf node, continue traverasl
    {
        BTNonLeafNode sibNode;
        //locate child so we can recursively call on it
        nonLeaf.read(pid,pf);

        rc=nonLeaf.locateChildPtr(key,nextPid);

        //return on failure
        if(rc)
            return rc;

        //insert recursively into child
        rc=insertRecursively(key,rid,currHeight+1,nextPid,pKey,pPid);

        //insertion failed
        if(rc && rc!=1000)
            return rc;

        //insertion successful but split occured, handle any overflow insertions
        if(rc==1000)
        {
            //attempt insertion
            rc=nonLeaf.insert(pKey,pPid);
            if(!rc)//success
            {
                if(pid==0)
                    memcpy(nonLeaf.getBuffer()+PageFile::PAGE_SIZE-sizeof(int),&treeHeight,sizeof(int));
                nonLeaf.write(pid,pf);
                return 0;
            }
            else//insertion failure: overflow
            {
                nonLeaf.insertAndSplit(pKey,pPid,sibNode,sibKey);
                sibPid=pf.endPid();
                //write data
                sibNode.write(sibPid,pf);
                nonLeaf.write(pid,pf);

                //transfer data to p variables
                pKey=sibKey;
                pPid=sibPid;

                return 1000;
            }
        }
    
        //success
        return rc;
    }

}


/**
 * Run the standard B+Tree key search algorithm and identify the
 * leaf node where searchKey may exist. If an index entry with
 * searchKey exists in the leaf node, set IndexCursor to its location
 * (i.e., IndexCursor.pid = PageId of the leaf node, and
 * IndexCursor.eid = the searchKey index entry number.) and return 0.
 * If not, set IndexCursor.pid = PageId of the leaf node and
 * IndexCursor.eid = the index entry immediately after the largest
 * index key that is smaller than searchKey, and return the error
 * code RC_NO_SUCH_RECORD.
 * Using the returned "IndexCursor", you will have to call readForward()
 * to retrieve the actual (key, rid) pair from the index.
 * @param key[IN] the key to find
 * @param cursor[OUT] the cursor pointing to the index entry with
 *                    searchKey or immediately behind the largest key
 *                    smaller than searchKey.
 * @return 0 if searchKey is found. Othewise an error code
 */
RC BTreeIndex::locate(int searchKey, IndexCursor& cursor)
{
    RC rc;
    PageId pid=rootPid;
    int tempEid=-1;
    int searchPid;
    BTLeafNode lNode;

    //root is leaf

    if(treeHeight==1)
    {
        if(cursor.pid==0)
        {
            return 0;
        }
        rc=lNode.read(pid,pf);
        if(rc)
            return rc;

        rc=lNode.locate(searchKey,tempEid);

        //regardless of success or failure, locate will set tempEid to the correct
        //value
        cursor.pid=pid;
        cursor.eid=tempEid/(sizeof(RecordId)+sizeof(int));
        return rc;

    }
    else//root is non leaf
    {
        rc=locateRecursively(searchKey,pid,tempEid,1);
        cursor.pid=pid;
        cursor.eid=tempEid/(sizeof(RecordId)+sizeof(int));
        return rc;

    }
    return 0;
}


/**
 *  Recursively traverses the tree going from node to node
 *  sets pid and eid to values we will set locate's indexcursor values to
 */
RC BTreeIndex::locateRecursively(int searchKey, PageId& pid, PageId& eid, int currHeight)
{
    RC rc;
    BTNonLeafNode nlNode;
    BTLeafNode lNode;
    //search node
    rc=nlNode.read(pid,pf);
    if(rc)
        return rc;

    //sets pid to the next pid
    rc=nlNode.locateChildPtr(searchKey,pid);

    //if next is leaf

    if(currHeight+1==treeHeight)
    {
        rc=lNode.read(pid,pf);

        if(rc)
            return rc;

        //if searchkey was found eid is at it, 
        //if not found, set to entry after largest key smaller
        rc=lNode.locate(searchKey,eid);
        return rc;
    }
    else
    {
        rc=locateRecursively(searchKey,pid,eid,currHeight+1);

        return rc;
    }
}

/*
 * Read the (key, rid) pair at the location specified by the index cursor,
 * and move foward the cursor to the next entry.
 * @param cursor[IN/OUT] the cursor pointing to an leaf-node index entry in the b+tree
 * @param key[OUT] the key stored at the index cursor location.
 * @param rid[OUT] the RecordId stored at the index cursor location.
 * @return error code. 0 if no error
 */
RC BTreeIndex::readForward(IndexCursor& cursor, int& key, RecordId& rid)
{
    RC rc;
    BTLeafNode lNode;
    PageId pid=cursor.pid;
    int eid=cursor.eid;

    //read into node
    rc=lNode.read(pid,pf);

    if(rc)
        return rc;

    //read node data into key and rid
    int temp_eid=eid*(sizeof(RecordId)+sizeof(int));
    rc=lNode.readEntry(temp_eid,key,rid);

    if(rc)
        return rc;

    if((eid+1)>=(lNode.getKeyCount()))
    {
        //next index should be in the next node
        pid=lNode.getNextNodePtr();
        if(pid==0)
        {
            eid=-1;
        }
        else
            eid=0;
    }
    else
    {
        eid+=1;
    }

    //set cursor info
    cursor.pid=pid;
    cursor.eid=eid;

    return 0;
}


void BTreeIndex::print()
{

    if(treeHeight==1)
    {
        BTLeafNode root;
        root.read(rootPid,pf);
        root.print();
    }
    else if(treeHeight>1)
    {
        printRecNL(rootPid,1);
    }
}

void BTreeIndex::printRecNL(PageId pid,int heightLevel)
{
        BTNonLeafNode nonLeaf;
        nonLeaf.read(pid,pf);

        nonLeaf.print();
        PageId first;
        memcpy(&first,nonLeaf.getBuffer()+sizeof(int),sizeof(PageId));



        if(heightLevel+1==treeHeight)
        {
                printLeaf(first);
        }
        else
        {
            int temporary=nonLeaf.getKeyCount();
             for(int a=0;a<temporary+1;a++)
            {
                //recursive call on all children
                printRecNL(first, heightLevel+1);

                memcpy(&first,nonLeaf.getBuffer()+sizeof(int)+((a+1)*(sizeof(PageId)+sizeof(int))),sizeof(PageId));
            }
        }

}

void BTreeIndex::printLeaf(PageId pid)
{
    BTLeafNode firstLeaf;
    firstLeaf.read(pid,pf);
    firstLeaf.print();
    PageId tempPid;
    memcpy(&tempPid,firstLeaf.getBuffer()+(firstLeaf.getKeyCount()*(sizeof(RecordId)+sizeof(int))),sizeof(PageId));

    if(pid!=0 && tempPid!=0 && tempPid<10000)
        printLeaf(tempPid);
    return;
}
