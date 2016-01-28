#include "BTreeNode.h"
#include <math.h>
#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <iostream>
using namespace std;

#define L_PAIR_SIZE (sizeof(RecordId)+sizeof(int))
#define NL_PAIR_SIZE (sizeof(PageId)+sizeof(int))

/*
 * Initializes variables
 */
 BTLeafNode::BTLeafNode()
 {
    memset(buffer, '\0', PageFile::PAGE_SIZE);
 }


/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::read(PageId pid, const PageFile& pf)
{ 
    //read disk page with PageId pid into buffer and return
    return pf.read(pid, buffer);
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::write(PageId pid, PageFile& pf)
{
    //write the contents of buffer into the disk page with PageId pid
    return pf.write(pid, buffer);
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTLeafNode::getKeyCount()
{   
    int keyCounter=0;
    char* tmp=buffer;
 
    int curr;
    int i=4;//int i=0
    //1008 because buffer is 1024, pairsize should be 12 so 1008+12 means
    //1020 + the PageId which is 4 bytes.
    while(i<1024)
    {
        memcpy(&curr,tmp,sizeof(int));
        if(curr==0)
            break;
        //increment everything
        keyCounter++;
        tmp+=L_PAIR_SIZE;
        i+=L_PAIR_SIZE;
    }

    if(keyCounter>1)
    {
        memcpy(&curr,tmp-sizeof(int),sizeof(int));
        if(curr==0)
        {
            memcpy(&curr,tmp-2*sizeof(int),sizeof(int));
            if(curr==0)
            {
                keyCounter--;
            }
        }
    }

    return keyCounter;
}

/*
 * Returns the maximum number of keys that can be stored in the node
 * @return the max keys that can be stored in the node 
 */
int BTLeafNode::getMaxKeys()
{
    //maxPairs should be 85
    int maxPairs=floor((PageFile::PAGE_SIZE-sizeof(PageId))/(L_PAIR_SIZE));
    return maxPairs;
}

/*
 * Insert a (key, rid) pair to the node.
 * @param key[IN] the key to insert
 * @param rid[IN] the RecordId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTLeafNode::insert(int key, const RecordId& rid)
{ 
    //if adding another key would go over the max key count return that
    //node is full
    if(getKeyCount()+1>getMaxKeys())
        return RC_NODE_FULL;

    //find the index of buffer where we want to insert
    int insertIndex;
    locate(key,insertIndex);
  
    //create a new buffer where we will copy everything to and zero it out
    char* buffer2=(char*)malloc(PageFile::PAGE_SIZE);
    memset(buffer2, '\0', PageFile::PAGE_SIZE);

    //copy buffer up to where we want to insert
    if(insertIndex>=0)
        memcpy(buffer2,buffer,insertIndex);

    //insert (key, rid) pair to buffer
    memcpy(buffer2+insertIndex,&key,sizeof(int));
    memcpy(buffer2+insertIndex+sizeof(int),&rid,sizeof(RecordId));

    //copy the rest of the original buffer into buffer2
    //this will include the PageId of the next node
    memcpy(buffer2+insertIndex+sizeof(int)+sizeof(RecordId),buffer+insertIndex,(PageFile::PAGE_SIZE-insertIndex-sizeof(int)-sizeof(RecordId)));

    //replace old buffer with new buffer(buffer2)
    memcpy(buffer,buffer2,PageFile::PAGE_SIZE);

    //free up buffer2
    free(buffer2);
    //if we reach here we are successful
    return 0;
}

/*
 * Insert the (key, rid) pair to the node
 * and split the node half and half with sibling.
 * The first key of the sibling node is returned in siblingKey.
 * @param key[IN] the key to insert.
 * @param rid[IN] the RecordId to insert.
 * @param sibling[IN] the sibling node to split with. This node MUST be EMPTY when this function is called.
 * @param siblingKey[OUT] the first key in the sibling node after split.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::insertAndSplit(int key, const RecordId& rid, 
                              BTLeafNode& sibling, int& siblingKey)
{ 
    int keyCount=getKeyCount();

    if(getMaxKeys()>keyCount+1)
        return RC_INVALID_ATTRIBUTE;

    if(sibling.getKeyCount()!=0)
        return RC_INVALID_ATTRIBUTE;

    //find the index of buffer where we want to insert
    int insertIndex;
    locate(key,insertIndex);

    //create a new buffer where we will copy everything to and zero it out
    char* buffer2=(char*)malloc(2*(PageFile::PAGE_SIZE));
    memset(buffer2, '\0', (2*PageFile::PAGE_SIZE));

    //copy buffer up to where we want to insert
    memcpy(buffer2,buffer,insertIndex);

    //insert (key, rid) pair to buffer
    memcpy(buffer2+insertIndex,&key,sizeof(int));
    memcpy(buffer2+insertIndex+sizeof(int),&rid,sizeof(RecordId));
    
    //copy the rest of the original buffer into buffer2
    memcpy(buffer2+insertIndex+sizeof(int)+sizeof(RecordId),buffer+insertIndex,(PageFile::PAGE_SIZE-insertIndex));

    //ceiling so the first node will have more than second
    double dKey=keyCount+1;
    double first=ceil((dKey)/2);

    int splitIndex=first*L_PAIR_SIZE;

    //replace old buffer with new buffer
    memcpy(buffer,buffer2,splitIndex);

    //replace sibling buffer with new buffer
    memcpy(sibling.buffer,buffer2+splitIndex,PageFile::PAGE_SIZE+L_PAIR_SIZE-splitIndex);

    //zero out the rest of buffer
    memset(buffer+splitIndex,'\0',PageFile::PAGE_SIZE-splitIndex);

    //zero out the rest of sibling.buffer
    memset(sibling.buffer+(PageFile::PAGE_SIZE+L_PAIR_SIZE-splitIndex),'\0',splitIndex-L_PAIR_SIZE);

    //free the temp buffer
    free(buffer2);

    //copy first sibling key into siblingKey
    memcpy(&siblingKey,sibling.buffer,sizeof(int));
    
    return 0; 
}

/**
 * If searchKey exists in the node, set eid to the index entry
 * with searchKey and return 0. If not, set eid to the index entry
 * immediately after the largest index key that is smaller than searchKey,
 * and return the error code RC_NO_SUCH_RECORD.
 * Remember that keys inside a B+tree node are always kept sorted.
 * @param searchKey[IN] the key to search for.
 * @param eid[OUT] the index entry number with searchKey or immediately
                   behind the largest key smaller than searchKey.
 * @return 0 if searchKey is found. Otherwise return an error code.
 */
RC BTLeafNode::locate(int searchKey, int& eid)
{ 
    int i=0;
    int keyCount=getKeyCount();
    if(keyCount==0)
    {
        eid=i*L_PAIR_SIZE;
        return RC_NO_SUCH_RECORD;
    }

    for(i=0;i<keyCount;i++)
    {
        int currKey;
        memcpy(&currKey,buffer+(i*L_PAIR_SIZE),sizeof(int));
       
        if(searchKey==currKey)
        {
            eid=i*L_PAIR_SIZE;
            return 0;
        }
        else if(searchKey<currKey)
        {
            eid=i*L_PAIR_SIZE;
            return RC_NO_SUCH_RECORD;
        }
    }


    eid=i*L_PAIR_SIZE;
    //we did not find searchKey and there are no keys larger than it
    return RC_NO_SUCH_RECORD; 
}

/*
 * Read the (key, rid) pair from the eid entry.
 * @param eid[IN] the entry number to read the (key, rid) pair from
 * @param key[OUT] the key from the entry
 * @param rid[OUT] the RecordId from the entry
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::readEntry(int eid, int& key, RecordId& rid)
{ 
    if(eid<0)
        return RC_NO_SUCH_RECORD;
    if(eid>=(getKeyCount()*L_PAIR_SIZE))
        return RC_NO_SUCH_RECORD;

    int shift=eid;
    memcpy(&key,buffer+shift,sizeof(int));
    memcpy(&rid,buffer+shift+sizeof(int),sizeof(RecordId));

    return 0;
}

/*
 * Return the pid of the next sibling node.
 * @return the PageId of the next sibling node 
 */
PageId BTLeafNode::getNextNodePtr()
{ 
    PageId pid;
    memcpy(&pid, buffer+(getKeyCount()*L_PAIR_SIZE),sizeof(PageId));
    return pid;
}

/*
 * Set the pid of the next sibling node.
 * @param pid[IN] the PageId of the next sibling node 
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTLeafNode::setNextNodePtr(PageId pid)
{
    if(pid<0)
        return RC_INVALID_PID;
    memcpy(buffer+(getKeyCount()*L_PAIR_SIZE),&pid,sizeof(PageId));
    return 0;
}

void BTLeafNode::print()
{
    char* temp=buffer;
    for(int i=0;i<getKeyCount();i++)
    {
        int tempInt;
        memcpy(&tempInt,temp,sizeof(int));
        cout<<"key: "<<tempInt<<endl;
        temp+=L_PAIR_SIZE;
    }
}

/*
 *Constructor for BTNonLeafNode
 * initializes variables
 */
BTNonLeafNode::BTNonLeafNode()
{
    //zero out buffer
    memset(buffer, '\0', PageFile::PAGE_SIZE);
    memset(buffer,0,sizeof(int));
}

/*
 * Read the content of the node from the page pid in the PageFile pf.
 * @param pid[IN] the PageId to read
 * @param pf[IN] PageFile to read from
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::read(PageId pid, const PageFile& pf)
{ 
    return pf.read(pid,buffer);
}
    
/*
 * Write the content of the node to the page pid in the PageFile pf.
 * @param pid[IN] the PageId to write to
 * @param pf[IN] PageFile to write to
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::write(PageId pid, PageFile& pf)
{ 
    return pf.write(pid,buffer);
}

/*
 * Return the number of keys stored in the node.
 * @return the number of keys in the node
 */
int BTNonLeafNode::getKeyCount()
{ 
    int numKeys;
    memcpy(&numKeys,buffer,sizeof(int));
    return numKeys;
}

/*
 * Returns the maximum number of keys that can be stored in the node
 * @return the max keys that can be stored in the node 
 */
int BTNonLeafNode::getMaxKeys()
{
    //maxPairs should be 127
    //subtract sizeof numKeys and left pageid
    int maxPairs=floor((PageFile::PAGE_SIZE-sizeof(int)-sizeof(PageId))/(NL_PAIR_SIZE));
    return maxPairs-1;
}

/*
 * Insert a (key, pid) pair to the node.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @return 0 if successful. Return an error code if the node is full.
 */
RC BTNonLeafNode::insert(int key, PageId pid)
{ 
    int numKeys=getKeyCount();
    //if adding another key would go over the max key count return that
    //node is full
    if(numKeys+1>getMaxKeys())
        return RC_NODE_FULL;

    //find the index of buffer where we want to insert
    int insertIndex;
    locate(key,insertIndex);

    //if key was found we will get index of key
    //else we obtain the index of the first key that is larger
    //we insert there and shift everything over

    //create a new buffer where we will copy everything to and zero it out
    char* buffer2=(char*)malloc(PageFile::PAGE_SIZE);
    memset(buffer2, '\0', PageFile::PAGE_SIZE);

    //copy buffer up to where we want to insert
    memcpy(buffer2,buffer,insertIndex);

    //insert (key, pid) pair to buffer
    memcpy(buffer2+insertIndex,&key,sizeof(int));
    memcpy(buffer2+insertIndex+sizeof(int),&pid,sizeof(PageId));

    //copy the rest of the original buffer into buffer2
    memcpy(buffer2+insertIndex+sizeof(int)+sizeof(PageId),buffer+insertIndex,(PageFile::PAGE_SIZE-insertIndex-sizeof(int)-sizeof(PageId)));

    //replace old buffer with new buffer(buffer2)
    memcpy(buffer,buffer2,PageFile::PAGE_SIZE);

    //free up buffer2
    free(buffer2);

    //set buffer's first 4 bytes to numKeys
    numKeys++;
    memcpy(buffer,&numKeys,sizeof(int));
    
    //if we reach here we are successful
    return 0;
}

/*
 * Insert the (key, pid) pair to the node
 * and split the node half and half with sibling.
 * The middle key after the split is returned in midKey.
 * @param key[IN] the key to insert
 * @param pid[IN] the PageId to insert
 * @param sibling[IN] the sibling node to split with. This node MUST be empty when this function is called.
 * @param midKey[OUT] the key in the middle after the split. This key should be inserted to the parent node.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::insertAndSplit(int key, PageId pid, BTNonLeafNode& sibling, int& midKey)
{ 
    //steps:
    //insert into node
    //copy second half into sibling
    //copy middle key to midKey and remove from the node
    //other functions will insert midKey and point it to sibling node

    //assumed that you call only when there is overflow
    //implied that there is getMaxKeys() +1 keys
    //if node is not full return error
    int keyCount=getKeyCount();

    if(getMaxKeys()>keyCount+1)
        return RC_INVALID_ATTRIBUTE;

    //sibling should be a new, empty node
    if (sibling.getKeyCount()!=0)
        return RC_INVALID_ATTRIBUTE;

    //find the index of buffer where we want to insert
    int insertIndex;
    locate(key,insertIndex);

    //create a new buffer where we will copy everything to and zero it out
    char* buffer2=(char*)malloc(2*(PageFile::PAGE_SIZE));
    memset(buffer2, '\0', (2*PageFile::PAGE_SIZE));

    //copy buffer up to where we want to insert
    memcpy(buffer2,buffer,insertIndex);

    //insert (key, pid) pair to buffer
    memcpy(buffer2+insertIndex,&key,sizeof(int));
    memcpy(buffer2+insertIndex+sizeof(int),&pid,sizeof(PageId));
    
    //copy the rest of the original buffer into buffer2
    memcpy(buffer2+insertIndex+sizeof(int)+sizeof(PageId),buffer+insertIndex,(PageFile::PAGE_SIZE-insertIndex));
 
    //ceiling so the first node will have more than the second
    double dKey=keyCount;

    double first=ceil((dKey+1)/2);

    //shift over due to the numKeys being placed at the start of the buffer
    //buffer+splitIndex will be the start of the left pageid of the split key
    int splitIndex=(first*NL_PAIR_SIZE)+sizeof(int);

    //replace old buffer with new buffer
    memcpy(buffer,buffer2,splitIndex);

    //replace sibling buffer with new buffer
    memcpy(sibling.buffer+sizeof(int),buffer2+splitIndex,PageFile::PAGE_SIZE+NL_PAIR_SIZE-splitIndex);
    
    //zero out the rest of buffer
    memset(buffer+splitIndex,'\0',PageFile::PAGE_SIZE-splitIndex);

    //zero out the rest of sibling.buffer
    memset(sibling.buffer+(PageFile::PAGE_SIZE+NL_PAIR_SIZE-splitIndex+sizeof(PageId)),'\0',splitIndex-NL_PAIR_SIZE-sizeof(PageId));

    //set sibling's numKey
    int siblingNumKey=keyCount+1-first;
    memcpy(sibling.buffer,&siblingNumKey,sizeof(int));

    //set node's new numKey
    int newKey=first-1;
    memcpy(buffer,&newKey,sizeof(int));

    //free the temp buffer
    free(buffer2);

    //Now that we have split and inserted, we need to set midKey
    //and remove it from the node

    //set midKey
    int midKeyPos=sizeof(int)+((first-1)*NL_PAIR_SIZE)+sizeof(PageId);
    memcpy(&midKey,buffer+midKeyPos,sizeof(int));

    //remove midKey from the node. It shouldn't have a pageid next to it
    //because we should have given that to sibling
    memset(buffer+midKeyPos,'\0',sizeof(int));

    return 0;
}

/**
 * If searchKey exists in the node, set eid to the index entry
 * with searchKey and return 0. If not, set eid to the index entry
 * immediately after the largest index key that is smaller than searchKey,
 * and return the error code RC_NO_SUCH_RECORD.
 * Remember that keys inside a B+tree node are always kept sorted.
 * @param searchKey[IN] the key to search for.
 * @param eid[OUT] the index entry number with searchKey or immediately
                   behind the largest key smaller than searchKey.
 * @return 0 if searchKey is found. Otherwise return an error code.
 */
RC BTNonLeafNode::locate(int searchKey, int& eid)
{ 
    //this will give us the index in buffer of the key
    //to obtain the pageid we shift over.
    int i;

    for(i=0;i<getKeyCount();i++)
    {
        int currKey;
        //starting pos shifted by pageid and the 4 bytes that store the number of keys
        //and the pairs we already went through
        memcpy(&currKey,buffer+(i*NL_PAIR_SIZE)+sizeof(PageId)+sizeof(int),sizeof(int));
        if(searchKey==currKey)
        {
            eid=(i*NL_PAIR_SIZE)+sizeof(PageId)+sizeof(int);
            return 0;
        }
        else if(searchKey<currKey)
        {
            eid=(i*NL_PAIR_SIZE)+sizeof(PageId)+sizeof(int);
            return RC_NO_SUCH_RECORD;
        }
    }
    //if still room in the node, return index of open space
    if(i<getMaxKeys())
    {
        eid=(i*NL_PAIR_SIZE)+sizeof(PageId)+sizeof(int);
        return RC_NO_SUCH_RECORD;
    }

    //no more room in node
    //we did not find searchKey and there are no keys larger than it 
    //we set eid to the index to insert at, the index
    eid=((i)*NL_PAIR_SIZE)+sizeof(PageId)+sizeof(int);

    return RC_NO_SUCH_RECORD; 
}

/*
 * Given the searchKey, find the child-node pointer to follow and
 * output it in pid.
 * @param searchKey[IN] the searchKey that is being looked up.
 * @param pid[OUT] the pointer to the child node to follow.
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::locateChildPtr(int searchKey, PageId& pid)
{ 
    //numKeys,pageid,key,pageid,key,...pageid
    int tmpKey;
    PageId tmpPid;
    int i;

    int numKeys=getKeyCount();
    for(i=0;i<numKeys;i++)
    {
        memcpy(&tmpKey,buffer+(i*NL_PAIR_SIZE)+sizeof(PageId)+sizeof(int),sizeof(int));
        //if searchKey < tmpKey we take pid to the left of tmpKey
        if(searchKey<tmpKey)
        {
            memcpy(&tmpPid,buffer+(i*NL_PAIR_SIZE)+sizeof(int),sizeof(PageId));
            pid=tmpPid;
            return 0;
        }
        //if we are on the last key we shift to the last pid
        //if we are on the last key and reach here, searchKey must be > all keys in
        //the node
        if(i==numKeys-1)
        {
            memcpy(&tmpPid,buffer+((i+1)*NL_PAIR_SIZE)+sizeof(int),sizeof(PageId));
            pid=tmpPid;
            return 0;
        }
    }


    return RC_NO_SUCH_RECORD;
}

/*
 * Initialize the root node with (pid1, key, pid2).
 * @param pid1[IN] the first PageId to insert
 * @param key[IN] the key that should be inserted between the two PageIds
 * @param pid2[IN] the PageId to insert behind the key
 * @return 0 if successful. Return an error code if there is an error.
 */
RC BTNonLeafNode::initializeRoot(PageId pid1, int key, PageId pid2)
{ 
    //zero out everything
    memset(buffer, '\0', PageFile::PAGE_SIZE);

    //new roots only have 1 key
    int numKeys=1;
    memcpy(buffer,&numKeys,sizeof(int));

    //insert root's pids and key
    //numKeys,pid,key,pid
    memcpy(buffer+sizeof(int),&pid1,sizeof(PageId));
    memcpy(buffer+sizeof(int)+sizeof(PageId),&key,sizeof(int));
    memcpy(buffer+sizeof(int)+sizeof(PageId)+sizeof(int),&pid2,sizeof(PageId));

    return 0;
}

void BTNonLeafNode::print()
{
    return;
    char* temp=buffer;
    int keyss=getKeyCount();
    for(int i=0;i<1024;i+=sizeof(int))
    {
        int tempInt;
        memcpy(&tempInt,temp,sizeof(int));
        cout<<tempInt<<" ";
        temp+=sizeof(int);
    }
    cout<<endl;
}