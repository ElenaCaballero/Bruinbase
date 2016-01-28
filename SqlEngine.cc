/**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */

#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include "Bruinbase.h"
#include "SqlEngine.h"
#include "BTreeIndex.h"

using namespace std;

// external functions and variables for load file and sql command parsing 
extern FILE* sqlin;
int sqlparse(void);


RC SqlEngine::run(FILE* commandline)
{
  fprintf(stdout, "Bruinbase> ");

  // set the command line input and start parsing user input
  sqlin = commandline;
  sqlparse();  // sqlparse() is defined in SqlParser.tab.c generated from
               // SqlParser.y by bison (bison is GNU equivalent of yacc)

  return 0;
}

RC SqlEngine::select(int attr, const string& table, const vector<SelCond>& cond)
{
  RecordFile rf;   // RecordFile containing the table
  RecordId   rid;  // record cursor for table scanning

  RC     rc;
  int    key;     
  string value;
  int    count;
  int    diff;

  BTreeIndex btree;
  rid.pid = 0;
  rid.sid = 1;
  count = 0;
  IndexCursor cursor;
  cursor.eid=-1;
  cursor.pid=-1;

  // open the table file
  if ((rc = rf.open(table + ".tbl", 'r')) < 0) {
    fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
    return rc;
  }
    int startKey=0;
    bool startKeyInit=false;
    bool key_inequality=false;
    bool key_condition_exist=false;
    bool avoid_index=false;
    bool eq_set=false;
    int eq_val;
    bool gt_set=false;
    int gt_val;
    bool ge_set=false;
    int ge_val;
    bool count_from_beginning=true;
    bool count_or_not_to_count=false;
    int temp;

    //conditions
    for(int i=0;i<cond.size();i++)
    {
      //if is a key, do, else nothing
      if(cond[i].attr==1)
      {
        if(cond[i].comp==SelCond::NE)
        {
          key_inequality=true;
          continue;
        }
        else
          key_condition_exist=true;

        if(cond[i].comp==SelCond::EQ)
        {
          count_from_beginning=false;
          temp=atoi(cond[i].value);

          //set variables
          startKey=temp;
          startKeyInit=true;
 
          continue;
        }
        else if(cond[i].comp==SelCond::GE)
        {
          count_from_beginning=false;
          temp=atoi(cond[i].value);
          if(startKeyInit)
          {
            if(temp>startKey)
                startKey=temp;
          }
          else
          {
       
            startKey=temp;
            startKeyInit=true;
          }
        }
        else if(cond[i].comp==SelCond::GT)
        {

          count_from_beginning=false;
          temp=atoi(cond[i].value)+1;

          if(startKeyInit)
          {
            if(temp>startKey)
                startKey=temp;
          }
          else
          {
 
            startKey=temp;
            startKeyInit=true;
          }
        }
      }
    }

    //cond on value and not on keys means have to start at beginning
    //only saves time on small datasets
     if(attr==2&&!key_condition_exist)
    {
      avoid_index=true;
    }

    //no key conditions besides key inequality
    if(!key_condition_exist && key_inequality)
    {
      avoid_index=true;
    }

    if(attr==4||attr==3)
    {
      count_or_not_to_count=true;
    }

    //checks done
    if(count_from_beginning&&count_or_not_to_count)
    {
      cursor.eid=0;
      cursor.pid=0;
    }

  //index exists and we dont want to avoid the index
  if(!btree.open(table + ".idx",'r')&&!avoid_index)
  {
      btree.locate(startKey,cursor);

    while(btree.readForward(cursor,key,rid)==0)
    {
      //count(*) doesnt need values
      if(attr!=4)
      {
        if ((rc = rf.read(rid, key, value)) < 0) 
        {
          fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
          goto exit_select;
        }
      }

      for(unsigned i=0;i<cond.size();i++)
      {
        if(cond[i].attr==1)
        {
          diff=key-atoi(cond[i].value);
        }
        else if(cond[i].attr==2)
        {
          diff=strcmp(value.c_str(),cond[i].value);
        }

        switch(cond[i].comp)
        {
          case SelCond::EQ:
            if(diff!=0)
            {
              //key, we started from this key, if wasn't equal we done
              if(cond[i].attr==1)
                goto early_exit_select;
              else//values not sorted so must continue
                goto continue_check;
            }
            break;

          case SelCond::NE:
            if(diff==0)
              goto continue_check;
            break;

          case SelCond::GT:
            if(diff<=0)
              goto continue_check;
            break;

          case SelCond::LT:
            if(diff>=0)
            {
              //if key, then we hit max 
              if(cond[i].attr==1)
                goto early_exit_select;
              goto continue_check;
            }
            break;

          case SelCond::GE:
            if(diff<0)
              goto continue_check;
            break;

          case SelCond::LE:
            if(diff>0)
            {
              //if key, then we hit max
              if(cond[i].attr==1)
                goto early_exit_select;
              goto continue_check;
            }
            break;
        }

      }
       
      // the condition is met for the tuple. 
      // increase matching tuple counter
      count++;

      // print the tuple 
      switch (attr) {
      case 1:  // SELECT key
        fprintf(stdout, "%d\n", key);
        break;
      case 2:  // SELECT value
        fprintf(stdout, "%s\n", value.c_str());
        break;
      case 3:  // SELECT *
        fprintf(stdout, "%d '%s'\n", key, value.c_str());
        break;
      }
       continue_check:
       if(cursor.eid==-1)
        {
          break;
        }
    }
  }
  else//index doesnt exist, default implementation
  {
    // scan the table file from the beginning

    while (rid < rf.endRid()) {
      // read the tuple
      if ((rc = rf.read(rid, key, value)) < 0) {
        fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
        goto exit_select;
      }

      // check the conditions on the tuple
      for (unsigned i = 0; i < cond.size(); i++) {
        // compute the difference between the tuple value and the condition value
        switch (cond[i].attr) {
        case 1:
  	diff = key - atoi(cond[i].value);
  	break;
        case 2:
  	diff = strcmp(value.c_str(), cond[i].value);
  	break;
        }

        // skip the tuple if any condition is not met
        switch (cond[i].comp) {
        case SelCond::EQ:
  	if (diff != 0) goto next_tuple;
  	break;
        case SelCond::NE:
  	if (diff == 0) goto next_tuple;
  	break;
        case SelCond::GT:
  	if (diff <= 0) goto next_tuple;
  	break;
        case SelCond::LT:
  	if (diff >= 0) goto next_tuple;
  	break;
        case SelCond::GE:
  	if (diff < 0) goto next_tuple;
  	break;
        case SelCond::LE:
  	if (diff > 0) goto next_tuple;
  	break;
        }
      }

      // the condition is met for the tuple. 
      // increase matching tuple counter
      count++;

      // print the tuple 
      switch (attr) {
      case 1:  // SELECT key
        fprintf(stdout, "%d\n", key);
        break;
      case 2:  // SELECT value
        fprintf(stdout, "%s\n", value.c_str());
        break;
      case 3:  // SELECT *
        fprintf(stdout, "%d '%s'\n", key, value.c_str());
        break;
      }

      // move to the next tuple
      next_tuple:
      ++rid;
    }
  }

  //early exit, invalid but still possibly need to print count(*)
  early_exit_select:

  // print matching tuple count if "select count(*)"
  if (attr == 4) {
    fprintf(stdout, "%d\n", count);
  }
  rc = 0;

  // close the table file and return
  exit_select:
  rf.close();
  return rc;
}

RC SqlEngine::load(const string& table, const string& loadfile, bool index)
{
    RC rc;
    RecordFile rf;
    fstream fs;
    int key;
    string val;
    RecordId rid;
    string line;
    BTreeIndex btree;

    //open stream
    fs.open(loadfile.c_str(),fstream::in);

    if(!fs.is_open())
      fprintf(stderr,"Error: Could not open %s\n",loadfile.c_str());

    //open record file in write mode. on fail return
    if(rf.open(table + ".tbl", 'w'))
        return RC_FILE_OPEN_FAILED;

    //if index is true use B+ tree index
    if(index)//should work but double check
    {
        rc=rf.append(key,val,rid);
      int iterator=0;
      rc=btree.open(table + ".idx",'w');
      if(!rc)
      {
        int iterator=0;

        while(getline(fs,line))
        {
          rc=parseLoadLine(line,key,val);
          if(rc)
            break;

          rc=rf.append(key,val,rid);
          if(rc)
            break;

          rc=btree.insert(key,rid);
          if(rc)
            break;
        }
        //close tree
        btree.close();
      }
    }
    else{//no index
      //get lines, parse them, and append them
      while(!fs.eof()){
          getline(fs, line);

          //parse line, on failure rc will be set to errorcode
          rc=parseLoadLine(line, key, val);
          if(rc)
              break;

          //append, on failure rc will be set to errorcode
          rc=rf.append(key, val, rid);
          if(rc)
              break;
      }
    }

    //close stream
    fs.close();

    if(rf.close())
        return RC_FILE_CLOSE_FAILED;

    //return 0 if loaded properly and errorcode on failure
    return rc;
}

RC SqlEngine::parseLoadLine(const string& line, int& key, string& value)
{
    const char *s;
    char        c;
    string::size_type loc;
    
    // ignore beginning white spaces
    c = *(s = line.c_str());
    while (c == ' ' || c == '\t') { c = *++s; }

    // get the integer key value
    key = atoi(s);

    // look for comma
    s = strchr(s, ',');
    if (s == NULL) { return RC_INVALID_FILE_FORMAT; }

    // ignore white spaces
    do { c = *++s; } while (c == ' ' || c == '\t');
    
    // if there is nothing left, set the value to empty string
    if (c == 0) { 
        value.erase();
        return 0;
    }

    // is the value field delimited by ' or "?
    if (c == '\'' || c == '"') {
        s++;
    } else {
        c = '\n';
    }

    // get the value string
    value.assign(s);
    loc = value.find(c, 0);
    if (loc != string::npos) { value.erase(loc); }

    return 0;
}
