  /**
 * Copyright (C) 2008 by The Regents of the University of California
 * Redistribution of this file is permitted under the terms of the GNU
 * Public License (GPL).
 *
 * @author Junghoo "John" Cho <cho AT cs.ucla.edu>
 * @date 3/24/2008
 */
#include <climits>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <iostream>
#include <fstream>
#include "Bruinbase.h"
#include "SqlEngine.h"

using namespace std;

// external functions and variables for load file and sql command parsing 
extern FILE* sqlin;
int sqlparse(void);
#define fueraBichos 0
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

  // open the table file
  if ((rc = rf.open(table + ".tbl", 'r')) < 0) {
    fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
    return rc;
  }

  // scan the table file from the beginning
  rid.pid = rid.sid = 0;
  count = 0;
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
  /* your code here */

  return 0;
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



RC SqlEngine::indexSelect(int attr, const std::string& table, BTreeIndex &btree, const vector<SelCond>& cond) {
 
  IndexCursor cursor;     
  IndexCursor end_cursor; 
  RecordFile rf;          
  RecordId rid;          
  string value;
  int key;

  int rc = 0;
  rid.pid = 0; 
  rid.sid = 0; 
  
  
  int lowerKey = INT_MIN;  
  int upperKey = INT_MAX;
  int key_exact = 0;      
  bool EQ_active = false; 
  bool NEQ_active = false;
  vector<int> keyValues; 
  
  
  string val_exact = "";
  bool val_cond = false;
  bool val_NEQ_active = false;
  bool val_EQ_active = false;
  vector<string> NEQ_Values;

  
  bool fatal_error = false;

  
  if ((rc = rf.open(table + ".tbl", 'r')) < 0) {
    fprintf(stderr, "Error: table %s does not exist\n", table.c_str());
    return rc;
  }

  
  for (unsigned i = 0; i < cond.size(); i++) {
    if (cond[i].attr == 1) { 
      int current_key = atoi(cond[i].value);
      switch (cond[i].comp) {
        case SelCond::EQ:
          if (EQ_active && key_exact != current_key) 
            fatal_error = true;
          else if (NEQ_active && key_exact == current_key) 
            fatal_error = true;

          key_exact = current_key;    
          EQ_active = true;

          break;
        case SelCond::NE:
          if (EQ_active && key_exact == current_key)  
            fatal_error = true;

          keyValues.push_back(current_key);
          NEQ_active = true;

          break;
        case SelCond::GT: 
          current_key++; 
          if (lowerKey < current_key)
            lowerKey = current_key;

          break;
        case SelCond::GE:
          if (lowerKey < current_key)
            lowerKey = current_key;
          
          break;
        case SelCond::LT: 
          current_key--;
          if (upperKey > current_key)
            upperKey = current_key;
          
          break;
        case SelCond::LE:
          if (upperKey > current_key)
            upperKey = current_key;
          
          break;
      }
    } else { 
      val_cond = true;
     
      string current_value(cond[i].value);
      switch (cond[i].comp) {
        case SelCond::EQ:
          if (val_EQ_active && val_exact != current_value) 
            fatal_error = true;
          else if (val_NEQ_active && val_exact == current_value)  
            fatal_error = true;

          val_exact.assign(cond[i].value);
          val_EQ_active = true;

          break;
        case SelCond::NE:
        if (val_EQ_active && val_exact == current_value) 
            fatal_error = true;

          NEQ_Values.push_back(current_value);
          val_NEQ_active = true;

          break;
        default:
          if (fueraBichos) printf("\nInvalid condition %d (which is not EQ or NEQ) detected with attr=2",cond[i].comp);
          fatal_error = true;
          break;
      }
    }
  }

  if (fueraBichos) printf("SQLENGINE -- Conditions have been determined:\n");
  if (fueraBichos) printf("lowerKey = %d, upperKey = %d, key_exact = %d;\n", lowerKey, upperKey, key_exact);
  if (fueraBichos) printf("EQ_active = %d, NEQ_active = %d\n", EQ_active, NEQ_active);


  if (!fatal_error) {

    if (!EQ_active) { 
      btree.locate(lowerKey, cursor, 0);
      btree.locate(upperKey, end_cursor, 0);

      if (fueraBichos) printf("EQuality NOT active, located key min at cursor.pid = %d, cursor.eid = %d\n", cursor.pid, cursor.eid);
      if (fueraBichos) printf("EQuality NOT active, located key max at end_cursor.pid = %d, end_cursor.eid = %d\n", end_cursor.pid, end_cursor.eid);


    } else { 
      btree.locate(key_exact, cursor, 0);

      if (fueraBichos) printf("EQuality active, located key_exact at cursor.pid = %d, cursor.eid = %d\n", cursor.pid, cursor.eid);

      btree.readForward(cursor, key, rid); 

      if (key == key_exact) {
        if ((rc = rf.read(rid, key, value)) < 0) {
          fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
          rf.close();
          return rc;
        }


        switch (attr) {
            case 1: 
              fprintf(stdout, "%d\n", key);
              break;
            case 2:  
              fprintf(stdout, "%s\n", value.c_str());
              break;
            case 3:  
              fprintf(stdout, "%d '%s'\n", key, value.c_str());
              break;
            case 4: 
              fprintf(stdout, "%d\n", 1);  
              break;
        }
      } else {

      }

      rf.close();
      return 0;
    }

    const int neq_val_len = NEQ_Values.size();
    const int neq_key_len = keyValues.size();

    int count = 0;

    while(btree.readForward(cursor, key, rid) == 0) {

  
      if (fueraBichos) printf("readForward, currently key = %d, rid.pid = %d, rid.sid = %d\n", key, rid.pid, rid.sid);
      if (fueraBichos) printf("Searching, lower bound at cursor.pid = %d, cursor.eid = %d\n", cursor.pid, cursor.eid);
      if (fueraBichos) printf("Searching, upper bound at end_cursor.pid = %d, end_cursor.eid = %d\n", end_cursor.pid, end_cursor.eid);
      if (fueraBichos) printf("Searching, currently read count = %d\n", count);

      if (attr != 4) { 
        if ((rc = rf.read(rid, key, value)) < 0) {
          fprintf(stderr, "Error: while reading a tuple from table %s\n", table.c_str());
          rf.close();
          return rc;
        }
      }


      bool violation = false;


      if (NEQ_active) {
        for (int i = 0; i < neq_key_len; i++) {
            if (keyValues[i] == key) 
              violation = true; 
        }
      }

      if (val_NEQ_active) {
        for (int i = 0; i < neq_val_len; i++) {
            if (NEQ_Values[i] == value) 
              violation = true;
        }
      }

      if (!violation) {

        switch (attr) {
            case 1:  
              fprintf(stdout, "%d\n", key);
              break;
            case 2:  
              fprintf(stdout, "%s\n", value.c_str());
              break;
            case 3:  
              fprintf(stdout, "%d '%s'\n", key, value.c_str());
              break;
        }
        count++;
      } else {
        printf("There is violation in the query\n");
        continue; 
      }

      if ((cursor.pid == end_cursor.pid) && (cursor.eid == end_cursor.eid)) { 


        break; 
      }
    }

    if (attr == 4) {
      fprintf(stdout, "%d\n", count);
    }
  } else {
    printf("\nSelect error");
  }

  rf.close();
  return 0;
}
