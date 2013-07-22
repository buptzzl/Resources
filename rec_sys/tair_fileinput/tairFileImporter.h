/*
 * =====================================================================================
 *
 *       Filename:  tairClient.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  12/15/10 17:59:45
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (), 
 *        Company:  
 *
 * =====================================================================================
 */
#ifndef _TAIRFILEIMPORTER_H
#define _TAIRFILEIMPORTER_H
#include <stdio.h>
#include <vector>
#include <string>
#include <vector>
#include <set>
#include <ext/hash_map>
#include <fstream>
using std::vector;
using std::set;
using __gnu_cxx::hash_map;

#include <tair_client_api.hpp>
#include <pthread.h>

using namespace std;
using namespace tair;
class TairFileImporter;
struct TairItem{
	data_entry key;
	data_entry val;
};

struct ImportThreadRet{
	int retNo;
	int successfulCount;
	int failingCount;
};

struct strListNode{
	static const size_t SIZE = 1024;
	char data[SIZE + 1];
	strListNode *next;
};

struct Tair {
	std::string master;
	std::string slave;
	std::string group;
	int area;
};

struct ThreadData {
	Tair tair;
	TairFileImporter * importer;
};
static vector<string> vecFiles;
class TairFileImporter{
public:
	TairFileImporter(const string &resultpath, const string &separator, const Tair &tair,int nThreadCount = 1, int repeatNum = 2, int expire = -1,int version = 0,int maxRowBeforeSleep = 1000);
	~TairFileImporter(){}
	int import(vector<string> files);
private:
	int getNextRecord(TairItem &,Tair tair,ImportThreadRet *ret,int& sendCount,tair_client_api *tairClient,ifstream& infile,string& name);
	int getNextRecords(int ncount,vector<TairItem> &);
	int putTair(const TairItem &,Tair tair,tair_client_api *tairClient);
	void splitString(const string& input, const string& numerics, vector<string>& output);
	const char* getTime();
private:
	char *_fgets(FILE *fp);
	int lock(){
		return pthread_mutex_lock(&m_lock);
	}
	int unlock(){
		return pthread_mutex_unlock(&m_lock);
	}
	
private:
	FILE* m_filestream;
	string m_resultpath;
	string m_separator;
	int m_threadCount;
	int m_repeatNum;
	int m_maxRowBeforeSleep;

private:
	Tair m_tair;
	int m_expire;
	int m_version;

private:
	static void* importWorkThread(void * arg);
	pthread_mutex_t m_lock;
	pthread_t *m_threads;
};
#endif  /*_TAIRFILEIMPORTER_H*/
