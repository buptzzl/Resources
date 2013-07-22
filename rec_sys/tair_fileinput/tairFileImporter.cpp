/*
 * =====================================================================================
 *
 *       Filename:  tairFileImporter.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  12/17/10 11:00:32
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (), 
 *        Company:  
 *
 * =====================================================================================
 */

#include <unistd.h>
#include <time.h>
#include <sys/types.h>
#include <sys/syscall.h> 
#include "tairFileImporter.h"
#define YIQIFA 1
#define YIGAO 2
#define EMBOX 3
#define GOUWUKE 4
#define GOOGLE 5010
#define TANX 5020
#define TENCENT 5030
#define ADEXCHANGE 50
#define ADNETWORK 40
#define SHEQU 30
#define MEDIA 20
#define MAX_KEY_SIZE 1024
#define MAX_VAL_SIZE 1000000
TairFileImporter::TairFileImporter( const string &resultpath, const string &separator, const Tair &tair,
        int nThreadCount, int repeatNum, int expire, int version, int maxRowBeforeSleep){
    m_resultpath = resultpath;
    m_separator = separator;
    m_threadCount = nThreadCount;
    m_repeatNum = repeatNum;
    m_tair = tair;
    m_maxRowBeforeSleep = maxRowBeforeSleep;
    m_expire = expire + time(NULL);
    m_version = version;
}

int TairFileImporter::import(vector<string> files){
    vecFiles = files;
    m_resultpath += "/filelist.dat";
    m_filestream = fopen(m_resultpath.c_str(),"w");
    if(m_filestream == NULL){
	printf("[%s]:error on create filelist in %s\n",getTime(),m_resultpath.c_str());
	return -1;
    }
    if (pthread_mutex_init(&m_lock,NULL)) {
        printf("init lock error\n");
        return -1;
    }
    size_t fileNum = vecFiles.size();
    if(fileNum < m_threadCount ){
	 m_threadCount = fileNum;	
    }
    m_threads = (pthread_t *)malloc(sizeof(pthread_t) * m_threadCount);
    printf("[%s]: begin to import, start %d threads \n", getTime(), m_threadCount);

    ThreadData thdata;
    for (int i = 0; i < m_threadCount; i++) {
	thdata.tair.master = m_tair.master;
	thdata.tair.slave = m_tair.slave;
	thdata.tair.group = m_tair.group;
	thdata.importer = this;
        pthread_create(m_threads + i, NULL, TairFileImporter::importWorkThread, &thdata);
    }
    
    ImportThreadRet *ret = NULL;
    int succeed = 0;

    for (int i = 0; i < m_threadCount; i++) {
        pthread_join(m_threads[i], (void **)&ret);
        if (ret->retNo == -1) {
            succeed = -1;
        } else {
            succeed += ret->failingCount;
        }
        free(ret);
    }

    pthread_mutex_destroy(&m_lock);
    if(m_filestream){
    	fclose(m_filestream);
    }
    printf("[%s]: end import\n", getTime());
    free(m_threads);
    return succeed;
}

void * TairFileImporter::importWorkThread(void *arg) {
    ImportThreadRet *ret = (ImportThreadRet *)malloc(sizeof(ImportThreadRet));	
    memset(ret,0,sizeof(ImportThreadRet));
    ThreadData *thdata = (ThreadData *)arg;
    TairFileImporter * importer = thdata->importer;
    int i = importer->lock();
    string fileName = vecFiles[0];
    vecFiles.erase(vecFiles.begin());
    int j = importer->unlock();
    printf("[%s]: thread %lu start import work \n",importer->getTime(),pthread_self());
    ifstream infile;
    infile.open(fileName.c_str());
    if(infile.is_open()== false)
    {
        printf("[%s]: thread %lu  error: open file %s fail\n", importer->getTime(),pthread_self(),fileName.c_str());
        ret->retNo = -1;
        return (void *)ret;
    }
    tair_client_api *tairClient = new tair_client_api;
    if (!tairClient->startup(thdata->tair.master.c_str(), thdata->tair.slave.c_str(),
                thdata->tair.group.c_str())){
        printf("[%s]: thread %lu  connect to tair error, master is %s, slave is %s and group is %s\n", importer->getTime(),pthread_self(),thdata->tair.master.c_str(), thdata->tair.slave.c_str(),
                thdata->tair.group.c_str());
        ret->retNo = -1;
        return (void *)ret;
    }
    else{
	printf("[%s]: thread %lu  connect to tair success\n",importer->getTime(),pthread_self());	
    }

    int sendCount = 0;
    TairItem item;
    while(1){
    if(importer->getNextRecord(item,thdata->tair,ret,sendCount,tairClient,infile,fileName)==-1){
    	break;
    }
    }
    delete tairClient;

    printf("[%s]: thread %lu end import work,failingCount=%d and successfulCount=%d\n", importer->getTime(),pthread_self(),ret->failingCount,ret->successfulCount);
    return (void *)ret;
}

int TairFileImporter::putTair(const TairItem& item, Tair tair,tair_client_api *tairClient) {
    int ret;
    for (int i = 0; i < m_repeatNum; i++) {
        ret = tairClient->put(tair.area, item.key, item.val, m_expire, m_version);
        if (ret == 0){
        	//printf("[%s]:debug: the [%d]th put to tair[master = %s, slave = %s, group = %s, area = %d], key is %s, value is %s, tair error msg is [%s]\n", getTime(), i, m_tair.master.c_str(), m_tair.slave.c_str(), m_tair.group.c_str(), m_tair.area, item.key.get_data(), item.val.get_data(), tairClient->get_error_msg(ret)); 
	   return 0;
	}
        printf("[%s]: error: the [%d]th put to tair[master = %s, slave = %s, group = %s, area = %d], key is %s, value is %s, tair error msg is [%s], error code is [%d]\n", getTime(), i, tair.master.c_str(), tair.slave.c_str(), tair.group.c_str(), tair.area, item.key.get_data(), item.val.get_data(), tairClient->get_error_msg(ret),ret); 
        /*tairClient->close();

        sleep(1);
        if (!tairClient->startup(tair.master.c_str(),tair.slave.c_str(),tair.group.c_str())) {
            printf("[%s]: error: cannot connect to tair, [master = %s, slave = %s, group = %s, area = %d]\n", getTime(), tair.master.c_str(), tair.slave.c_str(), tair.group.c_str(), tair.area);*/
//        }
    }
    return 1;
}

int TairFileImporter::getNextRecord(TairItem &item, Tair tair, ImportThreadRet *ret,int& sendCount,tair_client_api *tairClient,ifstream&  infile,string& name ){
	printf("[%s]: thread %lu start process file %s \n",getTime(), pthread_self(),name.c_str());
	string line;
	//while((line = _fgets(m_file)) == ""){
	vector<string> vecStr;
	while(getline(infile,line)){
		vecStr.clear();  
		splitString(line,m_separator,vecStr);
		if(vecStr.size()==3){	
			string strkey = vecStr[0];
			string strarea =  vecStr[2];
			string strval = vecStr[1];

			int type = atoi(strarea.c_str());
			if(type == YIQIFA||type == YIGAO||type == EMBOX||type == GOUWUKE){
				tair.area = 2;
			}
			else if(type == GOOGLE||type == TANX||type == TENCENT){
				tair.area = 3;
			}
			else
			{
				printf("[%s]: plate_type is unknown in %s\n",getTime(),line.c_str());
				ret->failingCount++;
				continue;
			}
			//if(tair.area == 2){
				if(strval.size()>= MAX_KEY_SIZE||strkey.size()>=MAX_VAL_SIZE){
					printf("[%s]:key or value too large in %s\n",getTime(),line.c_str());
					ret->failingCount++;
					continue;
				}
				else{
					item.key = data_entry(strval.c_str(),strval.size(),false);
					item.val = data_entry(strkey.c_str(),strkey.size(),false);
				}
			//}
			/*else if(tair.area == 3){
				if(strkey.size()>= MAX_KEY_SIZE||strval.size()>=MAX_VAL_SIZE){
					printf("[%s]:key or value too large in %s\n",getTime(),line.c_str());
					ret->failingCount++;
                                        continue;
                                }
                                else{
					item.key = data_entry(strkey.c_str(),strkey.size(),false);
					item.val = data_entry(strval.c_str(),strval.size(),false);
				}	
			}*/
			if(item.key.get_data()==NULL||item.val.get_data()==NULL)
			{
				printf("[%s]: %s format error\n",getTime(),line.c_str());
				ret->failingCount++;
				continue;
			}
			if (putTair(item,tair, tairClient)) {
				ret->failingCount++;
			}
			else {
				ret->successfulCount++;
			}

			sendCount++;
			if(sendCount >= m_maxRowBeforeSleep){
				sendCount = 0;
				sleep(1);
			}
			continue;
		}
		printf("[%s]: %s format error\n",getTime(),line.c_str());
		ret->failingCount++;
		continue;
	}
	infile.close();
	printf("[%s]: thread %lu process file [%s] end :failingCount=%d and successfulCount=%d\n",getTime(),pthread_self(),name.c_str(),ret->failingCount,ret->successfulCount);
	fprintf(m_filestream,"%s %d %d\n",name.c_str(),ret->failingCount,ret->successfulCount);
	fflush(m_filestream); 
	if(vecFiles.size() > 0){
		lock();
		string fileName = vecFiles[0];
		vecFiles.erase(vecFiles.begin());
		unlock();
		if (access(fileName.c_str(), R_OK)) {
                        printf("[%s]: error: can't access the file %s\n", getTime(), fileName.c_str());
                        return -1;
                }

                infile.open(fileName.c_str());
                if(infile.is_open()== false)
                {
                        printf("[%s]: error: open file %s fail\n",getTime(), fileName.c_str());
                        return -1;
                }
		ret->failingCount = 0;
		ret->successfulCount = 0;
		return getNextRecord(item, tair, ret, sendCount, tairClient, infile,fileName);
	}
	return -1;
}
void TairFileImporter::splitString(const string& input, const string& numerics, vector<string>& output)
{
	string::size_type pos = 0, prev_pos = 0;
	while( ( pos = input.find_first_of(numerics, pos) ) != string::npos )
	{
		output.push_back( input.substr(prev_pos, pos-prev_pos) );
		for(size_t i = 1; i< numerics.length(); i++){
			++pos;
		}
		prev_pos = ++pos;
	}
	output.push_back( input.substr(prev_pos) );
}
char *TairFileImporter::_fgets(FILE *file){

    if(feof(file)){
        return NULL;
    }

    strListNode *head = (strListNode *)malloc(sizeof(strListNode));
    head->next = NULL;
    strListNode * currNode = head;

    int fullNode = 0;
    memset(currNode->data,0,currNode->SIZE + 1);

    while(fgets(currNode->data,currNode->SIZE + 1,file)){
        if(strlen(currNode->data) < currNode->SIZE){
            break;
        }	

        currNode->next = (strListNode *)malloc(sizeof(strListNode));
        currNode = currNode->next;
        currNode->next = NULL;
        fullNode++;

    }

    int size = fullNode * strListNode::SIZE + strlen(currNode->data);

    char *ret = (char *)malloc(sizeof(char) * (size + 1));
    char *iter = ret;

    while(head){
        int currentSize = strlen(head->data);
        memcpy(iter,head->data,currentSize);
        iter = iter + currentSize;

        strListNode *next = head->next;
        free(head);
        head = next;
    }

    if(ret[size - 1] == '\n'){
        ret[size - 1] = '\0';
    }
    else{
        ret[size] = '\0';
    }

    return ret;
}

const char* TairFileImporter::getTime()
{
    time_t timer;
    struct tm *tblock;
    timer = time(NULL);
    tblock = localtime(&timer);
    std::string timeStr(asctime(tblock));

    return timeStr.substr(0, timeStr.size() - 1).c_str();
}
