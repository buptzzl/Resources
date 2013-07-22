/*
 * =====================================================================================
 *
 *       Filename:  main.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  12/20/10 17:26:16
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (), 
 *        Company:  
 *
 * =====================================================================================
 */

#include <unistd.h>
#include <stdio.h>
#include <vector>
#include <string>

#include "tairFileImporter.h"
#include <dirent.h>
#include <sys/types.h>
using namespace std;
//

struct Config{
    string fileName;
    string resultPath;
    string separator;
    int threadCount;
    int repeatNum;
    int maxRowBeforeSleep;
    Tair tair;
    int expire;
    int version;

    int parseArgument(int argc,char *argv[]){
        int ch = 0;
	threadCount = 1;
        repeatNum = 2;
	separator = "";
	resultPath = string("/dmp/data");
        while((ch = getopt(argc,argv,"d:p:l:m:s:g:c:r:e:M:vh"))!=-1) {
            switch(ch){
                case 'd':
                    fileName = optarg;
                    break;
		case 'p':
		    separator = optarg;
		    break;	
		case 'l':
		    resultPath = optarg;
		    break;	
  /*              case 'a':
                    tair.area = atoi(optarg);
                    break;
    */          case 'm':
                    tair.master = optarg;
                    break;
                case 's':
                    tair.slave = optarg;
                    break;
                case 'g':
                    tair.group = optarg;
                    break;
                case 'c':
                    threadCount = atoi(optarg);
		    if (threadCount == 0) //default is 1
			threadCount =1;
                    break;
                case 'r':
                    repeatNum = atoi(optarg);
                    if (repeatNum == 0)// default is 2
                        repeatNum = 2;
                    break;
                case 'e':
                    expire = atoi(optarg);
                    break;
                case 'M':
                    maxRowBeforeSleep = atoi(optarg);
                    break;
                case 'v':
                    printf("tail import client : version 1.0.0\n");
                    return 0;
                case 'h':
                    return 1;
                case '?':
                    return -1;
            }

        }
        return 0;

    }

    int verify(){
        return 0;
    }

};


int usage(char *cmd){
    printf("usage: %s [<options>]\n",cmd);
    printf("Where <options> are: \n");
    printf("  -d <dirname>\t\tthe dir to import\n");
    printf("  -p <dirname>\t\tseparator of line, default is [CTRL A]\n");
    printf("  -l <resultpath>\t\tprocess logpath, default is /dmp/data/\n");
//    printf("  -a <area>\t\tnamespace to import\n");
    printf("  -m <master addr>\tmaster configserver addr [ip:port], default port is 5198\n");
    printf("  -s <slave addr>\tslave configserver addr [ip:port], default port is 5198\n");
    printf("  -g <group>\t\tgroup name\n");
    printf("  -c <concurrency>\tNumber of thread to import, default is 1\n");
    printf("  -r <repeat numbers>\trepeat numbers to put to tair, default is 2\n");
    printf("  -e <expire>\t\texpire time, default will never expire\n");
    printf("  -M <rowBeforeSleep>\tafter import rowBeforeSleep key-value pairs, sleep. default is 1000\n");
    printf("  -v <version>, if version > 0, using enhanced version, else using the old version\t\n");
    printf("  -h <help>\t\n\n\n");

    return 0;
}

int main(int argc,char *argv[]){
	TBSYS_LOGGER.setLogLevel("ERROR");
	Config config;
	vector<string> vecFiles;
	if(config.parseArgument(argc,argv) || config.verify()){
		usage(argv[0]);
		return -1;
	}
	DIR    *dir;
	char    fullpath[1024];
	struct    dirent    *s_dir;
	memset(fullpath,0,sizeof(fullpath));
	strcpy(fullpath,config.fileName.c_str());
	if(0 == access(fullpath,F_OK)){
		dir=opendir(fullpath);
		if(dir == NULL)
		{
			printf( "open dir %s error\n",config.fileName.c_str());
			return -1;
		}
		while((s_dir=readdir(dir))!=NULL){
			if((strcmp(s_dir->d_name,".")==0)||(strcmp(s_dir->d_name,"..")==0))
				continue;
			char currfile[1024];
			memset(currfile,0,sizeof(currfile));
			sprintf(currfile,"%s/%s",fullpath,s_dir->d_name);
			string fileName(currfile);
			if(strncmp(s_dir->d_name,"0",1) == 0){
				vecFiles.push_back(fileName);		
			}
		}
		closedir(dir);
		TairFileImporter importer(config.resultPath,config.separator, config.tair, config.threadCount, config.repeatNum, config.expire, 0,config.maxRowBeforeSleep);
		importer.import(vecFiles);
	}
	else{
		printf("directory %s is not exist",fullpath);
		return -1;
	}
	return 0;
}
