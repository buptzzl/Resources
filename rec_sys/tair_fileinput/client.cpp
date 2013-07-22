/*
 * =====================================================================================
 *
 *       Filename:  client.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  12/23/10 18:03:50
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (), 
 *        Company:  
 *
 * =====================================================================================
 */

#include <stdio.h>
#include <tair_client_api.hpp>
#include <string>
#include <vector>
#include <fstream>
#include <iostream>
#define YIQIFA 1
#define YIGAO 2
#define EMBOX 3
#define GOUWUKE 4
#define GOOGLE 5010
#define TANX 5020
#define TENCENT 5030
using namespace std;
using namespace tair;
void splitString(const string& input, const string& numerics, vector<string>& output)
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
int main(int argc,char *argv[]){
	TBSYS_LOGGER.setLogLevel("ERROR");
	tair_client_api *m_tairClient = new tair_client_api;
	if(!m_tairClient->startup("122.49.4.213:5198","122.49.4.214:5198","group_1")){
		printf("connect error");
		return -1;
	}
	data_entry* val;
	/*if(!(m_tairClient->get(atoi(argv[2]),data_entry(argv[1]),val))){
               printf("val is %s \n",val->get_data());
        }*/
	int result = m_tairClient->put(atoi(argv[1]),data_entry(argv[2]),data_entry(argv[3]),0,0);
	if(result ==0)
	{
		printf("success\n");
	}
/*	string line;
	ifstream infile("/emaryun/i_base_user/000002_0");
	vector<string> vecStr;
	int count = 0;
	while(getline(infile,line)){
		vecStr.clear();
		splitString(line,"",vecStr);
		if(vecStr.size()==3){
			string strkey = vecStr[0];
			string strarea =  vecStr[1];
			string strval = vecStr[2];
			int type = atoi(strarea.c_str());
			int area = 0;
			if(type == YIQIFA||type == YIGAO||type == EMBOX||type == GOUWUKE){
				area = 2;
			}
			else if(type == GOOGLE||type == TANX||type == TENCENT){
				area = 3;
			}
			else
			{
				continue;
			}	
			data_entry* val;
			if(area == 2){
				if(!(m_tairClient->get(area,data_entry(strval.c_str()),val))){
					count++;
				}
				else
				{
					cout << strval <<endl;
				}
			}
			else if(area == 3){
				if(!(m_tairClient->get(area,data_entry(strkey.c_str()),val))){
					count++;
				}
				else
				{
					cout << strkey <<endl;
				}
			}
			if(count%100000 == 0)
			{
				cout << count <<endl;
			}
		}
	}
	infile.close();
	cout<< count <<endl;
*/	return 0;
}
