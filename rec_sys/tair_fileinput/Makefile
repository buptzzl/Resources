OBJS = tairFileImporter.o

LDPATH=-L/home/zhangyuchao/tair_bin/lib/\
	   -L ../lib64/lib/

INCLUDE=-I/home/zhangyuchao/tair_bin/include/ \
		-I ../lib64/include/tbsys/ \
		-I ../lib64/include/tbnet

LIBS=-ltairclientapi -ltbnet -ltbsys -lpthread

CFLAG = -c -g -Wall $(INCLUDE)
#CFLAG = -c -O3 -Wall $(INCLUDE)

GCC = g++
CC = gcc

.SUFFIXES: .o .cpp


all: tairFileImporter tairclient

version:
	$(GCC) -v

tairFileImporter:main.o $(OBJS)
	$(GCC) $(LDPATH) -o $@ main.o $(OBJS)  $(LIBS)
test: tairFileImporter
	#./tairFileImporter  -f test.input -a 1 -m "10.254.2.67:5198" -s "10.254.2.68:5198" -g "group_1" -c 1 -e 0 -M 1000
	#./tairFileImporter  -f test.input -a 22 -m "10.232.12.141:5198" -s "10.232.12.141:5198" -g "group_1" -c 1 -e 10000 -M 1000
	./tairFileImporter  -f /home/a/search/data/unifiedAdvertising/unifiedAdvertising_adboards.xml -a 0 -m "10.254.2.68:5198" -s "10.254.2.68:5198" -g "group_1" -c 1 -e 10000 -M 1000

tairclient: client.o
	$(GCC) $(LDPATH) -o $@ client.o $(OBJS)  $(LIBS)

.cpp.o:
	$(GCC) $(CFLAG) -c $<

clean:
	rm -rf *.o
	rm -rf tairFileImporter tairclient 
