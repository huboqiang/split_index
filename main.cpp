#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <algorithm>
#include <map>
#include <set>
#include <cmath>
#include <inttypes.h>
#include <zlib.h>
#include <pthread.h>
#include "gzstream.h"
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>

using namespace std;

int threadNum1 = 1;
int len			= 101;
int bar_err 	= 1;
uint64_t bufferNum = 1000000;  //read bufferNum reads as one block into memory each time
int *gotReads1;
uint64_t readNum1;
uint64_t *num_reads1;
uint64_t *num_reads1_undef;
uint64_t *total_reads1;
uint64_t total_reads1_undef;
string *StoreReads1;
string *StoreHeads1;
string *StoreQuals1;
uint64_t readNum2;
uint64_t *num_reads2;
uint64_t *num_reads2_undef;
uint64_t *total_reads2;
uint64_t total_reads2_undef;
string *StoreReads2;
string *StoreHeads2;
string *StoreQuals2;

uint64_t *error_reads ;

vector<string> V_Barcode;

ogzstream outfile_file[5000];
igzstream infile_file[5000];

ogzstream output_file[5000];

void* split_barCode(void* threadId_p);

void usage() 
{	cout << "./barcode_split  <reads_x.1.fq.gz> <reads_x.2.fq.gz> Barcodes[seperate by comma] " << endl;
	cout << "   -t <int>   run the program in multiple thread mode, default = " << threadNum1 << endl;
	cout << "   -l <int>   read length                             ,default = " << len << endl; 
	cout << "   -b <int>   barcode error permit                    ,default = " << bar_err << endl; 
	cout << "   -h         get help information" << endl;
	cout << "e.g:" << endl;
	cout << "./barcode_split -t 10 -l 101 -b 1 test1.fq.gz test2.fq.gz TTAGGC,TGACCA,GCCAAT,CAGATC,ACTTGA,GATCAG " << endl;
	exit(0);
}

void* split_barCode(void* threadId_p1)
{
	int threadId = *((int*)threadId_p1);

	while (1)
	{
		usleep(1);

		if (gotReads1[threadId] == 1)
		{
			for (uint64_t i=0; i<readNum1; i++)
			{
				if (i%threadNum1 == threadId)
				{
					int is_undef = 1;
					for ( int j=0; j<V_Barcode.size(); j++){
						int error_in_bar = 0;
						
						vector<string> lineVecIdx;
						boost::split( lineVecIdx,StoreHeads1[i],boost::is_any_of(":"),boost::token_compress_on );
						string index_code = lineVecIdx[ lineVecIdx.size()-1 ];
//						cout << index_code << endl;
						
						for ( int k=0; k < V_Barcode[j].size();k++){
								
							if ( V_Barcode[j][k]  != index_code[k] ){
								error_in_bar += 1;
								if (error_in_bar > bar_err){
									break;
								}
//								cout << j << "\t" << k << "\t" << V_Barcode[j].size() << "\t" << V_Barcode[j] << "\t" << StoreReads2[i][k] << "\t" << StoreReads2[i] << endl;
//								cout <<  k << "\t"<<  V_Barcode[j].size() << "\t" << V_Barcode.size() << endl;
							}
						}
						if (error_in_bar <= bar_err){
							outfile_file[ 2*( j*threadNum1+threadId )   ] << StoreHeads1[i] << endl  << StoreReads1[i] << endl << "+" << endl << StoreQuals1[i] << endl;
							outfile_file[ 2*( j*threadNum1+threadId )+1 ] << StoreHeads2[i] << endl  << StoreReads2[i] << endl << "+" << endl << StoreQuals2[i] << endl;
							num_reads1[ threadId*V_Barcode.size()+j ]++;
							num_reads2[ threadId*V_Barcode.size()+j ]++;
							is_undef = 0;
//							cout << threadId << "\t" << j << "\t" << num_reads1[ threadId*V_Barcode.size()+j ] << endl;
						}
					}
					if (is_undef){
						outfile_file[ 2*( V_Barcode.size()*threadNum1+threadId )   ] << StoreHeads1[i] << endl  << StoreReads1[i] << endl << "+" << endl << StoreQuals1[i] << endl;
						outfile_file[ 2*( V_Barcode.size()*threadNum1+threadId )+1 ] << StoreHeads2[i] << endl  << StoreReads2[i] << endl << "+" << endl << StoreQuals2[i] << endl;
						num_reads1_undef[ threadId ] ++;
						num_reads2_undef[ threadId ] ++;
					}
					
				}
				
				
			}
			gotReads1[threadId] = 0;
		}
		else if (gotReads1[threadId] == 2)
		{
			return NULL;
		}
	}
}

int main(int argc, char *argv[])
{	
	//get options from command line
	int c;
	while((c=getopt(argc, argv, "t:l:b:h")) !=-1) {
		switch(c) {
			case 't': threadNum1 = atoi(optarg);break;
			case 'l': len       = atoi(optarg); break;
			case 'b': bar_err   = atoi(optarg); break;			
			case 'h':             usage();      break;
			default: usage();
		}
	}
	if (argc < 2) usage();

	string    in_reads1_file = argv[optind++];
	string    in_reads2_file = argv[optind++];
	string    barcodes       = argv[optind++];
	
	boost::split(V_Barcode,barcodes, boost::is_any_of(","), boost::token_compress_on);

	
	igzstream infile1 (in_reads1_file.c_str());
	igzstream infile2 (in_reads2_file.c_str());		
	
	
	for (int j=0;j<V_Barcode.size();j++){
		for (int p=0;p<threadNum1;p++){
			string out_read1_file = in_reads1_file + "." + V_Barcode[j] + ".block" + boost::lexical_cast<string>(p) + ".gz";
			string out_read2_file = in_reads2_file + "." + V_Barcode[j] + ".block" + boost::lexical_cast<string>(p) + ".gz";
			outfile_file[ 2*( j*threadNum1+p )   ].open( out_read1_file.c_str() );
			outfile_file[ 2*( j*threadNum1+p )+1 ].open( out_read2_file.c_str() );			
		}
	}

	for (int p=0;p<threadNum1;p++){
		string out_read1_file_undef = in_reads1_file + ".undef.block" + boost::lexical_cast<string>(p) + ".gz";
		string out_read2_file_undef = in_reads2_file + ".undef.block" + boost::lexical_cast<string>(p) + ".gz";
		outfile_file[ 2*( V_Barcode.size()*threadNum1+p )   ].open( out_read1_file_undef.c_str() );
		outfile_file[ 2*( V_Barcode.size()*threadNum1+p )+1 ].open( out_read2_file_undef.c_str() );
	}

	StoreReads1 = new string[bufferNum];
	StoreHeads1	= new string[bufferNum];
	StoreQuals1	= new string[bufferNum];
	StoreReads2	= new string[bufferNum];
	StoreHeads2	= new string[bufferNum];
	StoreQuals2	= new string[bufferNum];
	
	total_reads1 = new uint64_t[ V_Barcode.size() ];
   total_reads2 = new uint64_t[ V_Barcode.size() ];	
	total_reads1_undef = 0;
	total_reads2_undef = 0;
	
	num_reads1 = new uint64_t[ threadNum1*V_Barcode.size() ];
	num_reads2 = new uint64_t[ threadNum1*V_Barcode.size() ];
	num_reads1_undef = new uint64_t[ threadNum1 ];
	num_reads2_undef = new uint64_t[ threadNum1 ];

	for (int cnt =0;cnt< V_Barcode.size();cnt++){
		total_reads1[cnt] = 0;
		total_reads2[cnt] = 0;
	}
	
	for (int t=0;t<threadNum1;t++){
		for (int cnt=0;cnt<V_Barcode.size();cnt++ ){
			num_reads1[ t*V_Barcode.size()+cnt ] = 0;
			num_reads2[ t*V_Barcode.size()+cnt ] = 0;			
		}
		num_reads1_undef[ t ] = 0;
		num_reads2_undef[ t ] = 0;
	}
	
	
//	for (int k=0;k<V_Barcode.size();k++){
//		cerr << V_Barcode[k] << "\t" << total_reads1[k] << "\t" << total_reads2[k] << endl;
//	}
//
	pthread_t *pthread1 = new pthread_t[threadNum1];
	int *pthreadId1 = new int[threadNum1];
	gotReads1 = new int[threadNum1];
	for (int i=0; i<threadNum1; i++)
	{	
		num_reads1[i] = 0;
		gotReads1[i]  = 0;
		pthreadId1[i] = i;
		pthread_create((pthread1+i), NULL, split_barCode, (void*)(pthreadId1+i));
	}
	
	//Read data-blocks and processed in memory
	while (1)
	{	
		readNum1 = 0;
		readNum2 = 0;
		// Read bufferNum reads one time in one thread
		// Read the fastq.gz file
		string empty_line;
		while ( readNum1 < bufferNum && getline( infile1, StoreHeads1[readNum1], '\n' ) )
		{	
			if (StoreHeads1[readNum1][0] == '@') 
			{	
				getline( infile1, StoreReads1[readNum1], '\n');
				getline( infile1, empty_line, '\n');
				getline( infile1, StoreQuals1[readNum1], '\n');
				readNum1 ++;
			}
		}
		while ( readNum2 < bufferNum && getline( infile2, StoreHeads2[readNum2], '\n' ) )
		{	
			if (StoreHeads2[readNum2][0] == '@') 
			{	
				getline( infile2, StoreReads2[readNum2], '\n');
				getline( infile2, empty_line, '\n');
				getline( infile2, StoreQuals2[readNum2], '\n');
				readNum2 ++;
			}
		}


		/* 
			Thread processing, untile Loop_id equal to readNum1, then the child-thread would be in wait-statue
			Let the main-process go to the next loop to read the data
		*/
		for (int i=0; i<threadNum1; i++)
		{
			gotReads1[i] = 1;
		}
		
		// Wait the child-thread, untile all gotReads1 equal to 0, job done, reading next data.

		while (1)
		{
			usleep(1);
			int i=0;
			for (; i<threadNum1; i++)
			{	if (gotReads1[i] == 1)
				{	break;
				}
			}
			if (i == threadNum1)
			{	break;
			}
		}

		// If File-handle to the end of the file, terminate all child-threads and exit the while loop for reading files.

		if (readNum1 < bufferNum)
		{	for (int i=0; i<threadNum1; i++)
			{	gotReads1[i] = 2;
			}
			break; //break if reads to the end of the file
		}
	}
	// Wait all child-threads done
	for (int i=0; i<threadNum1; i++)
	{
		pthread_join(pthread1[i], NULL);
	}
	//综合统计结果
	for (int i=0; i<threadNum1; i++)
	{	
		for (int k=0;k<V_Barcode.size();k++){
			
			total_reads1[k] += num_reads1[ i*V_Barcode.size()+k ];
			total_reads2[k] += num_reads2[ i*V_Barcode.size()+k ];
		}
//		cerr << num_reads1_undef[i] << endl;
		total_reads1_undef +=num_reads1_undef[i];
		total_reads2_undef +=num_reads2_undef[i];
	}
	
	for (int k=0;k<V_Barcode.size();k++){
		cerr << V_Barcode[k] << "\t" << total_reads1[k] << "\t" << total_reads2[k] << endl;
	}
	cerr << "Undefine" << "\t" << total_reads1_undef << "\t" << total_reads2_undef << endl;
	
	delete[] num_reads1;
	delete[] num_reads2;
	delete[]	num_reads1_undef;
	delete[]	num_reads2_undef;

	delete[] total_reads1;
	delete[] total_reads2;
	delete[] pthread1;
	delete[] pthreadId1;
	delete[] gotReads1;
	delete[] StoreReads1;
	delete[] StoreHeads1;
	delete[] StoreQuals1;
	delete[] StoreReads2;
	delete[] StoreHeads2;
	delete[] StoreQuals2;

	
	infile1.close();
	infile2.close();

	for (int j=0;j<V_Barcode.size();j++){
		for (int p=0;p<threadNum1;p++){
			outfile_file[ 2*( j*threadNum1+p )   ].close();
			outfile_file[ 2*( j*threadNum1+p )+1 ].close();			
		}
	}

	for (int p=0;p<threadNum1;p++){
		outfile_file[ 2*( V_Barcode.size()*threadNum1+p )   ].close();
		outfile_file[ 2*( V_Barcode.size()*threadNum1+p )+1 ].close();
	}
	

}  