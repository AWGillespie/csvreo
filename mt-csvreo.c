/*******************************************************************************
 * $Id: csvreo.c 4504 2015-04-03 19:44:23Z agille $
 *
 * This file is free software. You can redistribute it and/or modify it
 * under the terms of the FreeBSD License which follows.
 * -----------------------------------------------------------------------------
 * Copyright (c) 2013, Arkansas Research Center (arc.arkansas.gov)
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *    * Redistributions of source code must retain the above copyright notice,
 *      this list of conditions and the following disclaimer.
 *
 *    * Redistributions in binary form must reproduce the above copyright
 *      notice, this list of conditions and the following disclaimer in the
 *      documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 * -----------------------------------------------------------------------------
 *
 *
 * TODO: allow reading from input file in addition to stdin
 *		 config file
 *
 *
 ******************************************************************************/

#include <time.h>
#include <stdio.h>
#include <ctype.h>
#include <string.h>
#include <getopt.h>
#include <stdlib.h>
#include <pthread.h>
#include "libcsv-3.0.3/csv.h"

#define VERSION "2.1"

//1 MiB
#define BUFSIZE 1048576

#define DEFAULT_PROG 1000000

#define true  1
#define false 0

#define ERR_CODE_AOK 0  /* success (a-OK) */
#define ERR_CODE_CSV 1  /* Error in the libcsv parser */
#define ERR_CODE_XKY 2  /* Too many keys (eXtra KeYs) */
#define ERR_CODE_NKY 3  /* No key */
#define ERR_CODE_KOR 4  /* Key out of range */
#define ERR_CODE_BDQ 5  /* Invalid quote */
#define ERR_CODE_BDD 6  /* Invalid delimiter */
#define ERR_CODE_EQD 7  /* Equal quote and delimiter */
#define ERR_CODE_MEM 8  /* Memory error */
#define ERR_CODE_FIL 9  /* File error */
#define ERR_CODE_PTH 10 /* Thread error */

#define EMPTY 0
#define FULL  1
#define BUSY  2
#define DONE  3

struct option long_options[] =
{
   {"quote",    required_argument, 0, 'q'},
   {"outquote", required_argument, 0, 'Q'},
   {"delim",    required_argument, 0, 'd'},
   {"outdelim", required_argument, 0, 'D'},
   {"keys",     required_argument, 0, 'k'},
   {"keys",     required_argument, 0, 'K'},
   {"file",     required_argument, 0, 'f'},
   {"file",     required_argument, 0, 'F'},
   {"progress", required_argument, 0, 'p'},
   {"progress", required_argument, 0, 'P'},
   {"reverse",  no_argument,       0, 'r'},
   {"reverse",  no_argument,       0, 'R'},
   {"all",		no_argument,       0, 'a'},
   {"all",		no_argument,       0, 'A'},
   {"help",     no_argument,       0, 'h'},
   {"help",     no_argument,       0, 'H'},
   {0, 0, 0, 0}
};

//structure for each output file
struct output_file
{
    FILE* outfile;                  //the file being written to
    int* keyorder;                  //array of keys, in the specified order
    size_t num_keys;                //number of keys selected (above array size)
    struct output_file* next;       //next output file structure
	short rev;						//reverse flag
	short all;						//all flag
    char outdelim;                  //delimiter for output
    char outquote;                  //quote for output
};

struct data
{
    FILE* infile;                   //the input file
    struct output_file* outputs;    //linked list of output file structures
    size_t current_field;           //the current field being processed
    size_t* field_lengths;          //the length of each field in the field array
    size_t* field_capacity;         //the capacity of each field in the field array
    char** field;                   //array of buffers for the parser
	struct output_file* last;		//most recently allocated output struct
	size_t row;						//number of rows read thus far
    size_t progress;                //interval for progress messages
	size_t num_fields;              //the number of columns in the file
    char delim;                     //input file delimiter
    char quote;                     //quote character
};

struct thread_data
{
	char *buff[2];
	int status[2];
	size_t size[2];			//used size of the buffer (not the capacity)
	struct csv_parser* csv;	//pointer to the struct used by the parser
	struct data* dat; 		//pointer to data struct used by callbacks

};

void usage(int code);
void check_opts(struct data *o);
void fileAssign(struct data *d, char *optarg);
void keyAssign(struct data *d, char *optarg);
void* thread_io_scan(void* data_ptr);

/* callback functions */
void cb1(void *, size_t, void *);
void cb2(int, void *);

int main(int argc, char** argv)
{
	clock_t begin = clock();

	char ch;
	struct data dat;
	dat.infile = stdin;
	dat.outputs = NULL;
	dat.current_field = 0;
	dat.row = 0;
	dat.progress = DEFAULT_PROG;
	dat.num_fields = 0;
	dat.field_lengths = NULL;
	dat.field_capacity = NULL;
	dat.delim = '|';
	dat.quote = '"';
	dat.field = NULL;
	dat.last = NULL;

	//parse options
	while((ch = getopt_long(argc, argv, "q:d:k:f:p:rahQ:D:K:f:P:RAH", long_options, NULL)) != -1)
	{
		switch(ch)
		{
			case 'q':
				dat.quote = optarg[0];
			case 'Q':
				if(dat.last == NULL)
				{
					fileAssign(&dat, "");
				}
				dat.last->outquote = optarg[0];
			break;

			case 'd':
				dat.delim = optarg[0];
			case 'D':
				if(dat.last == NULL)
				{
					fileAssign(&dat, "");
				}
				dat.last->outdelim = optarg[0];
			break;

			case 'p':
			case 'P':
				dat.progress = atoi(optarg);
			break;

			case 'k':
			case 'K':
				keyAssign(&dat, optarg);
			break;

			case 'f':
			case 'F':
				fileAssign(&dat, optarg);
			break;

			case 'r':
			case 'R':
				if(dat.last == NULL)
				{
					fileAssign(&dat, "");
				}
				dat.last->rev = true;
			break;

			case 'a':
			case 'A':
				if(dat.last == NULL)
				{
					fileAssign(&dat, "");
				}
				dat.last->all = true;
			break;

			case 'h':
			case 'H':
				usage(ERR_CODE_AOK);
			break;
		}
	}

	check_opts(&dat);

	//initialize parser and buffers
	struct csv_parser csv;
	struct thread_data t_data;
	t_data.buff[0] = malloc(sizeof(char) * BUFSIZE);
	t_data.buff[1] = malloc(sizeof(char) * BUFSIZE);
	t_data.status[0] = EMPTY;
	t_data.status[1] = EMPTY;
	t_data.size[0] = 0;
	t_data.size[1] = 0;
	t_data.csv = &csv;
	t_data.dat = &dat;
	pthread_t thread_id;
	size_t c_count = 0;

	csv_init(&csv, CSV_APPEND_NULL);

	//set parser options
	csv_set_quote(&csv, dat.quote);
	if (csv_get_quote(&csv) != dat.quote)
	{
		printf("cannot correctly set quote to %c\n", dat.quote);
		exit(ERR_CODE_CSV);
	}

	csv_set_delim(&csv, dat.delim);
	if (csv_get_delim(&csv) != dat.delim)
	{
		printf("cannot correctly set delimiter to %c\n", dat.delim);
		exit(ERR_CODE_CSV);
	}
	
	//fprintf(stderr, "data address: %p\n", &t_data);

	//start thread
	if(pthread_create(&thread_id, NULL, thread_io_scan, &t_data) != 0)
	{
		perror("ERROR while creating thread");
	}

	int idx;
	int done = false;
	//parse
	while(!done)
	{
		for(idx = 0; idx < 2 && !done; ++idx)
		{
			if(t_data.status[idx] == EMPTY)
			{
				c_count = fread(t_data.buff[idx], 1, BUFSIZE, stdin);

				if(c_count == BUFSIZE)
				{
					t_data.size[idx] = c_count;
					t_data.status[idx] = FULL;
					//fprintf(stderr, "marking %i FULL\n", idx);
				}
				else if(c_count > 0)
				{
					t_data.size[idx] = c_count;
					t_data.status[idx] = FULL;
					//fprintf(stderr, "marking %i FULL (short)\n", idx);
					done = true;
				}
				else
				{
					t_data.status[idx] = DONE;
					//fprintf(stderr, "marking %i DONE\n", idx);
					done = true;
				}
			}
		}
	}

	while((t_data.status[0] != DONE) && (t_data.status[1] != DONE))
	{
		for(idx = 0; idx < 2; ++idx)
		{
			if(t_data.status[idx] == EMPTY)
			{
				t_data.status[idx] = DONE;
				//fprintf(stderr, "marking %i DONE\n", idx);
			}
		}
	}

	//fprintf(stderr, "joining...\n");
	pthread_join(thread_id, NULL);
	//fprintf(stderr, "joined\n");

	//finish and free memory
	csv_fini(&csv, cb1, cb2, &dat);
	csv_free(&csv);

	//maybe we should clean up the structures?

	//fprintf(stderr, "Job complete, %i total records processed, %i bad records.\n", dat.row, dat.badRows);
	fprintf(stderr, "Job complete, %zi total records processed.\n", dat.row);
	fprintf(stderr, "Time taken: %.3f s\n", ((double)clock() - begin)/CLOCKS_PER_SEC);

	return 0;
}

void cb1(void *c, size_t n, void *vp)
{
	struct data *d = vp;

	//dynamically determine the number of columns, and initialize data
	if(d->row == 0)
	{
		d->num_fields++;
		d->field = realloc(d->field, d->num_fields*sizeof(char*));
		d->field_capacity = realloc(d->field_capacity, d->num_fields*sizeof(size_t));
		
		d->field_lengths = realloc(d->field_lengths, d->num_fields*sizeof(size_t));
		d->field_capacity[d->current_field] = n * sizeof(char);
		d->field[d->current_field] = malloc(n * sizeof(char));
	}
	//if the string buffers are too small, allocate enough space
	else if(n > d->field_capacity[d->current_field])
	{
		free(d->field[d->current_field]);
		d->field[d->current_field] = malloc(n * sizeof(char));
		d->field_capacity[d->current_field] = n * sizeof(char);
	}
	//copy string
	memcpy(d->field[d->current_field], c, n);
	d->field_lengths[d->current_field] = n;
	d->current_field++;
}

void cb2(int n __attribute__ ((unused)), void *vp)
{
	struct data* d = vp;

	/* maybe flag to suppress lines with incorrect number of fields
	if(d->current_field != num_fields)
	{
		//do something based on options
	}
	*/

	d->row++;

	if(d->progress)
	{
		if(!(d->row % d->progress))
		{
			fprintf(stderr, "%zi records processed.\n", d->row);
		}
	}

	size_t ndx;
	struct output_file* output = d->outputs;
	while(output != NULL)
	{
		if(output->all == true)
		{
			csv_fwrite2(output->outfile,
						d->field[0],
						d->field_lengths[0],
						output->outquote);
			for(ndx = 1; ndx < d->num_fields; ++ndx)
			{
				fputc(output->outdelim, output->outfile);
				csv_fwrite2(output->outfile,
							d->field[ndx],
							d->field_lengths[ndx],
							output->outquote);
			}
		}
		else if(output->rev == true)
		{
			csv_fwrite2(output->outfile,
						d->field[d->num_fields - 1],
						d->field_lengths[d->num_fields - 1],
						output->outquote);
			for(ndx = 2; ndx <= d->num_fields; ++ndx)
			{
				fputc(output->outdelim, output->outfile);
				csv_fwrite2(output->outfile,
							d->field[d->num_fields - ndx],
							d->field_lengths[d->num_fields - ndx],
							output->outquote);
			}
		}
		else
		{
			csv_fwrite2(output->outfile,
						d->field[output->keyorder[0]],
						d->field_lengths[output->keyorder[0]],
						output->outquote);
			for(ndx = 1; ndx < output->num_keys; ++ndx)
			{
				fputc(output->outdelim, output->outfile);
				csv_fwrite2(output->outfile,
							d->field[output->keyorder[ndx]],
							d->field_lengths[output->keyorder[ndx]],
							output->outquote);
			}
		}
		fputc('\n', output->outfile);
		output = output->next;
	}
	d->current_field = 0;
}

void usage(int code)
{
   printf("\n");
   printf("******************************************************\n");
   printf("*                                                    *\n");
   printf("* csvreo v%s                                        *\n", VERSION);
   printf("*                                                    *\n");
   printf("* !!NOTE!! It is not recommended to use the same     *\n");
   printf("* file for input and output data (unless you LIKE    *\n");
   printf("* losing your file).                                 *\n");
   printf("*                                                    *\n");
   printf("* This function takes and reads a delimited file     *\n");
   printf("* from stdin, and outputs a new file that has the    *\n");
   printf("* specified fields in the specified order.           *\n");
   printf("*                                                    *\n");
   printf("* Short options are not case-sensitive, unless       *\n");
   printf("* otherwise noted.                                   *\n");
   printf("*                                                    *\n");
   printf("* Arguments:                                         *\n");
   printf("* --quote (-q) the quote used in the file (optional) *\n");
   printf("*   Defaults to \" if not specified.                  *\n");
   printf("*                                                    *\n");
   printf("* --delim (-d) the delimiter for the input file      *\n");
   printf("*   (optional, case-sensitive)                       *\n");
   printf("*   Defaults to | if not specified.                  *\n");
   printf("*                                                    *\n");
   printf("* --outdelim (-D) the delimiter for the output file  *\n");
   printf("*   (optional, case-sensitive)                       *\n");
   printf("*   Defaults to input delimiter if not specified.    *\n");
   printf("*                                                    *\n");
   printf("* --keys (-k) The columns (keys) to put in the       *\n");
   printf("*   output file.                                     *\n");
   printf("*   If no keys are specified, an error message is    *\n");
   printf("*   displayed, unless -r is used.                    *\n");
   printf("*   Only one key per -k, but -k may be used          *\n");
   printf("*   repeatedly.                                      *\n");
   printf("*   Order sensitive (optional).                      *\n");
   printf("*                                                    *\n");
   printf("* --file (-f) Specifies an output file. Any number   *\n");
   printf("*   of files (including 0, which defaults to stdout) *\n");
   printf("*   may be specified, each with its own key set,     *\n");
   printf("*   but every file must have a key set, or an error  *\n");
   printf("*   will be produced.                                *\n");
   printf("*   The keys to be written to each file must follow  *\n");
   printf("*   the file.                                        *\n");
   printf("*   Example:                                         *\n");
   printf("*    -ffilename1 -k3 -k4  -ffilename2 -k2 -k8        *\n");
   printf("*                                                    *\n");
   printf("* --help (-h) Displays this message and exits.       *\n");
   printf("*                                                    *\n");
   printf("* --reverse (-r) Prints all fields in reverse order  *\n");
   printf("*   keys (if supplied) are ignored.                  *\n");
   printf("*   This is not usable if files are specified.       *\n");
   printf("*                                                    *\n");
   printf("* --progress (-p) After how many records to print a  *\n");
   printf("*   progress message.  Default is %i            *\n", DEFAULT_PROG);
   printf("*   0 turns this off.                                *\n");
   printf("*                                                    *\n");
   printf("* Error Codes:                                       *\n");
   printf("*   These are the exit codes returned by this        *\n");
   printf("*   program:                                         *\n");
   printf("*                                                    *\n");
   printf("*   0: Success                                       *\n");
   printf("*   1: Error in the libcsv parser                    *\n");
   printf("*   2: Too many keys                                 *\n");
   printf("*   3: No key                                        *\n");
   printf("*   4: Key out of range                              *\n");
   printf("*   5: Invalid quote                                 *\n");
   printf("*   6: Invalid delimiter                             *\n");
   printf("*   7: Equal quote and delimiter                     *\n");
   printf("*   8: Memory error                                  *\n");
   printf("*   9: File error                                    *\n");
   printf("*                                                    *\n");
   printf("******************************************************\n\n");

   exit(code);
}

void check_opts(struct data *d)
{
	struct output_file* output = d->outputs;
	if(isalnum(d->delim))
	{
		fprintf(stderr, "ERROR: %c is not a valid delimiter.\n", d->delim);
		exit(ERR_CODE_BDD);
	}

	while(output != NULL)
	{
		if(isalnum(output->outdelim))
		{
			fprintf(stderr, "ERROR: %c is not a valid delimiter.\n", output->outdelim);
			exit(ERR_CODE_BDD);
		}
		output = output->next;
	}

	if(isalnum(d->quote))
	{
		fprintf(stderr, "ERROR: %c is not a valid quote.\n", d->quote);
		exit(ERR_CODE_BDQ);
	}

	output = d->outputs;
	while(output != NULL)
	{
		if(isalnum(output->outquote))
		{
			fprintf(stderr, "ERROR: %c is not a valid quote.\n", output->outquote);
			exit(ERR_CODE_BDQ);
		}
		output = output->next;
	}

	if(d->delim == d->quote)
	{
		fprintf(stderr, "ERROR: %c used as quote and delimiter.\n", d->quote);
		exit(ERR_CODE_EQD);
	}
	
	output = d->outputs;
	while(output != NULL)
	{
		if(output->outdelim == output->outquote)
		{
			fprintf(stderr, "ERROR: %c used as quote and delimiter.\n", output->outquote);
			exit(ERR_CODE_EQD);
		}
		output = output->next;
	}
	
	output = d->outputs;
	while(output != NULL)
	{
		if(output->num_keys == 0)
		{
			if((output->rev == false) && (output->all == false))
			{
				fprintf(stderr,"ERROR: no keys specified.\n");
				exit(ERR_CODE_NKY);
			}
		}
		output = output->next;
	}
}

void fileAssign(struct data *d, char *optarg)
{
	//fprintf(stderr, "optarg to fileAssign: %s\n", optarg);
	if(d->outputs == NULL)
	{
		d->outputs = malloc(sizeof(struct output_file));
		d->last = d->outputs;
	}
	else
	{
		d->last->next = malloc(sizeof(struct output_file));
		d->last = d->last->next;
	}

	if(strcmp(optarg, "") == 0)
	{
		d->last->outfile = stdout;
	}
	else
	{
		d->last->outfile = fopen(optarg, "w");
	}

	if(d->last->outfile == NULL)
	{
		fprintf(stderr, "ERROR: File %s failed to open.\n", optarg);
		exit(ERR_CODE_FIL);
	}

	d->last->keyorder = NULL;
	d->last->num_keys = 0;
	d->last->outdelim = d->delim;
	d->last->outquote = d->quote;
	d->last->rev = false;
	d->last->all = false;
	d->last->next = NULL;

	//fprintf(stderr, "leaving fileAssign: %s\n", optarg);
}

void keyAssign(struct data *d, char *optarg)
{
	int new_key = atoi(optarg);
	//fprintf(stderr, "new key: %i\n", new_key);

	if(new_key < 1)
	{
		fprintf(stderr, "ERROR: key value %i is too low\n", new_key);
		exit(ERR_CODE_KOR);
	}

	if(d->outputs == NULL)
	{
		fileAssign(d, "");
	}

	--new_key;
	
	d->last->num_keys++;
	d->last->keyorder = realloc(d->last->keyorder, d->last->num_keys * sizeof(int));
	d->last->keyorder[(d->last->num_keys) - 1] = new_key;
}

void* thread_io_scan(void* data_ptr)
{
	//fprintf(stderr, "thread started\n");
	struct thread_data* data = (struct thread_data*)data_ptr;

	//fprintf(stderr, "thread: data address: %p\n", data);

	while((data->status[0] != DONE) && (data->status[1] != DONE))
	{
		if(data->status[0] == FULL)
		{
			//fprintf(stderr, "thread: found buffer 0 FULL; marking busy\n");
			data->status[0] = BUSY;
			csv_parse(data->csv, data->buff[0], data->size[0], cb1, cb2, data->dat);
			if(csv_error(data->csv))
			{
				printf("ERROR: CSV-%s\n", csv_strerror(csv_error(data->csv)));
				exit(ERR_CODE_CSV);
			}
			//fprintf(stderr, "thread: finished buffer 0; marking EMPTY\n");
			data->status[0] = EMPTY;
		}
		if(data->status[1] == FULL)
		{
			//fprintf(stderr, "thread: found buffer 1 FULL; marking busy\n");
			data->status[1] = BUSY;
			csv_parse(data->csv, data->buff[1], data->size[1], cb1, cb2, data->dat);
			if(csv_error(data->csv))
			{
				printf("ERROR: CSV-%s\n", csv_strerror(csv_error(data->csv)));
				exit(ERR_CODE_CSV);
			}
			//fprintf(stderr, "thread: finished buffer 1; marking EMPTY\n");
			data->status[1] = EMPTY;
		}
	}
	//fprintf(stderr, "thread finished\n");
	
	return NULL;
}
