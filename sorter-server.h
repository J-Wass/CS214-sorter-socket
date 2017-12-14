#ifndef SORTER_SERVER_H
#define SORTER_SERVER_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <unistd.h>
#include <pthread.h>

void* FileHandler(void * socket);

//Global Variables used across all threads
int sort_on;        //respective column # to sort each csv on
int d;            //search flag - 0 means search current directory, 1 means search given directory
int o;            //insert flag - 0 means insert in current dir, 1 means insert in given directory
char *search_dir;     //directory to search
char *output_dir;     //directory to insert sorted files
int num_rows;       //size of the data array
int row_count;        //# of rows inserted into data array
pthread_t *threads;   //array of all threads
int thread_count;   //current thread index in array, also doubles as the # of threads created
int num_threads;    //size of the thread array

//mutex lock to help threads stay snychronized in adding row_data elements to data array
pthread_mutex_t store_data = PTHREAD_MUTEX_INITIALIZER;

//mutex lock to help threads stay snychronized in adding thread IDs to the thread array
pthread_mutex_t store_threads = PTHREAD_MUTEX_INITIALIZER;


//Pre-Defined Struct with respective column names
char headers[28][50] = {
  "color",
  "director_name",
  "num_critic_for_reviews",
  "duration",
  "director_facebook_likes",
  "actor_3_facebook_likes",
  "actor_2_name",
  "actor_1_facebook_likes",
  "gross",
  "genres",
  "actor_1_name",
  "movie_title",
  "num_voted_users",
  "cast_total_facebook_likes",
  "actor_3_name",
  "facenumber_in_poster",
  "plot_keywords",
  "movie_imdb_link",
  "num_user_for_reviews",
  "language",
  "country",
  "content_rating",
  "budget",
  "title_year",
  "actor_2_facebook_likes",
  "imdb_score",
  "aspect_ratio",
  "movie_facebook_likes"
};


//struct to hold all data in each row
typedef struct {
  char color[50];
  char director_name[100];
  int num_critic_for_reviews;
  int duration;
  int director_facebook_likes;
  int actor_3_facebook_likes;
  char actor_2_name[100];
  int actor_1_facebook_likes;
  int gross;
  char genres[200];
  char actor_1_name[100];
  char movie_title[100];
  int quote;
  int num_voted_users;
  int cast_total_facebook_likes;
  char actor_3_name[100];
  int facenumber_in_poster;
  char plot_keywords[200];
  char movie_imdb_link[500];
  int num_user_for_reviews;
  char language[50];
  char country[50];
  char content_rating[50];
  int budget;
  int title_year;
  int actor_2_facebook_likes;
  double imdb_score;
  double aspect_ratio;
  long movie_facebook_likes;
} row_data;
row_data *data;


//function that returns 1 if given string has a quote and 0 if it does not
int isQuote(char* token) {
  int length = strlen(token);
  int i;
  for (i=0; i<length; i++) if (token[i]=='"') return 1;
  return 0;
}


//function that returns 1 if given filename is a CSV and does not have 'sorted-col_name' in it
int isCSV(char *filename) {
  int length = strlen(filename);
  char *csv = strstr(filename, ".csv");
  if (!csv) return 0;
  char *sorted = strstr(filename, "sorted");
  if (sorted) return 0;
  return 1;
}

void printthreadarray() {
  int i=0;
  for(i=0; i<num_threads; i++) {
    fprintf(stdout, "%lu, ", threads[i]);
  }
  printf("\n-------------------------------------------------\n");
}


void read_params(int count, char* params[]);
void traverse_dir(char *search_dir);
void* thread_traverse_dir(void *search_dir);
void* parse_csv(void* filepath);
void sort_csv();
void print_csv(int* pid);
int sort_on;

#endif