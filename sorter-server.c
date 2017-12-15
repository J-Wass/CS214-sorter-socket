#include <stdio.h>
#include <string.h>
#include <math.h>
#include <ctype.h>
#include <stdlib.h>
#include <dirent.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h> /* superset of previous */
#include "sorter-server.h"
#include "mergesort.c"
#include "arpa/inet.h"
#define col_count 28

pthread_mutex_t sock_mutex = PTHREAD_MUTEX_INITIALIZER;

void error(char *msg)
{
    perror(msg);
    exit(1);
}
pthread_t * threads;
int threadCount;

int main(int argc, char *argv[])
{
     int sockfd, portno, clilen;
     struct sockaddr_in serv_addr, cli_addr;
     if (argc < 2) {
         fprintf(stderr,"ERROR, no port provided\n");
         exit(1);
     }
     sockfd = socket(AF_INET, SOCK_STREAM, 0);
     if (sockfd < 0)
        error("ERROR opening socket");
     bzero((char *) &serv_addr, sizeof(serv_addr));
     portno = atoi(argv[1]);
     serv_addr.sin_family = AF_INET;
     serv_addr.sin_addr.s_addr = INADDR_ANY;
     serv_addr.sin_port = htons(portno);
     if (bind(sockfd, (struct sockaddr *) &serv_addr,
              sizeof(serv_addr)) < 0)
              error("ERROR on binding");
     listen(sockfd,1000);
     clilen = htonl(sizeof(cli_addr));
     threads = (pthread_t *)malloc(sizeof(pthread_t) * 2048);
     threadCount = -1;
     printf("Received connections from:");
     fflush(stdout);
     while(1){
       //spawn a new thread for each socket
       int * sock_fd = malloc(sizeof(int));
       int newsockfd = accept(sockfd, (struct sockaddr *) &cli_addr, &clilen);
       *sock_fd = newsockfd;

       struct sockaddr_in* pV4Addr = (struct sockaddr_in*)&cli_addr;
       struct in_addr ipAddr = pV4Addr->sin_addr;
       char str[INET_ADDRSTRLEN];
       inet_ntop( AF_INET, &ipAddr, str, INET_ADDRSTRLEN );
       printf(" %s,",inet_ntoa(ipAddr));
       fflush(stdout);

       pthread_t thr;
       //race condition where newsock is changed before it gets dereferenced by file handler
       pthread_create (&thr, NULL, FileHandler, (void *)sock_fd);
       threads[++threadCount] = thr;
     }
     return 0;
}

void* FileHandler(void * socket){
  //get the socket fd before the while loop updates it
  pthread_mutex_lock(&sock_mutex);
  int newsock = *((int *)socket);
  free(socket);
  pthread_mutex_unlock(&sock_mutex);

  if (newsock < 0)
  {
    printf("connect error: %d\n", newsock);
    error("ERROR on accept");
  }
  int* pid = malloc(sizeof(int));
  int fileSize = 0;
  int flag = 0;
  int OGsock = newsock;

  read(newsock,&flag,4); //get flag
  newsock = OGsock;
  read(newsock,pid,4); //get pid
  newsock = OGsock;
  //printf("PID: %d\n", *pid);
  read(newsock,&sort_on,4); //get sort col
  newsock = OGsock;


  //printf("sort_on: %d \n", sort_on);

  //all finished signal, lets sort and build this
  if(flag == -1){
    //join all threads (besides this one)
    threadCount--;
    for(;threadCount >= 0; threadCount--){
      pthread_join(threads[threadCount], NULL);
    }
    //at this point, all threads are done and all files are written
    //printf("merging\n");
    //force the linked list to not be circular
    //send the length
    //send the entire list to the client
    //send -1 (all done)
    char pstring2[32];
    sprintf(pstring2, "%d", *pid);

    //initializes data array
    num_rows=10000000, row_count=0;
    data = (row_data*)malloc(num_rows*sizeof(row_data));

    //intializes threads array
    num_threads=2100, thread_count = -1;
    threads = (pthread_t*)malloc(num_threads*sizeof(pthread_t));

    traverse_dir(pstring2);

    int i=0;
    for (i=0; i<=thread_count; i++) {
      pthread_join(threads[i],NULL);
    }
    //sorts the data array
    //printf("sorting now\n");
    sort_csv();

    //prints the sorted data array
    //printf("printing now\n");
    print_csv(pid);

    char *filepath = (char*)malloc(sizeof(char)*500);
    char myburpman[16];
    sprintf(myburpman, "%d", *pid);

    strcpy(filepath, myburpman);
    strcat(filepath, "/Allfiles.csv");
    FILE* Allfiles = fopen(filepath, "r");
    fseeko(Allfiles, 0, SEEK_END);

    long buffSize = ftello(Allfiles);
    fseeko(Allfiles, 0, SEEK_SET);
    char * buffer = malloc(sizeof(char)*buffSize);
    fread(buffer, buffSize, sizeof(char),Allfiles);
    write(newsock, &buffSize, sizeof(long));
    //printf("allfiles size: %ld\n", buffSize);
    int n = write(newsock, buffer, buffSize);
    //printf("I wrote %d bytes!\n", n);
    fclose(Allfiles);

    free(pid);
    free(data);
  }
  else{
    read(newsock,&fileSize,8); //get size of incoming file
    newsock = OGsock;
    char buffer[fileSize+1];
    bzero(buffer, fileSize);
    int n = read(newsock, buffer,fileSize); //load file into buffer
    //printf("read %d bytes vs %d filesize\n", n, fileSize);
    newsock = OGsock;

    //write each received file to hard drive
    buffer[fileSize] = '\0';
    char pstring[32];

    char saak[16];
    sprintf(pstring, "%d", *pid);
    sprintf(saak, "%d", (int)pthread_self());

    struct stat st = {0};
    if(stat(pstring, &st) == -1){
        mkdir(pstring, 0700);
    }

    char fileName[128];
    strcpy(fileName,pstring);
    strcat(fileName, "/");
    strcat(fileName, saak);
    strcat(fileName, ".csv");

    FILE *fp = fopen(fileName, "w");
    if(fwrite(buffer, sizeof(char), fileSize, fp) != fileSize){
      printf("ERROR WRITING FILE");
    };
    fflush(fp);
    fclose(fp);
    //printf("%d - buffer:\n%s\n",newsock,buffer);
    //run buffer through the mergesort and attach it to the circular linked list
    }
  threadCount = -1;
  close(newsock);
  pthread_exit(NULL);
  return 0;
}

/*---------------------------------------------------------------*/

void traverse_dir(char *search_dir){
  DIR *dp = NULL;
  struct dirent *current = NULL;

  //checks if search directory exists
  if ((dp=opendir(search_dir))==NULL) {
    printf("error - search directory, '%s', does not exist\n", search_dir);
    exit(0);
  }

  //printf("SEARCHING DIRECTORY: $%s$\n", search_dir);

  //traverses through entire search directory
  while (current = readdir(dp)) {
    //printf("test: %s\n", current->d_name);
    if (!strcmp(".", current->d_name) || !strcmp("..", current->d_name)) continue;
    if (current->d_type==DT_REG && isCSV(current->d_name)==1) {
      char *new_file_dir = (char*)malloc(strlen(current->d_name)+strlen(search_dir)+3);
      new_file_dir[0] = '\0';
      strcpy(new_file_dir, search_dir);
      strcat(new_file_dir, "/");
      strcat(new_file_dir, current->d_name);
      //printf("file: %s\n", new_file_dir);
      //fflush(stdout);

      pthread_mutex_lock(&store_threads);

      /*if (thread_count==(num_threads-1)) {
        num_threads+=2;
        threads=(pthread_t*)realloc(threads,num_threads*sizeof(pthread_t));
        printf("REALLOCING THREADS %d\n", num_threads);
       }*/
      thread_count+=1;
      //pthread_create(&threads[thread_count], NULL, parse_csv, (void *)new_file_dir);
      if (thread_count<1021) {
        pthread_create(&threads[thread_count], NULL, parse_csv, (void *)new_file_dir);
      }
      else {
        fprintf(stdout, "\n--------------\nOur Program does not run well when it has to deal with >1022 threads. Exiting Now.\n--------------\n");
        exit(0);
      }
      pthread_mutex_unlock(&store_threads);
      /*if(pthread_create(&threads[thread_count], NULL, parse_csv, (void *)new_file_dir)!=0) {
        thread_count-=1;
        printthreadarray();
       }*/
      //parse_csv(new_file_dir);
      //free(new_file_dir);
    }
    else if(current->d_type==DT_DIR) {
      char *new_search_dir = (char*)malloc(strlen(current->d_name)+strlen(search_dir)+3);
      new_search_dir[0] = '\0';
      strcpy(new_search_dir, search_dir);
      strcat(new_search_dir, "/");
      strcat(new_search_dir, current->d_name);
      //fflush(stdout);
      //printf("DIR: %s\n", new_search_dir);

      pthread_mutex_lock(&store_threads);
      /*if (thread_count==(num_threads-1)) {
        num_threads*=2;
        threads=(pthread_t*)realloc(threads,num_threads*sizeof(pthread_t));
        printf("REALLOCING THREADS %d\n", num_threads);
       }*/
      thread_count+=1;

      //pthread_create(&threads[thread_count], NULL, thread_traverse_dir, (void *)new_search_dir);
      if(thread_count<1021) {
        pthread_create(&threads[thread_count], NULL, thread_traverse_dir, (void *)new_search_dir);
      }
      else {
        fprintf(stdout, "Our Program does not run well when it has to deal with more than 1022 threads. Exiting Now.\n");
        exit(0);
      }
      pthread_mutex_unlock(&store_threads);

      /*if(pthread_create(&threads[thread_count], NULL, thread_traverse_dir, (void *)new_search_dir)!=0) {
        thread_count-=1;
        printthreadarray();
       }*/
      //traverse_dir(new_search_dir);
      //free(new_search_dir);
    }
  }
  closedir(dp);
  return;
}

/*---------------------------------------------------------------*/

void* thread_traverse_dir(void* thread_search) {
  //printf("entering traverse thread\n");
  char* search_dir = (char*)thread_search;
  //printf("SEARCHING dir: $%s$\n",search_dir);
  traverse_dir(search_dir);
  //printf("freeing traverse thread\n");
  free(search_dir);
  //printf("exiting traverse thread\n");
  pthread_exit(NULL);
}

/*---------------------------------------------------------------*/

void* parse_csv(void* filepath_temp) {
  char* filepath = (char*)filepath_temp;

  //printf("PARSING FILE: $%s$\n", filepath);

  //opens the .csv file to be read
  FILE *fp = fopen(filepath, "r");

  free(filepath);

  char temp[1000];

  //checks if the header is properly formatted
  char header[1000];
  char header_print[1000];
  fgets(header,1000,fp);
  strcpy(header_print, header);
  char *header_ptr=header;
  int i_header = 0;
  char *header_token;
  while(header_token = strsep(&header_ptr, ",")) {
    if (i_header==27) header_token[strlen(header_token)-2]='\0';
    if (i_header==col_count || strcmp(headers[i_header],header_token)) {
      fclose(fp);
      pthread_exit(NULL);
      return;
    }
    i_header++;
  }

  //int row_count=0;
  pthread_mutex_lock(&store_data);
  while(fgets(temp,1000,fp)) {
    char *line = temp;
    char *token;
    int i=0;
    //printf("before lock\n");
    if (row_count==(num_rows-1)) {
      num_rows=num_rows*2;
      data=(row_data*)realloc(data,num_rows*sizeof(row_data));
      //printf("REALLOCING DATA %d\n", num_rows);
    }
    //printf("after lock\n");
    while (i<col_count) {
      //extracts a token
      token=strsep(&line, ",");
      char *trimmed = (char*)malloc(sizeof(char)*500);
      trimmed[0]='\0';

      //checks if there are quotes around movie_title column signifying commas
      //trims whitespace for movie_titles that have quotes/commas
      if (i==11 && isQuote(token)) {
        data[row_count].quote=1;
        char *forward = (char*)malloc(sizeof(char)*500);
        forward[0]='\0';
        strcat(forward, token);
        strcat(forward,",");
        do {
          token=strsep(&line, ",");
          strcat(forward, token);
          if (!isQuote(token)) strcat(forward, ",");
        } while (!isQuote(token));
        char *temp = (char*)malloc(sizeof(char*)*500);
        temp[0]='\0';
        int q;
        for(q=1;q<strlen(forward)-1;q++) {
          temp[q-1]=forward[q];
        }
        token=temp;
        free(temp);
        free(forward);
      }

      //trims whitepsace for all inputs
      int start = 0;
      //printf("$%s$\n", token);
      if(!token){
        switch(i) {
          case 0:
            strcpy(data[row_count].color,"");
          case 1:
            strcpy(data[row_count].director_name,"");
          case 2:
            data[row_count].num_critic_for_reviews = -99999;
          case 3:
            data[row_count].duration = -99999;
          case 4:
            data[row_count].director_facebook_likes = -99999;
          case 5:
            data[row_count].actor_3_facebook_likes = -99999;
          case 6:
            strcpy(data[row_count].actor_2_name,"");
          case 7:
            data[row_count].actor_1_facebook_likes = -99999;
          case 8:
            data[row_count].gross = -99999;
          case 9:
            strcpy(data[row_count].genres,"");
          case 10:
            strcpy(data[row_count].actor_1_name,"");
          case 11:
            strcpy(data[row_count].movie_title,"");
          case 12:
            data[row_count].num_voted_users = -99999;
          case 13:
            data[row_count].cast_total_facebook_likes = -99999;
          case 14:
            strcpy(data[row_count].actor_3_name,"");
          case 15:
            data[row_count].facenumber_in_poster = -99999;
          case 16:
            strcpy(data[row_count].plot_keywords,"");
          case 17:
            strcpy(data[row_count].movie_imdb_link,"");
          case 18:
            data[row_count].num_user_for_reviews = -99999;
          case 19:
            strcpy(data[row_count].language,"");
          case 20:
            strcpy(data[row_count].country,"");
          case 21:
            strcpy(data[row_count].content_rating,"");
          case 22:
            data[row_count].budget = -99999;
          case 23:
            data[row_count].title_year = -99999;
          case 24:
            data[row_count].actor_2_facebook_likes = -99999;
          case 25:
            sscanf("-99999", "%lf", &data[row_count].imdb_score);
          case 26:
            sscanf("-99999", "%lf", &data[row_count].aspect_ratio);
          case 27:
            data[row_count].movie_facebook_likes = -99999;
        }
        i++;
        trimmed='\0';
        free(trimmed);
        continue;
      }
      int end = strlen(token)-1;
      while(token[start]==' ') {
        if (start==end) trimmed[0]=' ';
        start++;
      }
      while(token[end]==' ') {
        if (start==end) trimmed[0]=' ';
        end--;
      }
      int j;
      for(j=start; j<=end; j++) {
        if (token[j+1]=='\n') continue;
        trimmed[j-start]=token[j];
      }

      //for strings, if a data point is empty, it saves it as an empty string
      //for integers/doubles, if a data point is empty, it saves it as -99999, temporarily
      if (!strcmp(trimmed,"") || !strcmp(trimmed," ")) {
        if (i==0 || i==1 || i==6 || i==9 || i==10 || i==11 || i==14 || i==16 || i==17 || i==19 || i==20 || i==21)
          trimmed="";
        else
          trimmed="-99999";
      }
      //printf("before: %d %d %d\n", num_rows, row_count, i);
      //assigns token to respective struct variable
      switch(i) {
        case 0:
          strcpy(data[row_count].color,trimmed);
        case 1:
          strcpy(data[row_count].director_name,trimmed);
        case 2:
          data[row_count].num_critic_for_reviews = atoi(trimmed);
        case 3:
          data[row_count].duration = atoi(trimmed);
        case 4:
          data[row_count].director_facebook_likes = atoi(trimmed);
        case 5:
          data[row_count].actor_3_facebook_likes = atoi(trimmed);
        case 6:
          strcpy(data[row_count].actor_2_name,trimmed);
        case 7:
          data[row_count].actor_1_facebook_likes = atoi(trimmed);
        case 8:
          data[row_count].gross = atoi(trimmed);
        case 9:
          strcpy(data[row_count].genres,trimmed);
        case 10:
          strcpy(data[row_count].actor_1_name,trimmed);
        case 11:
          strcpy(data[row_count].movie_title,trimmed);
        case 12:
          data[row_count].num_voted_users = atoi(trimmed);
        case 13:
          data[row_count].cast_total_facebook_likes = atoi(trimmed);
        case 14:
          strcpy(data[row_count].actor_3_name,trimmed);
        case 15:
          data[row_count].facenumber_in_poster = atoi(trimmed);
        case 16:
          strcpy(data[row_count].plot_keywords,trimmed);
        case 17:
          strcpy(data[row_count].movie_imdb_link,trimmed);
        case 18:
          data[row_count].num_user_for_reviews = atoi(trimmed);
        case 19:
          strcpy(data[row_count].language,trimmed);
        case 20:
          strcpy(data[row_count].country,trimmed);
        case 21:
          strcpy(data[row_count].content_rating,trimmed);
        case 22:
          data[row_count].budget = atoi(trimmed);
        case 23:
          data[row_count].title_year = atoi(trimmed);
        case 24:
          data[row_count].actor_2_facebook_likes = atoi(trimmed);
        case 25:
          sscanf(trimmed, "%lf", &data[row_count].imdb_score);
        case 26:
          sscanf(trimmed, "%lf", &data[row_count].aspect_ratio);
        case 27:
          data[row_count].movie_facebook_likes = atoi(trimmed);
      }
      i++;
      trimmed='\0';
      free(trimmed);
    }
    row_count++;
  }
  pthread_mutex_unlock(&store_data);
  fclose(fp);
  pthread_exit(NULL);
}

/*---------------------------------------------------------------*/

void sort_csv() {
  //sorts the csv based on sort_on variable using mergesort
  //handles the exception case where an empty csv is inputted with only a header row
  //printf("sortintcol: %d\n",sort_on);
  if(row_count!=0)
    mergeSort(data, 0, row_count-1, sort_on);
}

/*---------------------------------------------------------------*/

void print_csv(int* pid) {
  char *new_file_path = (char*)malloc(sizeof(char)*500);

  // if (o) {
  //   strcpy(new_file_path, output_dir);
  //   strcat(new_file_path, "/Allfiles-sorted-");
  // }
  // else strcpy(new_file_path, "Allfiles-sorted-");

  //create new sorted file path
  char myguy[16];
  sprintf(myguy, "%d", *pid);
  strcpy(new_file_path, myguy);
  strcat(new_file_path, "/Allfiles.csv");
  //printf("%s\n", new_file_path);

  //create file stream
  FILE *fp = fopen(new_file_path, "w");

  free(new_file_path);

  //prints the sorted csv data to respective file
  //fprintf(fp, "%s",file->header);
  //printf("hello1\n");
  fprintf(fp,"color,director_name,num_critic_for_reviews,duration,director_facebook_likes,"
  "actor_3_facebook_likes,actor_2_name,actor_1_facebook_likes,gross,genres,actor_1_name,"
  "movie_title,num_voted_users,cast_total_facebook_likes,actor_3_name,facenumber_in_poster,"
  "plot_keywords,movie_imdb_link,num_user_for_reviews,language,country,content_rating,"
  "budget,title_year,actor_2_facebook_likes,imdb_score,aspect_ratio,movie_facebook_likes\n");

  int z;
  for(z=0; z<=(row_count-1); z++) {
    fprintf(fp, "%s,",data[z].color);

    fprintf(fp, "%s,",data[z].director_name);

    if (data[z].num_critic_for_reviews==-99999) fprintf(fp, ",");
    else fprintf(fp, "%d,",data[z].num_critic_for_reviews);

    if (data[z].duration==-99999) fprintf(fp, ",");
    else fprintf(fp, "%d,",data[z].duration);

    if (data[z].director_facebook_likes==-99999) fprintf(fp, ",");
    else fprintf(fp, "%d,",data[z].director_facebook_likes);

    if (data[z].actor_3_facebook_likes==-99999) fprintf(fp, ",");
    else fprintf(fp, "%d,",data[z].actor_3_facebook_likes);

    fprintf(fp, "%s,",data[z].actor_2_name);

    if (data[z].actor_1_facebook_likes==-99999) fprintf(fp, ",");
    else fprintf(fp, "%d,",data[z].actor_1_facebook_likes);

    if (data[z].gross==-99999) fprintf(fp, ",");
    else fprintf(fp, "%d,",data[z].gross);

    fprintf(fp, "%s,",data[z].genres);

    fprintf(fp, "%s,",data[z].actor_1_name);

    if (data[z].quote) fprintf(fp,"\"%s\",", data[z].movie_title);
    else fprintf(fp, "%s,",data[z].movie_title);

    if (data[z].num_voted_users==-99999) fprintf(fp, ",");
    else fprintf(fp, "%d,",data[z].num_voted_users);

    if (data[z].cast_total_facebook_likes==-99999) fprintf(fp, ",");
    else fprintf(fp, "%d,",data[z].cast_total_facebook_likes);

    fprintf(fp, "%s,",data[z].actor_3_name);

    if (data[z].facenumber_in_poster==-99999) fprintf(fp, ",");
    else fprintf(fp, "%d,",data[z].facenumber_in_poster);

    fprintf(fp, "%s,",data[z].plot_keywords);

    fprintf(fp, "%s,",data[z].movie_imdb_link);

    if (data[z].num_user_for_reviews==-99999) fprintf(fp, ",");
    else fprintf(fp, "%d,",data[z].num_user_for_reviews);

    fprintf(fp, "%s,",data[z].language);

    fprintf(fp, "%s,",data[z].country);

    fprintf(fp, "%s,",data[z].content_rating);

    if (data[z].budget==-99999) fprintf(fp, ",");
    else fprintf(fp, "%d,",data[z].budget);

    if (data[z].title_year==-99999) fprintf(fp, ",");
    else fprintf(fp, "%d,",data[z].title_year);

    if (data[z].actor_2_facebook_likes==-99999) fprintf(fp, ",");
    else fprintf(fp, "%d,",data[z].actor_2_facebook_likes);

    if (data[z].imdb_score==-99999) fprintf(fp, ",");
    else fprintf(fp, "%lf,",data[z].imdb_score);

    if (data[z].aspect_ratio==-99999) fprintf(fp, ",");
    else fprintf(fp, "%lf,",data[z].aspect_ratio);

    fprintf(fp, "%d\n",data[z].movie_facebook_likes);
  }
  fclose(fp);
}

/*---------------------------------------------------------------*/
