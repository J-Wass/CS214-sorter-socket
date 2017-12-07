#include <stdio.h>
#include <string.h>
#include <math.h>
#include <ctype.h>
#include <stdlib.h>
#include <dirent.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/types.h>
#include "sorter-client.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>

char * hostname;
int port;
int sortInt;

int main(int argc, char ** argv){
  if(argc < 7){
    fprintf(stderr, "Too few arguments. Usage is ./sorter -c [sortcol] -h [host] -p [port number]\n");
    return 0;
  }
  char inDir[1024];
  char outDir[1024];
  char sortByCol[1024];

  if(strcmp("-c", argv[1]) != 0 || strcmp("-h", argv[3]) != 0 || strcmp("-p", argv[5]) != 0 ){
    fprintf(stderr, "Wrong order of argument. Usage is ./sorter -c [sortcol] -h [host] -p [port number]\n");
    return 0;
  }
  //copy args to local variables
  strcpy(sortByCol, argv[2]);
  port = atoi(argv[6]);

  hostname = (char *)malloc(sizeof(argv[4]));
  strcpy(hostname, argv[4]);

  //populate inDir and outDir with defaults
  if (getcwd(inDir, sizeof(inDir)) == NULL)
  {
    fprintf(stderr, "Could not read current working directory.\n");
    return 0;
  }
  strcpy(outDir, inDir);

  if(argc >= 9){
    if(strcmp("-d",argv[7]) != 0){
      fprintf(stderr, "Expecting -d flag. Usage is ./sorter -c [sortcol] -h [host] -p [port number] -d [in directory]\n");
      return 0;
    }
    strcpy(inDir, argv[8]);
  }

  if(argc == 10){
    if(strcmp("-o",argv[9]) != 0){
      fprintf(stderr, "Expecting -o flag. Usage is ./sorter -c [sortcol] -h [host] -p [port number] -d [in directory] -o [out directory]\n");
      return 0;
    }
    strcpy(outDir, argv[10]);
  }

  //parse sort by column into an int
  if(strcmp(sortByCol,  "color")==0) sortInt=0;
  else if(strcmp(sortByCol, "director_name")==0) sortInt=1;
  else if(strcmp(sortByCol, "num_critic_for_reviews")==0) sortInt=2;
  else if(strcmp(sortByCol, "duration")==0) sortInt=3;
  else if(strcmp(sortByCol, "director_facebook_likes")==0) sortInt=4;
  else if(strcmp(sortByCol, "actor_3_facebook_likes")==0) sortInt=5;
  else if(strcmp(sortByCol, "actor_2_name")==0) sortInt=6;
  else if(strcmp(sortByCol, "actor_1_facebook_likes")==0) sortInt=7;
  else if(strcmp(sortByCol, "gross")==0) sortInt=8;
  else if(strcmp(sortByCol, "genres")==0) sortInt=9;
  else if(strcmp(sortByCol, "actor_1_name")==0) sortInt=10;
  else if(strcmp(sortByCol, "movie_title")==0) sortInt=11;
  else if(strcmp(sortByCol, "num_voted_users")==0) sortInt=12;
  else if(strcmp(sortByCol, "cast_total_facebook_likes")==0) sortInt=13;
  else if(strcmp(sortByCol, "actor_3_name")==0) sortInt=14;
  else if(strcmp(sortByCol, "facenumber_in_poster")==0) sortInt=15;
  else if(strcmp(sortByCol, "plot_keywords")==0) sortInt=16;
  else if(strcmp(sortByCol, "movie_imdb_link")==0) sortInt=17;
  else if(strcmp(sortByCol, "num_user_for_reviews")==0) sortInt=18;
  else if(strcmp(sortByCol, "language")==0) sortInt=19;
  else if(strcmp(sortByCol, "country")==0) sortInt=20;
  else if(strcmp(sortByCol, "content_rating")==0) sortInt=21;
  else if(strcmp(sortByCol, "budget")==0) sortInt=22;
  else if(strcmp(sortByCol, "title_year")==0) sortInt=23;
  else if(strcmp(sortByCol, "actor_2_facebook_likes")==0) sortInt=24;
  else if(strcmp(sortByCol, "imdb_score")==0) sortInt=25;
  else if(strcmp(sortByCol, "aspect_ratio")==0) sortInt=26;
  else if(strcmp(sortByCol, "movie_facebook_likes")==0) sortInt=27;
  else{
    fprintf(stderr, "Please use a valid column name!\n");
    return 0;
  }

  DIR * inputDir = opendir (inDir);
  DIR * outputDir = opendir (outDir);
  if (inputDir == NULL) {
      fprintf(stderr,"Malformed input directory %s. Please make sure the directory spelling is correct before trying again.\n",
       inDir);
      return 0;
  }
  if (outputDir == NULL) {
      fprintf(stderr,"Malformed output directory %s. Please make sure the directory spelling is correct before trying again.\n",
       outDir);
      return 0;
  }
  printf("port: %d, host: %s, inDir: %s, outdir: %s, sortcol: %d\n", port, hostname, inDir, outDir, sortInt);
  sortCSVs(inputDir, inDir, outputDir, outDir);
  closedir(inputDir);
  closedir(outputDir);
  free(hostname); //sorting int is found in mergesort.c
  return 0;
}

void sortCSVs(DIR * inputDir, char * inDir, DIR * outputDir, char * outDir){
  struct dirent* inFile;
  char * isSorted;
  while((inFile = readdir(inputDir)) != NULL){
    isSorted = strstr(inFile->d_name, "-sorted-");
    if((strcmp(inFile->d_name, ".") == 0 || strcmp(inFile->d_name, "..") == 0) && inFile->d_type == 4){
      continue; //skip directory . and ..
    }
    if(isSorted){ //contains -sorted- or is . or is ..
    	continue; //find next file
    }
    char * name = inFile->d_name;
    int l = strlen(name);
    //REGULAR FILE
    if(inFile->d_type == 8 && name[l-4] == '.' && name[l-3] == 'c' && name[l-2] == 's' && name[l-1] == 'v'){
      char * path = (char *)malloc(sizeof(char)*(strlen(inDir) + 2 + strlen(name)));
      strcpy(path, inDir);
      strcat(path, "/");
      strcat(path, name);
      strcat(path, "\0");
      pthread_t thr;
      pthread_detach(thr);
      pthread_create (&thr, NULL, FileSortHandler, (void *)path);
    }
    //DIRECTORY
    if(inFile->d_type == 4){
      char newDir[1 + strlen(inDir) + strlen(name)];
      strcpy(newDir, inDir);
      strcat(newDir, "/");
      strcat(newDir, name);
      DIR * open = opendir(newDir);
      sortCSVs(open, newDir, outputDir, outDir);
      closedir(open);
    }
  }
  return;
}

void* FileSortHandler(void * filename){
  FILE * sortFile = fopen((char *)filename, "r");
  char * line = NULL;
  size_t nbytes = 0 * sizeof(char);

  getline(&line, &nbytes, sortFile); //skip over first row (just the table headers)
  int sockfd, portno;

  struct sockaddr_in serv_addr;
  struct hostent *server;
  portno = port;
  sockfd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
  if (sockfd < 0){
      fprintf(stderr,"ERROR opening socket\n");
  }
  server = gethostbyname(hostname);
  if (server == NULL) {
      fprintf(stderr,"ERROR, no such host\n");
      exit(0);
  }
  bzero((char *) &serv_addr, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  bcopy((char *)server->h_addr,
       (char *)&serv_addr.sin_addr.s_addr,
       server->h_length);
  serv_addr.sin_port = htons(portno);

  if (connect(sockfd,(struct sockaddr *)&serv_addr,sizeof(serv_addr)) < 0){
      fprintf(stderr,"ERROR connecting\n");
  }
  int pid = htonl((int)getpid());
  printf("Sending %d\n",pid);
  write(sockfd,&pid,sizeof(pid));
  while (getline(&line, &nbytes, sortFile) != -1) {
     write(sockfd, line,4);
  }
  fclose(sortFile);
  pthread_exit(NULL);
  return NULL;
}
