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
#include "sorter-server.h"
#include "mergesort.c"

pthread_mutex_t sock_mutex = PTHREAD_MUTEX_INITIALIZER;

void error(char *msg)
{
    perror(msg);
    exit(1);
}
pthread_t * threads;
int threadCount;
pthread_t * threads_;
int threadCount2;

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
     listen(sockfd,5);
     clilen = htonl(sizeof(cli_addr));
     threads = (pthread_t *)malloc(sizeof(pthread_t) * 2048);
     threadCount = -1;
     while(1){
       //spawn a new thread for each socket
       int * sock_fd = malloc(sizeof(int));
       int newsockfd = accept(sockfd, (struct sockaddr *) &cli_addr, &clilen);
       *sock_fd = newsockfd;

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
  int pid = 0;
  int sortby = 0;
  int fileSize = 0;
  int flag = 0;
  int OGsock = newsock;

  read(newsock,&flag,4); //get flag
  newsock = OGsock;
  read(newsock,&pid,4); //get pid
  newsock = OGsock;
  read(newsock,&sortby,4); //get sort col
  newsock = OGsock;
  read(newsock,&fileSize,8); //get size of incoming file
  newsock = OGsock;
  char buffer[fileSize+1];
  bzero(buffer, fileSize);
  read(newsock, buffer,fileSize); //load file into buffer
  newsock = OGsock;

  //all finished signal, lets sort and build this
  if(flag == -1){
    //join all threads (besides this one)
    threadCount--;
    for(;threadCount >= 0; threadCount--){
      pthread_join(threads[threadCount], NULL);
    }
    //at this point, all threads are done and all files are written
    printf("merging\n");
    threadCount2 = -1;
    //force the linked list to not be circular
    //send the length
    //send the entire list to the client
    //send -1 (all done)
    char pstring2[32];
    sprintf(pstring2, "%d", pid);
    DIR* inpDir = opendir(pstring2);
    DIR* outDir = opendir(pstring2);
    threads_ = (pthread_t *)malloc(sizeof(pthread_t) * 1);
    //sort everything
    sortCSVs(inpDir, pstring2, outDir, pstring2, 1, sortby, threads_);
    closedir(inpDir);
    closedir(outDir);
  }
  else{
    //write each received file to hard drive
    buffer[fileSize] = '\0';
    sortingInt = sortby;
    char pstring[32];

    char saak[16];

    sprintf(pstring, "%d", pid);
    sprintf(saak, "%d", OGsock);

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

    fwrite(buffer, sizeof(char), fileSize, fp);
    fflush(fp);

    fclose(fp);
    //printf("%d - buffer:\n%s\n",newsock,buffer);
    //run buffer through the mergesort and attach it to the circular linked list
    }
  close(newsock);
  pthread_exit(NULL);
  return 0;
}

void sortCSVs(DIR * inputDir, char * inDir, DIR * outputDir, char * outDir, short mainCall, int sortInt, pthread_t * threads_){
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
      pthread_create (&thr, NULL, FileSortHandler, (void *)path);
      ++(threadCount2);
      threads_[threadCount2] = thr;
    }
    //DIRECTORY
    if(inFile->d_type == 4){
      char newDir[1 + strlen(inDir) + strlen(name)];
      strcpy(newDir, inDir);
      strcat(newDir, "/");
      strcat(newDir, name);
      DIR * open = opendir(newDir);
      sortCSVs(open, newDir, outputDir, outDir, 0, sortInt, threads_);
      closedir(open);
    }
  }
  //only run if called from main()
  if(mainCall == 1){
    int counter = threadCount2;
    Record * linkedlist = NULL;
    while(counter >= 0){
      Record * head;
      pthread_join(threads_[counter--], (void *)&head);
      if(linkedlist == NULL){
        linkedlist = head;
      }
      else{
        //combine new linked list with main circular linked list
        Record * temp = linkedlist->next;
        linkedlist->next = head->next;
        head->next = temp;
      }
    }
    //break linked list and attempt to sort
    Record * tempH = linkedlist;
    linkedlist = linkedlist->next;
    tempH->next = NULL;
    Record * sortedHead;
    Record ** sorted_head;
    sorted_head = mergesort(&linkedlist);
    sortedHead= *sorted_head;
    //sortedHead = linkedlist;

    //write total file to hard drive
    char newFile[21 + strlen(outDir)];
    strcpy(newFile, outDir);
    strcat(newFile, "/AllFiles.csv");
    FILE * writeFile = fopen(newFile, "w");
    fprintf(writeFile,"color,director_name,num_critic_for_reviews,duration,director_facebook_likes,"
    "actor_3_facebook_likes,actor_2_name,actor_1_facebook_likes,gross,genres,actor_1_name,"
    "movie_title,num_voted_users,cast_total_facebook_likes,actor_3_name,facenumber_in_poster,"
    "plot_keywords,movie_imdb_link,num_user_for_reviews,language,country,content_rating,"
    "budget,title_year,actor_2_facebook_likes,imdb_score,aspect_ratio,movie_facebook_likes\n");
    while(sortedHead != NULL){
      Record * r = sortedHead;
      char numCritic[50] = "";
      char duration[50] = "";
      char directLikes[50] = "";
      char actor3Likes[50] = "";
      char actor1Likes[50] = "";
      char gross[50] = "";
      char numVoted[50] = "";
      char castLikes[50] = "";
      char faceNumber[50] = "";
      char numReviews[50] = "";
      char budget[50] = "";
      char actor2Likes[50] = "";
      char titleYear[50] = "";
      char imdbScore[50] = "";
      char aspectRatio[50] = "";
      char movieLikes[50] = "";

      if(r->num_critic_for_reviews != -1){
          snprintf(numCritic, 5000, "%d",r->num_critic_for_reviews);
      }
      if(r->duration != -1){
          snprintf(duration, 5000, "%d",r->duration);
      }
      if(r->director_facebook_likes != -1){
          snprintf(directLikes, 5000, "%d",r->director_facebook_likes);
      }
      if(r->actor_3_facebook_likes != -1){
          snprintf(actor3Likes, 5000, "%d",r->actor_3_facebook_likes);
      }
      if(r->actor_1_facebook_likes != -1){
          snprintf(actor1Likes, 5000, "%d",r->actor_1_facebook_likes);
      }
      if(r->gross != -1){
          snprintf(gross, 5000, "%d",r->gross);
      }
      if(r->num_voted_users != -1){
          snprintf(numVoted, 5000, "%d",r->num_voted_users);
      }
      if(r->cast_total_facebook_likes != -1){
          snprintf(castLikes, 5000, "%d",r->cast_total_facebook_likes);
      }
      if(r->facenumber_in_poster != -1){
          snprintf(faceNumber, 5000, "%d",r->facenumber_in_poster);
      }
      if(r->num_critic_for_reviews != -1){
          snprintf(numReviews, 5000, "%d",r->num_critic_for_reviews);
      }
      if(r->budget != -1){
          snprintf(budget, 5000, "%li",r->budget);
      }
      if(r->actor_2_facebook_likes != -1){
          snprintf(actor2Likes, 5000, "%d",r->actor_2_facebook_likes);
      }
      if(r->title_year != -1){
          snprintf(titleYear, 5000, "%d",r->title_year);
      }
      if(r->imdb_score != -1){
          snprintf(imdbScore, 5000, "%f",r->imdb_score);
      }
      if(r->aspect_ratio != -1){
          snprintf(aspectRatio, 5000, "%f",r->aspect_ratio);
      }
      if(r->movie_facebook_likes != -1){
          snprintf(movieLikes, 5000, "%d",r->movie_facebook_likes);
      }

      if(strchr(r->movie_title, ',') == NULL){ //no commas in this movie title
        fprintf(writeFile,"%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
              r->color, r->director_name, numCritic, duration,
              directLikes, actor3Likes, r->actor_2_name,
              actor1Likes, gross, r->genres, r->actor_1_name,
              r->movie_title, numVoted, castLikes,
              r->actor_3_name, faceNumber, r->plot_keywords, r->movie_imdb_link,
              numReviews,r->language, r->country, r->content_rating,
              budget, titleYear, actor2Likes, imdbScore,
              aspectRatio, movieLikes);
      }
      else{ //put quotes around the movie title
        fprintf(writeFile,"%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,\"%s\",%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
                r->color, r->director_name, numCritic, duration,
                directLikes, actor3Likes, r->actor_2_name,
                actor1Likes, gross, r->genres, r->actor_1_name,
                r->movie_title, numVoted, castLikes,
                r->actor_3_name, faceNumber, r->plot_keywords, r->movie_imdb_link,
                numReviews,r->language, r->country, r->content_rating,
                budget, titleYear, actor2Likes, imdbScore,
                aspectRatio, movieLikes);
      }
      Record * temp = sortedHead;
      sortedHead = sortedHead->next;
      free(temp->content_rating);
      free(temp->country);
      free(temp->language);
      free(temp->movie_imdb_link);
      free(temp->plot_keywords);
      free(temp->actor_3_name);
      free(temp->movie_title);
      free(temp->actor_1_name);
      free(temp->genres);
      free(temp->actor_2_name);
      free(temp->director_name);
      free(temp->color);
      free(temp);
    }
    //send AllFiles.csv to client


    fclose(writeFile);
  }
}

//returns filename as a linked list of records
void* FileSortHandler(void * filename){
  FILE * sortFile = fopen((char *)filename, "r");
  char * line = NULL;
  size_t nbytes = 0 * sizeof(char);
  Record * prevRec = NULL;
  Record * head = NULL;
  Record * last;
  getline(&line, &nbytes, sortFile); //skip over first row (just the table headers)
  //eat sortFile line by line
  while (getline(&line, &nbytes, sortFile) != -1) {
    printf("%d - %s\n", (int)pthread_self(), line);
    head = (Record *)malloc(sizeof(Record));
    int start = 0;
    int end = 0;
    char lookAhead = line[end];
    int colId = 0;
    short inString = 0;
    while((lookAhead = line[end]) != '\n'){
      if(lookAhead == '"'){
        inString = inString == 0 ? 1 : 0; //keep track if we are inside of quotes
      }
      else{ //normal char
        if(lookAhead == ',' && inString == 0){ //token found!
          char * token = (char *)malloc(sizeof(char));
          token[0] = '\0';
          if(end != start){ //if end == start, this is an empty entry
            int tempEnd = end - 1;
            if(line[start] == '"' && line[end-1] == '"'){ //trim quotes
              tempEnd--;
              start++;
            }
            tempEnd++;//move past last valid character
            //trim whitespace
            while(isspace(line[tempEnd-1])){
              tempEnd--;
            }
            while(isspace(line[start])){
              start++;
            }
            if(line[tempEnd - 1] == ' '){
              line[tempEnd - 1] = '\0';
            }
            else{
              line[tempEnd] = '\0';
            }
            token = (char *)realloc(token, sizeof(char) * (tempEnd-start+1));
            memcpy(token, line + start, tempEnd - start+1);
          }
          switch(colId){
            case 0:
              head->color = token;
              break;
            case 1:
              head->director_name = token;
              break;
            case 2:
              head->num_critic_for_reviews = token[0] == '\0' ? -1 : atoi(token);
              free(token);
              break;
            case 3:
              head->duration = token[0] == '\0' ? -1 : atoi(token);
              free(token);
              break;
            case 4:
              head->director_facebook_likes = token[0] == '\0' ? -1 : atoi(token);
              free(token);
              break;
            case 5:
              head->actor_3_facebook_likes = token[0] == '\0' ? -1 : atoi(token);
              free(token);
              break;
            case 6:
              head->actor_2_name = token;
              break;
            case 7:
              head->actor_1_facebook_likes = token[0] == '\0' ? -1 : atoi(token);
              free(token);
              break;
            case 8:
              head->gross = token[0] == '\0' ? -1 : atoi(token);
              free(token);
              break;
            case 9:
              head->genres = token;
              break;
            case 10:
              head->actor_1_name = token;
              break;
            case 11:
              head->movie_title = token;
              break;
            case 12:
              head->num_voted_users = token[0] == '\0' ? -1 : atoi(token);
              free(token);
              break;
            case 13:
              head->cast_total_facebook_likes = token[0] == '\0' ? -1 : atoi(token);
              free(token);
              break;
            case 14:
              head->actor_3_name = token;
              break;
            case 15:
              head->facenumber_in_poster = token[0] == '\0' ? -1 : atoi(token);
              free(token);
              break;
            case 16:
              head->plot_keywords = token;
              break;
            case 17:
              head->movie_imdb_link = token;
              break;
            case 18:
              head->num_user_for_reviews = token[0] == '\0' ? -1 : atoi(token);
              free(token);
              break;
            case 19:
              head->language = token;
              break;
            case 20:
              head->country = token;
              break;
            case 21:
              head->content_rating = token;
              break;
            case 22:
              head->budget = token[0] == '\0' ? -1 :atol(token);
              free(token);
              break;
            case 23:
              head->title_year = token[0] == '\0' ? -1 : atoi(token);
              free(token);
              break;
            case 24:
              head->actor_2_facebook_likes = token[0] == '\0' ? -1 : atoi(token);
              free(token);
              break;
            case 25:
              head->imdb_score = token[0] == '\0' ? -1 : atof(token);
              free(token);
              break;
            case 26:
              head->aspect_ratio = token[0] == '\0' ? -1 : atof(token);
              free(token);
              break;
            case 27:
              //this case never happens lol
              head->movie_facebook_likes = token[0] == '\0' ? -1 : atoi(token);
              free(token);
              break;
            default:
              break;
          }
          colId++;
          start = ++end;
          continue;
        }
      }
      end++;
    }
    //add final column
    char * token = (char *)malloc(sizeof(char) * (end-start+1));
    memset(token, '\0', sizeof(char)* (end-start+1));
    int tempEnd = end;
    while(isspace(line[tempEnd])){
      tempEnd--;
    }
    while(isspace(line[start])){
      start++;
    }
    memcpy(token, line + start, tempEnd - start + 1);
    head->movie_facebook_likes = token[0] == '\0' ? -1 : atoi(token);
    free(token);

    //create a new struct
    if(prevRec == NULL){
      last = head;
    }
    head->next = prevRec;
    prevRec = head;
  }
  last->next = head; //complete circular linked list
  Record * temp = head->next;
  printf("head: %d\n", head->gross);
  while(temp != head){
    printf("%d\n", temp->gross);
    temp = temp->next;
  }
  fclose(sortFile);
  pthread_exit(head);
  return NULL;
}
