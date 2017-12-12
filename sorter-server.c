 #include <stdio.h>
 #include <string.h>
 #include <math.h>
 #include <ctype.h>
 #include <stdlib.h>
 #include <dirent.h>
 #include <unistd.h>
 #include <pthread.h>
 #include <sys/types.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include "sorter-server.h"

void error(char *msg)
{
    perror(msg);
    exit(1);
}
pthread_t * threads;
int threadCount;
int main(int argc, char *argv[])
{
     int sockfd, newsockfd, portno, clilen;
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
       newsockfd = accept(sockfd, (struct sockaddr *) &cli_addr, &clilen);
       printf("creating socket: %d\n", newsockfd);
       pthread_t thr;
       pthread_create (&thr, NULL, FileHandler, (void *)&newsockfd);
       threads[++threadCount] = thr;
     }
     return 0;
}

void* FileHandler(void * socket){
  int newsock = *((int *)socket);
  if (newsock < 0)
  {
    printf("connect error: %d\n", newsock);
    error("ERROR on accept");
  }
  int pid = 0;
  int sortby = 0;
  int fileSize = 0;
  int OGsock = newsock;
  read(newsock,&pid,4); //get pid
  newsock = OGsock;
  printf("%d - PID: %d\n",newsock,pid);
  read(newsock,&sortby,4); //get sort col
  newsock = OGsock;
  printf("%d - sort int: %d\n",newsock,sortby);
  read(newsock,&fileSize,8); //get size of incoming file
  newsock = OGsock;
  char buffer[fileSize];
  bzero(buffer, fileSize);
  printf("%d - file size: %d\n",newsock,fileSize);
  read(newsock, buffer,fileSize); //load file into buffer
  newsock = OGsock;
  printf("%d - buffer: %s\n",newsock,buffer);

  //all finished signal, lets sort and build this
  if(pid == -1){
    //join all threads (besides this one)
    threadCount--;
    for(;threadCount >= 0; threadCount--){
      printf("%d => %lu\n", threadCount, threads[threadCount]);
      pthread_join(threads[threadCount], NULL);
      printf("joined\n");
    }
    printf("all done, merge into list and send");
    //force the linked list to not be circular
    //send the length
    //send the entire list to the client
    //send -1 (all done)
  }
  else{
    //run buffer through the mergesort and attach it to the circular linked list
    printf("%d - merging!\n", newsock);
  }
  close(newsock);
  pthread_exit(NULL);
  return 0;
}
