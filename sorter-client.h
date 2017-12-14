#ifndef SORTER_CLIENT_H
#define SORTER_CLIENT_H

void* FileSortHandler(void * filename);
void sortCSVs(DIR * inputDir, char * inDir, DIR * outputDir, char * outDir, pthread_t * threads, short isMain, char* sorting);


#endif
