#ifndef SORTER_THREAD_H
#define SORTER_THREAD_H

void* FileSortHandler(void * filename);
void sortCSVs(DIR * inputDir, char * inDir, DIR * outputDir, char * outDir);


#endif
