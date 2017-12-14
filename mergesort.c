#include "sorter-server.h"

/*
 Hersh Patel
 Saaketh Krosuri
 Systems Programming - 214
 Project 2: Multi-Threaded Data Sorter
 11/29/17
 */

/* Merge sort algorithm calls for recursively splitting array into separate elements
 and then merging off the individual elements while sorting them */

void merge(row_data orig[], int left, int mid, int right);
void merge1(row_data orig[], int left, int mid, int right);
void merge2(row_data orig[], int left, int mid, int right);
void merge3(row_data orig[], int left, int mid, int right);
void merge4(row_data orig[], int left, int mid, int right);
void merge5(row_data orig[], int left, int mid, int right);
void merge6(row_data orig[], int left, int mid, int right);
void merge7(row_data orig[], int left, int mid, int right);
void merge8(row_data orig[], int left, int mid, int right);
void merge9(row_data orig[], int left, int mid, int right);
void merge10(row_data orig[], int left, int mid, int right);
void merge11(row_data orig[], int left, int mid, int right);
void merge12(row_data orig[], int left, int mid, int right);
void merge13(row_data orig[], int left, int mid, int right);
void merge14(row_data orig[], int left, int mid, int right);
void merge15(row_data orig[], int left, int mid, int right);
void merge16(row_data orig[], int left, int mid, int right);
void merge17(row_data orig[], int left, int mid, int right);
void merge18(row_data orig[], int left, int mid, int right);
void merge19(row_data orig[], int left, int mid, int right);
void merge20(row_data orig[], int left, int mid, int right);
void merge21(row_data orig[], int left, int mid, int right);
void merge22(row_data orig[], int left, int mid, int right);
void merge23(row_data orig[], int left, int mid, int right);
void merge24(row_data orig[], int left, int mid, int right);
void merge25(row_data orig[], int left, int mid, int right);
void merge26(row_data orig[], int left, int mid, int right);
void merge27(row_data orig[], int left, int mid, int right);


void mergeSort(row_data orig[], int left, int right, int col){
  
  //Only perform recursive mergesort if working on valid portion of array
  if(left < right){
    
    //Find middle
    int mid = (left + right)/2;
    
    //Perform mergeSort on array values to left and right of middle point
    mergeSort(orig, left, mid, col);
    mergeSort(orig, mid+1, right, col);
    
    /* After mergeSort has reached arrays with "one value", merge back.
     Although this is not the most efficient way, we decided to break
     the merging aspect into 28 different merges based on the different
     types of columns that are possible within the struct / dataset. */
    switch (col){
       //color - string
      case 0:
        merge(orig, left, mid, right);
        break;
       
       //director_name - string
      case 1:
        merge1(orig, left, mid, right);
        break;
        
       //num_critic_for_reviews - numeric
      case 2:
        merge2(orig, left, mid, right);
        break;
        
       //duration - dateTime (numeric)
      case 3:
        merge3(orig, left, mid, right);
        break;
        
       //director_facebook_likes - numeric
      case 4:
        merge4(orig, left, mid, right);
        break;
        
       //actor_3_facebook_likes - numeric
      case 5:
        merge5(orig, left, mid, right);
        break;
        
       //actor_2_name - string
      case 6:
        merge6(orig, left, mid, right);
        break;
        
       //actor_1_facebook_likes - numeric
      case 7:
        merge7(orig, left, mid, right);
        break;
        
       //gross - numeric
      case 8:
        merge8(orig, left, mid, right);
        break;
        
       //genres - string
      case 9:
        merge9(orig, left, mid, right);
        break;
        
       //actor_1_name - string
      case 10:
        merge10(orig, left, mid, right);
        break;
        
       //movie_title - string
      case 11:
        merge11(orig, left, mid, right);
        break;
        
       //num_voted_users - numeric
      case 12:
        merge12(orig, left, mid, right);
        break;
        
       //cast_total_facebook_likes - numeric
      case 13:
        merge13(orig, left, mid, right);
        break;
        
       //actor_3_name - string
      case 14:
        merge14(orig, left, mid, right);
        break;
        
       //facenumber_in_poster - numeric
      case 15:
        merge15(orig, left, mid, right);
        break;
        
       //plot_keywords - string
      case 16:
        merge16(orig, left, mid, right);
        break;
        
       //movie_imdb_link - string
      case 17:
        merge17(orig, left, mid, right);
        break;
        
       //num_user_for_reviews - numeric
      case 18:
        merge18(orig, left, mid, right);
        break;
        
       //language - string
      case 19:
        merge19(orig, left, mid, right);
        break;
        
       //country - string
      case 20:
        merge20(orig, left, mid, right);
        break;
        
       //content_rating - string
      case 21:
        merge21(orig, left, mid, right);
        break;
        
       //budget - numeric
      case 22:
        merge22(orig, left, mid, right);
        break;
        
       //title_year - numeric
      case 23:
        merge23(orig, left, mid, right);
        break;
        
       //actor_2_facebook_likes - numeric
      case 24:
        merge24(orig, left, mid, right);
        break;
        
       //imdb_score - numeric (float)
      case 25:
        merge25(orig, left, mid, right);
        break;
        
       //aspect_ratio - numeric (float)
      case 26:
        merge26(orig, left, mid, right);
        break;
        
       //movie_facebook_likes - numeric
      case 27:
        merge27(orig, left, mid, right);
        break;
    }
  }
}

//color - string
void merge(row_data orig[], int left, int mid, int right){
  //Create temporary arrays to copy elements from original array into.
  //Right Array is from middle + 1 to right index
  //Left Array is from left index to middle; add 1 to size because left index can be 0
  int right_size = right - mid;
  int left_size = mid - left + 1;
  
  row_data *RightArr= malloc(right_size*sizeof(row_data));
  row_data *LeftArr= malloc(left_size*sizeof(row_data));
  //row_data RightArr[right_size];
  //row_data LeftArr[left_size];
  
  //Copy elements into temporary arrays
  int i;
  for(i=0; i<right_size; i++){
    RightArr[i] = orig[mid + i + 1];
  }
  for(i=0; i<left_size; i++){
    LeftArr[i] = orig[left + i];
  }
  
  //Merge arrays back into original array,
  int x = 0;
  int y = 0;
  int z = left;
  
  //Use strcasecmp function for lexicographical sort of str1, str2
  //Returns < 0 if str1 < str2
  //Returns = 0 if str1 = str2
  //Returns > 0 if str1 > str2
  while(x<left_size && y<right_size){
    if(strncasecmp(LeftArr[x].color, RightArr[y].color,1)==0){
      if(strcmp(LeftArr[x].color, RightArr[y].color)>0){
        orig[z] = RightArr[y];
        y = y+1;
      }else{
        orig[z] = LeftArr[x];
        x = x+1;
      }
    }
    else if(strcasecmp(LeftArr[x].color, RightArr[y].color)>0){
      orig[z] = RightArr[y];
      y = y+1;
    }else{
      orig[z] = LeftArr[x];
      x = x+1;
    }
    z = z+1;
  }
  
  //Make sure all remaining elements of Left or Right are added back
  if(y<right_size){
    while(y<right_size){
      orig[z] = RightArr[y];
      z++;
      y++;
    }
  }
  
  if(x<left_size){
    while(x<left_size){
      orig[z] = LeftArr[x];
      z++;
      x++;
    }
  }
  //free(RightArr);
  free(LeftArr);
}

//director_name - string
void merge1(row_data orig[], int left, int mid, int right){
  //Create temporary arrays to copy elements from original array into.
  //Right Array is from middle + 1 to right index
  //Left Array is from left index to middle; add 1 to size because left index can be 0
  int right_size = right - mid;
  int left_size = mid - left + 1;
  
  row_data *RightArr= malloc(right_size*sizeof(row_data));
  row_data *LeftArr= malloc(left_size*sizeof(row_data));
  //row_data RightArr[right_size];
  //row_data LeftArr[left_size];
  
  //Copy elements into temporary arrays
  int i;
  for(i=0; i<right_size; i++){
    RightArr[i] = orig[mid + i + 1];
  }
  for(i=0; i<left_size; i++){
    LeftArr[i] = orig[left + i];
  }
  
  //Merge arrays back into original array,
  int x = 0;
  int y = 0;
  int z = left;
  
  //Use strcasecmp function for lexicographical sort of str1, str2
  //Returns < 0 if str1 < str2
  //Returns = 0 if str1 = str2
  //Returns > 0 if str1 > str2
  while(x<left_size && y<right_size){
    if(strncasecmp(LeftArr[x].director_name, RightArr[y].director_name,1)==0){
      if(strcmp(LeftArr[x].director_name, RightArr[y].director_name)>0){
        orig[z] = RightArr[y];
        y = y+1;
      }else{
        orig[z] = LeftArr[x];
        x = x+1;
      }
    }
    else if(strcasecmp(LeftArr[x].director_name, RightArr[y].director_name)>0){
      orig[z] = RightArr[y];
      y = y+1;
    }else{
      orig[z] = LeftArr[x];
      x = x+1;
    }
    z = z+1;
  }
  
  //Make sure all remaining elements of Left or Right are added back
  if(y<right_size){
    while(y<right_size){
      orig[z] = RightArr[y];
      z++;
      y++;
    }
  }
  
  if(x<left_size){
    while(x<left_size){
      orig[z] = LeftArr[x];
      z++;
      x++;
    }
  }
  free(RightArr);
  free(LeftArr);
}

//num_critic_for_reviews - numeric
void merge2(row_data orig[], int left, int mid, int right){
  //Create temporary arrays to copy elements from original array into.
  //Right Array is from middle + 1 to right index
  //Left Array is from left index to middle; add 1 to size because left index can be 0
  int right_size = right - mid;
  int left_size = mid - left + 1;
  
  row_data *RightArr= malloc(right_size*sizeof(row_data));
  row_data *LeftArr= malloc(left_size*sizeof(row_data));
  
  //Copy elements into temporary arrays
  int i;
  for(i=0; i<right_size; i++){
    RightArr[i] = orig[mid + i + 1];
  }
  for(i=0; i<left_size; i++){
    LeftArr[i] = orig[left + i];
  }
  
  //Merge arrays back into original array,
  int x = 0;
  int y = 0;
  int z = left;
  
  while(x<left_size && y<right_size){
    if(LeftArr[x].num_critic_for_reviews > RightArr[y].num_critic_for_reviews){
      orig[z] = RightArr[y];
      y = y+1;
    }else{
      orig[z] = LeftArr[x];
      x = x+1;
    }
    z = z+1;
  }
  
  //Make sure all remaining elements of Left or Right are added back
  if(y<right_size){
    while(y<right_size){
      orig[z] = RightArr[y];
      z++;
      y++;
    }
  }
  
  if(x<left_size){
    while(x<left_size){
      orig[z] = LeftArr[x];
      z++;
      x++;
    }
  }
  free(RightArr);
  free(LeftArr);
}

//duration - dateTime (numeric)
void merge3(row_data orig[], int left, int mid, int right){
  //Create temporary arrays to copy elements from original array into.
  //Right Array is from middle + 1 to right index
  //Left Array is from left index to middle; add 1 to size because left index can be 0
  int right_size = right - mid;
  int left_size = mid - left + 1;
  
  row_data *RightArr= malloc(right_size*sizeof(row_data));
  row_data *LeftArr= malloc(left_size*sizeof(row_data));
  
  //Copy elements into temporary arrays
  int i;
  for(i=0; i<right_size; i++){
    RightArr[i] = orig[mid + i + 1];
  }
  for(i=0; i<left_size; i++){
    LeftArr[i] = orig[left + i];
  }
  
  //Merge arrays back into original array,
  int x = 0;
  int y = 0;
  int z = left;
  
  while(x<left_size && y<right_size){
    if(LeftArr[x].duration > RightArr[y].duration){
      orig[z] = RightArr[y];
      y = y+1;
    }else{
      orig[z] = LeftArr[x];
      x = x+1;
    }
    z = z+1;
  }
  
  //Make sure all remaining elements of Left or Right are added back
  if(y<right_size){
    while(y<right_size){
      orig[z] = RightArr[y];
      z++;
      y++;
    }
  }
  
  if(x<left_size){
    while(x<left_size){
      orig[z] = LeftArr[x];
      z++;
      x++;
    }
  }
  free(RightArr);
  free(LeftArr);
}


//director_facebook_likes - numeric
void merge4(row_data orig[], int left, int mid, int right){
  //Create temporary arrays to copy elements from original array into.
  //Right Array is from middle + 1 to right index
  //Left Array is from left index to middle; add 1 to size because left index can be 0
  int right_size = right - mid;
  int left_size = mid - left + 1;
  
  row_data *RightArr= malloc(right_size*sizeof(row_data));
  row_data *LeftArr= malloc(left_size*sizeof(row_data));
  
  //Copy elements into temporary arrays
  int i;
  for(i=0; i<right_size; i++){
    RightArr[i] = orig[mid + i + 1];
  }
  for(i=0; i<left_size; i++){
    LeftArr[i] = orig[left + i];
  }
  
  //Merge arrays back into original array,
  int x = 0;
  int y = 0;
  int z = left;
  
  while(x<left_size && y<right_size){
    if(LeftArr[x].director_facebook_likes > RightArr[y].director_facebook_likes){
      orig[z] = RightArr[y];
      y = y+1;
    }else{
      orig[z] = LeftArr[x];
      x = x+1;
    }
    z = z+1;
  }
  
  //Make sure all remaining elements of Left or Right are added back
  if(y<right_size){
    while(y<right_size){
      orig[z] = RightArr[y];
      z++;
      y++;
    }
  }
  
  if(x<left_size){
    while(x<left_size){
      orig[z] = LeftArr[x];
      z++;
      x++;
    }
  }
  free(RightArr);
  free(LeftArr);
}


//actor_3_facebook_likes - numeric
void merge5(row_data orig[], int left, int mid, int right){
  //Create temporary arrays to copy elements from original array into.
  //Right Array is from middle + 1 to right index
  //Left Array is from left index to middle; add 1 to size because left index can be 0
  int right_size = right - mid;
  int left_size = mid - left + 1;
  
  row_data *RightArr= malloc(right_size*sizeof(row_data));
  row_data *LeftArr= malloc(left_size*sizeof(row_data));
  
  //Copy elements into temporary arrays
  int i;
  for(i=0; i<right_size; i++){
    RightArr[i] = orig[mid + i + 1];
  }
  for(i=0; i<left_size; i++){
    LeftArr[i] = orig[left + i];
  }
  
  //Merge arrays back into original array,
  int x = 0;
  int y = 0;
  int z = left;
  
  while(x<left_size && y<right_size){
    if(LeftArr[x].actor_3_facebook_likes > RightArr[y].actor_3_facebook_likes){
      orig[z] = RightArr[y];
      y = y+1;
    }else{
      orig[z] = LeftArr[x];
      x = x+1;
    }
    z = z+1;
  }
  
  //Make sure all remaining elements of Left or Right are added back
  if(y<right_size){
    while(y<right_size){
      orig[z] = RightArr[y];
      z++;
      y++;
    }
  }
  
  if(x<left_size){
    while(x<left_size){
      orig[z] = LeftArr[x];
      z++;
      x++;
    }
  }
  free(RightArr);
  free(LeftArr);
}


//actor_2_name - string
void merge6(row_data orig[], int left, int mid, int right){
  //Create temporary arrays to copy elements from original array into.
  //Right Array is from middle + 1 to right index
  //Left Array is from left index to middle; add 1 to size because left index can be 0
  int right_size = right - mid;
  int left_size = mid - left + 1;
  
  row_data *RightArr= malloc(right_size*sizeof(row_data));
  row_data *LeftArr= malloc(left_size*sizeof(row_data));
  //row_data RightArr[right_size];
  //row_data LeftArr[left_size];
  
  //Copy elements into temporary arrays
  int i;
  for(i=0; i<right_size; i++){
    RightArr[i] = orig[mid + i + 1];
  }
  for(i=0; i<left_size; i++){
    LeftArr[i] = orig[left + i];
  }
  
  //Merge arrays back into original array,
  int x = 0;
  int y = 0;
  int z = left;
  
  //Use strcasecmp function for lexicographical sort of str1, str2
  //Returns < 0 if str1 < str2
  //Returns = 0 if str1 = str2
  //Returns > 0 if str1 > str2
  while(x<left_size && y<right_size){
    if(strncasecmp(LeftArr[x].actor_2_name, RightArr[y].actor_2_name,1)==0){
      if(strcmp(LeftArr[x].actor_2_name, RightArr[y].actor_2_name)>0){
        orig[z] = RightArr[y];
        y = y+1;
      }else{
        orig[z] = LeftArr[x];
        x = x+1;
      }
    }
    else if(strcasecmp(LeftArr[x].actor_2_name, RightArr[y].actor_2_name)>0){
      orig[z] = RightArr[y];
      y = y+1;
    }else{
      orig[z] = LeftArr[x];
      x = x+1;
    }
    z = z+1;
  }
  
  //Make sure all remaining elements of Left or Right are added back
  if(y<right_size){
    while(y<right_size){
      orig[z] = RightArr[y];
      z++;
      y++;
    }
  }
  
  if(x<left_size){
    while(x<left_size){
      orig[z] = LeftArr[x];
      z++;
      x++;
    }
  }
  free(RightArr);
  free(LeftArr);
}

//actor_1_facebook_likes - numeric
void merge7(row_data orig[], int left, int mid, int right){
  //Create temporary arrays to copy elements from original array into.
  //Right Array is from middle + 1 to right index
  //Left Array is from left index to middle; add 1 to size because left index can be 0
  int right_size = right - mid;
  int left_size = mid - left + 1;
  
  row_data *RightArr= malloc(right_size*sizeof(row_data));
  row_data *LeftArr= malloc(left_size*sizeof(row_data));
  
  //Copy elements into temporary arrays
  int i;
  for(i=0; i<right_size; i++){
    RightArr[i] = orig[mid + i + 1];
  }
  for(i=0; i<left_size; i++){
    LeftArr[i] = orig[left + i];
  }
  
  //Merge arrays back into original array,
  int x = 0;
  int y = 0;
  int z = left;
  
  while(x<left_size && y<right_size){
    if(LeftArr[x].actor_1_facebook_likes > RightArr[y].actor_1_facebook_likes){
      orig[z] = RightArr[y];
      y = y+1;
    }else{
      orig[z] = LeftArr[x];
      x = x+1;
    }
    z = z+1;
  }
  
  //Make sure all remaining elements of Left or Right are added back
  if(y<right_size){
    while(y<right_size){
      orig[z] = RightArr[y];
      z++;
      y++;
    }
  }
  
  if(x<left_size){
    while(x<left_size){
      orig[z] = LeftArr[x];
      z++;
      x++;
    }
  }
  free(RightArr);
  free(LeftArr);
}


//gross - numeric
void merge8(row_data orig[], int left, int mid, int right){
  //Create temporary arrays to copy elements from original array into.
  //Right Array is from middle + 1 to right index
  //Left Array is from left index to middle; add 1 to size because left index can be 0
  int right_size = right - mid;
  int left_size = mid - left + 1;
  
  row_data *RightArr= malloc(right_size*sizeof(row_data));
  row_data *LeftArr= malloc(left_size*sizeof(row_data));
  
  //Copy elements into temporary arrays
  int i;
  for(i=0; i<right_size; i++){
    RightArr[i] = orig[mid + i + 1];
  }
  for(i=0; i<left_size; i++){
    LeftArr[i] = orig[left + i];
  }
  
  //Merge arrays back into original array,
  int x = 0;
  int y = 0;
  int z = left;
  
  while(x<left_size && y<right_size){
    if(LeftArr[x].gross > RightArr[y].gross){
      orig[z] = RightArr[y];
      y = y+1;
    }else{
      orig[z] = LeftArr[x];
      x = x+1;
    }
    z = z+1;
  }
  
  //Make sure all remaining elements of Left or Right are added back
  if(y<right_size){
    while(y<right_size){
      orig[z] = RightArr[y];
      z++;
      y++;
    }
  }
  
  if(x<left_size){
    while(x<left_size){
      orig[z] = LeftArr[x];
      z++;
      x++;
    }
  }
  free(RightArr);
  free(LeftArr);
}


//genres - string
void merge9(row_data orig[], int left, int mid, int right){
  //Create temporary arrays to copy elements from original array into.
  //Right Array is from middle + 1 to right index
  //Left Array is from left index to middle; add 1 to size because left index can be 0
  int right_size = right - mid;
  int left_size = mid - left + 1;
  
  row_data *RightArr= malloc(right_size*sizeof(row_data));
  row_data *LeftArr= malloc(left_size*sizeof(row_data));
  //row_data RightArr[right_size];
  //row_data LeftArr[left_size];
  
  //Copy elements into temporary arrays
  int i;
  for(i=0; i<right_size; i++){
    RightArr[i] = orig[mid + i + 1];
  }
  for(i=0; i<left_size; i++){
    LeftArr[i] = orig[left + i];
  }
  
  //Merge arrays back into original array,
  int x = 0;
  int y = 0;
  int z = left;
  
  //Use strcasecmp function for lexicographical sort of str1, str2
  //Returns < 0 if str1 < str2
  //Returns = 0 if str1 = str2
  //Returns > 0 if str1 > str2
  while(x<left_size && y<right_size){
    if(strncasecmp(LeftArr[x].genres, RightArr[y].genres,1)==0){
      if(strcmp(LeftArr[x].genres, RightArr[y].genres)>0){
        orig[z] = RightArr[y];
        y = y+1;
      }else{
        orig[z] = LeftArr[x];
        x = x+1;
      }
    }
    else if(strcasecmp(LeftArr[x].genres, RightArr[y].genres)>0){
      orig[z] = RightArr[y];
      y = y+1;
    }else{
      orig[z] = LeftArr[x];
      x = x+1;
    }
    z = z+1;
  }
  
  //Make sure all remaining elements of Left or Right are added back
  if(y<right_size){
    while(y<right_size){
      orig[z] = RightArr[y];
      z++;
      y++;
    }
  }
  
  if(x<left_size){
    while(x<left_size){
      orig[z] = LeftArr[x];
      z++;
      x++;
    }
  }
  free(RightArr);
  free(LeftArr);
}


//actor_1_name - string
void merge10(row_data orig[], int left, int mid, int right){
  //Create temporary arrays to copy elements from original array into.
  //Right Array is from middle + 1 to right index
  //Left Array is from left index to middle; add 1 to size because left index can be 0
  int right_size = right - mid;
  int left_size = mid - left + 1;
  
  row_data *RightArr= malloc(right_size*sizeof(row_data));
  row_data *LeftArr= malloc(left_size*sizeof(row_data));
  //row_data RightArr[right_size];
  //row_data LeftArr[left_size];
  
  //Copy elements into temporary arrays
  int i;
  for(i=0; i<right_size; i++){
    RightArr[i] = orig[mid + i + 1];
  }
  for(i=0; i<left_size; i++){
    LeftArr[i] = orig[left + i];
  }
  
  //Merge arrays back into original array,
  int x = 0;
  int y = 0;
  int z = left;
  
  //Use strcasecmp function for lexicographical sort of str1, str2
  //Returns < 0 if str1 < str2
  //Returns = 0 if str1 = str2
  //Returns > 0 if str1 > str2
  while(x<left_size && y<right_size){
    if(strncasecmp(LeftArr[x].actor_1_name, RightArr[y].actor_1_name,1)==0){
      if(strcmp(LeftArr[x].actor_1_name, RightArr[y].actor_1_name)>0){
        orig[z] = RightArr[y];
        y = y+1;
      }else{
        orig[z] = LeftArr[x];
        x = x+1;
      }
    }
    else if(strcasecmp(LeftArr[x].actor_1_name, RightArr[y].actor_1_name)>0){
      orig[z] = RightArr[y];
      y = y+1;
    }else{
      orig[z] = LeftArr[x];
      x = x+1;
    }
    z = z+1;
  }
  
  //Make sure all remaining elements of Left or Right are added back
  if(y<right_size){
    while(y<right_size){
      orig[z] = RightArr[y];
      z++;
      y++;
    }
  }
  
  if(x<left_size){
    while(x<left_size){
      orig[z] = LeftArr[x];
      z++;
      x++;
    }
  }
  free(RightArr);
  free(LeftArr);
}


//movie_title - string
void merge11(row_data orig[], int left, int mid, int right){
  //Create temporary arrays to copy elements from original array into.
  //Right Array is from middle + 1 to right index
  //Left Array is from left index to middle; add 1 to size because left index can be 0
  int right_size = right - mid;
  int left_size = mid - left + 1;
  
  row_data *RightArr= malloc(right_size*sizeof(row_data));
  row_data *LeftArr= malloc(left_size*sizeof(row_data));
  //row_data RightArr[right_size];
  //row_data LeftArr[left_size];
  
  //Copy elements into temporary arrays
  int i;
  for(i=0; i<right_size; i++){
    RightArr[i] = orig[mid + i + 1];
  }
  for(i=0; i<left_size; i++){
    LeftArr[i] = orig[left + i];
  }
  
  //Merge arrays back isnto original array,
  int x = 0;
  int y = 0;
  int z = left;
  
  //Use strcasecmp function for lexicographical sort of str1, str2
  //Returns < 0 if str1 < str2
  //Returns = 0 if str1 = str2
  //Returns > 0 if str1 > str2
  while(x<left_size && y<right_size){
    if(strncasecmp(LeftArr[x].movie_title, RightArr[y].movie_title,1)==0){
      if(strcmp(LeftArr[x].movie_title, RightArr[y].movie_title)>0){
        orig[z] = RightArr[y];
        y = y+1;
      }else{
        orig[z] = LeftArr[x];
        x = x+1;
      }
    }
    else if(strcasecmp(LeftArr[x].movie_title, RightArr[y].movie_title)>0){
      orig[z] = RightArr[y];
      y = y+1;
    }else{
      orig[z] = LeftArr[x];
      x = x+1;
    }
    z = z+1;
  }
  
  
  //Make sure all remaining elements of Left or Right are added back
  if(y<right_size){
    //printf("check10002\n");
    while(y<right_size){
      orig[z] = RightArr[y];
      z++;
      y++;
    }
  }
  
  if(x<left_size){
    //printf("check1000\n");
    while(x<left_size){
      orig[z] = LeftArr[x];
      z++;
      x++;
    }
  }
  free(RightArr);
  free(LeftArr);
}


//num_voted_users - numeric
void merge12(row_data orig[], int left, int mid, int right){
  //Create temporary arrays to copy elements from original array into.
  //Right Array is from middle + 1 to right index
  //Left Array is from left index to middle; add 1 to size because left index can be 0
  int right_size = right - mid;
  int left_size = mid - left + 1;
  
  row_data *RightArr= malloc(right_size*sizeof(row_data));
  row_data *LeftArr= malloc(left_size*sizeof(row_data));
  
  //Copy elements into temporary arrays
  int i;
  for(i=0; i<right_size; i++){
    RightArr[i] = orig[mid + i + 1];
  }
  for(i=0; i<left_size; i++){
    LeftArr[i] = orig[left + i];
  }
  
  //Merge arrays back into original array,
  int x = 0;
  int y = 0;
  int z = left;
  
  while(x<left_size && y<right_size){
    if(LeftArr[x].num_voted_users > RightArr[y].num_voted_users){
      orig[z] = RightArr[y];
      y = y+1;
    }else{
      orig[z] = LeftArr[x];
      x = x+1;
    }
    z = z+1;
  }
  
  //Make sure all remaining elements of Left or Right are added back
  if(y<right_size){
    while(y<right_size){
      orig[z] = RightArr[y];
      z++;
      y++;
    }
  }
  
  if(x<left_size){
    while(x<left_size){
      orig[z] = LeftArr[x];
      z++;
      x++;
    }
  }
  free(RightArr);
  free(LeftArr);
}


//cast_total_facebook_likes - numeric
void merge13(row_data orig[], int left, int mid, int right){
  //Create temporary arrays to copy elements from original array into.
  //Right Array is from middle + 1 to right index
  //Left Array is from left index to middle; add 1 to size because left index can be 0
  int right_size = right - mid;
  int left_size = mid - left + 1;
  
  row_data *RightArr= malloc(right_size*sizeof(row_data));
  row_data *LeftArr= malloc(left_size*sizeof(row_data));
  
  //Copy elements into temporary arrays
  int i;
  for(i=0; i<right_size; i++){
    RightArr[i] = orig[mid + i + 1];
  }
  for(i=0; i<left_size; i++){
    LeftArr[i] = orig[left + i];
  }
  
  //Merge arrays back into original array,
  int x = 0;
  int y = 0;
  int z = left;
  
  while(x<left_size && y<right_size){
    if(LeftArr[x].cast_total_facebook_likes > RightArr[y].cast_total_facebook_likes){
      orig[z] = RightArr[y];
      y = y+1;
    }else{
      orig[z] = LeftArr[x];
      x = x+1;
    }
    z = z+1;
  }
  
  //Make sure all remaining elements of Left or Right are added back
  if(y<right_size){
    while(y<right_size){
      orig[z] = RightArr[y];
      z++;
      y++;
    }
  }
  
  if(x<left_size){
    while(x<left_size){
      orig[z] = LeftArr[x];
      z++;
      x++;
    }
  }
  free(RightArr);
  free(LeftArr);
}


//actor_3_name - string
void merge14(row_data orig[], int left, int mid, int right){
  //Create temporary arrays to copy elements from original array into.
  //Right Array is from middle + 1 to right index
  //Left Array is from left index to middle; add 1 to size because left index can be 0
  int right_size = right - mid;
  int left_size = mid - left + 1;
  
  row_data *RightArr= malloc(right_size*sizeof(row_data));
  row_data *LeftArr= malloc(left_size*sizeof(row_data));
  //row_data RightArr[right_size];
  //row_data LeftArr[left_size];
  
  //Copy elements into temporary arrays
  int i;
  for(i=0; i<right_size; i++){
    RightArr[i] = orig[mid + i + 1];
  }
  for(i=0; i<left_size; i++){
    LeftArr[i] = orig[left + i];
  }
  
  //Merge arrays back into original array,
  int x = 0;
  int y = 0;
  int z = left;
  
  while(x<left_size && y<right_size){
    if(strncasecmp(LeftArr[x].actor_3_name, RightArr[y].actor_3_name,1)==0){
      if(strcmp(LeftArr[x].actor_3_name, RightArr[y].actor_3_name)>0){
        orig[z] = RightArr[y];
        y = y+1;
      }else{
        orig[z] = LeftArr[x];
        x = x+1;
      }
    }
    else if(strcasecmp(LeftArr[x].actor_3_name, RightArr[y].actor_3_name)>0){
      orig[z] = RightArr[y];
      y = y+1;
    }else{
      orig[z] = LeftArr[x];
      x = x+1;
    }
    z = z+1;
  }
  
  //Make sure all remaining elements of Left or Right are added back
  if(y<right_size){
    while(y<right_size){
      orig[z] = RightArr[y];
      z++;
      y++;
    }
  }
  
  if(x<left_size){
    while(x<left_size){
      orig[z] = LeftArr[x];
      z++;
      x++;
    }
  }
  free(RightArr);
  free(LeftArr);
}


//facenumber_in_poster - numeric
void merge15(row_data orig[], int left, int mid, int right){
  //Create temporary arrays to copy elements from original array into.
  //Right Array is from middle + 1 to right index
  //Left Array is from left index to middle; add 1 to size because left index can be 0
  int right_size = right - mid;
  int left_size = mid - left + 1;
  
  row_data *RightArr= malloc(right_size*sizeof(row_data));
  row_data *LeftArr= malloc(left_size*sizeof(row_data));
  
  //Copy elements into temporary arrays
  int i;
  for(i=0; i<right_size; i++){
    RightArr[i] = orig[mid + i + 1];
  }
  for(i=0; i<left_size; i++){
    LeftArr[i] = orig[left + i];
  }
  
  //Merge arrays back into original array,
  int x = 0;
  int y = 0;
  int z = left;
  
  while(x<left_size && y<right_size){
    if(LeftArr[x].facenumber_in_poster > RightArr[y].facenumber_in_poster){
      orig[z] = RightArr[y];
      y = y+1;
    }else{
      orig[z] = LeftArr[x];
      x = x+1;
    }
    z = z+1;
  }
  
  //Make sure all remaining elements of Left or Right are added back
  if(y<right_size){
    while(y<right_size){
      orig[z] = RightArr[y];
      z++;
      y++;
    }
  }
  
  if(x<left_size){
    while(x<left_size){
      orig[z] = LeftArr[x];
      z++;
      x++;
    }
  }
  free(RightArr);
  free(LeftArr);
}


//plot_keywords - string
void merge16(row_data orig[], int left, int mid, int right){
  //Create temporary arrays to copy elements from original array into.
  //Right Array is from middle + 1 to right index
  //Left Array is from left index to middle; add 1 to size because left index can be 0
  int right_size = right - mid;
  int left_size = mid - left + 1;
  
  row_data *RightArr= malloc(right_size*sizeof(row_data));
  row_data *LeftArr= malloc(left_size*sizeof(row_data));
  //row_data RightArr[right_size];
  //row_data LeftArr[left_size];
  
  //Copy elements into temporary arrays
  int i;
  for(i=0; i<right_size; i++){
    RightArr[i] = orig[mid + i + 1];
  }
  for(i=0; i<left_size; i++){
    LeftArr[i] = orig[left + i];
  }
  
  //Merge arrays back into original array,
  int x = 0;
  int y = 0;
  int z = left;
  
  //Use strcasecmp function for lexicographical sort of str1, str2
  //Returns < 0 if str1 < str2
  //Returns = 0 if str1 = str2
  //Returns > 0 if str1 > str2
  while(x<left_size && y<right_size){
    if(strncasecmp(LeftArr[x].plot_keywords, RightArr[y].plot_keywords,1)==0){
      if(strcmp(LeftArr[x].plot_keywords, RightArr[y].plot_keywords)>0){
        orig[z] = RightArr[y];
        y = y+1;
      }else{
        orig[z] = LeftArr[x];
        x = x+1;
      }
    }
    else if(strcasecmp(LeftArr[x].plot_keywords, RightArr[y].plot_keywords)>0){
      orig[z] = RightArr[y];
      y = y+1;
    }else{
      orig[z] = LeftArr[x];
      x = x+1;
    }
    z = z+1;
  }
  
  //Make sure all remaining elements of Left or Right are added back
  if(y<right_size){
    while(y<right_size){
      orig[z] = RightArr[y];
      z++;
      y++;
    }
  }
  
  if(x<left_size){
    while(x<left_size){
      orig[z] = LeftArr[x];
      z++;
      x++;
    }
  }
  free(RightArr);
  free(LeftArr);
}


//movie_imdb_link - string
void merge17(row_data orig[], int left, int mid, int right){
  //Create temporary arrays to copy elements from original array into.
  //Right Array is from middle + 1 to right index
  //Left Array is from left index to middle; add 1 to size because left index can be 0
  int right_size = right - mid;
  int left_size = mid - left + 1;
  
  row_data *RightArr= malloc(right_size*sizeof(row_data));
  row_data *LeftArr= malloc(left_size*sizeof(row_data));
  //row_data RightArr[right_size];
  //row_data LeftArr[left_size];
  
  //Copy elements into temporary arrays
  int i;
  for(i=0; i<right_size; i++){
    RightArr[i] = orig[mid + i + 1];
  }
  for(i=0; i<left_size; i++){
    LeftArr[i] = orig[left + i];
  }
  
  //Merge arrays back into original array,
  int x = 0;
  int y = 0;
  int z = left;
  
  //Use strcasecmp function for lexicographical sort of str1, str2
  //Returns < 0 if str1 < str2
  //Returns = 0 if str1 = str2
  //Returns > 0 if str1 > str2
  while(x<left_size && y<right_size){
    if(strncasecmp(LeftArr[x].movie_imdb_link, RightArr[y].movie_imdb_link,1)==0){
      if(strcmp(LeftArr[x].movie_imdb_link, RightArr[y].movie_imdb_link)>0){
        orig[z] = RightArr[y];
        y = y+1;
      }else{
        orig[z] = LeftArr[x];
        x = x+1;
      }
    }
    else if(strcasecmp(LeftArr[x].movie_imdb_link, RightArr[y].movie_imdb_link)>0){
      orig[z] = RightArr[y];
      y = y+1;
    }else{
      orig[z] = LeftArr[x];
      x = x+1;
    }
    z = z+1;
  }
  
  //Make sure all remaining elements of Left or Right are added back
  if(y<right_size){
    while(y<right_size){
      orig[z] = RightArr[y];
      z++;
      y++;
    }
  }
  
  if(x<left_size){
    while(x<left_size){
      orig[z] = LeftArr[x];
      z++;
      x++;
    }
  }
  free(RightArr);
  free(LeftArr);
}


//num_user_for_reviews - numeric
void merge18(row_data orig[], int left, int mid, int right){
  //Create temporary arrays to copy elements from original array into.
  //Right Array is from middle + 1 to right index
  //Left Array is from left index to middle; add 1 to size because left index can be 0
  int right_size = right - mid;
  int left_size = mid - left + 1;
  
  row_data *RightArr= malloc(right_size*sizeof(row_data));
  row_data *LeftArr= malloc(left_size*sizeof(row_data));
  
  //Copy elements into temporary arrays
  int i;
  for(i=0; i<right_size; i++){
    RightArr[i] = orig[mid + i + 1];
  }
  for(i=0; i<left_size; i++){
    LeftArr[i] = orig[left + i];
  }
  
  //Merge arrays back into original array,
  int x = 0;
  int y = 0;
  int z = left;
  
  while(x<left_size && y<right_size){
    if(LeftArr[x].num_user_for_reviews > RightArr[y].num_user_for_reviews){
      orig[z] = RightArr[y];
      y = y+1;
    }else{
      orig[z] = LeftArr[x];
      x = x+1;
    }
    z = z+1;
  }
  
  //Make sure all remaining elements of Left or Right are added back
  if(y<right_size){
    while(y<right_size){
      orig[z] = RightArr[y];
      z++;
      y++;
    }
  }
  
  if(x<left_size){
    while(x<left_size){
      orig[z] = LeftArr[x];
      z++;
      x++;
    }
  }
  free(RightArr);
  free(LeftArr);
}

//language - string
void merge19(row_data orig[], int left, int mid, int right){
  //Create temporary arrays to copy elements from original array into.
  //Right Array is from middle + 1 to right index
  //Left Array is from left index to middle; add 1 to size because left index can be 0
  int right_size = right - mid;
  int left_size = mid - left + 1;
  
  row_data *RightArr= malloc(right_size*sizeof(row_data));
  row_data *LeftArr= malloc(left_size*sizeof(row_data));
  //row_data RightArr[right_size];
  //row_data LeftArr[left_size];
  
  //Copy elements into temporary arrays
  int i;
  for(i=0; i<right_size; i++){
    RightArr[i] = orig[mid + i + 1];
  }
  for(i=0; i<left_size; i++){
    LeftArr[i] = orig[left + i];
  }
  
  //Merge arrays back into original array,
  int x = 0;
  int y = 0;
  int z = left;
  
  //Use strcasecmp function for lexicographical sort of str1, str2
  //Returns < 0 if str1 < str2
  //Returns = 0 if str1 = str2
  //Returns > 0 if str1 > str2
  while(x<left_size && y<right_size){
    if(strncasecmp(LeftArr[x].language, RightArr[y].language,1)==0){
      if(strcmp(LeftArr[x].language, RightArr[y].language)>0){
        orig[z] = RightArr[y];
        y = y+1;
      }else{
        orig[z] = LeftArr[x];
        x = x+1;
      }
    }
    else if(strcasecmp(LeftArr[x].language, RightArr[y].language)>0){
      orig[z] = RightArr[y];
      y = y+1;
    }else{
      orig[z] = LeftArr[x];
      x = x+1;
    }
    z = z+1;
  }
  
  //Make sure all remaining elements of Left or Right are added back
  if(y<right_size){
    while(y<right_size){
      orig[z] = RightArr[y];
      z++;
      y++;
    }
  }
  
  if(x<left_size){
    while(x<left_size){
      orig[z] = LeftArr[x];
      z++;
      x++;
    }
  }
  free(RightArr);
  free(LeftArr);
}


//country - string
void merge20(row_data orig[], int left, int mid, int right){
  //Create temporary arrays to copy elements from original array into.
  //Right Array is from middle + 1 to right index
  //Left Array is from left index to middle; add 1 to size because left index can be 0
  int right_size = right - mid;
  int left_size = mid - left + 1;
  
  row_data *RightArr= malloc(right_size*sizeof(row_data));
  row_data *LeftArr= malloc(left_size*sizeof(row_data));
  //row_data RightArr[right_size];
  //row_data LeftArr[left_size];
  
  //Copy elements into temporary arrays
  int i;
  for(i=0; i<right_size; i++){
    RightArr[i] = orig[mid + i + 1];
  }
  for(i=0; i<left_size; i++){
    LeftArr[i] = orig[left + i];
  }
  
  //Merge arrays back into original array,
  int x = 0;
  int y = 0;
  int z = left;
  
  //Use strcasecmp function for lexicographical sort of str1, str2
  //Returns < 0 if str1 < str2
  //Returns = 0 if str1 = str2
  //Returns > 0 if str1 > str2
  while(x<left_size && y<right_size){
    if(strncasecmp(LeftArr[x].country, RightArr[y].country,1)==0){
      if(strcmp(LeftArr[x].country, RightArr[y].country)>0){
        orig[z] = RightArr[y];
        y = y+1;
      }else{
        orig[z] = LeftArr[x];
        x = x+1;
      }
    }
    else if(strcasecmp(LeftArr[x].country, RightArr[y].country)>0){
      orig[z] = RightArr[y];
      y = y+1;
    }else{
      orig[z] = LeftArr[x];
      x = x+1;
    }
    z = z+1;
  }
  
  //Make sure all remaining elements of Left or Right are added back
  if(y<right_size){
    while(y<right_size){
      orig[z] = RightArr[y];
      z++;
      y++;
    }
  }
  
  if(x<left_size){
    while(x<left_size){
      orig[z] = LeftArr[x];
      z++;
      x++;
    }
  }
  free(RightArr);
  free(LeftArr);
}


//content_rating - string
void merge21(row_data orig[], int left, int mid, int right){
  //Create temporary arrays to copy elements from original array into.
  //Right Array is from middle + 1 to right index
  //Left Array is from left index to middle; add 1 to size because left index can be 0
  int right_size = right - mid;
  int left_size = mid - left + 1;
  
  row_data *RightArr= malloc(right_size*sizeof(row_data));
  row_data *LeftArr= malloc(left_size*sizeof(row_data));
  //row_data RightArr[right_size];
  //row_data LeftArr[left_size];
  
  //Copy elements into temporary arrays
  int i;
  for(i=0; i<right_size; i++){
    RightArr[i] = orig[mid + i + 1];
  }
  for(i=0; i<left_size; i++){
    LeftArr[i] = orig[left + i];
  }
  
  //Merge arrays back into original array,
  int x = 0;
  int y = 0;
  int z = left;
  
  //Use strcasecmp function for lexicographical sort of str1, str2
  //Returns < 0 if str1 < str2
  //Returns = 0 if str1 = str2
  //Returns > 0 if str1 > str2
  while(x<left_size && y<right_size){
    if(strncasecmp(LeftArr[x].content_rating, RightArr[y].content_rating,1)==0){
      if(strcmp(LeftArr[x].content_rating, RightArr[y].content_rating)>0){
        orig[z] = RightArr[y];
        y = y+1;
      }else{
        orig[z] = LeftArr[x];
        x = x+1;
      }
    }
    else if(strcasecmp(LeftArr[x].content_rating, RightArr[y].content_rating)>0){
      orig[z] = RightArr[y];
      y = y+1;
    }else{
      orig[z] = LeftArr[x];
      x = x+1;
    }
    z = z+1;
  }
  
  //Make sure all remaining elements of Left or Right are added back
  if(y<right_size){
    while(y<right_size){
      orig[z] = RightArr[y];
      z++;
      y++;
    }
  }
  
  if(x<left_size){
    while(x<left_size){
      orig[z] = LeftArr[x];
      z++;
      x++;
    }
  }
  free(RightArr);
  free(LeftArr);
}


//budget - numeric
void merge22(row_data orig[], int left, int mid, int right){
  //Create temporary arrays to copy elements from original array into.
  //Right Array is from middle + 1 to right index
  //Left Array is from left index to middle; add 1 to size because left index can be 0
  int right_size = right - mid;
  int left_size = mid - left + 1;
  
  row_data *RightArr= malloc(right_size*sizeof(row_data));
  row_data *LeftArr= malloc(left_size*sizeof(row_data));
  
  //Copy elements into temporary arrays
  int i;
  for(i=0; i<right_size; i++){
    RightArr[i] = orig[mid + i + 1];
  }
  for(i=0; i<left_size; i++){
    LeftArr[i] = orig[left + i];
  }
  
  //Merge arrays back into original array,
  int x = 0;
  int y = 0;
  int z = left;
  
  while(x<left_size && y<right_size){
    if(LeftArr[x].budget > RightArr[y].budget){
      orig[z] = RightArr[y];
      y = y+1;
    }else{
      orig[z] = LeftArr[x];
      x = x+1;
    }
    z = z+1;
  }
  
  //Make sure all remaining elements of Left or Right are added back
  if(y<right_size){
    while(y<right_size){
      orig[z] = RightArr[y];
      z++;
      y++;
    }
  }
  
  if(x<left_size){
    while(x<left_size){
      orig[z] = LeftArr[x];
      z++;
      x++;
    }
  }
  free(RightArr);
  free(LeftArr);
}


//title_year - numeric
void merge23(row_data orig[], int left, int mid, int right){
  //Create temporary arrays to copy elements from original array into.
  //Right Array is from middle + 1 to right index
  //Left Array is from left index to middle; add 1 to size because left index can be 0
  int right_size = right - mid;
  int left_size = mid - left + 1;
  
  row_data *RightArr= malloc(right_size*sizeof(row_data));
  row_data *LeftArr= malloc(left_size*sizeof(row_data));
  
  //Copy elements into temporary arrays
  int i;
  for(i=0; i<right_size; i++){
    RightArr[i] = orig[mid + i + 1];
  }
  for(i=0; i<left_size; i++){
    LeftArr[i] = orig[left + i];
  }
  
  //Merge arrays back into original array,
  int x = 0;
  int y = 0;
  int z = left;
  
  while(x<left_size && y<right_size){
    if(LeftArr[x].title_year > RightArr[y].title_year){
      orig[z] = RightArr[y];
      y = y+1;
    }else{
      orig[z] = LeftArr[x];
      x = x+1;
    }
    z = z+1;
  }
  
  //Make sure all remaining elements of Left or Right are added back
  if(y<right_size){
    while(y<right_size){
      orig[z] = RightArr[y];
      z++;
      y++;
    }
  }
  
  if(x<left_size){
    while(x<left_size){
      orig[z] = LeftArr[x];
      z++;
      x++;
    }
  }
  free(RightArr);
  free(LeftArr);
}


//actor_2_facebook_likes - numeric
void merge24(row_data orig[], int left, int mid, int right){
  //Create temporary arrays to copy elements from original array into.
  //Right Array is from middle + 1 to right index
  //Left Array is from left index to middle; add 1 to size because left index can be 0
  int right_size = right - mid;
  int left_size = mid - left + 1;
  
  row_data *RightArr= malloc(right_size*sizeof(row_data));
  row_data *LeftArr= malloc(left_size*sizeof(row_data));
  
  //Copy elements into temporary arrays
  int i;
  for(i=0; i<right_size; i++){
    RightArr[i] = orig[mid + i + 1];
  }
  for(i=0; i<left_size; i++){
    LeftArr[i] = orig[left + i];
  }
  
  //Merge arrays back into original array,
  int x = 0;
  int y = 0;
  int z = left;
  
  while(x<left_size && y<right_size){
    if(LeftArr[x].actor_2_facebook_likes > RightArr[y].actor_2_facebook_likes){
      orig[z] = RightArr[y];
      y = y+1;
    }else{
      orig[z] = LeftArr[x];
      x = x+1;
    }
    z = z+1;
  }
  
  //Make sure all remaining elements of Left or Right are added back
  if(y<right_size){
    while(y<right_size){
      orig[z] = RightArr[y];
      z++;
      y++;
    }
  }
  
  if(x<left_size){
    while(x<left_size){
      orig[z] = LeftArr[x];
      z++;
      x++;
    }
  }
  free(RightArr);
  free(LeftArr);
}


//imdb_score - numeric (float)
void merge25(row_data orig[], int left, int mid, int right){
  //Create temporary arrays to copy elements from original array into.
  //Right Array is from middle + 1 to right index
  //Left Array is from left index to middle; add 1 to size because left index can be 0
  int right_size = right - mid;
  int left_size = mid - left + 1;
  
  row_data *RightArr= malloc(right_size*sizeof(row_data));
  row_data *LeftArr= malloc(left_size*sizeof(row_data));
  
  //Copy elements into temporary arrays
  int i;
  for(i=0; i<right_size; i++){
    RightArr[i] = orig[mid + i + 1];
  }
  for(i=0; i<left_size; i++){
    LeftArr[i] = orig[left + i];
  }
  
  //Merge arrays back into original array,
  int x = 0;
  int y = 0;
  int z = left;
  
  while(x<left_size && y<right_size){
    if(LeftArr[x].imdb_score > RightArr[y].imdb_score){
      orig[z] = RightArr[y];
      y = y+1;
    }else{
      orig[z] = LeftArr[x];
      x = x+1;
    }
    z = z+1;
  }
  
  //Make sure all remaining elements of Left or Right are added back
  if(y<right_size){
    while(y<right_size){
      orig[z] = RightArr[y];
      z++;
      y++;
    }
  }
  
  if(x<left_size){
    while(x<left_size){
      orig[z] = LeftArr[x];
      z++;
      x++;
    }
  }
  free(RightArr);
  free(LeftArr);
}


//aspect_ratio - numeric (float)
void merge26(row_data orig[], int left, int mid, int right){
  //Create temporary arrays to copy elements from original array into.
  //Right Array is from middle + 1 to right index
  //Left Array is from left index to middle; add 1 to size because left index can be 0
  int right_size = right - mid;
  int left_size = mid - left + 1;
  
  row_data *RightArr= malloc(right_size*sizeof(row_data));
  row_data *LeftArr= malloc(left_size*sizeof(row_data));
  
  //Copy elements into temporary arrays
  int i;
  for(i=0; i<right_size; i++){
    RightArr[i] = orig[mid + i + 1];
  }
  for(i=0; i<left_size; i++){
    LeftArr[i] = orig[left + i];
  }
  
  //Merge arrays back into original array,
  int x = 0;
  int y = 0;
  int z = left;
  
  while(x<left_size && y<right_size){
    if(LeftArr[x].aspect_ratio > RightArr[y].aspect_ratio){
      orig[z] = RightArr[y];
      y = y+1;
    }else{
      orig[z] = LeftArr[x];
      x = x+1;
    }
    z = z+1;
  }
  
  //Make sure all remaining elements of Left or Right are added back
  if(y<right_size){
    while(y<right_size){
      orig[z] = RightArr[y];
      z++;
      y++;
    }
  }
  
  if(x<left_size){
    while(x<left_size){
      orig[z] = LeftArr[x];
      z++;
      x++;
    }
  }
  free(RightArr);
  free(LeftArr);
}


//movie_facebook_likes - numeric
void merge27(row_data orig[], int left, int mid, int right){
  //Create temporary arrays to copy elements from original array into.
  //Right Array is from middle + 1 to right index
  //Left Array is from left index to middle; add 1 to size because left index can be 0
  int right_size = right - mid;
  int left_size = mid - left + 1;
  
  row_data *RightArr= malloc(right_size*sizeof(row_data));
  row_data *LeftArr= malloc(left_size*sizeof(row_data));
  
  //Copy elements into temporary arrays
  int i;
  for(i=0; i<right_size; i++){
    RightArr[i] = orig[mid + i + 1];
  }
  for(i=0; i<left_size; i++){
    LeftArr[i] = orig[left + i];
  }
  
  //Merge arrays back into original array,
  int x = 0;
  int y = 0;
  int z = left;
  
  while(x<left_size && y<right_size){
    if(LeftArr[x].movie_facebook_likes > RightArr[y].movie_facebook_likes){
      orig[z] = RightArr[y];
      y = y+1;
    }else{
      orig[z] = LeftArr[x];
      x = x+1;
    }
    z = z+1;
  }
  
  //Make sure all remaining elements of Left or Right are added back
  if(y<right_size){
    while(y<right_size){
      orig[z] = RightArr[y];
      z++;
      y++;
    }
  }
  
  if(x<left_size){
    while(x<left_size){
      orig[z] = LeftArr[x];
      z++;
      x++;
    }
  }
  free(RightArr);
  free(LeftArr);
}
