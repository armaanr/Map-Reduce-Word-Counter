#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/epoll.h>

#include "libmapreduce.h"
#include "libds/libds.h"


static const int BUFFER_SIZE = 2048;  /**< Size of the buffer used by read_from_fd(). */

pthread_t thread;
int processes;
int **fd;
char ** buffer;    

/**
 * Adds the key-value pair to the mapreduce data structure.  This may
 * require a reduce() operation.
 *
 * @param key
 *    The key of the key-value pair.  The key has been malloc()'d by
 *    read_from_fd() and must be free()'d by you at some point.
 * @param value
 *    The value of the key-value pair.  The value has been malloc()'d
 *    by read_from_fd() and must be free()'d by you at some point.
 * @param mr
 *    The pass-through mapreduce data structure (from read_from_fd()).
 */
static void process_key_value(const char *key, const char *value, mapreduce_t *mr)
{
	
	
	unsigned long rev;
	
	char * reduced;
	reduced = datastore_get(&mr->data,key,&rev);

	if (reduced == NULL)  
	{
		datastore_put(&mr->data,  key,  value);
	}
	else
	{
		char* temp = reduced;
		reduced = mr->myreducer(temp, value);
		
		datastore_update(&(mr->data), key, reduced, rev);
free(temp);		
free(reduced);
		
	}
	
	free(key);
	free(value);
	
}



/**
 * Helper function.  Reads up to BUFFER_SIZE from a file descriptor into a
 * buffer and calls process_key_value() when for each and every key-value
 * pair that is read from the file descriptor.
 *
 * Each key-value must be in a "Key: Value" format, identical to MP1, and
 * each pair must be terminated by a newline ('\n').
 *
 * Each unique file descriptor must have a unique buffer and the buffer
 * must be of size (BUFFER_SIZE + 1).  Therefore, if you have two
 * unique file descriptors, you must have two buffers that each have
 * been malloc()'d to size (BUFFER_SIZE + 1).
 *
 * Note that read_from_fd() makes a read() call and will block if the
 * fd does not have data ready to be read.  This function is complete
 * and does not need to be modified as part of this MP.
 *
 * @param fd
 *    File descriptor to read from.
 * @param buffer
 *    A unique buffer associated with the fd.  This buffer may have
 *    a partial key-value pair between calls to read_from_fd() and
 *    must not be modified outside the context of read_from_fd().
 * @param mr
 *    Pass-through mapreduce_t structure (to process_key_value()).
 *
 * @retval 1
 *    Data was available and was read successfully.
 * @retval 0
 *    The file descriptor fd has been closed, no more data to read.
 * @retval -1
 *    The call to read() produced an error.
 */
static int read_from_fd(int fd, char *buffer, mapreduce_t *mr)
{
	/* Find the end of the string. */
	int offset = strlen(buffer);
    
	/* Read bytes from the underlying stream. */
	int bytes_read = read(fd, buffer + offset, BUFFER_SIZE - offset);
	if (bytes_read == 0)
		return 0;
	else if(bytes_read < 0)
	{
		fprintf(stderr, "error in read.\n");
		return -1;
	}
    
	buffer[offset + bytes_read] = '\0';
    
	/* Loop through each "key: value\n" line from the fd. */
	char *line;
	while ((line = strstr(buffer, "\n")) != NULL)
	{
		*line = '\0';
        
		/* Find the key/value split. */
		char *split = strstr(buffer, ": ");
		if (split == NULL)
			continue;
        
		/* Allocate and assign memory */
		char *key = malloc((split - buffer + 1) * sizeof(char));
		char *value = malloc((strlen(split) - 2 + 1) * sizeof(char));
        
		strncpy(key, buffer, split - buffer);
		key[split - buffer] = '\0';
        
		strcpy(value, split + 2);
        
		/* Process the key/value. */
		process_key_value(key, value, mr);
        
		/* Shift the contents of the buffer to remove the space used by the processed line. */
		memmove(buffer, line + 1, BUFFER_SIZE - ((line + 1) - buffer));
		buffer[BUFFER_SIZE - ((line + 1) - buffer)] = '\0';
	}
    
	return 1;
}


/**
 * Initialize the mapreduce data structure, given a map and a reduce
 * function pointer.
 */
void mapreduce_init(mapreduce_t *mr,
                    void (*mymap)(int, const char *),
                    const char *(*myreduce)(const char *, const char *))
{
    datastore_init(&mr->data);
    mr->mymapper = mymap;
    mr->myreducer = myreduce;
}


/**
 * Starts the map() processes for each value in the values array.
 * (See the MP description for full details.)
 */
int process_check(const char ** values)
{
    int i, count =0;
    
    for (i=0; values[i]!=NULL; i++)
    {
        if(values!=NULL)
        {
            count++;
        }
    }
    
    return count;
}

void* worker(void * mr)
{	
	
	// Create the epoll() 
	int epoll_fd = epoll_create(processes);
	
	// A struct epoll_event for each process
	struct epoll_event events[processes];
	
	memset(events, 0, (sizeof(struct epoll_event)*processes)); 

	int i;
	for (i = 0; i < processes; i++)
	{
		// Setup the epoll_event for this process
		events[i].events = EPOLLIN;
		events[i].data.fd = fd[i][0];
		epoll_ctl(epoll_fd, EPOLL_CTL_ADD, fd[i][0], &events[i]);
	}
	
	int count = 0;

	// Read data via epoll_wait() 
	while(1)
	{
		struct epoll_event ev;
		int no = epoll_wait(epoll_fd, &ev, 1, -1);
		int num;
		
		for(i=0;i<processes;i++)
		{
			if(ev.data.fd == events[i].data.fd)
			num = i;
		}
		
		for(i=0;i<no; i++)
		{

		int bytes = read_from_fd(events[num].data.fd, buffer[i], mr);
		
			if(bytes==0)
			{
				count++;
				epoll_ctl(epoll_fd, EPOLL_CTL_DEL, events[num].data.fd, NULL);
			}

		}

		if(count==processes)
		{
			break;
		}

		

	}
    
}

void mapreduce_map_all(mapreduce_t *mr, const char **values)
{
    int i =0; 
    
    processes = process_check(values);
    
    if(processes == 0)
    {
	puts("no datasets");
        return ;
    }

    fd = (int **) malloc(processes*sizeof(int*));

    buffer = (char **) malloc(processes*sizeof(char*));

    for(i=0;i<processes;i++)
    {
         buffer[i] = (char *) malloc(sizeof(char)*(BUFFER_SIZE+1)); 
	 fd[i]= malloc(2*sizeof(int));		
	
         buffer[i][0] = '\0';
    }

    pid_t child_pid[processes];
    

    for (i=0; i<processes; i++)
    {
	
        pipe(fd[i]);
        
        child_pid[i] = fork();
        
        
        if(child_pid[i] == -1)
        {
            perror("fork");
            exit(1);
        }
        
        if(child_pid[i] > 0)
        {
          
            close(fd[i][1]);
        }
        else
        {
           
            close(fd[i][0]);
            mr->mymapper(fd[i][1], values[i]);
            exit(0);
        }
        
    }
    
    
    // Worker thread here
    pthread_create(&thread,NULL,worker,(void *)mr);
    
}


/**
 * Blocks until all the reduce() operations have been completed.
 * (See the MP description for full details.)
 */
void mapreduce_reduce_all(mapreduce_t *mr)
{

	pthread_join(thread, NULL);

}


/**
 * Gets the current value for a key.
 * (See the MP description for full details.)
 */
const char *mapreduce_get_value(mapreduce_t *mr, const char *result_key)
{
	unsigned long rev;
     	char* temp = datastore_get(&mr->data, result_key, &rev);
	return temp;
}


/**
 * Destroys the mapreduce data structure.
 */
void mapreduce_destroy(mapreduce_t *mr)
{
   
	datastore_destroy(&mr->data);
	
	int i;
	
	for(i=0;i<processes;i++)
	{
	free(buffer[i]);
	free(fd[i]);
	}
	
	free(fd);
	free(buffer);
	
	
}


