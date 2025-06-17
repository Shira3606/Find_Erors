#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>

#define MAX_ERROR_LEN 50
#define INITIAL_CAPACITY 16
#define NUM_THREADS 4
#define BUFFER_SIZE 8192
#define FILENAME "/mnt/c/Users/user1/Downloads/projeclogs.txt"

typedef struct {
    char code[MAX_ERROR_LEN];
    int count;
} ErrorEntry;

typedef struct {
    ErrorEntry *entries;
    int size;
    int capacity;
} HashTable;
typedef struct {
    off_t start;
    off_t end;
    HashTable *table;
} ThreadArgs;

pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

int hash(const char *code) {
    int hash_value = 0;
    while (*code) {
        hash_value = (hash_value * 31 + *code++) % INITIAL_CAPACITY;
    }
    return hash_value;
}

void init_table(HashTable *table) {
    table->entries = (ErrorEntry *)malloc(INITIAL_CAPACITY * sizeof(ErrorEntry));
    table->size = 0;
    table->capacity = INITIAL_CAPACITY;
    for (int i = 0; i < table->capacity; i++) {
        table->entries[i].count = 0;
    }
}

void resize_table(HashTable *table) {
    int new_capacity = table->capacity * 2;
    ErrorEntry *new_entries = (ErrorEntry *)malloc(new_capacity * sizeof(ErrorEntry));
    for (int i = 0; i < new_capacity; i++) {
        new_entries[i].count = 0;
    }

    for (int i = 0; i < table->capacity; i++) {
        if (table->entries[i].count > 0) {
            int index = hash(table->entries[i].code) % new_capacity;
            while (new_entries[index].count > 0) {
                index = (index + 1) % new_capacity;
            }
            new_entries[index] = table->entries[i];
        }
    }

    free(table->entries);
    table->entries = new_entries;
    table->capacity = new_capacity;
}

void add_error(HashTable *table, const char *code, int count_to_add) {
    int index = hash(code) % table->capacity;
    while (table->entries[index].count > 0 && strcmp(table->entries[index].code, code) != 0) {
        index = (index + 1) % table->capacity;
    }

    if (table->entries[index].count == 0) {
        strncpy(table->entries[index].code, code, MAX_ERROR_LEN - 1);
        table->entries[index].code[MAX_ERROR_LEN - 1] = '\0';
    }
    
    table->entries[index].count += count_to_add;  // הוספת המונה שמתקבל

    table->size++;

    if (table->size >= table->capacity / 2) {
        resize_table(table);
    }
}


int compare_counts(const void *a, const void *b) {
    return ((ErrorEntry *)b)->count - ((ErrorEntry *)a)->count;
}

void *process_chunk(void *arg) {
    ThreadArgs *args = (ThreadArgs *)arg;
    int fd = open(FILENAME, O_RDONLY);
    if (fd < 0) {
        perror("open");
        pthread_exit(NULL);
    }

    lseek(fd, args->start, SEEK_SET);
    char buffer[BUFFER_SIZE + 1];
    char line[1024];
    int line_pos = 0;
    off_t current_pos = args->start;

    while (current_pos < args->end) {
        ssize_t bytes_to_read = (args->end - current_pos < BUFFER_SIZE) ? args->end - current_pos : BUFFER_SIZE;
        ssize_t bytes_read = read(fd, buffer, bytes_to_read);
        if (bytes_read <= 0) break;
        buffer[bytes_read] = '\0';

        for (int i = 0; i < bytes_read; i++) {
            if (buffer[i] == '\n' || line_pos >= 1023) {
                line[line_pos] = '\0';

                // חיפוש מיקום השגיאה
                char *pos = strstr(line, "Error: ");
                if (pos) {
                    pos += 7;
                    char code[MAX_ERROR_LEN];
                    int len = 0;
                    while (*pos && *pos != '"' && *pos != '\n' && len < MAX_ERROR_LEN - 1) {
                        code[len++] = *pos++;
                    }
                    code[len] = '\0';

                    // הוספת השגיאה עם המונה 1
                    add_error(args->table, code, 1);
                }

                line_pos = 0;
            } else {
                line[line_pos++] = buffer[i];
            }
        }

        current_pos += bytes_read;
    }

    close(fd);
    pthread_exit(NULL);
}


void merge_tables(HashTable *final, HashTable *local) {
    pthread_mutex_lock(&mutex);

    for (int i = 0; i < local->capacity; i++) {
        if (local->entries[i].count > 0) {
            add_error(final, local->entries[i].code, local->entries[i].count);  // מעביר את count
        }
    }

    pthread_mutex_unlock(&mutex);
}


int main() {
    int top_n;
    printf("Enter number of top errors to display: ");
    scanf("%d", &top_n);

    struct stat st;
    if (stat(FILENAME, &st) != 0) {
        perror("stat");
        return 1;
    }

    off_t file_size = st.st_size;
    off_t chunk = file_size / NUM_THREADS;

    pthread_t threads[NUM_THREADS];
    ThreadArgs args[NUM_THREADS];
    HashTable final_table;
    init_table(&final_table);

    for (int i = 0; i < NUM_THREADS; i++) {
        args[i].start = i * chunk;
        args[i].end = (i == NUM_THREADS - 1) ? file_size : (i + 1) * chunk;
        args[i].table = (HashTable *)malloc(sizeof(HashTable));
        init_table(args[i].table);
        pthread_create(&threads[i], NULL, process_chunk, &args[i]);
    }

    for (int i = 0; i < NUM_THREADS; i++) {
        pthread_join(threads[i], NULL);
    }

    for (int i = 0; i < NUM_THREADS; i++) {
        merge_tables(&final_table, args[i].table);
        free(args[i].table);
    }

    // מיון הטבלה הסופית
    qsort(final_table.entries, final_table.size, sizeof(ErrorEntry), compare_counts);

    for (int i = 0; i < top_n && i < final_table.size; i++) {
        printf("Error: %s, Count: %d\n", final_table.entries[i].code, final_table.entries[i].count);
    }

    free(final_table.entries);
    return 0;
}
