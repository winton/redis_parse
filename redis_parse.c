#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

#include "hiredis/hiredis.h"
#include "uthash/src/uthash.h"

// Column vars
char columns[][20] = {
	"referrer",
	"time",
	"tags",
	"article_id",
	"keywords",
	"type",
	"tnt"
};
unsigned int columns_count = sizeof(columns) / sizeof(columns[0]);

// Hash struct
struct hash_struct {
	char *field;
	int count;
	UT_hash_handle hh;
};
struct hash_struct *hash_record;

// Query vars
struct query {
	// Struct used?
	bool used;
	// Display count or group?
	bool display_count, display_group;
	// Query empty?
	bool query_empty;
	// Exclude empty?
	bool exclude_empty;
	// Query empty at index?
	bool query_empty_at[7];
	// Exclude empty at index?
	bool exclude_empty_at[7];
	// Array of query strings (by column)
	char **query;
	// Array of exclusionary query strings (by column)
	char **exclude;
	// Column to group by
	int column;
	// Match count
	int count;
	// Limit results (group)
	int limit;
	// Results displayed so far
	int displayed;
	// Start value for score
	int start;
	// End value for score
	int finish;
	// Hash for grouping
	struct hash_struct *hash;
};
struct query queries[100];

// Pass scores into #process_record?
bool with_scores = false;

// Other vars
char empty[] = "";

// Functions
int column_to_index(char *key);
int count_sort(struct hash_struct *a, struct hash_struct *b);
bool hash_add_or_update(int query_index, char *record);
bool limit_reached();
void process_record(char *record, int score);
char **to_fields(char *record);
char *xstrtok(char *line, char *delims);

// Hiredis hack functions
static void *createArrayObject(const redisReadTask *task, int elements);
static void *createIntegerObject(const redisReadTask *task, long long value);
static void *createNilObject(const redisReadTask *task);
static redisReply *createReplyObject(int type);
static void *createStringObject(const redisReadTask *task, char *str, size_t len);
static void freeObject(void *ptr);

redisReplyObjectFunctions redisExtReplyObjectFunctions = {
	createStringObject,
	createArrayObject,
	createIntegerObject,
	createNilObject,
	freeObject
};

int main(int argc, char *argv[]) {
	// Argv vars
	char *host = argv[1];
	int port = atoi(argv[2]);
	char *cmd = argv[3];

	with_scores = strstr(cmd, "WITHSCORES") ? true : false;

	int x, y, start;
	for (x = 0; x < ((argc - 4) / 7); x++) {
		start = x * 7 + 4;

		queries[x].used = true;
		queries[x].display_count = false;
		queries[x].display_group = false;
		queries[x].query_empty = true;
		queries[x].exclude_empty = true;
		queries[x].count = 0;
		queries[x].displayed = 0;
		queries[x].hash = NULL;

		if (strcmp(argv[start], "COUNT") == 0)
			queries[x].display_count = true;

		if (strcmp(argv[start], "GROUP") == 0)
			queries[x].display_group = true;

		queries[x].start = atoi(argv[start+1]);
		queries[x].finish = atoi(argv[start+2]);
		queries[x].limit = atoi(argv[start+3]);
		queries[x].column = column_to_index(argv[start+4]);
		queries[x].query = to_fields(argv[start+5]);
		queries[x].exclude = to_fields(argv[start+6]);

		// Query empty?
		for (y = 0; y < columns_count; y++) {
			if (strlen(queries[x].query[y]) > 0) {
				queries[x].query_empty = false;
				queries[x].query_empty_at[y] = false;
			} else
				queries[x].query_empty_at[y] = true;
				
			if (strlen(queries[x].exclude[y]) > 0) {
				queries[x].exclude_empty = false;
				queries[x].exclude_empty_at[y] = false;
			} else
				queries[x].exclude_empty_at[y] = true;
		}
	}

	// Connect to redis
	struct timeval timeout = { 1, 500000 }; // 1.5 seconds
	redisContext *c = redisConnectWithTimeout(host, port, timeout);
	if (c->err) {
		printf("Connection error: %s\n", c->errstr);
		exit(0);
	}

	// Use custom Hiredis reader functions
	c->reader->fn = &redisExtReplyObjectFunctions;

	// Execute command
	redisReply *reply = redisCommand(c, cmd);

	// Display results
	int q;
	for (q = 0; q < 100; q++) {
		if (queries[q].used != true)
			continue;

		if (queries[q].display_count) {
			printf("%i\n", queries[q].count);
		}

		if (queries[q].display_group) {
			HASH_SORT(queries[q].hash, count_sort);
			for(hash_record = queries[q].hash; hash_record != NULL; hash_record = hash_record->hh.next) {
				if (!limit_reached(&queries[q]))
					printf("%s|%i\n", hash_record->field, hash_record->count);
			}	
		}

		if (queries[q + 1].used)
			printf("[END]\n");
	}

	return 0;
}

int column_to_index(char *key) {
	int i, index = -1;

	for (i = 0; i < columns_count; ++i)
		if (strcmp(columns[i], key) == 0)
			index = i;
	
	return index;
}

int count_sort(struct hash_struct *a, struct hash_struct *b) {
	return (b->count - a->count);
}

bool hash_add_or_update(int query_index, char *record) {
	if (!record)
		return false;
	else {
		HASH_FIND_STR(queries[query_index].hash, record, hash_record);
		if (hash_record) {
			hash_record->count++;
		} else {
			hash_record = (struct hash_struct*)malloc(sizeof(struct hash_struct));
			hash_record->field = record;
			hash_record->count = 1;
			HASH_ADD_KEYPTR(hh, queries[query_index].hash, hash_record->field, strlen(hash_record->field), hash_record);
		}
		return true;
	}
}

bool limit_reached(struct query *q) {
	if (q->limit == -1 || q->displayed >= q->limit)
		return true;
	q->displayed++;
	return false;
}

void process_record(char *record, int score) {
	bool exclude, match;
	char *field;
	char **fields;
	fields = to_fields(record);

	// For each query
	int q, x;
	for (q = 0; q < 100; q++) {
		// Skip unused query structs
		if (queries[q].used != true)
			continue;
		
		// Skip if score not within range
		if (score >= 0) {
			if (queries[q].finish >= 0 && score > queries[q].finish)
				continue;
			if (queries[q].start >= 0 && score < queries[q].start)
				continue;
		}

		match = true;
		if (!queries[q].exclude_empty)
			// For each column
			for (x = 0; x < columns_count; x++) {
				// Exclude matches
				if (!queries[q].exclude_empty_at[x] && strstr(fields[x], queries[q].exclude[x])) {
					match = false;
					break;
				}
			}

		if (match && !queries[q].query_empty)
			// For each column
			for (x = 0; x < columns_count; x++) {
				// Include matches
				if (!queries[q].query_empty_at[x] && !strstr(fields[x], queries[q].query[x])) {
					match = false;
					break;
				}
			}

		if (match) {
			if (queries[q].display_count)
				queries[q].count++;
			if (queries[q].display_group && queries[q].column > -1) {
				field = malloc(strlen(fields[queries[q].column]) + 1);
				strcpy(field, fields[queries[q].column]);
				if (queries[q].column == 2) { // tag
					hash_add_or_update(q, xstrtok(field, ","));
					while (hash_add_or_update(q, xstrtok(NULL, ",")));
				} else
					hash_add_or_update(q, field);
			}
		}
	}

	free(fields);
	free(record);
}

char **to_fields(char *record) {
	char **fields = (char **)malloc(sizeof(char*) * columns_count);
	char *field;
	int i;

	for(i = 0; i < columns_count; ++i) {
		field = xstrtok(i == 0 ? record : NULL, "|");
		if (field == NULL)
			field = empty;
		fields[i] = field;
	}

	return fields;
}

char *xstrtok(char *line, char *delims) {
	static char *saveline = NULL;
	char *p;
	int n;

	if(line != NULL)
		saveline = line;

	if (saveline == NULL || *saveline == '\0')
		return NULL;
	
	n = strcspn(saveline, delims);
	p = saveline;

	saveline += n;

	if (*saveline != '\0')
		*saveline++ = '\0';

	return p;
}


// Hiredis hack functions

static void *createArrayObject(const redisReadTask *task, int elements) {
	redisReply *r, *parent;

	r = createReplyObject(REDIS_REPLY_ARRAY);
	if (r == NULL)
		return NULL;

	if (elements > 0) {
		r->element = calloc(elements,sizeof(redisReply*));
		if (r->element == NULL) {
			freeReplyObject(r);
			return NULL;
		}
	}

	r->elements = elements;

	if (task->parent) {
		parent = task->parent->obj;
		parent->element[task->idx] = r;
	}
	return r;
}

static void *createIntegerObject(const redisReadTask *task, long long value) {}
static void *createNilObject(const redisReadTask *task) {}

static redisReply *createReplyObject(int type) {
	redisReply *r = calloc(1,sizeof(*r));

	if (r == NULL)
		return NULL;

	r->type = type;
	return r;
}

static void *createStringObject(const redisReadTask *task, char *str, size_t len) {
	static char *record = NULL;
	static int score = -1;

	if (with_scores && record != NULL)
		score = atoi(str);
	else {
		record = malloc(len + 1);
		memcpy(record, str, len);
		record[len] = '\0';	
	}

	if (!with_scores || score != -1) {
		process_record(record, score);
		record = NULL;
		score = -1;
	}

	redisReply *r;
	return r;
}

static void freeObject(void *ptr) {}