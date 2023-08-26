#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

typedef unsigned int uint32_t;
typedef unsigned long long uint64_t;

typedef struct
{
	char *buffer;
	size_t buffer_length;
	size_t input_length;
} InputBuffer;

/* SQL return */
typedef enum
{
	META_COMMAND_SUCCESS,
	META_COMMAND_UNRECOGNIZED_COMMAND,
} MetaCommandResult;

typedef enum
{
	PREPARE_SUCCESS,
	PREPARE_NEGTIVE_ID,
	PREPARE_STRING_TOO_LONG,
	PREPARE_SYNTAX_ERROR,
	PREPARE_UNRECOGNIZED_STATEMENT,
} PrepareResult;

typedef enum
{
	STATEMENT_INSERT,
	STATEMENT_SELECT,
} StatementType;

typedef enum
{
	EXECUTE_SUCCESS,
	EXECUTE_TABLE_FULL,
} ExecuteResult;

#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
typedef struct
{
	uint32_t id;
	char username[COLUMN_USERNAME_SIZE + 1];
	char email[COLUMN_EMAIL_SIZE + 1];
} Row;

typedef struct
{
	StatementType type;
	Row row_to_insert; /* inset 1 123@qq.com 对象*/
} Statement;

#define size_of_attribute(Struct, Attribute) sizeof(((Struct *)0)->Attribute)
#define ID_SIZE size_of_attribute(Row, id)
#define USERNAME_SIZE size_of_attribute(Row, username)
#define EMAIL_SIZE size_of_attribute(Row, email)

#define ID_OFFSET 0
#define USERNAME_OFFSET (ID_OFFSET + ID_SIZE)
#define EMAIL_OFFSET (USERNAME_OFFSET + USERNAME_SIZE)
#define ROW_SIZE (EMAIL_OFFSET + EMAIL_SIZE)

void serialize_row(Row *source, void *destination)
{
	memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
	strncpy(destination + USERNAME_OFFSET, source->username, USERNAME_SIZE);
	strncpy(destination + EMAIL_OFFSET, source->email, EMAIL_SIZE);
}

void deserialize_row(void *source, Row *destination)
{
	memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
	memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
	memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

/* 页表 由table掌握多个page, 每个page放多个row*/
#define PAGE_SIZE 4096
#define TABLE_MAX_PAGES 100
#define ROWS_PER_PAGE (PAGE_SIZE / ROW_SIZE)
#define TABLE_MAX_ROWS (ROWS_PER_PAGE * TABLE_MAX_PAGES)

/* 把记录放在文件中，pager作为cache 文件作为低速缓存*/
typedef struct {
	int file_descriptor; /* 文件描述符 */
	uint32_t file_size;  /* 文件大小 */
	void* pages[TABLE_MAX_PAGES];
} Pager;

typedef struct
{
	uint32_t num_rows;
	Pager *pager;
} Table;

/* 游标定位Cursor*/
typedef struct{
	Table* table;
	uint32_t atRowNum;
	uint32_t end_of_table;
} Cursor;

/* 表前后游标 */
Cursor* table_start(Table* table)
{
	Cursor* cur = (Cursor*)malloc(sizeof(Cursor));
	cur->table = table;
	cur->atRowNum = 0;
	cur->end_of_table = (table->num_rows == 0);
	return cur;
}

Cursor* table_end(Table* table)
{
	Cursor* cur = (Cursor*)malloc(sizeof(Cursor));
	cur->table = table;
	cur->atRowNum = table->num_rows;
	cur->end_of_table = 1;
	return cur;
}


/* 连接数据库 */
Pager* pager_open(const char* filename)
{
	int fd = open(filename, O_RDWR | O_CREAT, S_IWUSR | S_IRUSR);
	if (fd < 0) {
		printf("Unable to open file\n");
		exit(EXIT_FAILURE);
	}
	/* 从0 到文件结尾的 size*/
	off_t file_size = lseek(fd, 0, SEEK_END);
	Pager* pager = (Pager*)malloc(sizeof(Pager));
	pager->file_descriptor = fd;
	pager->file_size = file_size;

	for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
		pager->pages[i] = NULL;
	}
	return pager;
}

Table *db_open(const char* filename)
{
	Pager* pager = pager_open(filename);
	uint32_t num_rows = pager->file_size / ROW_SIZE;

	Table *table = (Table *)malloc(sizeof(Table));

	table->num_rows = num_rows;
	table->pager = pager;
	return table;
}

/*关闭连接时，将内内存写入文件*/
void page_push(Pager* pager, uint32_t page_num, size_t size)
{
	if (pager->pages[page_num] == NULL) {
		printf("Tried to flush null page\n");
	    exit(EXIT_FAILURE);
	}
	/*打开一个文件的下一次读写的开始位置*/
	off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
	if (offset == -1) {
    	printf("Error seeking.\n");
    	exit(EXIT_FAILURE);
  	}

	ssize_t bytes_write = write(pager->file_descriptor, pager->pages[page_num], size);
	if (bytes_write == -1) {
    	printf("Error writing.\n");
    	exit(EXIT_FAILURE);
  	}
}

void db_close(Table *table)
{
	Pager* pager = table->pager;
	uint32_t num_full_pages = table->num_rows / ROWS_PER_PAGE;
	for (uint32_t i = 0; i < num_full_pages; i++) {
		if (pager->pages[i] == NULL) {
			continue;
		}
		page_push(pager, i, PAGE_SIZE);
		free(pager->pages[i]);
		pager->pages[i] = NULL;
	}	

	/* 不满页的记录 */
	uint32_t num_additonal_rows = table->num_rows % ROWS_PER_PAGE;
	if (num_additonal_rows > 0) {
		uint32_t page_num = num_full_pages;
		if (pager->pages[page_num]) {
			page_push(pager, page_num, num_additonal_rows * ROW_SIZE);
			free(pager->pages[page_num]);
			pager->pages[page_num] = NULL;
		}
	}

	/* 关闭文件描述符*/
	int ret = close(pager->file_descriptor);
	if (ret == -1) {
		printf("Error closing db file.\n");
		exit(EXIT_FAILURE);
	}

	for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
		if (pager->pages[i]) {
			free(pager->pages[i]);
			pager->pages[i] = NULL;
		}
	}
	free(pager);
	free(table);
}

void print_row(Row* row) 
{
  printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

void* get_page(Pager *pager, uint32_t page_num)
{
	if (page_num > TABLE_MAX_PAGES) {
		printf("Tried to fetch page number out of bounds. %d > %d\n", page_num, TABLE_MAX_PAGES);
		exit(EXIT_FAILURE);
	}

	/* 不在内存中*/
	if (pager->pages[page_num] == NULL) {
		void* page = malloc(sizeof(PAGE_SIZE));
		uint32_t num_pages = pager->file_size / PAGE_SIZE;

		if (pager->file_size % PAGE_SIZE) {
			num_pages++;
		}
		/*在文件中的话*/
		if (page_num <= num_pages) {
			lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
			ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
			if (bytes_read == -1) {
				printf("Error reading file\n");
				exit(EXIT_FAILURE);
			}
		}

		pager->pages[page_num] = page;
	}
	return pager->pages[page_num];
}

void *cursor_point(Cursor* cur)
{
	if (cur == NULL) {
		return NULL;
	}
	uint32_t row_num = cur->atRowNum;
	uint32_t page_num = row_num / ROWS_PER_PAGE;
	void* page = get_page(cur->table->pager, page_num);
	return page + (row_num % ROWS_PER_PAGE) * ROW_SIZE;
}

void cursor_advance(Cursor* cur)
{
	cur->atRowNum++;
	if (cur->atRowNum >= cur->table->num_rows) {
		cur->end_of_table = 1;
	}
}

InputBuffer *new_input_buffer(void)
{
	InputBuffer *input_buffer = (InputBuffer *)malloc(sizeof(InputBuffer));
	input_buffer->buffer = NULL;
	input_buffer->buffer_length = 0;
	input_buffer->input_length = 0;

	return input_buffer;
}

void close_input_buffer(InputBuffer *input_buffer)
{
	if (input_buffer) {
		free(input_buffer->buffer);
		free(input_buffer);
	}
}

void print_prompt(void)
{
	printf("db > ");
}

void read_input(InputBuffer *input_buffer)
{
	size_t bytes_read = getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);

	if (bytes_read <= 0) {
		printf("Error reading input\n");
		exit(EXIT_FAILURE);
	}

	// Ignore trailing newline
	input_buffer->input_length = bytes_read - 1;
	input_buffer->buffer[bytes_read - 1] = 0;
}

MetaCommandResult do_meta_command(InputBuffer *input_buffer, Table* table)
{
	if (strcmp(input_buffer->buffer, ".exit") == 0) {
		db_close(table);
		exit(EXIT_SUCCESS);
	}
	else {
		return META_COMMAND_UNRECOGNIZED_COMMAND;
	}
}

PrepareResult prepare_insert(InputBuffer *input_buffer,
								Statement *statement)
{
	statement->type = STATEMENT_INSERT;
	char* type = strtok(input_buffer->buffer, " ");
	char* id_string = strtok(NULL, " ");
	char* name = strtok(NULL, " ");
	char* email = strtok(NULL, " ");
	if (id_string == NULL || name == NULL || email == NULL) {
		return PREPARE_SYNTAX_ERROR;
	}
	if (atoi(id_string) < 0) {
		return PREPARE_NEGTIVE_ID;
	}
	if (strlen(name) > COLUMN_USERNAME_SIZE || strlen(email) > COLUMN_EMAIL_SIZE) {
		return PREPARE_STRING_TOO_LONG;
	}
	statement->row_to_insert.id = atoi(id_string);
	strncpy(statement->row_to_insert.username, name, strlen(name) + 1);
	strncpy(statement->row_to_insert.email, email, strlen(email) + 1);
	return PREPARE_SUCCESS;
}

PrepareResult prepare_statement(InputBuffer *input_buffer,
								Statement *statement)
{
	if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
		return prepare_insert(input_buffer, statement);
	}
	if (strcmp(input_buffer->buffer, "select") == 0) {
		statement->type = STATEMENT_SELECT;
		return PREPARE_SUCCESS;
	}

	return PREPARE_UNRECOGNIZED_STATEMENT;
}

ExecuteResult execute_insert(Statement *statement, Table *table)
{
	if (table == NULL || table->num_rows > TABLE_MAX_ROWS) {
		return EXECUTE_TABLE_FULL;
	}
	Row *row_to_inset = &(statement->row_to_insert);
	Cursor* cur = table_end(table);
	serialize_row(row_to_inset, cursor_point(cur));
	table->num_rows++;
	free(cur);
	return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement *statement, Table *table)
{
	Row row;
	Cursor* cur = table_start(table);
	while (!cur->end_of_table) {
		deserialize_row(cursor_point(cur), &row);
		print_row(&row);
		cursor_advance(cur);
	}
	free(cur);
	return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement *statement, Table *table)
{
	switch (statement->type) {
		case (STATEMENT_INSERT):
			return execute_insert(statement, table);
		case (STATEMENT_SELECT):
			return execute_select(statement, table);
	}
}

int main(int argc, char *argv[])
{
	InputBuffer *input_buffer = new_input_buffer();
	if (argc < 2) {
		printf("Must supply a database filename.\n");
		exit(EXIT_FAILURE);
	}
	char* filename = argv[1];
	Table* table = db_open(filename);
	while (1) {
		print_prompt();
		read_input(input_buffer);

		if (input_buffer->buffer[0] == '.') {
			switch (do_meta_command(input_buffer, table)) {
			case (META_COMMAND_SUCCESS):
				continue;
			case (META_COMMAND_UNRECOGNIZED_COMMAND):
				printf("Unrecognized command '%s'\n", input_buffer->buffer);
				continue;
			}
		}

		Statement statement;
		switch (prepare_statement(input_buffer, &statement)) {
		case (PREPARE_SUCCESS):
			break;
		case (PREPARE_SYNTAX_ERROR):
			printf("Syntax error. Could not parse statement.\n");
			continue;
		case (PREPARE_NEGTIVE_ID):
			printf("ID error.\n");
			continue;
		case (PREPARE_STRING_TOO_LONG):
			printf("string is too long.\n");
			continue;
		case (PREPARE_UNRECOGNIZED_STATEMENT):
			printf("Unrecognized keyword at start of '%s'.\n",
				   input_buffer->buffer);
			continue;
		}

		switch (execute_statement(&statement, table)) {
      		case (EXECUTE_SUCCESS):
        	printf("Executed.\n");
        	break;
      	case (EXECUTE_TABLE_FULL):
        	printf("Error: Table full.\n");
       		break;
	    }
	}
	return 0;
}