#include "stdio.h"
#include "stdlib.h"
#include "string.h"
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
	memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
	memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
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

typedef struct
{
	uint32_t num_rows;
	void *pages[TABLE_MAX_PAGES];
} Table;

Table *new_table()
{
	Table *table = (Table *)malloc(sizeof(Table));
	table->num_rows = 0;
	for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
		table->pages[i] = NULL;
	}
	return table;
}

void free_table(Table *table)
{
	for (int i = 0; table->pages[i]; i++) {
		free(table->pages[i]);
	}
	free(table);
}

void print_row(Row* row) 
{
  printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

void *row_slot(Table *table, uint32_t row_num)
{
	if (table == NULL) {
		return NULL;
	}
	uint32_t page_num = row_num / ROWS_PER_PAGE;
	void *page = table->pages[page_num];
	if (page == NULL) {
		page = table->pages[page_num] = malloc(PAGE_SIZE);
	}
	return page + (row_num % ROWS_PER_PAGE) * ROW_SIZE;
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

MetaCommandResult do_meta_command(InputBuffer *input_buffer)
{
	if (strcmp(input_buffer->buffer, ".exit") == 0) {
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
	serialize_row(row_to_inset, row_slot(table, table->num_rows));
	table->num_rows++;
	return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement *statement, Table *table)
{
	Row row;
	for (uint32_t i = 0; i < table->num_rows; i++) {
		deserialize_row(row_slot(table, i), &row);
		print_row(&row);
	}
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
	Table* table = new_table();
	while (1) {
		print_prompt();
		read_input(input_buffer);

		if (input_buffer->buffer[0] == '.') {
			switch (do_meta_command(input_buffer)) {
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