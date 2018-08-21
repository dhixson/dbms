#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct InputBuffer_t {
  char* buffer;
  size_t buffer_length;
  ssize_t input_length;
};
typedef struct InputBuffer_t InputBuffer;

enum ExecuteResult_t { EXECUTE_SUCCESS, EXECUTE_TABLE_FULL };
typedef enum ExecuteResult_t ExecuteResult;

enum MetaCommandResult_t {
  META_COMMAND_SUCCESS,
  META_COMMAND_UNRECOGNIZED_COMMAND
};
typedef enum MetaCommandResult_t MetaCommandResult;

enum PrepareResult_t {
  PREPARE_SUCCESS,
  PREPARE_NEGATIVE_REV,
  PREPARE_STRING_TO_LONG,
  PREPARE_SYNTAX_ERROR,
  PREPARE_UNRECOGNIZED_STATEMENT
};
typedef enum PrepareResult_t PrepareResult;

enum StatementType_t { STATEMENT_INSERT, STATEMENT_SELECT };
typedef enum StatementType_t StatementType;

const uint32_t COLUMN_STB_SIZE = 32;
const uint32_t COLUMN_TITLE_SIZE = 255;
const uint32_t COLUMN_PROVIDER_SIZE = 255;
const uint32_t COLUMN_DATE_SIZE = 10;
const uint32_t COLUMN_TIME_SIZE = 4;
struct Row_t {
  char stb[COLUMN_STB_SIZE + 1];
  char title[COLUMN_TITLE_SIZE + 1];
  char provider[COLUMN_PROVIDER_SIZE + 1];
  char date[COLUMN_DATE_SIZE + 1];
  float rev;
  char time[COLUMN_TIME_SIZE + 1];
};
typedef struct Row_t Row;

struct Statement_t {
  StatementType type;
  Row row_to_insert;  // only used by insert statement
};
typedef struct Statement_t Statement;

const uint32_t STB_SIZE = sizeof(((Row*)0)->stb);
const uint32_t TITLE_SIZE = sizeof(((Row*)0)->title);
const uint32_t PROVIDER_SIZE = sizeof(((Row*)0)->provider);
const uint32_t DATE_SIZE = sizeof(((Row*)0)->date);
const uint32_t REV_SIZE = sizeof(((Row*)0)->rev);
const uint32_t TIME_SIZE = sizeof(((Row*)0)->time);
const uint32_t STB_OFFSET = 0;
const uint32_t TITLE_OFFSET = STB_OFFSET + STB_SIZE;
const uint32_t PROVIDER_OFFSET = TITLE_OFFSET + TITLE_SIZE;
const uint32_t DATE_OFFSET = PROVIDER_OFFSET + PROVIDER_SIZE;
const uint32_t REV_OFFSET = DATE_OFFSET + DATE_SIZE;
const uint32_t TIME_OFFSET = REV_OFFSET + REV_SIZE;
const uint32_t ROW_SIZE = STB_SIZE + TITLE_SIZE + PROVIDER_SIZE + DATE_SIZE + REV_SIZE + TIME_SIZE;

const uint32_t PAGE_SIZE = 4096;
const uint32_t TABLE_MAX_PAGES = 100;
const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

struct Table_t {
  void* pages[TABLE_MAX_PAGES];
  uint32_t num_rows;
};
typedef struct Table_t Table;

void print_row(Row* row) {
  printf("(%s, %s, %s, %s, %f, %s)\n", row->stb, row->title, row->provider, row->date, row->rev, row->time);
}

void serialize_row(Row* source, void* destination) {
  memcpy(destination + STB_OFFSET, &(source->stb), STB_SIZE);
  memcpy(destination + TITLE_OFFSET, &(source->title), TITLE_SIZE);
  memcpy(destination + PROVIDER_OFFSET, &(source->provider), PROVIDER_SIZE);
  memcpy(destination + DATE_OFFSET, &(source->date), DATE_SIZE);
  memcpy(destination + REV_OFFSET, &(source->rev), REV_SIZE);
  memcpy(destination + TIME_OFFSET, &(source->time), TIME_SIZE);
}

void deserialize_row(void* source, Row* destination) {
  memcpy(&(destination->stb), source + STB_OFFSET, STB_SIZE);
  memcpy(&(destination->title), source + TITLE_OFFSET, TITLE_SIZE);
  memcpy(&(destination->provider), source + PROVIDER_OFFSET, PROVIDER_SIZE);
  memcpy(&(destination->date), source + DATE_OFFSET, DATE_SIZE);
  memcpy(&(destination->rev), source + REV_OFFSET, REV_SIZE);
  memcpy(&(destination->time), source + TIME_OFFSET, TIME_SIZE);
}

void* row_slot(Table* table, uint32_t row_num) {
  uint32_t page_num = row_num / ROWS_PER_PAGE;
  void* page = table->pages[page_num];
  if (!page) {
    // Allocate memory only when we try to access page
    page = table->pages[page_num] = malloc(PAGE_SIZE);
  }
  uint32_t row_offset = row_num % ROWS_PER_PAGE;
  uint32_t byte_offset = row_offset * ROW_SIZE;
  return page + byte_offset;
}

Table* new_table() {
  Table* table = malloc(sizeof(Table));
  table->num_rows = 0;

  return table;
}

InputBuffer* new_input_buffer() {
  InputBuffer* input_buffer = malloc(sizeof(InputBuffer));
  input_buffer->buffer = NULL;
  input_buffer->buffer_length = 0;
  input_buffer->input_length = 0;

  return input_buffer;
}

void print_prompt() { printf("db > "); }

void read_input(InputBuffer* input_buffer) {
  ssize_t bytes_read =
      getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);

  if (bytes_read <= 0) {
    printf("Error reading input\n");
    exit(EXIT_FAILURE);
  }

  // Ignore trailing newline
  input_buffer->input_length = bytes_read - 1;
  input_buffer->buffer[bytes_read - 1] = 0;
}

MetaCommandResult do_meta_command(InputBuffer* input_buffer) {
  if (strcmp(input_buffer->buffer, ".exit") == 0) {
    exit(EXIT_SUCCESS);
  } else {
    return META_COMMAND_UNRECOGNIZED_COMMAND;
  }
}

PrepareResult prepare_insert(InputBuffer* input_buffer, Statement* statement) {
    statement->type = STATEMENT_INSERT;
    char* keyword = strtok(input_buffer->buffer, " ");
    char* stb = strtok(NULL, " ");
    char* title = strtok(NULL, " ");
    char* provider = strtok(NULL, " ");
    char* date = strtok(NULL, " ");
    char* rev_string = strtok(NULL, " ");
    char* time = strtok(NULL, " ");

    if (stb == NULL || title == NULL || provider == NULL || date == NULL ||
            rev_string == NULL || time == NULL) {
        return PREPARE_SYNTAX_ERROR;
    }

    float rev = atof(rev_string);
    if (rev < 0) {
        return PREPARE_NEGATIVE_REV;
    }
    if (
            strlen(stb) > COLUMN_STB_SIZE ||
            strlen(title) > COLUMN_TITLE_SIZE ||
            strlen(provider) > COLUMN_PROVIDER_SIZE ||
            strlen(date) > COLUMN_DATE_SIZE ||
            strlen(time) > COLUMN_TIME_SIZE
       ) {
        return PREPARE_STRING_TO_LONG;
    }

    strcpy(statement->row_to_insert.stb, stb);
    strcpy(statement->row_to_insert.title, title);
    strcpy(statement->row_to_insert.provider, provider);
    strcpy(statement->row_to_insert.date, date);
    statement->row_to_insert.rev = rev;
    strcpy(statement->row_to_insert.time, time);

    return PREPARE_SUCCESS;
}

PrepareResult prepare_statement(InputBuffer* input_buffer,
                                Statement* statement) {
  if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
    return prepare_insert(input_buffer, statement);
  }
  if (strcmp(input_buffer->buffer, "select") == 0) {
    statement->type = STATEMENT_SELECT;
    return PREPARE_SUCCESS;
  }

  return PREPARE_UNRECOGNIZED_STATEMENT;
}

ExecuteResult execute_insert(Statement* statement, Table* table) {
  if (table->num_rows >= TABLE_MAX_ROWS) {
    return EXECUTE_TABLE_FULL;
  }

  Row* row_to_insert = &(statement->row_to_insert);

  serialize_row(row_to_insert, row_slot(table, table->num_rows));
  table->num_rows += 1;

  return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* statement, Table* table) {
  Row row;
  for (uint32_t i = 0; i < table->num_rows; i++) {
    deserialize_row(row_slot(table, i), &row);
    print_row(&row);
  }
  return EXECUTE_SUCCESS;
}

ExecuteResult execute_statement(Statement* statement, Table* table) {
  switch (statement->type) {
    case (STATEMENT_INSERT):
      return execute_insert(statement, table);
    case (STATEMENT_SELECT):
      return execute_select(statement, table);
  }
}

int main(int argc, char* argv[]) {
  Table* table = new_table();
  InputBuffer* input_buffer = new_input_buffer();
  while (true) {
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
      case (PREPARE_NEGATIVE_REV):
        printf("REV must be positive.\n");
        continue;
      case (PREPARE_STRING_TO_LONG):
        printf("String is too long.\n");
        continue;
      case (PREPARE_SYNTAX_ERROR):
        printf("Syntax error. Could not parse statement.\n");
        continue;
      case (PREPARE_UNRECOGNIZED_STATEMENT):
        printf("Unrecognized keyword at start of '%s'.\n",
               input_buffer->buffer);
        continue;
    }

    switch (execute_statement(&statement, table)) {
      case (PREPARE_SUCCESS):
        printf("Executed.\n");
        break;
      case (EXECUTE_TABLE_FULL):
        printf("Error: Table full.\n");
        break;
    }
  }
}
