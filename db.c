#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

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

struct Pager_t {
    int file_descriptor;
    uint32_t file_length;
    void* pages[TABLE_MAX_PAGES];
};
typedef struct Pager_t Pager;

struct Table_t {
  Pager* pager;
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

void* get_page(Pager* pager, uint32_t page_num) {
    if (page_num > TABLE_MAX_PAGES) {
        printf("Tried to fetch page number out of bounds. %d > %d\n", page_num, TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }

    if (pager->pages[page_num] == NULL) {
        // item not in cahce, load from file
        void * page = malloc(PAGE_SIZE);
        uint32_t num_pages = pager->file_length / PAGE_SIZE;

        // increment page num if page is only partially in file
        if (pager->file_length % PAGE_SIZE) {
            num_pages += 1;
        }

        if (page_num <= num_pages) {
            lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
            ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
            if (bytes_read == -1) {
                printf("Error reading file: %d\n", errno);
                exit(EXIT_FAILURE);
            }
        }
        pager->pages[page_num] = page;
    }
    
    return pager->pages[page_num];
}

void* row_slot(Table* table, uint32_t row_num) {
  uint32_t page_num = row_num / ROWS_PER_PAGE;
  void* page = get_page(table->pager, page_num);
  uint32_t row_offset = row_num % ROWS_PER_PAGE;
  uint32_t byte_offset = row_offset * ROW_SIZE;
  return page + byte_offset;
}

Pager* pager_open(const char* filename) {
  int fd = open(filename,
          O_RDWR |
          O_CREAT, 
          S_IWUSR |
          S_IRUSR
          );

  if (fd == -1) {
      printf("Unable to open file\n");
      exit(EXIT_FAILURE);
  }

  off_t file_length = lseek(fd, 0, SEEK_END);

  Pager* pager = malloc(sizeof(Pager));
  pager->file_descriptor = fd;
  pager->file_length = file_length;

  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
      pager->pages[i] = NULL;
  }

  return pager;
}

void pager_flush(Pager* pager, uint32_t page_num, uint32_t size) {
    if (pager->pages[page_num] == NULL) {
        printf("Tried to flush null page\n");
        exit(EXIT_FAILURE);
    }

    ssize_t bytes_written = 
        write(pager->file_descriptor, pager->pages[page_num], size);

    if (bytes_written == -1) {
        printf("Error writing: %d\n", errno);
        exit(EXIT_FAILURE);
    }
}

void db_close(Table* table) {
    Pager* pager = table->pager;
    uint32_t num_full_pages = table->num_rows / ROWS_PER_PAGE;

    for (uint32_t i = 0; i < num_full_pages; i++) {
        if (pager->pages[i] == NULL) {
            continue;
        }
        pager_flush(pager, i, PAGE_SIZE);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }

    uint32_t num_additional_rows = table->num_rows % ROWS_PER_PAGE;
    if (num_additional_rows > 0) {
        uint32_t page_num = num_full_pages;
        if (pager->pages[page_num] != NULL) {
            pager_flush(pager, page_num, num_additional_rows * ROW_SIZE);
            free(pager->pages[page_num]);
            pager->pages[page_num] = NULL;
        }
    }

    int result = close(pager->file_descriptor);
    if (result == -1) {
        printf("Error closing db file.\n");
        exit(EXIT_FAILURE);
    }
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        void* page = pager->pages[i];
        if (page) {
            free(page);
            pager->pages[i] = NULL;
        }
    }
    free(pager);
}


Table* db_open(const char* filename) {
  Pager* pager = pager_open(filename);
  uint32_t num_rows = pager->file_length / ROW_SIZE;

  Table* table = malloc(sizeof(Table));
  table->pager = pager;
  table->num_rows = num_rows;

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

MetaCommandResult do_meta_command(InputBuffer* input_buffer, Table* table) {
  if (strcmp(input_buffer->buffer, ".exit") == 0) {
    db_close(table);
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
  if (argc < 2) {
      printf("Must supply a database filename.\n");
      exit(EXIT_FAILURE);
  }

  char* filename = argv[1];
  Table* table = db_open(filename);
  InputBuffer* input_buffer = new_input_buffer();
  while (true) {
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
