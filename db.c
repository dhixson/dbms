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

struct Pager_t {
    int file_descriptor;
    uint32_t file_length;
    uint32_t num_pages;
    void* pages[TABLE_MAX_PAGES];
};
typedef struct Pager_t Pager;

struct Table_t {
  Pager* pager;
  uint32_t root_page_num;
};
typedef struct Table_t Table;

struct Cursor_t{
    Table* table;
    uint32_t page_num;
    uint32_t cell_num;
    bool end_of_table;
};
typedef struct Cursor_t Cursor;

void print_row(Row* row) {
  printf("(%s, %s, %s, %s, %f, %s)\n", row->stb, row->title, row->provider, row->date, row->rev, row->time);
}


// B+ tree functions
enum NodeType_t {
    NODE_INTERNAL,
    NODE_LEAF
};
typedef enum NodeType_t NodeType;   

// Node Header Layout
const uint32_t NODE_TYPE_SIZE = sizeof(uint32_t);
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t IS_ROOT_SIZE = sizeof(uint32_t);
const uint32_t IS_ROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t PARENT_POINTER_SIZE = sizeof(uint32_t);
const uint32_t PARENT_POINTER_OFFSET = IS_ROOT_OFFSET + IS_ROOT_SIZE;
const uint8_t COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE;

// Leaf Node Header Layout
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;

// Leaf Node Body Layout
const uint32_t LEAF_NODE_KEY_SIZE = STB_SIZE + TITLE_SIZE + DATE_SIZE + 1;
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE;
const uint32_t LEAF_NODE_VALUE_OFFSET = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE;
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;


uint32_t* leaf_node_num_cells(void* node) {
  return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

void* leaf_node_cell(void* node, uint32_t cell_num) {
    return (char *)node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

char* leaf_node_key(void* node, uint32_t cell_num) {
    return leaf_node_cell(node, cell_num) ;
}

void* leaf_node_value(void* node, uint32_t cell_num) {
    return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

void print_constants() {
    printf("ROW_SIZE: %d\n", ROW_SIZE);
    printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
    printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
    printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
    printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
    printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}


void print_leaf_node(void* node) {
    uint32_t num_cells = *leaf_node_num_cells(node);
    printf("leaf (size %d)\n", num_cells);
    for (uint32_t i = 0; i < num_cells; i++) {
        char* key = leaf_node_key(node, i);
        printf(" - %d : %s\n", i, key);
    }
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

void initialize_leaf_node(void* node) {
    *leaf_node_num_cells(node) = 0;
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

        if (page_num >= pager->num_pages) {
            pager->num_pages = page_num + 1;
        }
    }
    
    return pager->pages[page_num];
}

Cursor* table_start(Table* table) {
    Cursor* cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->page_num = table->root_page_num;

    void* root_node = get_page(table->pager, table->root_page_num);
    uint32_t num_cells = *leaf_node_num_cells(root_node);
    cursor->cell_num = num_cells;
    cursor->end_of_table = true;

    return cursor;
}

Cursor* table_end(Table* table) {
    Cursor* cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->page_num = table->root_page_num;
    
    void* root_node = get_page(table->pager, table->root_page_num);
    uint32_t num_cells = *leaf_node_num_cells(root_node);
    cursor->cell_num = num_cells;
    cursor->end_of_table = true;

    return cursor;
}

void* cursor_value(Cursor* cursor) {
  uint32_t page_num = cursor->page_num;
  void* page = get_page(cursor->table->pager, page_num);
  return leaf_node_value(page, cursor->cell_num);
}

void cursor_advance(Cursor* cursor) {
    uint32_t page_num = cursor->page_num;
    void* node = get_page(cursor->table->pager, page_num);

    cursor->cell_num += 1;
    if (cursor->cell_num >= (*leaf_node_num_cells(node))) {
        cursor->end_of_table = true;
    }
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
  pager->num_pages = (file_length / PAGE_SIZE);

  if (file_length % PAGE_SIZE != 0) {
      printf("Db file is not a whole number of pages. Corrupt file.\n");
      exit(EXIT_FAILURE);
  }

  for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
      pager->pages[i] = NULL;
  }

  return pager;
}

void pager_flush(Pager* pager, uint32_t page_num) {
    if (pager->pages[page_num] == NULL) {
        printf("Tried to flush null page\n");
        exit(EXIT_FAILURE);
    }

    ssize_t bytes_written = 
        write(pager->file_descriptor, pager->pages[page_num], PAGE_SIZE);

    if (bytes_written == -1) {
        printf("Error writing: %d\n", errno);
        exit(EXIT_FAILURE);
    }
}

void db_close(Table* table) {
    Pager* pager = table->pager;

    for (uint32_t i = 0; i < pager->num_pages; i++) {
        if (pager->pages[i] == NULL) {
            continue;
        }
        pager_flush(pager, i);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
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

  Table* table = malloc(sizeof(Table));
  table->pager = pager;

  if (pager->num_pages ==0) {
      void* root_node = get_page(pager, 0);
      initialize_leaf_node(root_node);
  }

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
  } else if (strcmp(input_buffer->buffer, ".btree") == 0) {
      printf("Tree:\n");
      print_leaf_node(get_page(table->pager, 0));
      return META_COMMAND_SUCCESS;
  } else if (strcmp(input_buffer->buffer, ".constants") == 0) {
    printf("Constants:\n");
    print_constants();
    return META_COMMAND_SUCCESS;
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

void leaf_node_insert(Cursor* cursor, char * key, Row* value) {
    void* node = get_page(cursor->table->pager, cursor->page_num);

    uint32_t num_cells = *leaf_node_num_cells(node);
    if (num_cells >= LEAF_NODE_MAX_CELLS) {
        printf("Need to implement splitting a leaf node.\n");
        exit(EXIT_FAILURE);
    }

    if (cursor->cell_num < num_cells) {
        for (uint32_t i = num_cells; i > cursor->cell_num; i--) {
            memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i - 1), LEAF_NODE_KEY_SIZE);
        }
    }

    *(leaf_node_num_cells(node)) += 1;
    //*(leaf_node_key(node, cursor->cell_num)) = *key;
    strcpy(leaf_node_key(node, cursor->cell_num), key);
    serialize_row(value, leaf_node_value(node, cursor->cell_num));
}


ExecuteResult execute_insert(Statement* statement, Table* table) {
  void* node = get_page(table->pager, table->root_page_num);
  if((*leaf_node_num_cells(node) >= LEAF_NODE_MAX_CELLS)) {
    return EXECUTE_TABLE_FULL;
  }

  Row* row_to_insert = &(statement->row_to_insert);
  Cursor* cursor = table_end(table);
  char* stb = row_to_insert->stb;
  char* title = row_to_insert->title;
  char* date = row_to_insert->date;
  char key[LEAF_NODE_KEY_SIZE];
  snprintf(key, sizeof(key), "%s%s%s", stb, title, date);
  leaf_node_insert(cursor, key, row_to_insert);

  free(cursor);

  return EXECUTE_SUCCESS;
}

ExecuteResult execute_select(Statement* statement, Table* table) {
  Cursor* cursor = table_start(table);
  Row row;
  while (!(cursor->end_of_table)) {
    deserialize_row(cursor_value(cursor), &row);
    print_row(&row);
    cursor_advance(cursor);
  }

  free(cursor);

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
