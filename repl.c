#include<stdio.h>
#include<stdlib.h>
#include<stdbool.h>
#include<string.h>

struct InputBuffer_t {
    char* buffer;
    size_t buffer_length;
    ssize_t input_length;
};
typedef struct InputBuffer_t InputBuffer;

enum MetaCommandResult_t {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
};
typedef enum MetaCommandResult_t MetaCommandResult;

enum PrepareResult_t {
    PREPARE_SUCCESS,
    PREPARE_UNRECOGNIZED_STATEMENT
};
typedef enum PrepareResult_t PrepareResult;

enum StatementType_t {
    STATEMENT_INSERT,
    STATEMENT_SELECT
};
typedef enum StatementType_t StatementType;

struct Statement_t {
    StatementType type;
};
typedef struct Statement_t Statement;

InputBuffer* new_input_buffer() {
    InputBuffer* buffer = malloc(sizeof(InputBuffer));
    buffer->buffer = NULL;
    buffer->buffer_length = 0;
    buffer->input_length = 0;

    return buffer;
}

MetaCommandResult do_meta_command(InputBuffer* input) {
    if (strcmp(input->buffer, ".exit") == 0) {
        exit(EXIT_SUCCESS);
    } else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

PrepareResult prepare_statement(InputBuffer* input, Statement* statement) {
    if (strncmp(input->buffer, "insert", 6) == 0) {
        statement->type = STATEMENT_INSERT;
        return PREPARE_SUCCESS;
    }
    if (strcmp(input->buffer, "select") == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }

    return PREPARE_UNRECOGNIZED_STATEMENT;
}

void execute_statement(Statement* statement) {
    switch (statement->type) {
        case (STATEMENT_INSERT):
            printf("this is an insert\n");
            break;
        case (STATEMENT_SELECT):
            printf("this is a select\n");
            break;
    }
}

void print_prompt(){ printf("db > "); }

void read_input(InputBuffer* input) {
    ssize_t bytes_read =
        getline(&(input->buffer), &(input->buffer_length), stdin);

    if (bytes_read <= 0) {
        printf("Error Reading Input\n");
        exit(EXIT_FAILURE);
    }

    input->input_length = bytes_read -1;
    input->buffer[bytes_read -1] = 0;
}

int main(int argc, char* argv[]) {
    InputBuffer* input = new_input_buffer();
    while (true) {
        print_prompt();
        read_input(input);

        if (input->buffer[0] == '.') {
            switch (do_meta_command(input)) {
                case (META_COMMAND_SUCCESS):
                    continue;
                case (META_COMMAND_UNRECOGNIZED_COMMAND):
                    printf("Unrecognized command '%s'\n", input->buffer);
                    continue;
            }
        }

        Statement statement;
        switch (prepare_statement(input, &statement)) {
            case (PREPARE_SUCCESS):
                break;
            case (PREPARE_UNRECOGNIZED_STATEMENT):
                printf("Unrecognized keyword at start of '%s'.\n",
                        input->buffer);
                continue;
        }

        execute_statement(&statement);
        printf("Executed.\n");
    }
}
