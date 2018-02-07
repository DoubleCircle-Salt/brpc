#include <gflags/gflags.h>
#include <butil/logging.h>
#include <brpc/server.h>
#include <brpc/stream.h>
#include "echo.pb.h"
#include <fstream>
#include <sstream>

DEFINE_bool(send_attachment, true, "Carry attachment along with response");
DEFINE_int32(stream_max_buf_size, -1, "");
DEFINE_int32(default_buffer_size, 1024, "");
DEFINE_string(file_name, "filename:", "");
DEFINE_string(command_type, "commandtype:", "");

#define EXEC_COMMAND 1
#define EXEC_POSTFILE 2
#define EXEC_GETFILE 3

typedef struct _STRUCT_STREAM{
        std::string filename;
        int32_t commandtype;
        int64_t filelength;
        int64_t length;
        std::fstream file;
}STRUCT_STREAM;

typedef struct _STRUCT_COMMAND{
    int32_t commandtype;
    std::string commandname;
}STRUCT_COMMAND;

typedef std::map<brpc::StreamId, STRUCT_STREAM> StreamFileMap;

bool exec_cmd(const char *command, std::string *final_msg)
{
    assert(command);
    char buffer[FLAGS_default_buffer_size] = {'\0'};
    // the exit status of the command.
    int rc = 0;

    char cmd[FLAGS_default_buffer_size] = {'\0'};
    snprintf(cmd, sizeof(cmd), "%s 2>&1", command);

    FILE *fp = popen(cmd, "r");
    if (NULL == fp)
    {
        snprintf(buffer, sizeof(buffer), "popen failed. %s, with errno %d.\n", strerror(errno), errno);
        *final_msg = buffer;
        //LOG(INFO) << "命令[" << command << "]执行发生错误，err: " << *final_msg;
        return false;
    }

    char result[FLAGS_default_buffer_size] = {'\0'};
    std::string child_result;
    while (fgets(result, sizeof(result), fp) != NULL)
    {
        if ('\n' == result[strlen(result) - 1])
        {
            result[strlen(result) - 1] = '\0';
        }

        snprintf(buffer, sizeof(buffer), "%s \r\n", result);
        child_result += buffer;
    }

    // waits for the associated process to terminate and returns
    // the exit status of the command as returned by wait4(2).
    rc = pclose(fp);
    if (-1 == rc)
    {
        // return -1 if wait4(2) returns an error, or some other error is detected.
        // if pclose cannot obtain the child status, errno is set to ECHILD.
        *final_msg += child_result;
        if (ECHILD == errno)
        {
            *final_msg += "pclose cannot obtain the child status.\n";
        }
        else
        {
            snprintf(buffer, sizeof(buffer), "Close file failed. %s, with errno %d.\n", strerror(errno), errno);
            *final_msg += buffer;
        }
        //LOG(INFO) << "命令[" << command << "]执行发生错误，err: " << *final_msg;
        return false;
    }

    int status_child = WEXITSTATUS(rc);
    // the success message is here.
    //*final_msg += child_result;
    //snprintf(buffer, sizeof(buffer), "[%s]: command exit status [%d] and child process exit status [%d].\r\n", command, rc, status_child);
    *final_msg += buffer;
    if (status_child == 0)
    {
        // child process exits SUCCESS.
        //LOG(INFO) << "命令[" << command << "]执行成功.";
        return true;
    }
    else
    {
        // child process exits FAILED.
        //LOG(INFO) << "命令[" << command << "]执行发生错误，err: " << *final_msg;
        return false;
    }
}

std::string GetRealname(std::string filename) {
    while (true) {
        std::string::size_type nPosB = filename.find("/");
        if (nPosB != std::string::npos) {
            filename = filename.substr(nPosB + 1);
        }else {
            break;
        }
    }
    return filename;
}

