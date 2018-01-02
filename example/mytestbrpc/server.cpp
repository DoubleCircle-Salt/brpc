// Copyright (c) 2014 Baidu, Inc.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// A server to receive EchoRequest and send back EchoResponse.

#include <gflags/gflags.h>
#include <butil/logging.h>
#include <brpc/server.h>
#include "echo.pb.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <iostream>
#include <stdio.h>
#include <vector>
#include <unistd.h>
#include <sys/types.h>
#include <string.h>
#include <fcntl.h>



#define DEFAULT_BUFFER_SIZE 1024

DEFINE_bool(send_attachment, true, "Carry attachment along with response");
DEFINE_int32(port, 8003, "TCP Port of this server");
DEFINE_int32(idle_timeout_s, -1, "Connection will be closed if there is no "
             "read/write operations during the last `idle_timeout_s'");
DEFINE_int32(logoff_ms, 2000, "Maximum duration of server's LOGOFF state "
             "(waiting for client to close connection before server stops)");


void init_daemon(void)
{
    if (fork() != 0) exit(0);
    setsid();
    chdir ("/");
    int fd = open ("/dev/null", O_RDWR, 0);
    if (fd != -1)
    {
        dup2 (fd, STDIN_FILENO);
        dup2 (fd, STDOUT_FILENO);
        dup2 (fd, STDERR_FILENO);
        if (fd > 2) close (fd);
    }
    umask (0022);
    return;
}

bool exec_cmd(const char *command, std::string *final_msg)
{
    assert(command);
    char buffer[DEFAULT_BUFFER_SIZE] = {'\0'};
    // the exit status of the command.
    int rc = 0;

    char cmd[DEFAULT_BUFFER_SIZE] = {'\0'};
    snprintf(cmd, sizeof(cmd), "%s 2>&1", command);

    FILE *fp = popen(cmd, "r");
    if (NULL == fp)
    {
        snprintf(buffer, sizeof(buffer), "popen failed. %s, with errno %d.\n", strerror(errno), errno);
        *final_msg = buffer;
        LOG(INFO) << "命令[" << command << "]执行发生错误，err: " << *final_msg;
        return false;
    }

    char result[DEFAULT_BUFFER_SIZE] = {'\0'};
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
        LOG(INFO) << "命令[" << command << "]执行发生错误，err: " << *final_msg;
        return false;
    }

    int status_child = WEXITSTATUS(rc);
    // the success message is here.
    *final_msg += child_result;
    snprintf(buffer, sizeof(buffer), "[%s]: command exit status [%d] and child process exit status [%d].\r\n", command, rc, status_child);
    *final_msg += buffer;
    if (status_child == 0)
    {
        // child process exits SUCCESS.
        LOG(INFO) << "命令[" << command << "]执行成功.";

        return true;
    }
    else
    {
        // child process exits FAILED.
        LOG(INFO) << "命令[" << command << "]执行发生错误，err: " << *final_msg;
        return false;
    }
}



void handler(google::protobuf::RpcController* cntl_base,
                      const example::EchoRequest* request,
                      example::EchoResponse* response){

    brpc::Controller* cntl =
            static_cast<brpc::Controller*>(cntl_base);

    LOG(INFO) << "Received request[log_id=" << cntl->log_id() 
                  << "] from " << cntl->remote_side()
                  << ": " << request->message()
                  << " (attached=" << cntl->request_attachment() << ")";

    std::string final_msg;
    exec_cmd(request->message().c_str(), &final_msg);

    response->set_message(final_msg);

    if (FLAGS_send_attachment) {
            // Set attachment which is wired to network directly instead of
            // being serialized into protobuf messages.
            cntl->response_attachment().append("bar");
        }
}

// Your implementation of example::EchoService
class EchoServiceImpl : public example::EchoService {
public:
    EchoServiceImpl() {};
    virtual ~EchoServiceImpl() {};
    virtual void Echo(google::protobuf::RpcController* cntl_base,
                      const example::EchoRequest* request,
                      example::EchoResponse* response,
                      google::protobuf::Closure* done) {
        // This object helps you to call done->Run() in RAII style. If you need
        // to process the request asynchronously, pass done_guard.release().
        brpc::ClosureGuard done_guard(done);
        
        handler(cntl_base, request, response);
    }
};

int main(int argc, char* argv[]) {
    
    //守护进程
    init_daemon();

    // Parse gflags. We recommend you to use gflags as well.
    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);

    // Generally you only need one Server.
    brpc::Server server;

    // Instance of your service.
    EchoServiceImpl echo_service_impl;

    // Add the service into server. Notice the second parameter, because the
    // service is put on stack, we don't want server to delete it, otherwise
    // use brpc::SERVER_OWNS_SERVICE.
    if (server.AddService(&echo_service_impl, 
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }

    // Start the server.
    brpc::ServerOptions options;
    options.idle_timeout_sec = FLAGS_idle_timeout_s;
    if (server.Start(FLAGS_port, &options) != 0) {
        LOG(ERROR) << "Fail to start EchoServer";
        return -1;
    }

    // Wait until Ctrl-C is pressed, then Stop() and Join() the server.
    server.RunUntilAskedToQuit();
    return 0;
}
