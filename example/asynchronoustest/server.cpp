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
#include <brpc/stream.h>
#include "echo.pb.h"
#include <fstream>

DEFINE_bool(send_attachment, true, "Carry attachment along with response");
DEFINE_int32(port, 8003, "TCP Port of this server");
DEFINE_int32(idle_timeout_s, -1, "Connection will be closed if there is no "
             "read/write operations during the last `idle_timeout_s'");
DEFINE_int32(logoff_ms, 2000, "Maximum duration of server's LOGOFF state "
             "(waiting for client to close connection before server stops)");
DEFINE_int32(stream_max_buf_size, -1, "");

DEFINE_int32(default_buffer_size, 1024, "");

typedef struct _STRUCT_STREAM{
        std::string filename;
        int64_t filelength;
        std::ofstream fout;
}STRUCT_STREAM;

std::string exec_cmd(const char *command, std::string *final_msg)
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
        LOG(INFO) << "命令[" << command << "]执行发生错误，err: " << *final_msg;
        return "false";
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
        LOG(INFO) << "命令[" << command << "]执行发生错误，err: " << *final_msg;
        return "false";
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

        return "true";
    }
    else
    {
        // child process exits FAILED.
        LOG(INFO) << "命令[" << command << "]执行发生错误，err: " << *final_msg;
        return "false";
    }
}

typedef std::map<brpc::StreamId, STRUCT_STREAM> StreamFoutMap;
class StreamReceiver : public brpc::StreamInputHandler {
public:
    virtual int on_received_messages(brpc::StreamId id, 
                                     butil::IOBuf *const messages[], 
                                     size_t size) {
        size_t i = 0;
        if(!streamfoutmap[id].fout.is_open()) {
            streamfoutmap[id].fout.open((*messages[i++]).to_string());        
        }
        
        for (; i < size; i++) {
            streamfoutmap[id].fout.write((*messages[i]).to_string().c_str(), (*messages[i]).to_string().length());
        }
        
        return 0;
    }
    virtual void on_idle_timeout(brpc::StreamId id) {
        LOG(INFO) << "Stream=" << id << " has no data transmission for a while";
        brpc::StreamClose(id);
        streamfoutmap[id].fout.close();
    }
    virtual void on_closed(brpc::StreamId id) {
        LOG(INFO) << "Stream=" << id << " is closed";
        brpc::StreamClose(id);
        streamfoutmap[id].fout.close();
    }
private:
    StreamFoutMap streamfoutmap;
};

// Your implementation of example::EchoService
class EchoServiceImpl : public exec::EchoService {
public:
    EchoServiceImpl() : _sd(brpc::INVALID_STREAM_ID) {};
    virtual ~EchoServiceImpl() {
        brpc::StreamClose(_sd);
    };
    virtual void ExecCommand(google::protobuf::RpcController* cntl_base,
                      const exec::CommandRequest* request,
                      exec::CommandResponse* response,
                      google::protobuf::Closure* done) {

        brpc::ClosureGuard done_guard(done);

        brpc::Controller* cntl =
            static_cast<brpc::Controller*>(cntl_base);

        std::string final_msg;
        std::string flag = exec_cmd(request->command().c_str(), &final_msg);
        response->set_message(final_msg);
        if (FLAGS_send_attachment) {
            // Set attachment which is wired to network directly instead of
            // being serialized into protobuf messages.
            cntl->response_attachment().append(flag);
        }
    }
    virtual void PostFile(google::protobuf::RpcController* cntl_base,
                      const exec::FileRequest* request,
                      exec::FileResponse* response,
                      google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl =
            static_cast<brpc::Controller*>(cntl_base);

        brpc::StreamOptions stream_options;
        stream_options.handler = &_receiver;
        if (brpc::StreamAccept(&_sd, *cntl, &stream_options) != 0) {
            cntl->SetFailed("Fail to accept stream");
            return;
        }
        response->set_message("123");
    }
    virtual void GetFile(google::protobuf::RpcController* cntl_base,
                      const exec::FileRequest* request,
                      exec::FileResponse* response,
                      google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl =
            static_cast<brpc::Controller*>(cntl_base);

        

        brpc::StreamOptions stream_options;
        stream_options.max_buf_size = FLAGS_stream_max_buf_size;
        if (brpc::StreamAccept(&_sd, *cntl, &stream_options) != 0) {
            cntl->SetFailed("Fail to accept stream");
            return;
        }

        std::ifstream fin(request->filename());
        if (!fin) {
            cntl->SetFailed("Failed To Open the File!");
            return;
        }

        butil::IOBuf msg;
        msg.append(request->filename());
        CHECK_EQ(0, brpc::StreamWrite(_sd, msg));

        while(!fin.eof()) {
            msg.clear();
            char buffer[FLAGS_default_buffer_size + 1] = {'\0'};
            int32_t length = fin.read(buffer, FLAGS_default_buffer_size).gcount();
            msg.append(buffer, length);
            CHECK_EQ(0, brpc::StreamWrite(_sd, msg));  
        }

    }
private:
    StreamReceiver _receiver;
    brpc::StreamId _sd;
};

int main(int argc, char* argv[]) {
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
