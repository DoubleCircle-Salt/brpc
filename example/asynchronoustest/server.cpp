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

#include "common.h"
#include <brpc/server.h>

DEFINE_int32(port, 8003, "TCP Port of this server");
DEFINE_int32(idle_timeout_s, -1, "Connection will be closed if there is no "
             "read/write operations during the last `idle_timeout_s'");
DEFINE_int32(logoff_ms, 2000, "Maximum duration of server's LOGOFF state "
             "(waiting for client to close connection before server stops)");

class StreamReceiver : public brpc::StreamInputHandler {
public:
    virtual int on_received_messages(brpc::StreamId id, 
                                     butil::IOBuf *const messages[], 
                                     size_t size) {
        size_t i = 0;
        if(!streamfilemap[id].file.is_open()) {
            std::string::size_type nPosB = (*messages[i]).to_string().find(" ");
            if (nPosB != std::string::npos){
                streamfilemap[id].filename = (*messages[i]).to_string().substr(0, nPosB);
                streamfilemap[id].filelength = atoi((*messages[i++]).to_string().substr(nPosB + 1).c_str());
            }else{
                streamfilemap[id].filename = (*messages[i++]).to_string();
                streamfilemap[id].filelength = -1;
            }
            streamfilemap[id].length = 0;
            if (streamfilemap[id].filelength >= 0) {
                streamfilemap[id].file.open(streamfilemap[id].filename, std::ios::out);
            }
            else {
                streamfilemap[id].file.open(streamfilemap[id].filename, std::ios::in);
            }
        }

        //写文件
        if (streamfilemap[id].filelength >= 0) {
            for (; i < size; i++) {
                streamfilemap[id].file.write((*messages[i]).to_string().c_str(), (*messages[i]).to_string().length());
                streamfilemap[id].length += (*messages[i]).to_string().length();
            }

            //文件传输完毕,返回文件长度
            if (streamfilemap[id].length == streamfilemap[id].filelength){
                streamfilemap[id].file.close();

                std::string command = "ls -l " + streamfilemap[id].filename + " | awk '{print $5}'";
                std::string final_msg;
                exec_cmd(command.c_str(), &final_msg);

                std::string::size_type nPosB = final_msg.find(" "); 

                if (nPosB != std::string::npos) {
                    butil::IOBuf msg;
                    msg.append(final_msg.substr(0, nPosB));
                    CHECK_EQ(0, brpc::StreamWrite(id, msg));
                }
            }
        }else{  //读文件
            streamfilemap[id].file.seekg(0, std::ios::end);
            int64_t filelength = streamfilemap[id].file.tellg();
            streamfilemap[id].file.seekg(0, std::ios::beg);

            std::stringstream filelengthstream;
            filelengthstream << filelength;

            butil::IOBuf msg;
            msg.append(streamfilemap[id].filename + " " + filelengthstream.str());
            CHECK_EQ(0, brpc::StreamWrite(id, msg));

            while(!streamfilemap[id].file.eof()) {
                msg.clear();
                char buffer[FLAGS_default_buffer_size + 1] = {'\0'};
                int32_t length = streamfilemap[id].file.read(buffer, FLAGS_default_buffer_size).gcount();
                msg.append(buffer, length);
                CHECK_EQ(0, brpc::StreamWrite(id, msg));  
            }

        }
        
        return 0;
    }
    virtual void on_idle_timeout(brpc::StreamId id) {
        LOG(INFO) << "Stream=" << id << " has no data transmission for a while";
        brpc::StreamClose(id);
        streamfilemap[id].file.close();
    }
    virtual void on_closed(brpc::StreamId id) {
        LOG(INFO) << "Stream=" << id << " is closed";
        brpc::StreamClose(id);
        streamfilemap[id].file.close();
    }
private:
    StreamFileMap streamfilemap;
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
        stream_options.handler = &_receiver;
        if (brpc::StreamAccept(&_sd, *cntl, &stream_options) != 0) {
            cntl->SetFailed("Fail to accept stream");
            return;
        }
        response->set_message("123");

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
