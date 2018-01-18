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

// A client sending requests to server asynchronously every 1 second.

#include <gflags/gflags.h>
#include <butil/logging.h>
#include <butil/time.h>
#include <brpc/channel.h>
#include "echo.pb.h"
#include <fstream>

DEFINE_bool(send_attachment, true, "Carry attachment along with requests");
DEFINE_string(protocol, "baidu_std", "Protocol type. Defined in src/brpc/options.proto");
DEFINE_string(connection_type, "", "Connection type. Available values: single, pooled, short");
DEFINE_string(server, "0.0.0.0:8003", "IP Address of server");
DEFINE_string(load_balancer, "", "The algorithm for load balancing");
DEFINE_int32(timeout_ms, 100, "RPC timeout in milliseconds");
DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)"); 
DEFINE_int32(default_buffer_size, 1024, "");

void HandleCommandResponse(
        brpc::Controller* cntl,
        exec::CommandResponse* response) {

    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    std::unique_ptr<exec::CommandResponse> response_guard(response);

    if (cntl->Failed()) {
        LOG(WARNING) << "Fail to send EchoRequest, " << cntl->ErrorText();
        return;
    }
    LOG(INFO) << "Received response from " << cntl->remote_side()
        << ": " << response->message() << " (attached="
        << cntl->response_attachment() << ")"
        << " latency=" << cntl->latency_us() << "us";
}

void HandleFileResponse(
        brpc::Controller* cntl,
        exec::FileResponse* response) {

    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    std::unique_ptr<exec::FileResponse> response_guard(response);

    if (cntl->Failed()) {
        LOG(WARNING) << "Fail to send EchoRequest, " << cntl->ErrorText();
        return;
    }
    LOG(INFO) << "Received response from " << cntl->remote_side()
        << ": " << response->message() << " (attached="
        << cntl->response_attachment() << ")"
        << " latency=" << cntl->latency_us() << "us";
}


void ExecCommand() {

    brpc::Channel channel;


    brpc::ChannelOptions options;
    options.protocol = FLAGS_protocol;
    options.connection_type = FLAGS_connection_type;
    options.timeout_ms = FLAGS_timeout_ms/*milliseconds*/;
    options.max_retry = FLAGS_max_retry;
    if (channel.Init(FLAGS_server.c_str(), FLAGS_load_balancer.c_str(), &options) != 0) {
        LOG(ERROR) << "Fail to initialize channel";
        return;
    }

    exec::EchoService_Stub stub(&channel);

    exec::CommandResponse* response = new exec::CommandResponse();
    brpc::Controller* cntl = new brpc::Controller();


    exec::CommandRequest request;
    request.set_command("mkdir /home/yanyuanyuan/brpc/brpc/example/asynchronoustest/123");

    cntl->set_log_id(0);  
    if (FLAGS_send_attachment) {

        cntl->request_attachment().append("foo");
    }

    google::protobuf::Closure* done = brpc::NewCallback(
        &HandleCommandResponse, cntl, response);
    stub.ExecCommand(cntl, &request, response, done);
}

void PostFile() {
    brpc::Channel channel;


    brpc::ChannelOptions options;
    options.protocol = FLAGS_protocol;
    options.connection_type = FLAGS_connection_type;
    options.timeout_ms = FLAGS_timeout_ms/*milliseconds*/;
    options.max_retry = FLAGS_max_retry;
    if (channel.Init(FLAGS_server.c_str(), FLAGS_load_balancer.c_str(), &options) != 0) {
        LOG(ERROR) << "Fail to initialize channel";
        return;
    }

    exec::EchoService_Stub stub(&channel);

    exec::FileResponse* response = new exec::FileResponse();
    brpc::Controller* cntl = new brpc::Controller();


    exec::FileRequest request;
    request.set_filename("test.conf_bak");
    std::ifstream fin("test.conf");
    if (!fin) {
        LOG(INFO) << "Failed To Open the File!";
        return;
    }
    std::string filecontent = "";
    int32_t filelength = 0;
    while(!fin.eof()) {
        char buffer[FLAGS_default_buffer_size + 1] = {'\0'};
        int32_t length = fin.read(buffer, FLAGS_default_buffer_size).gcount();
        filelength += length;
        for(int32_t i = 0; i < length; i++)
            filecontent += buffer[i];
    }
    request.set_filecontent(filecontent);
    request.set_filelength(filelength); 

    google::protobuf::Closure* done = brpc::NewCallback(
        &HandleFileResponse, cntl, response);
    stub.PostFile(cntl, &request, response, done);
}

int main(int argc, char* argv[]) {

    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);
      
    PostFile();
    sleep(1);

    LOG(INFO) << "EchoClient is going to quit";
    return 0;
}
