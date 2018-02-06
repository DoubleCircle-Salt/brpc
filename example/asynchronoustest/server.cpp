#include "common.h"
#include <brpc/server.h>

DEFINE_int32(port, 8003, "TCP Port of this server");
DEFINE_int32(idle_timeout_s, -1, "Connection will be closed if there is no "
             "read/write operations during the last `idle_timeout_s'");
DEFINE_int32(logoff_ms, 2000, "Maximum duration of server's LOGOFF state "
             "(waiting for client to close connection before server stops)");

std::string local_side;

void PostFileByStream(StreamFileMap streamfilemap, brpc::StreamId id, butil::IOBuf *const messages[], size_t size, size_t i) {
    for (; i < size; i++) {
        streamfilemap[id].file.write((*messages[i]).to_string().c_str(), (*messages[i]).to_string().length());
        streamfilemap[id].length += (*messages[i]).to_string().length();
    }
    if (streamfilemap[id].length == streamfilemap[id].filelength) {
        streamfilemap[id].file.close();
        std::string command = "ls -l " + streamfilemap[id].filename + " | awk '{print $5}'";
        std::string final_msg;
        exec_cmd(command.c_str(), &final_msg);
        std::string::size_type nPosB = final_msg.find(" "); 
        if (nPosB != std::string::npos) {
            butil::IOBuf msg;
            msg.append(local_side + ":" + final_msg.substr(0, nPosB));
            CHECK_EQ(0, brpc::StreamWrite(id, msg));
        }
    }
}

void GetFileByStream(StreamFileMap streamfilemap, brpc::StreamId id, butil::IOBuf *const messages[], size_t size, size_t i) {
    streamfilemap[id].file.seekg(0, std::ios::end);
    int64_t filelength = streamfilemap[id].file.tellg();
    streamfilemap[id].file.seekg(0, std::ios::beg);
    butil::IOBuf msg;
    msg.append(local_side + "/" + GetRealname(streamfilemap[id].filename) + " " + filelengthstream.str());
    CHECK_EQ(0, brpc::StreamWrite(id, msg));
    while(!streamfilemap[id].file.eof()) {
        msg.clear();
        char buffer[FLAGS_default_buffer_size + 1] = {'\0'};
        int32_t length = streamfilemap[id].file.read(buffer, FLAGS_default_buffer_size).gcount();
        msg.append(buffer, length);
        CHECK_EQ(0, brpc::StreamWrite(id, msg));  
    }
    streamfilemap[id].file.close();
}

class StreamReceiver : public brpc::StreamInputHandler {
public:
    virtual int on_received_messages(brpc::StreamId id, 
                                     butil::IOBuf *const messages[], 
                                     size_t size) {
        size_t i = 0;
        if(!streamfilemap[id].file.is_open()) {
            std::string::size_type nPosType = (*messages[i]).to_string().find(FLAGS_command_type);
            if (nPosType != std::string::npos){
                std::string streamstring = (*messages[i]).to_string().substr(nPosB + 1);
                std::string::size_type nPosName = streamstring.find(FLAGS_file_name);

                if(nPosName != std::string::npos) {
                    streamfilemap[id].commandtype = atoi(streamstring.substr(0, nPosName));
                    streamstring = streamstring.substr(nPosName + 1);

                    if(streamfilemap[id].commandtype == EXEC_GETFILE||streamfilemap[id].commandtype == EXEC_COMMAND){
                        streamfilemap[id].filename = streamstring;
                        streamfilemap[id].filelength = -1;
                    }else if(streamfilemap[id].commandtype == EXEC_POSTFILE){
                        std::string::size_type nPosSize = streamstring.find(" ");
                        if(nPosSize != std::string::npos) {
                            streamfilemap[id].filename = streamstring.substr(0, nPosSize);
                            streamfilemap[id].filelength = atoi(streamstring.substr(nPosSize + 1).c_str());
                        }else {
                            return -1;
                        }
                    }else {
                        return -1;
                    }
                }else {
                    return -1;
                }
            }else {
                return -1;
            }
            if(streamfilemap[id].commandtype == EXEC_POSTFILE) {
                streamfilemap[id].file.open(streamfilemap[id].filename, std::ios::out);
            }
            else if(streamfilemap[id].commandtype == EXEC_GETFILE) {
                streamfilemap[id].file.open(streamfilemap[id].filename, std::ios::in);
            }
        }

        switch(streamfilemap[id].commandtype) {
            case EXEC_COMMAND:
                //ExecCommandByStream();
                break;
            case EXEC_POSTFILE:
                PostFileByStream(streamfilemap, id, messages, size, i);                
                break;
            case EXEC_GETFILE:
                GetFileByStream(streamfilemap, id, messages, size, i);
                break;
            default:
                break;
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
                      const exec::Request* request,
                      exec::Response* response,
                      google::protobuf::Closure* done) {

        brpc::ClosureGuard done_guard(done);

        brpc::Controller* cntl =
            static_cast<brpc::Controller*>(cntl_base);
        local_side = butil::endpoint2str(cntl->local_side()).c_str();

        std::string final_msg;
        std::string flag = exec_cmd(request->message().c_str(), &final_msg);
        response->set_message(final_msg);
        if (FLAGS_send_attachment) {
            cntl->response_attachment().append(flag);
        }
    }
    virtual void PostFile(google::protobuf::RpcController* cntl_base,
                      const exec::Request* request,
                      exec::Response* response,
                      google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl =
            static_cast<brpc::Controller*>(cntl_base);
        local_side = butil::endpoint2str(cntl->local_side()).c_str();

        brpc::StreamOptions stream_options;
        stream_options.handler = &_receiver;
        if (brpc::StreamAccept(&_sd, *cntl, &stream_options) != 0) {
            cntl->SetFailed("Fail to accept stream");
            return;
        }
        response->set_message("123");
    }
    virtual void GetFile(google::protobuf::RpcController* cntl_base,
                      const exec::Request* request,
                      exec::Response* response,
                      google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        brpc::Controller* cntl =
            static_cast<brpc::Controller*>(cntl_base);
        local_side = butil::endpoint2str(cntl->local_side()).c_str();

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
