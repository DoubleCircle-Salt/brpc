#include "common.h"
#include <brpc/server.h>

DEFINE_int32(port, 8003, "TCP Port of this server");
DEFINE_int32(idle_timeout_s, -1, "Connection will be closed if there is no "
             "read/write operations during the last `idle_timeout_s'");
DEFINE_int32(logoff_ms, 2000, "Maximum duration of server's LOGOFF state "
             "(waiting for client to close connection before server stops)");

StreamFileMap streamfilemap;

void ExecCommandByStream(brpc::StreamId id) {
    std::string final_msg;
    if(exec_cmd(streamfilemap[id].filename.c_str(), &final_msg)) {
        final_msg = "命令[" + streamfilemap[id].filename + "]执行成功 " + final_msg;
    }else {
        final_msg = "命令[" + streamfilemap[id].filename + "]执行失败 " + final_msg;
    }
    butil::IOBuf msg;
    msg.append(FLAGS_command_type + "1" + final_msg);
    CHECK_EQ(0, brpc::StreamWrite(id, msg));
}

size_t PostFileByStream(brpc::StreamId id, butil::IOBuf *const messages[], size_t size, size_t i) {
    for (; i < size; i++) {
        streamfilemap[id].file.write((*messages[i]).to_string().c_str(), (*messages[i]).to_string().length());
        streamfilemap[id].length += (*messages[i]).to_string().length();

        if(streamfilemap[id].length == streamfilemap[id].filelength) {
            streamfilemap[id].file.close();
            std::string command = "ls -l " + streamfilemap[id].filename + " | awk '{print $5}'";
            std::string final_msg;
            exec_cmd(command.c_str(), &final_msg);
            std::string::size_type nPosB = final_msg.find(" "); 
            if (nPosB != std::string::npos) {
                butil::IOBuf msg;
                msg.append(FLAGS_command_type + "2" + "成功上传文件[" + streamfilemap[id].filename + "]，返回文件长度：" + final_msg.substr(0, nPosB));
                CHECK_EQ(0, brpc::StreamWrite(id, msg));
            }
            return i + 1;
        }else if(streamfilemap[id].length > streamfilemap[id].filelength) {
            streamfilemap[id].file.close();
            butil::IOBuf msg;
            msg.append(FLAGS_command_type + "2" + "接收上传文件[" + streamfilemap[id].filename + "]时发生错误" );
            CHECK_EQ(0, brpc::StreamWrite(id, msg));
            return 0;
        }
    }
    return i; 
}

void GetFileByStream(brpc::StreamId id, butil::IOBuf *const messages[], size_t size, size_t i) {
    streamfilemap[id].file.seekg(0, std::ios::end);
    int64_t filelength = streamfilemap[id].file.tellg();
    streamfilemap[id].file.seekg(0, std::ios::beg);

    std::stringstream filelengthstream;
    filelengthstream << filelength;

    butil::IOBuf msg;
    msg.append(FLAGS_command_type + "3" + GetRealname(streamfilemap[id].filename) + " " + filelengthstream.str());
    CHECK_EQ(0, brpc::StreamWrite(id, msg));

    if(filelength == 0) {
        streamfilemap[id].file.close();
        return;
    }
    while(!streamfilemap[id].file.eof()) {
        msg.clear();
        char buffer[FLAGS_default_buffer_size + 1] = {'\0'};
        int32_t length = streamfilemap[id].file.read(buffer, FLAGS_default_buffer_size).gcount();
        msg.append(buffer, length);
        if(brpc::StreamWrite(id, msg)) {
            break;
        }
    }
    streamfilemap[id].file.close();
}

size_t JudgeCommandType(brpc::StreamId id, butil::IOBuf *const messages[], size_t size, size_t i) {
    if(!streamfilemap[id].file.is_open()) {
        //获取命令类型
        std::string::size_type nPosType = (*messages[i]).to_string().find(FLAGS_command_type);
        if (nPosType != std::string::npos){
            std::string streamstring = (*messages[i++]).to_string().substr(nPosType + FLAGS_command_type.length());
            //获取命令内容
            std::string::size_type nPosName = streamstring.find(FLAGS_file_name);

            if(nPosName != std::string::npos) {
                streamfilemap[id].commandtype = atoi(streamstring.substr(0, nPosName).c_str());
                streamstring = streamstring.substr(nPosName + FLAGS_file_name.length());
                //下载文件与执行命令不包含文件长度
                if(streamfilemap[id].commandtype == EXEC_GETFILE||streamfilemap[id].commandtype == EXEC_COMMAND){
                    streamfilemap[id].filename = streamstring;
                    streamfilemap[id].filelength = -1;
                }else if(streamfilemap[id].commandtype == EXEC_POSTFILE){
                    std::string::size_type nPosSize = streamstring.find(" ");
                    if(nPosSize != std::string::npos) {
                        streamfilemap[id].filename = streamstring.substr(0, nPosSize);
                        streamfilemap[id].filelength = atoi_64t(streamstring.substr(nPosSize + 1).c_str());
                        streamfilemap[id].length = 0;
                    }else {
                        return 0;
                    }
                }else {
                    return 0;
                }
            }else {
                return 0;
            }
        }else {
            return 0;
        }

        if(streamfilemap[id].commandtype == EXEC_POSTFILE) {
            if(streamfilemap[id].filename.find("/") != std::string::npos) {
                std::string::size_type nPosPath = streamfilemap[id].filename.find(GetRealname(streamfilemap[id].filename));
                if(nPosPath != std::string::npos) {
                    std::string filepath = "mkdir -p " + streamfilemap[id].filename.substr(0, nPosPath);
                    std::string fmsg;
                    exec_cmd(filepath.c_str(), &fmsg);
                }
            }
            streamfilemap[id].file.open(streamfilemap[id].filename, std::ios::out);
            //文件长度为0，直接返回
            if(streamfilemap[id].filelength == 0) {
                streamfilemap[id].file.close();
                butil::IOBuf msg;
                msg.append(FLAGS_command_type + "2" + "成功上传文件[" + streamfilemap[id].filename + "]，返回文件长度：0");
                CHECK_EQ(0, brpc::StreamWrite(id, msg));
                return i;
            }
        }
        else if(streamfilemap[id].commandtype == EXEC_GETFILE) {
            streamfilemap[id].file.open(streamfilemap[id].filename, std::ios::in);
            if(!streamfilemap[id].file) {
                butil::IOBuf msg;
                msg.append(FLAGS_command_type + "1" + "下载文件失败，未能找到文件");
                CHECK_EQ(0, brpc::StreamWrite(id, msg));
                return i;
            }
        }
    }

    switch(streamfilemap[id].commandtype) {
        case EXEC_COMMAND:
            ExecCommandByStream(id);
            break;
        case EXEC_POSTFILE:
            i = PostFileByStream(id, messages, size, i);                
            break;
        case EXEC_GETFILE:
            GetFileByStream(id, messages, size, i);
            break;
        default:
            break;
    }
    return i;    
}

class StreamReceiver : public brpc::StreamInputHandler {
public:
    virtual int on_received_messages(brpc::StreamId id, 
                                     butil::IOBuf *const messages[], 
                                     size_t size) {
        for(size_t i = 0; i < size;) {
            i = JudgeCommandType(id, messages, size, i);
            if(i == 0) {
                return -1;
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
};

// Your implementation of example::EchoService
class EchoServiceImpl : public exec::EchoService {
public:
    EchoServiceImpl() : _sd(brpc::INVALID_STREAM_ID) {};
    virtual ~EchoServiceImpl() {
        brpc::StreamClose(_sd);
    };
    virtual void Echo(google::protobuf::RpcController* cntl_base,
                      const exec::Request* request,
                      exec::Response* response,
                      google::protobuf::Closure* done) {

        brpc::ClosureGuard done_guard(done);

        brpc::Controller* cntl =
            static_cast<brpc::Controller*>(cntl_base);

        brpc::StreamOptions stream_options;
        stream_options.idle_timeout_ms = FLAGS_idle_timeout_ms;
        stream_options.max_buf_size = FLAGS_stream_max_buf_size;
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

    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);
    brpc::Server server;
    EchoServiceImpl echo_service_impl;

    if (server.AddService(&echo_service_impl, 
                          brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(ERROR) << "Fail to add service";
        return -1;
    }

    brpc::ServerOptions options;
    options.idle_timeout_sec = FLAGS_idle_timeout_s;
    if (server.Start(FLAGS_port, &options) != 0) {
        LOG(ERROR) << "Fail to start EchoServer";
        return -1;
    }
    server.RunUntilAskedToQuit();
    return 0;
}
