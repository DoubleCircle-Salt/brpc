#include "common.h"
#include <butil/time.h>
#include <brpc/channel.h>

DEFINE_string(protocol, "baidu_std", "Protocol type. Defined in src/brpc/options.proto");
DEFINE_string(connection_type, "", "Connection type. Available values: single, pooled, short");
DEFINE_string(server, "0.0.0.0:8003", "IP Address of server");
DEFINE_string(load_balancer, "", "The algorithm for load balancing");
DEFINE_int32(timeout_ms, 10000, "RPC timeout in milliseconds");
DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)");

StreamIpMap streamipmap;

class StreamReceiver : public brpc::StreamInputHandler {
public:
    virtual int on_received_messages(brpc::StreamId id, 
                                     butil::IOBuf *const messages[], 
                                     size_t size) {
        size_t i = 0;
        if(!streamfilemap[id].file.is_open()) {
            std::string::size_type nPosType = (*messages[i]).to_string().find(FLAGS_command_type);
            if (nPosType != std::string::npos){
                std::string streamstring = (*messages[i++]).to_string().substr(nPosType + FLAGS_command_type.length());
                streamfilemap[id].commandtype = atoi(streamstring.substr(0, 1).c_str());                
                streamstring = streamstring.substr(2);
                if (streamfilemap[id].commandtype == EXEC_POSTFILE||streamfilemap[id].commandtype == EXEC_COMMAND) {
                    LOG(INFO) << streamstring;
                }else if (streamfilemap[id].commandtype == EXEC_GETFILE) {
                    std::string::size_type nPosSize = streamstring.find(" ");
                    if(nPosSize != std::string::npos) {
                        streamfilemap[id].filename = streamstring.substr(0, nPosSize);
                        streamfilemap[id].filelength = atoi(streamstring.substr(nPosSize + 1).c_str());
                        streamfilemap[id].file.open(streamfilemap[id].filename, std::ios::out);
                    }else {
                        return -1;
                    }
                }else {
                    return -1;
                }
            }else {
                return -1;
            }
        }
        //写文件
        if (streamfilemap[id].commandtype == EXEC_GETFILE) {
            for (; i < size; i++) {
                streamfilemap[id].file.write((*messages[i]).to_string().c_str(), (*messages[i]).to_string().length());
                streamfilemap[id].length += (*messages[i]).to_string().length();
            }
            if (streamfilemap[id].length == streamfilemap[id].filelength) {
                LOG(INFO) << "文件长度验证正确";
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

void HandleResponse(
        brpc::Controller* cntl,
        exec::Response* response) {

    std::unique_ptr<brpc::Controller> cntl_guard(cntl);
    std::unique_ptr<exec::Response> response_guard(response);

    if (cntl->Failed()) {
        LOG(WARNING) << "Fail to send EchoRequest, " << cntl->ErrorText();
        return;
    }
    LOG(INFO) << cntl->remote_side()
        << ": " << response->message() << " (attached="
        << cntl->response_attachment() << ")"
        << " latency=" << cntl->latency_us() << "us";
}


void ExecCommand(std::string command, brpc::StreamId stream, brpc::Channel *channel) {

    exec::EchoService_Stub stub(channel);
    exec::Response* response = new exec::Response();
    exec::Request request;
    request.set_message(command);

    google::protobuf::Closure* done = brpc::NewCallback(
        &HandleResponse, cntl, response);
    stub.Echo(cntl, &request, response, done);

    butil::IOBuf msg;
    msg.append(FLAGS_command_type + "1" + FLAGS_file_name + command);
    CHECK_EQ(0, brpc::StreamWrite(stream, msg));

}

void PostFile(std::string filename, brpc::StreamId stream, brpc::Channel *channel) {

        exec::EchoService_Stub stub(channel);
        exec::Response* response = new exec::Response();
        exec::Request request;

        std::ifstream fin(filename);
        if (!fin) {
            LOG(INFO) << "Failed To Open the File!";
            return;
        }

        fin.seekg(0, std::ios::end);
        int64_t filelength = fin.tellg();
        fin.seekg(0, std::ios::beg);

        request.set_message("123");

        google::protobuf::Closure* done = brpc::NewCallback(
            &HandleResponse, cntl, response);
        stub.Echo(cntl, &request, response, done);    

        std::stringstream filelengthstream;
        filelengthstream << filelength;

        butil::IOBuf msg;
        msg.append(FLAGS_command_type + "2" + FLAGS_file_name + GetRealname(filename) + " " + filelengthstream.str());
        CHECK_EQ(0, brpc::StreamWrite(stream, msg));

        while(!fin.eof()) {
            msg.clear();
            char buffer[FLAGS_default_buffer_size + 1] = {'\0'};
            int32_t length = fin.read(buffer, FLAGS_default_buffer_size).gcount();
            msg.append(buffer, length);
            CHECK_EQ(0, brpc::StreamWrite(stream, msg));  
        }
    }
}

void GetFile(std::string filename, brpc::StreamId stream, brpc::Channel *channel) {

        exec::EchoService_Stub stub(channel);
        exec::Response* response = new exec::Response();
        exec::Request request;

        request.set_message(filename);

        google::protobuf::Closure* done = brpc::NewCallback(
            &HandleResponse, cntl, response);
        stub.Echo(cntl, &request, response, done);    

        butil::IOBuf msg;
        msg.append(FLAGS_command_type + "3" + FLAGS_file_name + filename);
        CHECK_EQ(0, brpc::StreamWrite(stream, msg));
    }

}

void JudeCommandType(int32_t commandtype, std::string serverstring, std::string commandname) {
    std::string serverlist[FLAGS_default_buffer_size];
    size_t servernum = 0;
    while (true) {
        std::string::size_type nPosB = serverstring.find(" ");
        if (nPosB != std::string::npos) {
            serverlist[servernum++] = serverstring.substr(0, nPosB);
            serverstring = serverstring.substr(nPosB + 1);
        }else {
            serverlist[servernum++] = serverstring;
            break;
        }
    }

    brpc::ChannelOptions options;
    options.protocol = FLAGS_protocol;
    options.connection_type = FLAGS_connection_type;
    options.timeout_ms = FLAGS_timeout_ms/*milliseconds*/;
    options.max_retry = FLAGS_max_retry;


    for (size_t i = 0; i < servernum; i++) {
        brpc::Channel channel;
        if (channel.Init(serverlist[i].c_str(), FLAGS_load_balancer.c_str(), &options) != 0) {
            LOG(ERROR) << "Fail to initialize channel";
            return;
        }
        

        brpc::Controller* cntl = new brpc::Controller();
        
        exec::Request request;
        brpc::StreamId stream;
        brpc::StreamOptions stream_options;
        static StreamReceiver _receiver;
        stream_options.handler = &_receiver;
        stream_options.max_buf_size = FLAGS_stream_max_buf_size;
        if (brpc::StreamCreate(&stream, *cntl, &stream_options) != 0) {
            LOG(ERROR) << "Fail to create stream";
            return;
        }

        switch(commandtype) {
            case EXEC_COMMAND:
                ExecCommand(commandname, stream, &channel);
                break;
            case EXEC_POSTFILE:
                PostFile(commandname, stream, &channel);
                break;
            case EXEC_GETFILE:
                GetFile(commandname, stream, &channel);
                break;
        }
    }
}

int main(int argc, char* argv[]) {

    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);

    JudeCommandType(EXEC_COMMAND, "127.0.0.1:8003", "mkdir /home/yanyuanyuan/brpc/brpc_test/example/asynchronoustest/123");
    //JudeCommandType(EXEC_POSTFILE, "127.0.0.1:8003", "test.conf");
    //JudeCommandType(EXEC_GETFILE, "127.0.0.1:8003", "test.conf");
    //sleep(100);
    LOG(INFO) << "EchoClient is going to quit";
    return 0;
}
