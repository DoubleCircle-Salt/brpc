#include "common.h"
#include <butil/time.h>
#include <brpc/channel.h>

DEFINE_string(protocol, "baidu_std", "Protocol type. Defined in src/brpc/options.proto");
DEFINE_string(connection_type, "", "Connection type. Available values: single, pooled, short");
DEFINE_string(server, "0.0.0.0:8003", "IP Address of server");
DEFINE_string(load_balancer, "", "The algorithm for load balancing");
DEFINE_int32(timeout_ms, 10000, "RPC timeout in milliseconds");
DEFINE_int32(max_retry, 3, "Max retries(not including the first RPC)");
DEFINE_string(ip_conf, "server.conf", "");
DEFINE_string(cmd_conf, "command.conf", "");
DEFINE_string(ip, "", "");
DEFINE_string(cmd, "", "");
DEFINE_string(postfile, "", "");
DEFINE_string(getfile, "", "");
typedef std::map<brpc::StreamId, std::string> StreamIpMap;
StreamFileMap streamfilemap;
StreamIpMap streamipmap;
size_t JudgeCommandType(brpc::StreamId id, butil::IOBuf *const messages[], size_t size, size_t i) {
    if(!streamfilemap[id].file.is_open()) {
        std::string::size_type nPosType = (*messages[i]).to_string().find(FLAGS_command_type);
        if (nPosType != std::string::npos){
            std::string streamstring = (*messages[i++]).to_string().substr(nPosType + FLAGS_command_type.length());
            streamfilemap[id].commandtype = atoi(streamstring.substr(0, 1).c_str());                
            streamstring = streamstring.substr(1);
            if (streamfilemap[id].commandtype == EXEC_POSTFILE||streamfilemap[id].commandtype == EXEC_COMMAND) {
                LOG(INFO) << streamipmap[id] << ": " <<streamstring;
            }else if (streamfilemap[id].commandtype == EXEC_GETFILE) {
                std::string::size_type nPosSize = streamstring.find(" ");
                if(nPosSize != std::string::npos) {
                    streamfilemap[id].filename = streamstring.substr(0, nPosSize);
                    streamfilemap[id].filelength = atoi_64t(streamstring.substr(nPosSize + 1).c_str());
                    streamfilemap[id].length = 0;

                    std::string cmd = "mkdir " + streamipmap[id];
                    std::string fmsg;
                    exec_cmd(cmd.c_str(), &fmsg);                    
                    streamfilemap[id].filename = streamipmap[id] + "/" + streamfilemap[id].filename;

                    streamfilemap[id].file.open(streamfilemap[id].filename, std::ios::out);
                    if(streamfilemap[id].filelength == 0) {
                        LOG(INFO) << streamfilemap[id].filename << ": 成功下载文件，文件长度验证正确";
                        streamfilemap[id].file.close();
                        return i;
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
    }
    //写文件
    if (streamfilemap[id].commandtype == EXEC_GETFILE) {
        for (; i < size; i++) {
            streamfilemap[id].file.write((*messages[i]).to_string().c_str(), (*messages[i]).to_string().length());
            streamfilemap[id].length += (*messages[i]).to_string().length();
            if (streamfilemap[id].length == streamfilemap[id].filelength) {
                LOG(INFO) << streamfilemap[id].filename << ": 成功下载文件，文件长度验证正确";
                streamfilemap[id].file.close();
                return i + 1;
            }else if(streamfilemap[id].length > streamfilemap[id].filelength) {
                streamfilemap[id].file.close();
                LOG(INFO) << streamfilemap[id].filename << ": 读取下载文件时发生错误";
                return 0;
            }
        }        
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
        LOG(INFO) << streamipmap[id] <<": Stream=" << id << " has no data transmission for a while";
        brpc::StreamClose(id);
        streamfilemap[id].file.close();
    }
    virtual void on_closed(brpc::StreamId id) {
        LOG(INFO) << streamipmap[id] <<": Stream=" << id << " is closed";
        brpc::StreamClose(id);
        streamfilemap[id].file.close();
    }
private:
};

void ExecCommand(std::string command, brpc::StreamId stream) {
    butil::IOBuf msg;
    msg.append(FLAGS_command_type + "1" + FLAGS_file_name + command);
    if(brpc::StreamWrite(stream, msg)){
        LOG(INFO) << streamipmap[stream] << ": 发送指令失败，与服务端连接断开";
    }
}

void PostFile(std::string filename, brpc::StreamId stream) {

    std::string filepath;
    std::string::size_type nPosB = filename.find(" "); 
    if(nPosB != std::string::npos) {
        filepath = filename.substr(nPosB + 1);
        if(filepath.substr(filepath.length() - 1) != "/")
            filepath += "/";
        filename = filename.substr(0, nPosB);
    }

    std::ifstream fin(filename);
    if (!fin) {
        LOG(INFO) << "上传文件失败，未能找到文件[" << filename << "]";
        return;
    }

    fin.seekg(0, std::ios::end);
    int64_t filelength = fin.tellg();
    fin.seekg(0, std::ios::beg);

    std::stringstream filelengthstream;
    filelengthstream << filelength;

    butil::IOBuf msg;
    msg.append(FLAGS_command_type + "2" + FLAGS_file_name + filepath + GetRealname(filename) + " " + filelengthstream.str());
    if(brpc::StreamWrite(stream, msg)){
        LOG(INFO) << streamipmap[stream] << ": 上传文件失败，与服务端连接断开";
        fin.close();
        return;
    }

    if(filelength == 0) {
        fin.close();
        return;
    }
    while(!fin.eof()) {
        msg.clear();
        char buffer[FLAGS_default_buffer_size + 1] = {'\0'};
        int32_t length = fin.read(buffer, FLAGS_default_buffer_size).gcount();
        msg.append(buffer, length);
        if(brpc::StreamWrite(id, msg)) {
            LOG(INFO) << streamipmap[id] << ": 上传文件失败，与服务端连接断开";
            break;
        }
    }
    fin.close();
}

void GetFile(std::string filename, brpc::StreamId stream) {     

    butil::IOBuf msg;
    msg.append(FLAGS_command_type + "3" + FLAGS_file_name + filename);
    if(brpc::StreamWrite(stream, msg)){
        LOG(INFO) << streamipmap[stream] << ": 发送指令失败，与服务端连接断开";
    }

}

void *SendCommandToServer(void *arg) {

    STRUCT_COMMANDLISTALL *commandlistall = (struct STRUCT_COMMANDLISTALL *)arg;
    std::string servername = *((*commandlistall).servername);
    size_t commandnum = (*commandlistall).commandnum;
    STRUCT_COMMAND *commandlist = *((*commandlistall).commandlist);

    brpc::ChannelOptions options;
    options.protocol = FLAGS_protocol;
    options.connection_type = FLAGS_connection_type;
    options.timeout_ms = FLAGS_timeout_ms;
    options.max_retry = FLAGS_max_retry;

    brpc::Channel channel;
    if (channel.Init(servername.c_str(), FLAGS_load_balancer.c_str(), &options) != 0) {
        LOG(ERROR) << "Fail to initialize channel";
        return NULL;
    }

    brpc::Controller* cntl = new brpc::Controller();        
    brpc::StreamId stream;
    brpc::StreamOptions stream_options;
    static StreamReceiver _receiver;
    stream_options.handler = &_receiver;
    stream_options.max_buf_size = FLAGS_stream_max_buf_size;
    stream_options.idle_timeout_ms = FLAGS_idle_timeout_ms;
    if (brpc::StreamCreate(&stream, *cntl, &stream_options) != 0) {
        LOG(ERROR) << "Fail to create stream";
        return NULL;
    }
    streamipmap[stream] = servername; 
    exec::EchoService_Stub stub(&channel);
    exec::Response* response = new exec::Response();
    exec::Request request;

    request.set_message("123");
    stub.Echo(cntl, &request, response, NULL);
    if (cntl->Failed()) {
        LOG(WARNING) << "Fail to send EchoRequest, " << cntl->ErrorText();
        return NULL;
    }else {
        LOG(INFO) << "成功连接到: " << cntl->remote_side();
    }

    for (size_t i = 0; i < commandnum; i++) {
        switch(commandlist[i].commandtype) {
            case EXEC_COMMAND:
                ExecCommand(commandlist[i].commandname, stream);
                break;
            case EXEC_POSTFILE:
                PostFile(commandlist[i].commandname, stream);
                break;
            case EXEC_GETFILE:
                GetFile(commandlist[i].commandname, stream);
                break;
        }
    }
    return NULL;
}

void CreateThread(std::string serverlist[], size_t servernum, STRUCT_COMMAND *commandlist, size_t commandnum) {

    std::vector<bthread_t> tids;
    tids.resize(servernum);

    for (size_t i = 0; i < servernum; i++) {
        STRUCT_COMMANDLISTALL *commandlistall = (STRUCT_COMMANDLISTALL *)malloc(sizeof(STRUCT_COMMANDLISTALL));
        commandlistall->commandlist = &commandlist;
        commandlistall->commandnum = commandnum;
        commandlistall->servername = &serverlist[i];
        if(bthread_start_background(&tids[i], NULL, SendCommandToServer, (void *)commandlistall) != 0) {
            LOG(ERROR) << "Fail to create bthread";
            return;
        } 
    }
    for(size_t i = 0; i < servernum; i++)
        bthread_join(tids[i], NULL);
}

size_t GetServerlistFromFile(std::string filename, std::string serverlist[]) {

    std::ifstream fin(filename);
    if (!fin) {
        LOG(INFO) << "Failed To Open the Ip Config File!";
        return 0;
    }
    size_t servernum = 0;
    bool newline = true;
    while(!fin.eof()) {
        char buffer[FLAGS_default_buffer_size + 1] = {'\0'};
        int32_t length = fin.read(buffer, FLAGS_default_buffer_size).gcount();
        for(int32_t i = 0; i < length; i++) {
            if(buffer[i] == '#') {
                newline = false;
            }else if(buffer[i] == '\n') {
                newline = true;
            }
            if(newline) {
                if(buffer[i] == ' '||buffer[i] == '\n'||buffer[i] == '\t') {
                    if (serverlist[servernum] != "") {
                        servernum++;
                    }
                    continue;
                }
                serverlist[servernum] += buffer[i];
            }
        }
    }
    fin.close();
    if (serverlist[servernum] != "") 
        servernum ++;

    return servernum;    
}

size_t GetCommandlistFromFile(std::string filename, STRUCT_COMMAND commandlist[]) {
    std::ifstream fin(filename);
    if (!fin) {
        LOG(INFO) << "Failed To Open the Command Config File!";
        return 0;
    }
    size_t commandnum = 0;
    std::string type = "";
    bool newline = true;
    bool spaceflag = false;
    while(!fin.eof()) {
        char buffer[FLAGS_default_buffer_size + 1] = {'\0'};
        int32_t length = fin.read(buffer, FLAGS_default_buffer_size).gcount();
        for(int32_t i = 0; i < length; i++) {
            if(buffer[i] == '#') {
                newline = false;
            }else if(buffer[i] == '\n') {
                newline = true;
                spaceflag = false;
            }
            if(newline) {
                if(buffer[i] == '\n') {
                    if (commandlist[commandnum].commandname != "") {
                        commandnum++;
                    }
                    continue;
                }else if(buffer[i] == ' '||buffer[i] == '\t'){
                    if (commandlist[commandnum].commandname == "" || commandlist[commandnum].commandtype == EXEC_GETFILE) {
                        continue;
                    }else if(commandlist[commandnum].commandtype == EXEC_POSTFILE) {
                        if(spaceflag) {
                            continue;
                        }else {
                            if(buffer[i] == '\t')
                                buffer[i] = ' ';
                            spaceflag = true;
                        }
                    }        
                }
                if(!commandlist[commandnum].commandtype) {
                    type += buffer[i];
                    if(type == "CMD"||type == "POST"||type == "GET"||type == "1"||type == "2"||type == "3") {
                        if(type == "CMD"||type == "1")
                            commandlist[commandnum].commandtype = EXEC_COMMAND;
                        else if(type == "POST"||type == "2")
                            commandlist[commandnum].commandtype = EXEC_POSTFILE;
                        else
                            commandlist[commandnum].commandtype = EXEC_GETFILE;
                        type = "";
                    }else if(type.length() >= 4) {
                        commandlist[commandnum].commandtype = 4;
                        type = "";
                    }
                }else {
                    commandlist[commandnum].commandname += buffer[i];
                }
            }
        }
    }
    fin.close();

    if (commandlist[commandnum].commandname != "") 
        commandnum ++;

    return commandnum;
}

bool ShowInfo(std::string serverlist[], size_t servernum, STRUCT_COMMAND commandlist[], size_t commandnum) {
    LOG(INFO) << "ServerList:";
    for(size_t i = 0; i < servernum; i++) {
        LOG(INFO) << serverlist[i];
    }
    LOG(INFO) << "CommandList:";
    for(size_t i = 0; i < commandnum; i++) {
        std::string commandname;
        switch(commandlist[i].commandtype) {
            case EXEC_COMMAND:
                commandname = "CMD ";
                break;
            case EXEC_POSTFILE:
                commandname = "POST";
                break;
            case EXEC_GETFILE:
                commandname = "GET ";
                break;
            default:
                return false;
        }
        LOG(INFO) << commandname << "  " << commandlist[i].commandname;
    }
    return true;
}

size_t GetServerlistFromArg(const char* iplist, size_t length, std::string serverlist[]){
    size_t servernum = 0;
    for(size_t i = 0; i < length; i++) {
        if(iplist[i] == '\t' || iplist[i] == ' ' || iplist[i] == ',') {
            if(serverlist[servernum] != "")
                servernum ++;
            continue;
        }
        serverlist[servernum] += iplist[i];
    }

    if (serverlist[servernum] != "") 
        servernum ++;

    return servernum;
}

int main(int argc, char* argv[]) {       

    GFLAGS_NS::ParseCommandLineFlags(&argc, &argv, true);

    std::string serverlist[FLAGS_default_buffer_size];
    STRUCT_COMMAND commandlist[FLAGS_default_buffer_size];
    size_t servernum = 0, commandnum = 0;
    if(FLAGS_ip.length()) {
        servernum = GetServerlistFromArg(FLAGS_ip.c_str(), FLAGS_ip.length(), serverlist);
    }
    if(FLAGS_cmd.length() || FLAGS_postfile.length() || FLAGS_getfile.length()) {
        if(FLAGS_cmd.length()){
            commandlist[0].commandtype = EXEC_COMMAND;
            commandlist[0].commandname = FLAGS_cmd;
        }            
        else if(FLAGS_postfile.length()){
            commandlist[0].commandtype = EXEC_POSTFILE;
            commandlist[0].commandname = FLAGS_postfile;
        }
        else if(FLAGS_getfile.length()){
            commandlist[0].commandtype = EXEC_GETFILE;
            commandlist[0].commandname = FLAGS_getfile;
        }
        commandnum = 1;
    }
    if(!servernum)
        servernum = GetServerlistFromFile(FLAGS_ip_conf, serverlist);
    if(!commandnum)
        commandnum = GetCommandlistFromFile(FLAGS_cmd_conf, commandlist);

    if(!servernum||!commandnum||!ShowInfo(serverlist, servernum, commandlist, commandnum)) {
        LOG(INFO) << "Failed To Get the CommandList or ServerList!";
        return 0;
    }

    CreateThread(serverlist, servernum, commandlist, commandnum);

    while(!brpc::IsAskedToQuit())
        sleep(5);

    LOG(INFO) << "EchoClient is going to quit";
    return 0;
}
