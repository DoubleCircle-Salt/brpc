#include <gflags/gflags.h>
#include <butil/logging.h>
#include <brpc/server.h>
#include "echo.pb.h"
#include "handler.h"

int32_t handler(google::protobuf::RpcController* cntl_base,
                      const example::EchoRequest* request,
                      example::EchoResponse* response){

	brpc::Controller* cntl =
            static_cast<brpc::Controller*>(cntl_base);

	LOG(INFO) << "Received request[log_id=" << cntl->log_id() 
                  << "] from " << cntl->remote_side()
                  << ": " << request->message()
                  << " (attached=" << cntl->request_attachment() << ")";

    response->set_message(request->message());

    if (FLAGS_send_attachment) {
            // Set attachment which is wired to network directly instead of
            // being serialized into protobuf messages.
            cntl->response_attachment().append("bar");
        }
}