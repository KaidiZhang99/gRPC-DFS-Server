#include <regex>
#include <vector>
#include <string>
#include <thread>
#include <cstdio>
#include <chrono>
#include <errno.h>
#include <iostream>
#include <sstream>
#include <fstream>
#include <csignal>
#include <iomanip>
#include <getopt.h>
#include <unistd.h>
#include <limits.h>
#include <sys/inotify.h>
#include <grpcpp/grpcpp.h>

#include "dfslib-shared-p1.h"
#include "proto-src/dfs-service.grpc.pb.h"
#include "dfslib-clientnode-p1.h"
#include <google/protobuf/util/time_util.h>

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientWriter;
using grpc::Status;
using grpc::StatusCode;

using dfs_service::Empty;
using dfs_service::File;
using dfs_service::FileChunk;
using dfs_service::FileList;
using dfs_service::FileStat;

using std::chrono::milliseconds;
using std::chrono::system_clock;
using std::chrono::time_point;

using google::protobuf::Timestamp;
using google::protobuf::util::TimeUtil;
using namespace std;

//
// STUDENT INSTRUCTION:
//
// You may want to add aliases to your namespaced service methods here.
// All of the methods will be under the `dfs_service` namespace.
//
// For example, if you have a method named MyMethod, add
// the following:
//
//      using dfs_service::MyMethod
//

DFSClientNodeP1::DFSClientNodeP1() : DFSClientNode()
{
}

DFSClientNodeP1::~DFSClientNodeP1() noexcept {}

StatusCode DFSClientNodeP1::Store(const std::string &filename)
{

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to store a file here. This method should
    // connect to your gRPC service implementation method
    // that can accept and store a file.
    //
    // When working with files in gRPC you'll need to stream
    // the file contents, so consider the use of gRPC's ClientWriter.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the client
    // StatusCode::CANCELLED otherwise
    //
    // rpc Store (stream FileChunk) returns (Empty);

    // setting ddl. Once ddl exceeded, the rpc request will be cancelled.
    ClientContext context;
    // std::chrono::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout));

    const std::string &file_path = WrapPath(filename);
    ifstream ifile;
    ifile.open(file_path);

    // https://www.codegrepper.com/code-examples/cpp/c%2B%2B+check+if+file+exists
    if (!(ifile))
    {
        dfs_log(LL_ERROR) << "Store client message: "
                          << "File not found";
        return StatusCode::NOT_FOUND;
    }
    ifile.close();

    // try get the file status
    struct stat buf;
    if (stat(file_path.c_str(), &buf) != 0)
    {
        dfs_log(LL_ERROR) << "Store client message: "
                          << "File failed to obtain status";
        return StatusCode::CANCELLED;
    }
    // if successful, start reading and sending file to server
    Empty server_response;
    unique_ptr<ClientWriter<FileChunk>> writer = service_stub->Store(&context, &server_response);

    int file_size = buf.st_size;
    dfs_log(LL_SYSINFO) << "File size is " << file_size << " bytes.";
    int FILE_CHUNK_SIZE = 4096;

    ifile.open(file_path);
    FileChunk file_chunk;
    file_chunk.set_file_name(filename);
    int bytes_sent = 0;

    while (!ifile.eof() && bytes_sent < file_size)
    {
        int file_chunk_size = min(FILE_CHUNK_SIZE, file_size - bytes_sent);

        char temp_buf[FILE_CHUNK_SIZE + 1];
        ifile.read(temp_buf, file_chunk_size);
        file_chunk.clear_buffer();
        file_chunk.set_buffer(temp_buf, file_chunk_size);
        writer->Write(file_chunk);
        bytes_sent += file_chunk_size;
        dfs_log(LL_SYSINFO) << "Uploading " << file_size << " bytes, total uploaded " << bytes_sent << " bytes.";
    }
    ifile.close();
    writer->WritesDone();
    Status status = writer->Finish();
    if (bytes_sent != file_size)
    {
        dfs_log(LL_ERROR) << "Store client message: "
                          << "File uploaded wrong number of bytes.";
        return StatusCode::CANCELLED;
    }
    dfs_log(LL_SYSINFO) << "Fetch response message: "
                        << "File successfully sent.";
    return status.error_code();
}

StatusCode DFSClientNodeP1::Fetch(const std::string &filename)
{

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to fetch a file here. This method should
    // connect to your gRPC service implementation method
    // that can accept a file request and return the contents
    // of a file from the service.
    //
    // As with the store function, you'll need to stream the
    // contents, so consider the use of gRPC's ClientReader.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //
    //

    // setting ddl. Once ddl exceeded, the rpc request will be cancelled.
    ClientContext context;
    // std::chrono::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout));

    // class File, FileChunk declared in dfs-service.proto
    File requested_file;
    requested_file.set_file_name(filename);
    const std::string &file_path = WrapPath(filename);
    FileChunk buffers;
    unique_ptr<ClientReader<FileChunk>> response = service_stub->Fetch(&context, requested_file);

    // read the buffers and store in local path
    ofstream ofs;

    while (response->Read(&buffers))
    {
        // https://www.cplusplus.com/reference/fstream/ofstream/open/
        ofs.open(file_path, std::ofstream::out | std::ofstream::app);
        const string &str = buffers.buffer();
        ofs << buffers.buffer();
        dfs_log(LL_SYSINFO) << "Writing chunk of size " << str.length() << " bytes";
        ofs.close();
    }

    Status status = response->Finish();
    dfs_log(LL_ERROR) << "Fetch response message: " << status.error_message() << " code: " << status.error_code();
    return status.error_code();
}

StatusCode DFSClientNodeP1::Delete(const std::string &filename)
{

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to delete a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //
    //

    // setting ddl. Once ddl exceeded, the rpc request will be cancelled.
    ClientContext context;
    // std::chrono::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout));

    // class File, Empty declared in dfs-service.proto
    File requested_file;
    requested_file.set_file_name(filename);
    Empty response;

    // service_stub is the "client" used to call grpc server method
    // &response will store the response (type specified in proto file)
    Status status = service_stub->Delete(&context, requested_file, &response);
    dfs_log(LL_SYSINFO) << "Server response: " << status.error_message();
    return status.error_code();
}

StatusCode DFSClientNodeP1::Stat(const std::string &filename, void *file_status)
{
    // setting ddl. Once ddl exceeded, the rpc request will be cancelled.
    ClientContext context;
    // std::chrono::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout));

    // class File, FileStat declared in dfs-service.proto
    File requested_file;
    requested_file.set_file_name(filename);
    FileStat response;

    Status status = service_stub->Stat(&context, requested_file, &response);
    dfs_log(LL_SYSINFO) << "Server response: " << status.error_message();
    dfs_log(LL_SYSINFO) << "Server response: " << response.DebugString();
    return status.error_code();
    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to get the status of a file here. This method should
    // retrieve the status of a file on the server. Note that you won't be
    // tested on this method, but you will most likely find that you need
    // a way to get the status of a file in order to synchronize later.
    //
    // The status might include data such as name, size, mtime, crc, etc.
    //
    // The file_status is left as a void* so that you can use it to pass
    // a message type that you defined. For instance, may want to use that message
    // type after calling Stat from another method.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::NOT_FOUND - if the file cannot be found on the server
    // StatusCode::CANCELLED otherwise
    //
    //
}

StatusCode DFSClientNodeP1::List(std::map<std::string, int> *file_map, bool display)
{ // rpc List (Empty) returns (FileList);

    // setting ddl. Once ddl exceeded, the rpc request will be cancelled.
    ClientContext context;
    // std::chrono::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout));

    // class File, FileStat declared in dfs-service.proto
    Empty requested_file;
    FileList response;

    Status status = service_stub->List(&context, requested_file, &response);
    if (!status.ok())
    {
        dfs_log(LL_SYSINFO) << "Server response: " << status.error_message();
        return status.error_code();
    }

    if (display)
    {
        dfs_log(LL_SYSINFO) << "Success - response: " << response.DebugString();
    }

    for (const FileStat &stat : response.file())
    {
        int mtime = TimeUtil::TimestampToSeconds(stat.mtime());
        file_map->insert(std::pair<std::string, int>(stat.file_name(), mtime));
        dfs_log(LL_SYSINFO) << "Inserting " << stat.file_name() << " to file map";
    }
    return status.error_code();
    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to list all files here. This method
    // should connect to your service's list method and return
    // a list of files using the message type you created.
    //
    // The file_map parameter is a simple map of files. You should fill
    // the file_map with the list of files you receive with keys as the
    // file name and values as the modified time (mtime) of the file
    // received from the server.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::CANCELLED otherwise
    //
    //
}
//
// STUDENT INSTRUCTION:
//
// Add your additional code here, including
// implementations of your client methods
//
