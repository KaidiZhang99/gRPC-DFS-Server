#include <regex>
#include <mutex>
#include <vector>
#include <string>
#include <thread>
#include <cstdio>
#include <chrono>
#include <errno.h>
#include <csignal>
#include <iostream>
#include <sstream>
#include <fstream>
#include <iomanip>
#include <getopt.h>
#include <unistd.h>
#include <limits.h>
#include <sys/inotify.h>
#include <grpcpp/grpcpp.h>
#include <utime.h>

#include "src/dfs-utils.h"
#include "src/dfslibx-clientnode-p2.h"
#include "dfslib-shared-p2.h"
#include "dfslib-clientnode-p2.h"
#include "proto-src/dfs-service.grpc.pb.h"
#include <google/protobuf/util/time_util.h>

using dfs_service::Empty;
using dfs_service::File;
using dfs_service::FileChunk;
using dfs_service::FileList;
using dfs_service::FileMutex;
using dfs_service::FileStat;

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientWriter;
using grpc::Status;
using grpc::StatusCode;

using std::chrono::milliseconds;
using std::chrono::system_clock;
using std::chrono::time_point;

using google::protobuf::Timestamp;
using google::protobuf::util::TimeUtil;
using namespace std;
extern dfs_log_level_e DFS_LOG_LEVEL;

// const char *Metadata_ClientId = "client_id";
// const char *Metadata_CheckSum = "checksum";
// const char *Metadata_Mtime = "mtime";
//
// STUDENT INSTRUCTION:
//
// Change these "using" aliases to the specific
// message types you are using to indicate
// a file request and a listing of files from the server.
//
using FileRequestType = dfs_service::File;
using FileListResponseType = dfs_service::FileList;

DFSClientNodeP2::DFSClientNodeP2() : DFSClientNode() {}
DFSClientNodeP2::~DFSClientNodeP2() {}

grpc::StatusCode DFSClientNodeP2::RequestWriteAccess(const std::string &filename)
{
    // setting ddl. Once ddl exceeded, the rpc request will be cancelled.
    ClientContext context;
    // std::chrono::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout));
    context.AddMetadata(Metadata_ClientId, ClientId());

    File requested_file;
    requested_file.set_name(filename);
    FileMutex response;

    //  rpc GetMutex (File) returns (WriteLock);
    Status status = service_stub->GetMutex(&context, requested_file, &response);
    return status.error_code();
    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to obtain a write lock here when trying to store a file.
    // This method should request a write lock for the given file at the server,
    // so that the current client becomes the sole creator/writer. If the server
    // responds with a RESOURCE_EXHAUSTED response, the client should cancel
    // the current file storage
    //
    // The StatusCode response should be:
    //
    // OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //
}

grpc::StatusCode DFSClientNodeP2::Store(const std::string &filename)
{
    const std::string &file_path = WrapPath(filename);

    // setting ddl. Once ddl exceeded, the rpc request will be cancelled.
    ClientContext context;
    // std::chrono::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
    // context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout));
    // try get the file status
    struct stat buf;
    if (stat(file_path.c_str(), &buf) != 0)
    {
        dfs_log(LL_ERROR) << "Store client message: "
                          << "File failed to obtain status";
        return StatusCode::NOT_FOUND;
    }
    context.AddMetadata(Metadata_CheckSum, to_string(dfs_file_checksum(file_path, &(this->crc_table))));
    context.AddMetadata(Metadata_ClientId, ClientId());
    long int t = static_cast<long int>(buf.st_mtime);
    context.AddMetadata(Metadata_Mtime, to_string(t));
    context.AddMetadata(Metadata_FileName, filename);

    StatusCode file_lock = this->RequestWriteAccess(filename);
    if (file_lock != StatusCode::OK)
    {
        return StatusCode::RESOURCE_EXHAUSTED;
    }

    ifstream ifile;

    // if successful, start reading and sending file to server
    Empty server_response;
    unique_ptr<ClientWriter<FileChunk>> writer = service_stub->Store(&context, &server_response);

    int file_size = buf.st_size;
    dfs_log(LL_SYSINFO) << "File size is " << file_size << " bytes.";
    int FILE_CHUNK_SIZE = 4096;

    ifile.open(file_path);
    FileChunk file_chunk;
    file_chunk.set_name(filename);
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
        dfs_log(LL_SYSINFO) << "Uploading " << file_chunk_size << " bytes, total uploaded " << bytes_sent << " bytes.";
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
                        << "File successfully sent." << status.error_message();
    return status.error_code();

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to store a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation. However, you will
    // need to adjust this method to recognize when a file trying to be
    // stored is the same on the server (i.e. the ALREADY_EXISTS gRPC response).
    //
    // You will also need to add a request for a write lock before attempting to store.
    //
    // If the write lock request fails, you should return a status of RESOURCE_EXHAUSTED
    // and cancel the current operation.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::ALREADY_EXISTS - if the local cached file has not changed from the server version
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //
}

grpc::StatusCode DFSClientNodeP2::Fetch(const std::string &filename)
{
    bool already_found = false;
    // setting ddl. Once ddl exceeded, the rpc request will be cancelled.
    ClientContext context;
    const std::string &file_path = WrapPath(filename);
    struct stat buf;
    if (stat(file_path.c_str(), &buf) == 0)
    {
        dfs_log(LL_DEBUG) << "File found in local storage!";
        long int t = static_cast<long int>(buf.st_mtime);
        context.AddMetadata(Metadata_Mtime, to_string(t));
        already_found = true;
    }
    // std::chrono::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout));
    context.AddMetadata(Metadata_CheckSum, to_string(dfs_file_checksum(file_path, &(this->crc_table))));
    context.AddMetadata(Metadata_ClientId, ClientId());

    // class File, FileChunk declared in dfs-service.proto
    File requested_file;
    requested_file.set_name(filename);

    FileChunk buffers;
    unique_ptr<ClientReader<FileChunk>> response = service_stub->Fetch(&context, requested_file);
    dfs_log(LL_DEBUG) << "Fetched from grpc server";
    ofstream ofs;
    dfs_log(LL_DEBUG) << "start reading grpc response";
    // read the buffers and store in local path
    while (response->Read(&buffers))
    {
        dfs_log(LL_DEBUG) << "reading grpc response";
        if (already_found)
        {
            dfs_log(LL_DEBUG) << "grpc server sent new data. File already found. So truncate it";
            ofs.open(file_path, std::ofstream::out | ios::trunc);
            ofs.close();
            already_found = false;
        }
        // https://www.cplusplus.com/reference/fstream/ofstream/open/
        ofs.open(file_path, std::ofstream::out | std::ofstream::app);
        const string &str = buffers.buffer();
        ofs << buffers.buffer();
        dfs_log(LL_SYSINFO) << "Writing chunk of size " << str.length() << " bytes";
        ofs.close();
    }

    Status status = response->Finish();
    dfs_log(LL_DEBUG) << "fnished reading grpc response";
    dfs_log(LL_ERROR) << "Fetch response message: " << status.error_message() << " code: " << status.error_code();
    return status.error_code();
    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to fetch a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation. However, you will
    // need to adjust this method to recognize when a file trying to be
    // fetched is the same on the client (i.e. the files do not differ
    // between the client and server and a fetch would be unnecessary.
    //
    // The StatusCode response should be:
    //
    // OK - if all went well
    // DEADLINE_EXCEEDED - if the deadline timeout occurs
    // NOT_FOUND - if the file cannot be found on the server
    // ALREADY_EXISTS - if the local cached file has not changed from the server version
    // CANCELLED otherwise
    //
    // Hint: You may want to match the mtime on local files to the server's mtime
    //
}

grpc::StatusCode DFSClientNodeP2::Delete(const std::string &filename)
{
    // setting ddl. Once ddl exceeded, the rpc request will be cancelled.
    ClientContext context;
    // std::chrono::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout));
    context.AddMetadata(Metadata_ClientId, ClientId());

    StatusCode file_lock = this->RequestWriteAccess(filename);
    dfs_log(LL_DEBUG) << "Requested Write Access ";
    if (file_lock != StatusCode::OK)
    {
        dfs_log(LL_ERROR) << "Resource Exhausted!";
        return StatusCode::RESOURCE_EXHAUSTED;
    }
    // class File, Empty declared in dfs-service.proto
    File requested_file;
    requested_file.set_name(filename);
    Empty response;

    // service_stub is the "client" used to call grpc server method
    // &response will store the response (type specified in proto file)
    Status status = service_stub->Delete(&context, requested_file, &response);
    dfs_log(LL_SYSINFO) << "Server response: " << status.error_message();
    return status.error_code();
    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to delete a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You will also need to add a request for a write lock before attempting to delete.
    //
    // If the write lock request fails, you should return a status of RESOURCE_EXHAUSTED
    // and cancel the current operation.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::RESOURCE_EXHAUSTED - if a write lock cannot be obtained
    // StatusCode::CANCELLED otherwise
    //
    //
}

grpc::StatusCode DFSClientNodeP2::List(std::map<std::string, int> *file_map, bool display)
{
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
        file_map->insert(std::pair<std::string, int>(stat.name(), mtime));
        dfs_log(LL_SYSINFO) << "Inserting " << stat.name() << " to file map";
    }
    return status.error_code();

    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to list files here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation and add any additional
    // listing details that would be useful to your solution to the list response.
    //
    // The StatusCode response should be:
    //
    // StatusCode::OK - if all went well
    // StatusCode::DEADLINE_EXCEEDED - if the deadline timeout occurs
    // StatusCode::CANCELLED otherwise
    //
    //
}

grpc::StatusCode DFSClientNodeP2::Stat(const std::string &filename, void *file_status)
{
    // setting ddl. Once ddl exceeded, the rpc request will be cancelled.
    ClientContext context;
    // std::chrono::time_point deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout);
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_timeout));

    // class File, FileStat declared in dfs-service.proto
    File requested_file;
    requested_file.set_name(filename);
    FileStat response;

    Status status = service_stub->Stat(&context, requested_file, &response);
    dfs_log(LL_SYSINFO) << "Server response: " << status.error_message();
    dfs_log(LL_SYSINFO) << "Server response: " << response.DebugString();
    return status.error_code();
    //
    // STUDENT INSTRUCTION:
    //
    // Add your request to get the status of a file here. Refer to the Part 1
    // student instruction for details on the basics.
    //
    // You can start with your Part 1 implementation and add any additional
    // status details that would be useful to your solution.
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

void DFSClientNodeP2::InotifyWatcherCallback(std::function<void()> callback)
{

    //
    // STUDENT INSTRUCTION:
    //
    // This method gets called each time inotify signals a change
    // to a file on the file system. That is every time a file is
    // modified or created.
    //
    // You may want to consider how this section will affect
    // concurrent actions between the inotify watcher and the
    // asynchronous callbacks associated with the server.
    //
    // The callback method shown must be called here, but you may surround it with
    // whatever structures you feel are necessary to ensure proper coordination
    // between the async and watcher threads.
    //
    // Hint: how can you prevent race conditions between this thread and
    // the async thread when a file event has been signaled?
    //
    this->client_mutex.lock();
    callback();
    this->client_mutex.unlock();
}

//
// STUDENT INSTRUCTION:
//
// This method handles the gRPC asynchronous callbacks from the server.
// We've provided the base structure for you, but you should review
// the hints provided in the STUDENT INSTRUCTION sections below
// in order to complete this method.
//
void DFSClientNodeP2::HandleCallbackList()
{

    void *tag;

    bool ok = false;

    //
    // STUDENT INSTRUCTION:
    //
    // Add your file list synchronization code here.
    //
    // When the server responds to an asynchronous request for the CallbackList,
    // this method is called. You should then synchronize the
    // files between the server and the client based on the goals
    // described in the readme.
    //
    // In addition to synchronizing the files, you'll also need to ensure
    // that the async thread and the file watcher thread are cooperating. These
    // two threads could easily get into a race condition where both are trying
    // to write or fetch over top of each other. So, you'll need to determine
    // what type of locking/guarding is necessary to ensure the threads are
    // properly coordinated.
    //

    // Block until the next result is available in the completion queue.
    while (completion_queue.Next(&tag, &ok))
    {
        {
            //
            // STUDENT INSTRUCTION:
            //
            // Consider adding a critical section or RAII style lock here
            //

            // The tag is the memory location of the call_data object
            AsyncClientData<FileListResponseType> *call_data = static_cast<AsyncClientData<FileListResponseType> *>(tag);

            dfs_log(LL_DEBUG2) << "Received completion queue callback";

            // Verify that the request was completed successfully. Note that "ok"
            // corresponds solely to the request for updates introduced by Finish().
            // GPR_ASSERT(ok);
            if (!ok)
            {
                dfs_log(LL_ERROR) << "Completion queue callback not ok.";
            }

            if (ok && call_data->status.ok())
            {

                dfs_log(LL_DEBUG3) << "Handling async callback ";

                //
                // STUDENT INSTRUCTION:
                //
                // Add your handling of the asynchronous event calls here.
                // For example, based on the file listing returned from the server,
                // how should the client respond to this updated information?
                // Should it retrieve an updated version of the file?
                // Send an update to the server?
                // Do nothing?
                //
                this->client_mutex.lock();
                for (const FileStat &server_file_status : call_data->reply.file())
                {
                    const std::string &file_path = WrapPath(server_file_status.name());
                    // try get the file status
                    struct stat buf;
                    time_t server_mtime = TimeUtil::TimestampToTimeT(server_file_status.mtime());
                    // if file not found locally
                    if (stat(file_path.c_str(), &buf) != 0)
                    {
                        StatusCode fetch_status = this->Fetch(server_file_status.name());
                        if (fetch_status != StatusCode::OK)
                        {
                            dfs_log(LL_ERROR) << "Fetching file failed: " << server_file_status.name();
                        }
                    }
                    else if (server_mtime > buf.st_mtime)
                    { // server has newer version
                        StatusCode fetch_status = this->Fetch(server_file_status.name());
                        if (fetch_status == StatusCode::ALREADY_EXISTS)
                        {
                            dfs_log(LL_ERROR) << "File already exist on client, change mtime" << server_file_status.name();
                            struct utimbuf new_times;
                            time_t mtime = TimeUtil::TimestampToTimeT(server_file_status.mtime());
                            new_times.modtime = mtime;
                            utime(file_path.c_str(), &new_times);
                        }
                    }
                    else if (server_mtime < buf.st_mtime)
                    { // client has newer version
                        StatusCode store_status = this->Store(server_file_status.name());
                        if (store_status != StatusCode::OK)
                        {
                            dfs_log(LL_ERROR) << "Storing file failed: " << server_file_status.name();
                        }
                    }
                }
                this->client_mutex.unlock();
            }
            else
            {
                dfs_log(LL_ERROR) << "Status was not ok. Will try again in " << DFS_RESET_TIMEOUT << " milliseconds.";
                dfs_log(LL_ERROR) << call_data->status.error_message();
                std::this_thread::sleep_for(std::chrono::milliseconds(DFS_RESET_TIMEOUT));
            }

            // Once we're complete, deallocate the call_data object.
            delete call_data;

            //
            // STUDENT INSTRUCTION:
            //
            // Add any additional syncing/locking mechanisms you may need here
        }

        // Start the process over and wait for the next callback response
        dfs_log(LL_DEBUG3) << "Calling InitCallbackList";
        InitCallbackList();
    }
}

/**
 * This method will start the callback request to the server, requesting
 * an update whenever the server sees that files have been modified.
 *
 * We're making use of a template function here, so that we can keep some
 * of the more intricate workings of the async process out of the way, and
 * give you a chance to focus more on the project's requirements.
 */
void DFSClientNodeP2::InitCallbackList()
{
    CallbackList<FileRequestType, FileListResponseType>();
}

//
// STUDENT INSTRUCTION:
//
// Add any additional code you need to here
//
