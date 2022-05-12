#include <map>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <iostream>
#include <fstream>
#include <errno.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <getopt.h>
#include <grpcpp/grpcpp.h>
#include <filesystem>
#include <stdio.h>
#include <unistd.h>

#include "src/dfs-utils.h"
#include "dfslib-servernode-p1.h"
#include "proto-src/dfs-service.grpc.pb.h"
#include "dfslib-shared-p1.h"
#include <google/protobuf/util/time_util.h>

using namespace std;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::Status;
using grpc::StatusCode;
using grpc::string_ref;

using dfs_service::DFSService;
using dfs_service::Empty;
using dfs_service::File;
using dfs_service::FileChunk;
using dfs_service::FileList;
using dfs_service::FileStat;

using google::protobuf::Timestamp;
using google::protobuf::util::TimeUtil;

// STUDENT INSTRUCTION:
//
// DFSServiceImpl is the implementation service for the rpc methods
// and message types you defined in the `dfs-service.proto` file.
//
// You should add your definition overrides here for the specific
// methods that you created for your GRPC service protocol. The
// gRPC tutorial described in the readme is a good place to get started
// when trying to understand how to implement this class.
//
// The method signatures generated can be found in `proto-src/dfs-service.grpc.pb.h` file.
//
// Look for the following section:
//
//      class Service : public ::grpc::Service {
//
// The methods returning grpc::Status are the methods you'll want to override.
//
// In C++, you'll want to use the `override` directive as well. For example,
// if you have a service method named MyMethod that takes a MyMessageType
// and a ServerWriter, you'll want to override it similar to the following:
//
//      Status MyMethod(ServerContext* context,
//                      const MyMessageType* request,
//                      ServerWriter<MySegmentType> *writer) override {
//
//          /** code implementation here **/
//      }
//
class DFSServiceImpl final : public DFSService::Service
{

private:
    /** The mount path for the server **/
    std::string mount_path;

    /**
     * Prepend the mount path to the filename.
     *
     * @param filepath
     * @return
     */
    const std::string WrapPath(const std::string &filepath)
    {
        return this->mount_path + filepath;
    }

public:
    DFSServiceImpl(const std::string &mount_path) : mount_path(mount_path)
    {
    }

    ~DFSServiceImpl() {}

    //
    // STUDENT INSTRUCTION:
    //
    // Add your additional code here, including
    // implementations of your protocol service methods
    //
    Status Delete(ServerContext *context, const File *requested_file, Empty *response) override
    {
        const std::string &file_path = WrapPath(requested_file->file_name());
        ifstream ifile;
        ifile.open(file_path);

        if (context->IsCancelled())
        {
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or Client cancelled, abandoning.");
        }
        // https://www.codegrepper.com/code-examples/cpp/c%2B%2B+check+if+file+exists
        if (!(ifile))
        {
            return Status(StatusCode::NOT_FOUND, "File not found.");
        }
        ifile.close();
        // https://www.cplusplus.com/reference/cstdio/remove/
        if (remove(file_path.c_str()) != 0)
        {
            return Status(StatusCode::CANCELLED, "File failed to delete.");
        }
        else
        {
            return Status(StatusCode::OK, "File deleted");
        }
    }

    Status Stat(ServerContext *context, const File *requested_file, FileStat *response) override
    {
        const std::string &file_path = WrapPath(requested_file->file_name());
        ifstream ifile;
        ifile.open(file_path);
        // if failed to open, no file found
        if (!(ifile))
        {
            return Status(StatusCode::NOT_FOUND, "File not found.");
        }
        // close the file
        ifile.close();

        // if client cancelled the request, time limit exceeded
        if (context->IsCancelled())
        {
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or Client cancelled, abandoning.");
        }

        // try get the file status
        struct stat buf;
        if (stat(file_path.c_str(), &buf) != 0)
        {
            return Status(StatusCode::CANCELLED, "File failed to obtain status.");
        }
        // if successful, set the file status to the self-declared FileStat class (in proto file)
        else
        {
            Timestamp *mtime = new Timestamp(TimeUtil::TimeTToTimestamp(buf.st_mtime));
            Timestamp *ctime = new Timestamp(TimeUtil::TimeTToTimestamp(buf.st_ctime));
            response->set_allocated_mtime(mtime);
            response->set_allocated_ctime(ctime);
            response->set_size(buf.st_size);
            response->set_file_name(file_path);
            return Status(StatusCode::OK, "File Status Retrieved.");
        }
    }

    Status List(ServerContext *context, const Empty *requested_file, FileList *response) override
    {
        // https://stackoverflow.com/questions/612097/how-can-i-get-the-list-of-files-in-a-directory-using-c-or-c
        DIR *dir;
        struct dirent *ent;
        if ((dir = opendir(mount_path.c_str())) != NULL)
        {
            /* print all the files and directories within directory */
            while ((ent = readdir(dir)) != NULL)
            {
                // if client cancelled the request, time limit exceeded
                if (context->IsCancelled())
                {
                    return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or Client cancelled, abandoning.");
                }

                string dirEntry(ent->d_name);          // file name
                string file_path = WrapPath(dirEntry); // complete file path

                struct stat buf; // try get the file status
                if (stat(file_path.c_str(), &buf) != 0)
                {
                    return Status(StatusCode::CANCELLED, "File failed to obtain status.");
                }
                // if successful, set the file status to the self-declared FileStat class (in proto file)
                else
                {
                    // https://stackoverflow.com/questions/146924/how-can-i-tell-if-a-given-path-is-a-directory-or-a-file-c-c#:~:text=This%20is%20a%20simple%20method,it%20is%20a%20file%20path.
                    // if it's a directory, skip
                    if (buf.st_mode & S_IFDIR)
                    {
                        continue;
                    }
                    FileStat *file_stat = response->add_file();
                    Timestamp *mtime = new Timestamp(TimeUtil::TimeTToTimestamp(buf.st_mtime));
                    Timestamp *ctime = new Timestamp(TimeUtil::TimeTToTimestamp(buf.st_ctime));
                    file_stat->set_allocated_mtime(mtime);
                    file_stat->set_allocated_ctime(ctime);
                    file_stat->set_size(buf.st_size);
                    file_stat->set_file_name(ent->d_name);
                }
            }
            closedir(dir);
            return Status::OK;
        }
        else
        {
            /* could not open directory */
            dfs_log(LL_ERROR) << "Failed to open " << mount_path;
            return Status(StatusCode::CANCELLED, "Dir failed to open.");
        }
    }

    Status Fetch(ServerContext *context, const File *requested_file, ServerWriter<FileChunk> *writer) override
    {
        const std::string &file_path = WrapPath(requested_file->file_name());
        ifstream ifile;
        ifile.open(file_path);
        // if failed to open, no file found
        if (!(ifile))
        {
            dfs_log(LL_ERROR) << "Fetch response message: "
                              << "File Not Found";
            return Status(StatusCode::NOT_FOUND, "File not found.");
        }
        // close the file
        ifile.close();

        // try get the file status
        struct stat buf;
        if (stat(file_path.c_str(), &buf) != 0)
        {
            dfs_log(LL_ERROR) << "Fetch response message: "
                              << "File failed to obtain status";
            return Status(StatusCode::CANCELLED, "File failed to obtain status.");
        }
        // if successful, start reading and sending file to client
        int file_size = buf.st_size;
        dfs_log(LL_SYSINFO) << "File size is " << file_size << " bytes.";
        int FILE_CHUNK_SIZE = 4096;
        ifile.open(file_path);
        FileChunk file_chunk;
        file_chunk.set_file_name(requested_file->file_name());
        int bytes_sent = 0;

        while (!ifile.eof() && bytes_sent < file_size)
        {
            if (context->IsCancelled())
            {
                return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or Client cancelled, abandoning.");
            }
            int file_chunk_size = min(FILE_CHUNK_SIZE, file_size - bytes_sent);

            char temp_buf[FILE_CHUNK_SIZE + 1];
            ifile.read(temp_buf, file_chunk_size);
            file_chunk.clear_buffer();
            file_chunk.set_buffer(temp_buf, file_chunk_size);
            writer->Write(file_chunk);
            bytes_sent += file_chunk_size;
            dfs_log(LL_SYSINFO) << "Sending " << file_size << " bytes, total sent " << bytes_sent << " bytes.";
        }
        ifile.close();
        if (bytes_sent != file_size)
        {
            return Status(StatusCode::CANCELLED, "File sent wrong number of bytes");
        }
        dfs_log(LL_SYSINFO) << "Fetch response message: "
                            << "File successfully sent.";
        return Status::OK;
    }

    Status Store(ServerContext *context, ServerReader<FileChunk> *reader, Empty *response) override
    {
        // read the buffers and store in local path
        ofstream ofs;
        FileChunk buffers;

        while (reader->Read(&buffers))
        {
            if (context->IsCancelled())
            {
                return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or Client cancelled, abandoning.");
            }
            const std::string &file_path = WrapPath(buffers.file_name());
            ofs.open(file_path, std::ofstream::out | std::ofstream::app);
            const string &str = buffers.buffer();
            ofs << buffers.buffer();
            // dfs_log(LL_SYSINFO) << "Writing chunk of size " << str.length() << " bytes";
            ofs.close();
        }
        return Status::OK;
    }
};

//
// STUDENT INSTRUCTION:
//
// The following three methods are part of the basic DFSServerNode
// structure. You may add additional methods or change these slightly,
// but be aware that the testing environment is expecting these
/*The main server node constructor
 *
 * @param server_address
 * @param mount_path
 */
DFSServerNode::DFSServerNode(const std::string &server_address,
                             const std::string &mount_path,
                             std::function<void()> callback) : server_address(server_address),
                                                               mount_path(mount_path), grader_callback(callback) {}

/**
 * Server shutdown
 */
DFSServerNode::~DFSServerNode() noexcept
{
    dfs_log(LL_SYSINFO) << "DFSServerNode shutting down";
    this->server->Shutdown();
}

/** Server start **/
void DFSServerNode::Start()
{
    DFSServiceImpl service(this->mount_path);
    ServerBuilder builder;
    builder.AddListeningPort(this->server_address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    this->server = builder.BuildAndStart();
    dfs_log(LL_SYSINFO) << "DFSServerNode server listening on " << this->server_address;
    this->server->Wait();
}

//
// STUDENT INSTRUCTION:
//
// Add your additional DFSServerNode definitions here
//