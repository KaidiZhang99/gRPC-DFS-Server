#include <map>
#include <mutex>
#include <shared_mutex>
#include <chrono>
#include <string>
#include <thread>
#include <errno.h>
#include <iostream>
#include <cstdio>
#include <fstream>
#include <getopt.h>
#include <sys/stat.h>
#include <grpcpp/grpcpp.h>
#include <dirent.h>

#include "src/dfslibx-service-runner.h"
#include "src/dfs-utils.h"
#include "dfslib-shared-p2.h"
#include "proto-src/dfs-service.grpc.pb.h"
#include "src/dfslibx-call-data.h"
#include "dfslib-servernode-p2.h"
#include <google/protobuf/util/time_util.h>

using namespace std;
using dfs_service::DFSService;
using dfs_service::Empty;
using dfs_service::File;
using dfs_service::FileChunk;
using dfs_service::FileList;
using dfs_service::FileMutex;
using dfs_service::FileStat;

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerWriter;
using grpc::Status;
using grpc::StatusCode;
using grpc::string_ref;

using google::protobuf::Timestamp;
using google::protobuf::util::TimeUtil;
//
// STUDENT INSTRUCTION:
//
// Change these "using" aliases to the specific
// message types you are using in your `dfs-service.proto` file
// to indicate a file request and a listing of files from the server
//
using FileRequestType = dfs_service::File;
using FileListResponseType = dfs_service::FileList;

extern dfs_log_level_e DFS_LOG_LEVEL;
// const char *Metadata_ClientId = "client_id";
// const char *Metadata_CheckSum = "checksum";
// const char *Metadata_Mtime = "mtime";
//
// STUDENT INSTRUCTION:
//
// As with Part 1, the DFSServiceImpl is the implementation service for the rpc methods
// and message types you defined in your `dfs-service.proto` file.
//
// You may start with your Part 1 implementations of each service method.
//
// Elements to consider for Part 2:
//
// - How will you implement the write lock at the server level?
// - How will you keep track of which client has a write lock for a file?
//      - Note that we've provided a preset client_id in DFSClientNode that generates
//        a client id for you. You can pass that to the server to identify the current client.
// - How will you release the write lock?
// - How will you handle a store request for a client that doesn't have a write lock?
// - When matching files to determine similarity, you should use the `file_checksum` method we've provided.
//      - Both the client and server have a pre-made `crc_table` variable to speed things up.
//      - Use the `file_checksum` method to compare two files, similar to the following:
//
//          std::uint32_t server_crc = dfs_file_checksum(filepath, &this->crc_table);
//
//      - Hint: as the crc checksum is a simple integer, you can pass it around inside your message types.
//
class DFSServiceImpl final : public DFSService::WithAsyncMethod_CallbackList<DFSService::Service>,
                             public DFSCallDataManager<FileRequestType, FileListResponseType>
{

private:
    /** The runner service used to start the service and manage asynchronicity **/
    DFSServiceRunner<FileRequestType, FileListResponseType> runner;

    /** Mutex for managing the queue requests **/
    std::mutex queue_mutex;

    /** The mount path for the server **/
    std::string mount_path;

    /** The vector of queued tags used to manage asynchronous requests **/
    std::vector<QueueRequest<FileRequestType, FileListResponseType>> queued_tags;

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

    /** CRC Table kept in memory for faster calculations **/
    CRC::Table<std::uint32_t, 32> crc_table;

    // Lock for the entire mount directory, used for list / CallbackList
    shared_timed_mutex directory_lock;

    // which client id has the lock of a specific file
    // Key = file name, value = client id
    mutex file_client_map_lock;
    map<string, string> file_client_map;

    // mutex for the specific file
    // if file not in the map yet, will create a new mutex
    // Key = file name, value = mutex unique pointer
    mutex file_mutex_map_lock;
    map<string, unique_ptr<shared_timed_mutex>> file_mutex_map;

    // making an unique mutex for a specific file if it does not existed yet
    // mutex will be stored to file_mutex_map
    void InitializeFileMutex(string file_name)
    {
        file_mutex_map_lock.lock();
        if (file_mutex_map.find(file_name) != file_mutex_map.end())
        {
            file_mutex_map_lock.unlock();
            return;
        }
        file_mutex_map[file_name] = make_unique<shared_timed_mutex>();
        file_mutex_map_lock.unlock();
    }

    // get the file mutex for accessing a specific file
    shared_timed_mutex *GetFileMutex(string file_name)
    {
        auto file_mutex_iter = file_mutex_map.find(file_name);
        if (file_mutex_iter == file_mutex_map.end())
        {
            InitializeFileMutex(file_name);
        }
        file_mutex_iter = file_mutex_map.find(file_name);
        shared_timed_mutex *file_mutex = file_mutex_iter->second.get();
        return file_mutex;
    }

    string GetClientID(const multimap<string_ref, string_ref> &metadata)
    {
        auto client_id_iter = metadata.find(Metadata_ClientId);
        if (client_id_iter == metadata.end())
        {
            return "NOT_FOUND";
        }
        else
        {
            string client_id = string(client_id_iter->second.begin(), client_id_iter->second.end());
            return client_id;
        }
    }

    void ReleaseClientMutex(string file_name)
    {
        file_client_map_lock.lock();
        file_client_map.erase(file_name);
        for (auto it = file_client_map.begin(); it != file_client_map.end(); ++it)
            std::cout << it->first << " => " << it->second << '\n';
        file_client_map_lock.unlock();
    }

public:
    DFSServiceImpl(const std::string &mount_path, const std::string &server_address, int num_async_threads) : mount_path(mount_path), crc_table(CRC::CRC_32())
    {

        this->runner.SetService(this);
        this->runner.SetAddress(server_address);
        this->runner.SetNumThreads(num_async_threads);
        this->runner.SetQueuedRequestsCallback([&]
                                               { this->ProcessQueuedRequests(); });
    }

    ~DFSServiceImpl()
    {
        this->runner.Shutdown();
    }

    void Run()
    {
        this->runner.Run();
    }

    // rpc GetMutex (File) returns (FileMutex);
    Status GetMutex(ServerContext *context, const File *request, FileMutex *response) override
    {
        if (context->IsCancelled())
        {
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or Client cancelled, abandoning.");
        }
        const multimap<string_ref, string_ref> &metadata = context->client_metadata();
        // retrive the client ID, return an iterator to the element
        // auto client_id_iter = metadata.find(Metadata_ClientId);
        string client_id = GetClientID(metadata);
        // if (client_id_iter != metadata.end())
        if (client_id.compare("NOT_FOUND") != 0)
        {
            // iterator -> first = key; iterator -> second = value, i.e. string
            // string client_id = string(client_id_iter->second.begin(), client_id_iter->second.end());
            file_client_map_lock.lock();

            // client who locked this file
            auto locked_client_id_iter = file_client_map.find(request->name());
            // if found this file in the file_to_client map
            if (locked_client_id_iter != file_client_map.end())
            {
                string locked_client_id = string(locked_client_id_iter->second.begin(), locked_client_id_iter->second.end());
                // if this file is already requested/locked by another client
                if (locked_client_id.compare(client_id) != 0)
                {
                    file_client_map_lock.unlock();
                    dfs_log(LL_ERROR) << "File already in use by another client";
                    for (auto it = file_client_map.begin(); it != file_client_map.end(); ++it)
                        std::cout << it->first << " => " << it->second << '\n';
                    return Status(StatusCode::RESOURCE_EXHAUSTED, "File already in use by another client");
                }
                // if this client already has the lock of this file
                else if (locked_client_id.compare(client_id) == 0)
                {
                    file_client_map_lock.unlock();
                    dfs_log(LL_ERROR) << "File already acquired by this client";
                    return Status(StatusCode::OK, "File already acquired by this client");
                }
            }
            // else: this file had not been acquired by any client yet.
            // We will record it!
            else
            {
                file_client_map[request->name()] = client_id;
                file_client_map_lock.unlock();

                // Add file read write mutex if it doesn't exist
                InitializeFileMutex(request->name());
                dfs_log(LL_SYSINFO) << "File mutex had been granted to this client";
                return Status(StatusCode::OK, "File mutex had been granted to this client");
            }
        }
        else
        {
            // didn't find client id in metadata
            dfs_log(LL_ERROR) << "No client ID found in metadata";
            return Status(StatusCode::CANCELLED, "No client ID found in metadata.");
        }
        return Status(StatusCode::OK, "File mutex had been granted to this client");
    }

    Status Delete(ServerContext *context, const File *requested_file, Empty *response) override
    {
        if (context->IsCancelled())
        {
            ReleaseClientMutex(requested_file->name());
            dfs_log(LL_ERROR) << "Deadline exceeded or Client cancelled, abandoning.";
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or Client cancelled, abandoning.");
        }

        const std::string &file_path = WrapPath(requested_file->name());
        const multimap<string_ref, string_ref> &metadata = context->client_metadata();
        string client_id = GetClientID(metadata);
        if (client_id.compare("NOT_FOUND") == 0)
        {
            ReleaseClientMutex(requested_file->name());
            dfs_log(LL_ERROR) << "No client ID found in metadata";
            return Status(StatusCode::CANCELLED, "No client ID found in metadata");
        }

        // check to see if this client has the write lock
        file_client_map_lock.lock();
        auto locked_client_iter = file_client_map.find(requested_file->name());
        // // if didn't find this filename in this map, meaning the client hasn't requested the file lock yet
        // if (locked_client_iter == file_client_map.end())
        // {
        //     file_client_map_lock.unlock();
        //     stringstream ss;
        //     ss << "Your client id " << clientId << " doesn't have a write lock for file " << request->name();
        //     dfs_log(LL_ERROR) << ss.str();
        //     return Status(StatusCode::CANCELLED, "client hasn't requested the file lock");
        // }

        string locked_client = locked_client_iter->second;
        if (locked_client.compare(client_id) != 0)
        {
            file_client_map_lock.unlock();
            dfs_log(LL_ERROR) << "another client have the file lock: " << locked_client;
            return Status(StatusCode::RESOURCE_EXHAUSTED, "another client have the file lock");
        }
        file_client_map_lock.unlock();

        // until here, client owns the file lock!
        directory_lock.lock();
        shared_timed_mutex *file_lock = GetFileMutex(requested_file->name());
        file_lock->lock();

        ifstream ifile;
        ifile.open(file_path);

        // https://www.codegrepper.com/code-examples/cpp/c%2B%2B+check+if+file+exists
        if (!(ifile))
        {
            directory_lock.unlock();
            file_lock->unlock();
            ReleaseClientMutex(requested_file->name());
            dfs_log(LL_DEBUG) << "File not found. ";
            return Status(StatusCode::NOT_FOUND, "File not found.");
        }
        ifile.close();
        // https://www.cplusplus.com/reference/cstdio/remove/
        if (remove(file_path.c_str()) != 0)
        {
            directory_lock.unlock();
            file_lock->unlock();
            ReleaseClientMutex(requested_file->name());
            return Status(StatusCode::CANCELLED, "File failed to delete.");
        }
        else
        {
            directory_lock.unlock();
            file_lock->unlock();
            ReleaseClientMutex(requested_file->name());
            dfs_log(LL_DEBUG) << "File DELETED! ";
            return Status(StatusCode::OK, "File deleted");
        }
    }

    Status Stat(ServerContext *context, const File *requested_file, FileStat *response) override
    {
        // if client cancelled the request, time limit exceeded
        if (context->IsCancelled())
        {
            return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or Client cancelled, abandoning.");
        }

        const std::string &file_path = WrapPath(requested_file->name());
        ifstream ifile;

        // get the file specific mutex; if not existed yet, it will initialize it
        shared_timed_mutex *file_mutex = GetFileMutex(requested_file->name());

        file_mutex->lock_shared();
        ifile.open(file_path);
        // if failed to open, no file found
        if (!(ifile))
        {
            file_mutex->unlock_shared();
            return Status(StatusCode::NOT_FOUND, "File not found.");
        }
        // close the file
        ifile.close();

        // try get the file status
        struct stat buf;
        if (stat(file_path.c_str(), &buf) != 0)
        {
            file_mutex->unlock_shared();
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
            response->set_name(file_path);
            file_mutex->unlock_shared();
            return Status(StatusCode::OK, "File Status Retrieved.");
        }
    }

    Status List(ServerContext *context, const Empty *requested_file, FileList *response) override
    {
        // https://stackoverflow.com/questions/612097/how-can-i-get-the-list-of-files-in-a-directory-using-c-or-c
        directory_lock.lock_shared();
        DIR *dir;
        struct dirent *ent;
        if ((dir = opendir(mount_path.c_str())) != NULL)
        {
            /* print all the files and directories within directory */
            while ((ent = readdir(dir)) != NULL)
            {
                // if client cancelled the request, time limit exceeded
                // if (context->IsCancelled())
                // {
                //     directory_lock.unlock_shared();
                //     return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or Client cancelled, abandoning.");
                // }

                string dirEntry(ent->d_name);          // file name
                string file_path = WrapPath(dirEntry); // complete file path

                struct stat buf; // try get the file status
                if (stat(file_path.c_str(), &buf) != 0)
                {
                    directory_lock.unlock_shared();
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
                    file_stat->set_name(ent->d_name);
                }
            }
            directory_lock.unlock_shared();
            closedir(dir);
            return Status::OK;
        }
        else
        {
            directory_lock.unlock_shared();
            /* could not open directory */
            dfs_log(LL_ERROR) << "Failed to open " << mount_path;
            return Status(StatusCode::CANCELLED, "Dir failed to open.");
        }
    }

    Status Fetch(ServerContext *context, const File *requested_file, ServerWriter<FileChunk> *writer) override
    {
        dfs_log(LL_SYSINFO) << "trying to get mutex for file: " << requested_file->name();
        const std::string &file_path = WrapPath(requested_file->name());
        shared_timed_mutex *file_lock = GetFileMutex(requested_file->name());
        dfs_log(LL_SYSINFO) << "Got mutex for file: " << requested_file->name();
        // try get the file status
        struct stat buf;
        if (stat(file_path.c_str(), &buf) != 0)
        {
            dfs_log(LL_ERROR) << "Fetch response message: "
                              << "File failed to obtain status";
            return Status(StatusCode::NOT_FOUND, "File not found.");
        }

        // if file exist on server
        ifstream ifile;
        const multimap<string_ref, string_ref> &metadata = context->client_metadata();
        auto client_checksum = metadata.find(Metadata_CheckSum);
        unsigned long client_checksum_number = stoul(string(client_checksum->second.begin(), client_checksum->second.end()));
        unsigned long server_checksum_number = dfs_file_checksum(file_path, &(this->crc_table));
        dfs_log(LL_SYSINFO) << "Got client, server checksum values";
        if (client_checksum_number == server_checksum_number)
        {
            auto mtime_iter = metadata.find(Metadata_Mtime);
            long mtime = stol(string(mtime_iter->second.begin(), mtime_iter->second.end()));
            if (buf.st_mtime < mtime)
            {
                // if server mtime is earlier than client
                // update server mtime
                file_lock->lock();
                struct utimbuf new_times;
                new_times.modtime = mtime;
                utime(file_path.c_str(), &new_times);
                file_lock->unlock();
            }
            return Status(StatusCode::ALREADY_EXISTS, "File check_sum same");
        }

        dfs_log(LL_SYSINFO) << "Trying to lock the file lock";
        // until here, client owns the file lock!
        file_lock->lock_shared();
        dfs_log(LL_SYSINFO) << "File lock locked!";

        // if successful, start reading and sending file to client
        int file_size = buf.st_size;
        dfs_log(LL_SYSINFO) << "File size is " << file_size << " bytes.";
        int FILE_CHUNK_SIZE = 4096;
        ifile.open(file_path);
        FileChunk file_chunk;
        file_chunk.set_name(requested_file->name());
        int bytes_sent = 0;

        while (!ifile.eof() && bytes_sent < file_size)
        {
            if (context->IsCancelled())
            {
                dfs_log(LL_SYSINFO) << "Deadline exceeded or Client cancelled, abandoning.";
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
        file_lock->unlock_shared();
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
        const multimap<string_ref, string_ref> &metadata = context->client_metadata();
        auto client_id_iter = metadata.find(Metadata_ClientId);
        auto mtime_iter = metadata.find(Metadata_Mtime);
        auto client_checksum_iter = metadata.find(Metadata_CheckSum);
        auto filename_iter = metadata.find(Metadata_FileName);
        string filename = string(filename_iter->second.begin(), filename_iter->second.end());
        const std::string &file_path = WrapPath(filename);
        string clientId = string(client_id_iter->second.begin(), client_id_iter->second.end());
        long mtime = stol(string(mtime_iter->second.begin(), mtime_iter->second.end()));
        unsigned long client_checksum = stoul(string(client_checksum_iter->second.begin(), client_checksum_iter->second.end()));
        shared_timed_mutex *file_lock = GetFileMutex(filename);

        dfs_log(LL_SYSINFO) << "Extracted Date from metadata";
        try
        {
            // try get the file status
            struct stat buf;
            if (stat(file_path.c_str(), &buf) == 0) // if file exist in server
            {
                dfs_log(LL_SYSINFO) << "File Already Existed";
                directory_lock.lock();
                file_lock->lock();
                unsigned long server_checksum = dfs_file_checksum(file_path, &(this->crc_table));
                file_lock->unlock();
                directory_lock.unlock();
                if (client_checksum == server_checksum)
                {
                    dfs_log(LL_SYSINFO) << "File_checksum are the same" << client_checksum;
                    if (buf.st_mtime < mtime)
                    {
                        // if server mtime is earlier than client
                        // update server mtime
                        directory_lock.lock();
                        file_lock->lock();
                        struct utimbuf new_times;
                        new_times.modtime = mtime;
                        utime(file_path.c_str(), &new_times);
                        file_lock->unlock();
                        directory_lock.unlock();
                        dfs_log(LL_SYSINFO) << "Updating modified time";
                    }
                    // file_lock->unlock();
                    // directory_lock.unlock();
                    ReleaseClientMutex(filename);
                    return Status(StatusCode::ALREADY_EXISTS, "File check_sum same");
                }
                ofstream ofs;
                ofs.open(file_path, std::ofstream::out | ios::trunc);
                ofs.close();
            }

            // read the buffers and store in local path
            ofstream ofs;
            FileChunk buffers;
            file_lock->lock();
            directory_lock.lock();
            while (reader->Read(&buffers))
            {
                // if (context->IsCancelled())
                // {
                //     ReleaseClientMutex(filename);
                //     file_lock->unlock();
                //     return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline exceeded or Client cancelled, abandoning.");
                // }
                const std::string &file_path = WrapPath(buffers.name());
                ofs.open(file_path, std::ofstream::out | std::ofstream::app);
                const string &str = buffers.buffer();
                ofs << buffers.buffer();
                dfs_log(LL_SYSINFO) << "Writing chunk of size " << str.length() << " bytes";
                ofs.close();
            }
            file_lock->unlock();
            directory_lock.unlock();
            ReleaseClientMutex(filename);
            return Status::OK;
        }
        catch (exception const &ex)
        {
            file_lock->unlock();
            directory_lock.unlock();
            ReleaseClientMutex(filename);
            return Status(StatusCode::CANCELLED, "Catched Exception");
        }
    }

    Status CallbackList(ServerContext *context, const File *requested_file, FileList *response) override
    {
        Empty req;
        return this->List(context, &req, response);
    }
    /**
     * Request callback for asynchronous requests
     *
     * This method is called by the DFSCallData class during
     * an asynchronous request call from the client.
     *
     * Students should not need to adjust this.
     *
     * @param context
     * @param request
     * @param response
     * @param cq
     * @param tag
     */
    void RequestCallback(grpc::ServerContext *context,
                         FileRequestType *request,
                         grpc::ServerAsyncResponseWriter<FileListResponseType> *response,
                         grpc::ServerCompletionQueue *cq,
                         void *tag)
    {

        std::lock_guard<std::mutex> lock(queue_mutex);
        this->queued_tags.emplace_back(context, request, response, cq, tag);
    }

    /**
     * Process a callback request
     *
     * This method is called by the DFSCallData class when
     * a requested callback can be processed. You should use this method
     * to manage the CallbackList RPC call and respond as needed.
     *
     * See the STUDENT INSTRUCTION for more details.
     *
     * @param context
     * @param request
     * @param response
     */
    void ProcessCallback(ServerContext *context, FileRequestType *request, FileListResponseType *response)
    {

        //
        // STUDENT INSTRUCTION:
        //
        // You should add your code here to respond to any CallbackList requests from a client.
        // This function is called each time an asynchronous request is made from the client.
        //
        // The client should receive a list of files or modifications that represent the changes this service
        // is aware of. The client will then need to make the appropriate calls based on those changes.
        //
        Status status = this->CallbackList(context, request, response);
    }

    /**
     * Processes the queued requests in the queue thread
     */
    void ProcessQueuedRequests()
    {
        while (true)
        {

            //
            // STUDENT INSTRUCTION:
            //
            // You should add any synchronization mechanisms you may need here in
            // addition to the queue management. For example, modified files checks.
            //
            // Note: you will need to leave the basic queue structure as-is, but you
            // may add any additional code you feel is necessary.
            //

            // Guarded section for queue
            {
                dfs_log(LL_DEBUG2) << "Waiting for queue guard";
                std::lock_guard<std::mutex> lock(queue_mutex);

                for (QueueRequest<FileRequestType, FileListResponseType> &queue_request : this->queued_tags)
                {
                    this->RequestCallbackList(queue_request.context, queue_request.request,
                                              queue_request.response, queue_request.cq, queue_request.cq, queue_request.tag);
                    queue_request.finished = true;
                }

                // any finished tags first
                this->queued_tags.erase(std::remove_if(
                                            this->queued_tags.begin(),
                                            this->queued_tags.end(),
                                            [](QueueRequest<FileRequestType, FileListResponseType> &queue_request)
                                            { return queue_request.finished; }),
                                        this->queued_tags.end());
            }
        }
    }

    //
    // STUDENT INSTRUCTION:
    //
    // Add your additional code here, including
    // the implementations of your rpc protocol methods.
    //
};

//
// STUDENT INSTRUCTION:
//
// The following three methods are part of the basic DFSServerNode
// structure. You may add additional methods or change these slightly
// to add additional startup/shutdown routines inside, but be aware that
// the basic structure should stay the same as the testing environment
// will be expected this structure.
//
/**
 * The main server node constructor
 *
 * @param mount_path
 */
DFSServerNode::DFSServerNode(const std::string &server_address,
                             const std::string &mount_path,
                             int num_async_threads,
                             std::function<void()> callback) : server_address(server_address),
                                                               mount_path(mount_path),
                                                               num_async_threads(num_async_threads),
                                                               grader_callback(callback) {}
/**
 * Server shutdown
 */
DFSServerNode::~DFSServerNode() noexcept
{
    dfs_log(LL_SYSINFO) << "DFSServerNode shutting down";
}

/**
 * Start the DFSServerNode server
 */
void DFSServerNode::Start()
{
    DFSServiceImpl service(this->mount_path, this->server_address, this->num_async_threads);

    dfs_log(LL_SYSINFO) << "DFSServerNode server listening on " << this->server_address;
    service.Run();
}

//
// STUDENT INSTRUCTION:
//
// Add your additional definitions here
//
