#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>

#include "velox/connectors/clp/ClpColumnHandle.h"
#include "velox/connectors/clp/ClpConnectorSplit.h"
#include "velox/connectors/clp/ClpDataSource.h"
#include "velox/connectors/clp/ClpTableHandle.h"
#include "velox/vector/FlatVector.h"

#include "search_lib/search/Cursor.hpp"

namespace facebook::velox::connector::clp {

ClpDataSource::ClpDataSource(
    const RowTypePtr& outputType,
    const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<connector::ColumnHandle>>& columnHandles,
    velox::memory::MemoryPool* pool,
    std::shared_ptr<const ClpConfig>& clpConfig)
    : pool_(pool), outputType_(outputType) {
  executablePath_ = clpConfig->executablePath();
  VELOX_CHECK(!executablePath_.empty(), "Executable path must be set");
  polymorphicTypeEnabled_ = clpConfig->polymorphicTypeEnabled();
  auto archiveRootDir = clpConfig->archiveDir();
  VELOX_CHECK(!archiveRootDir.empty(), "Archive directory must be set");
  auto clpTableHandle = std::dynamic_pointer_cast<ClpTableHandle>(tableHandle);
  auto archiveDir =
      boost::filesystem::path(archiveRootDir) / clpTableHandle->tableName();
  archiveDir_ = archiveDir.string();
  auto query = clpTableHandle->query();
  if (query && !query->empty()) {
    kqlQuery_ = *query;
  } else {
    kqlQuery_ = "*";
  }

  auto outputNames = outputType->names();
  for (size_t i = 0; i < outputNames.size(); ++i) {
    auto columnHandle = columnHandles.find(outputNames[i]);
    VELOX_CHECK(
        columnHandle != columnHandles.end(),
        "ColumnHandle not found for output name: {}",
        outputNames[i]);
    auto clpColumnHandle =
        std::dynamic_pointer_cast<ClpColumnHandle>(columnHandle->second);
    VELOX_CHECK_NOT_NULL(
        clpColumnHandle,
        "ColumnHandle must be an instance of ClpColumnHandle for output name: {}",
        outputNames[i]);
    auto columnName = clpColumnHandle->columnName();
    columnIndices_[columnName] = i;
    if (polymorphicTypeEnabled_) {
      static const std::vector<std::string> suffixes = {
          "_varchar", "_double", "_bigint", "_boolean"};
      bool suffixFound = false;

      for (const auto& suffix : suffixes) {
        if (boost::algorithm::ends_with(columnName, suffix)) {
          // Strip the type suffix
          columnUntypedNames_.insert(
              columnName.substr(0, columnName.size() - suffix.size()));
          suffixFound = true;
          break;
        }
      }

      if (!suffixFound) {
        columnUntypedNames_.insert(columnName);
      }
    } else {
      columnUntypedNames_.insert(columnName);
    }
  }
}

ClpDataSource::~ClpDataSource() {
//  stopThread_ = true;
//  if (t_.joinable()) {
//    t_.join();
//  }
  if (pid_ > 0) {
    // Send SIGTERM to the process to terminate it gracefully
    kill(pid_, SIGTERM);
    // Wait for the process to terminate
    int status;
    if (waitpid(pid_, &status, 0) == -1) {
      out_file_ << std::this_thread::get_id()
                << " Error waiting for process termination: " << strerror(errno)
                << std::endl;
    } else {
      out_file_ << std::this_thread::get_id() << "Destructor Process " << pid_
                << " terminated with status: " << status << std::endl;
    }

    // Reset the PID to indicate that no process is running
    pid_ = -1;
  }
  out_file_.close();
}

void ClpDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  std::lock_guard<std::mutex> lock(mutex_);
  out_file_ << std::this_thread::get_id() << " ClpDataSource addSplit"
            << std::endl;
  auto clpSplit = std::dynamic_pointer_cast<ClpConnectorSplit>(split);
  auto tableName = clpSplit->tableName();
  auto archiveId = clpSplit->archiveId();
  VELOX_CHECK(!tableName.empty(), "Table name must be set");
  std::vector<std::string> commands = {
      "s", archiveDir_, "--archive-id", archiveId, kqlQuery_};
  if (!columnUntypedNames_.empty()) {
    commands.emplace_back("--projection");
    commands.insert(
        commands.end(), columnUntypedNames_.begin(), columnUntypedNames_.end());
  }
  arrayOffsets_.clear();
  line_buffer_.clear();
  if (pid_ > 0) {
    // Send SIGTERM to the process to terminate it gracefully
    kill(pid_, SIGTERM);
    // Wait for the process to terminate
    int status;
    if (waitpid(pid_, &status, 0) == -1) {
      out_file_ << std::this_thread::get_id()
                << " Error waiting for process termination: " << strerror(errno)
                << std::endl;
    } else {
      out_file_ << std::this_thread::get_id()
                << " Process terminated with status: " << status << std::endl;
    }

    // Reset the PID to indicate that no process is running
    pid_ = -1;
  }

  // Create a pipe for communication
  if (pipe(pipeFd_) == -1) {
    out_file_ << std::this_thread::get_id()
              << " Pipe failed: " << strerror(errno) << std::endl;
    return;
  }

  // Fork the process
  pid_ = fork();
  if (pid_ < 0) {
    out_file_ << std::this_thread::get_id()
              << " Fork failed: " << strerror(errno) << std::endl;
    return;
  }

  if (pid_ == 0) { // Child process
    // Redirect stdout to the write end of the pipe
    close(pipeFd_[0]); // Close the read end
    dup2(pipeFd_[1], STDOUT_FILENO); // Redirect stdout to pipe
    close(pipeFd_[1]); // Close the original write end after dup

    // Convert args to char* array for execvp
    std::vector<char*> exec_args;
    exec_args.push_back(const_cast<char*>(executablePath_.c_str()));
    for (const auto& arg : commands) {
      exec_args.push_back(const_cast<char*>(arg.c_str()));
    }
    exec_args.push_back(nullptr); // execvp expects a null-terminated array

    // Replace the child process with the new executable
    execvp(executablePath_.c_str(), exec_args.data());

    // If execvp fails
    out_file_ << std::this_thread::get_id()
              << " execvp failed: " << strerror(errno) << std::endl;
    _exit(1);
  } else { // Parent process
    out_file_ << std::this_thread::get_id()
              << " ClpDataSource addSplit pid:" << pid_ << std::endl;
    // Close the write end of the pipe
    close(pipeFd_[1]);
//    t_ = std::thread([this]() {
//      size_t count = 0;
//      while (!stopThread_ && count < 30) {
//        std::this_thread::sleep_for((std::chrono::seconds(1)));
//        count++;
//      }
//      close(pipeFd_[0]);
//    });
    auto flags = fcntl(pipeFd_[0], F_GETFL, 0);
    fcntl(pipeFd_[0], F_SETFL, flags | O_NONBLOCK);
  }
}

std::optional<RowVectorPtr> ClpDataSource::next(
    uint64_t size,
    ContinueFuture& future) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (pid_ <= 0) {
    out_file_ << std::this_thread::get_id() << " ClpDataSource next pid_ <= 0"
              << std::endl;
    return nullptr;
  }
  out_file_ << std::this_thread::get_id() << " ClpDataSource next: " << size
            << ", pid: " << pid_ << std::endl;

  //  std::lock_guard<std::mutex> lock(stream_mutex_);
  std::vector<VectorPtr> vectors;
  vectors.reserve(outputType_->size());
  auto nulls = AlignedBuffer::allocate<bool>(size, pool_, bits::kNull);
  for (const auto& childType : outputType_->children()) {
    // Create a vector with NULL values
    auto vector = BaseVector::create(childType, size, pool_);
    vector->setNulls(nulls);
    vectors.emplace_back(vector);
  }

  uint64_t localCompletedRows = 0;

  char buffer[4096];
  ssize_t count;
  size_t completedRead = 0;
  for (uint64_t i = 0; i < size; ++i) {
    size_t pos = std::string::npos;
    while (pid_ > 0 && fcntl(pipeFd_[0], F_GETFD) != -1 && (pos = line_buffer_.find('\n')) == std::string::npos) {
      if ((count = read(pipeFd_[0], buffer, sizeof(buffer))) > 0) {
        line_buffer_.append(buffer, count);
        completedRead += 1;
      } else if (count == -1 && (errno == EINTR || errno == EAGAIN)) {
        continue;
      } else {
        break;
      }
    }

    if (pos == std::string::npos) {
      break;
    }

    std::string line = line_buffer_.substr(0, pos + 1);
    localCompletedRows++;
    completedBytes_ += line.size();
    if (0 == outputType_->size()) {
      continue;
    }

    // Parse the line and return the RowVectorPtr
    simdjson::ondemand::parser parser;
    try {
      auto doc = parser.iterate(line);
      std::string path;
      parseJsonLine(doc, path, vectors, i);
      line_buffer_.erase(0, pos + 1);
    } catch (simdjson::simdjson_error& e) {
      out_file_ << i << " Error parsing JSON:" << e.what() << std::endl;
      out_file_ << line << std::endl;
      out_file_ << "line buffer: " << line_buffer_ << std::endl;
      out_file_ << "buffer: " << buffer << std::endl;
      exit(1);
    }
  }

  out_file_ << std::this_thread::get_id() << " ClpDataSource next: " << size
            << ", pid: " << pid_ << " Completed rows: " << localCompletedRows
            << " Completed read: " << completedRead << " fd: " << pipeFd_[0]
            << std::endl;
  if (localCompletedRows == 0) {
    return nullptr;
  }
  completedRows_ += localCompletedRows;
  return std::make_shared<RowVector>(
      pool_, outputType_, BufferPtr(), localCompletedRows, std::move(vectors));
}

void ClpDataSource::parseJsonLine(
    simdjson::ondemand::value element,
    std::string& path,
    std::vector<VectorPtr>& vectors,
    uint64_t index) {
  // Parse the json element and populate the vectors
  switch (element.type()) {
    case simdjson::ondemand::json_type::object:
      for (auto field : element.get_object()) {
        std::string_view key = field.unescaped_key();
        std::string newPath =
            path.empty() ? std::string(key) : path + "." + std::string(key);
        parseJsonLine(field.value(), newPath, vectors, index);
      }
      break;
    case simdjson::ondemand::json_type::string: {
      setValue(
          vectors,
          path,
          index,
          StringView(element.get_string().value()),
          "varchar");
      break;
    }
    case simdjson::ondemand::json_type::number: {
      simdjson::ondemand::number elementNumber = element.get_number();
      if (elementNumber.is_double()) {
        setValue(vectors, path, index, elementNumber.get_double(), "double");
      } else {
        setValue(vectors, path, index, elementNumber.get_int64(), "bigint");
      }
      break;
    }
    case simdjson::ondemand::json_type::boolean: {
      setValue(vectors, path, index, element.get_bool().value(), "boolean");
      break;
    }
    case simdjson::ondemand::json_type::array: {
      std::shared_ptr<ArrayVector> arrayVector;
      if (auto iter = columnIndices_.find(path); iter != columnIndices_.end()) {
        arrayVector =
            std::dynamic_pointer_cast<ArrayVector>(vectors[iter->second]);
      } else if (polymorphicTypeEnabled_) {
        auto typedPath = path + "_varchar";
        if (iter = columnIndices_.find(typedPath);
            iter != columnIndices_.end()) {
          arrayVector =
              std::dynamic_pointer_cast<ArrayVector>(vectors[iter->second]);
        }
      } else {
        break;
      }
      if (arrayOffsets_.find(path) == arrayOffsets_.end()) {
        arrayOffsets_[path] = 0;
      }
      auto arrayBeginOffset = arrayOffsets_[path];
      auto arrayEndOffset = arrayBeginOffset;
      auto elements = arrayVector->elements()->asFlatVector<StringView>();
      std::vector<std::string_view> arrayElements;
      for (auto arrayElement : element.get_array()) {
        // Get each array element as a string
        auto elementStringWithQuotes =
            simdjson::to_json_string(arrayElement).value();
        auto elementString = elementStringWithQuotes.substr(
            1, elementStringWithQuotes.size() - 2);
        arrayElements.emplace_back(elementString);
      }
      elements->resize(arrayEndOffset + arrayElements.size());

      for (auto& arrayElement : arrayElements) {
        // Set the element in the array vector
        elements->set(arrayEndOffset++, StringView(arrayElement));
      }
      arrayOffsets_[path] = arrayEndOffset;
      arrayVector->setOffsetAndSize(
          index, arrayBeginOffset, arrayEndOffset - arrayBeginOffset);
      arrayVector->setNull(index, false);
      break;
    }
    case simdjson::ondemand::json_type::null:
      break;
  }
}
} // namespace facebook::velox::connector::clp
