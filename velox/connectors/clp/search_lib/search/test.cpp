#include "Cursor.hpp"

int main() {
    clp_s::search::Cursor cursor("mongod-archive", std::nullopt, false);
    std::string query = "id: 22412";
    std::vector<std::string> output_columns;
    clp_s::search::ErrorCode error_code = cursor.execute_query(query, {"attr.snapshotId", "c", "id"});
    if (error_code != clp_s::search::ErrorCode::Success) {
        return 1;
    }

    std::vector<int64_t> s;
    s.resize(10000);
    std::vector<std::string> c;
    c.resize(10000);
    std::vector<int64_t> id;
    id.resize(10000);
    std::vector<std::variant<std::vector<int64_t>,
                 std::vector<bool>,
                 std::vector<std::string>,
                 std::vector<double>>> output;
    output.push_back(s);
    output.push_back(c);
    output.push_back(id);

    while (true) {
        auto i = cursor.fetch_next(10000, output);
        if (i == 0) {
            break;
        }
        std::cout << "Fetched " << i << " rows" << std::endl;
    }

    std::cout << "Success" << std::endl;

    query = "id: 22419";
    cursor.execute_query(query, {"s", "c", "id"});

    while (true) {
        auto i = cursor.fetch_next(10000, output);
        if (i == 0) {
            break;
        }
        std::cout << "Fetched " << i << " rows" << std::endl;
    }

    return 0;
}
