// MPI traffic simulator: C++17.
// Each MPI rank reads a separate section of the shared input file.
// Producers and consumers use a blocking bounded queue.
// Only the main thread calls MPI.

#include <mpi.h>
#include <algorithm>
#include <atomic>
#include <climits>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

using Count = unsigned long long;
using Key = std::pair<std::string, std::string>;
using Totals = std::map<Key, Count>;
using Batch = std::vector<std::string>;

// 1. Thread-safe bounded buffer.
// Capacity is measured in records.
class Buffer {
    std::deque<Batch> queue;
    size_t capacity;
    size_t records = 0;

    bool closed = false;
    bool cancelled = false;

    std::mutex mutex;
    std::condition_variable notEmpty;
    std::condition_variable notFull;

public:
    explicit Buffer(size_t value) : capacity(value) {}

    bool push(Batch batch) {
        std::unique_lock<std::mutex> lock(mutex);

        notFull.wait(lock, [&] {
            return cancelled ||
                   records + batch.size() <= capacity;
        });

        if (cancelled) return false;

        records += batch.size();
        queue.push_back(std::move(batch));

        lock.unlock();
        notEmpty.notify_one();
        return true;
    }

    bool pop(Batch& batch) {
        std::unique_lock<std::mutex> lock(mutex);

        notEmpty.wait(lock, [&] {
            return cancelled || closed || !queue.empty();
        });

        if (cancelled || queue.empty()) return false;

        batch = std::move(queue.front());
        queue.pop_front();
        records -= batch.size();

        lock.unlock();
        notFull.notify_all();
        return true;
    }

    void close() {
        {
            std::lock_guard<std::mutex> lock(mutex);
            closed = true;
        }
        notEmpty.notify_all();
    }

    void cancel() {
        {
            std::lock_guard<std::mutex> lock(mutex);
            cancelled = true;
        }
        notEmpty.notify_all();
        notFull.notify_all();
    }
};

// 2. Validate program settings.
size_t positive(const char* text) {
    std::string value(text);

    if (value.empty() ||
        value.find_first_not_of("0123456789") !=
            std::string::npos) {
        throw std::runtime_error(
            "Use positive integer settings");
    }

    Count number = std::stoull(value);

    if (number == 0 ||
        number > std::numeric_limits<size_t>::max()) {
        throw std::runtime_error("Setting is out of range");
    }

    return static_cast<size_t>(number);
}

// Add car counts and check for overflow.
void add(Totals& totals, const Key& key, Count cars) {
    Count& value = totals[key];

    if (cars > std::numeric_limits<Count>::max() - value) {
        throw std::runtime_error("Car total overflow");
    }

    value += cars;
}

// 3. Map each input record to (hour, traffic light) -> cars.
// Expected format: YYYY-MM-DD HH:MM:SS,TLid,cars
void mapRecord(const std::string& line, Totals& totals) {
    size_t a = line.find(',');
    size_t b = a == std::string::npos
        ? std::string::npos
        : line.find(',', a + 1);

    if (a != 19 ||
        b == std::string::npos ||
        b == a + 1 ||
        line.find(',', b + 1) != std::string::npos) {
        throw std::runtime_error(
            "Invalid CSV record: " + line);
    }

    std::string stamp = line.substr(0, a);

    if (stamp[4] != '-' ||
        stamp[7] != '-' ||
        stamp[10] != ' ' ||
        stamp[13] != ':' ||
        stamp[16] != ':') {
        throw std::runtime_error("Invalid timestamp");
    }

    for (size_t i = 0; i < stamp.size(); ++i) {
        if (i == 4 || i == 7 || i == 10 ||
            i == 13 || i == 16) {
            continue;
        }

        if (stamp[i] < '0' || stamp[i] > '9') {
            throw std::runtime_error("Invalid timestamp");
        }
    }

    if (std::stoi(stamp.substr(11, 2)) > 23 ||
        std::stoi(stamp.substr(14, 2)) > 59 ||
        std::stoi(stamp.substr(17, 2)) > 59) {
        throw std::runtime_error("Invalid time");
    }

    std::string count = line.substr(b + 1);

    if (count.empty() ||
        count.find_first_not_of("0123456789") !=
            std::string::npos) {
        throw std::runtime_error("Invalid car count");
    }

    Key key{
        stamp.substr(0, 13) + ":00",
        line.substr(a + 1, b - a - 1)
    };

    add(totals, key, std::stoull(count));
}

// 4. Assign each key to a reducer rank.
// The same key always belongs to the same rank.
int owner(const Key& key, int ranks) {
    uint64_t hash = 14695981039346656037ULL;

    for (unsigned char ch : key.first + "|" + key.second) {
        hash ^= ch;
        hash *= 1099511628211ULL;
    }

    return static_cast<int>(
        hash % static_cast<uint64_t>(ranks));
}

// Portable length-prefixed binary encoding. No raw structs or padding.
// Every integer uses big-endian bytes; arbitrary light IDs are retained.
void put64(std::string& out, uint64_t v) {
    for (int i=7;i>=0;--i) out.push_back(static_cast<char>((v>>(i*8))&255));
}
uint64_t get64(const std::string& in, size_t& pos) {
    if (in.size()-pos<8) throw std::runtime_error("Truncated binary integer");
    uint64_t v=0;
    for(int i=0;i<8;++i) v=(v<<8)|static_cast<unsigned char>(in[pos++]);
    return v;
}
void putString(std::string& out,const std::string& value) {
    put64(out,value.size()); out+=value;
}
std::string getString(const std::string& in,size_t& pos) {
    uint64_t n=get64(in,pos);
    if(n>in.size()-pos) throw std::runtime_error("Truncated binary string");
    auto value=in.substr(pos,static_cast<size_t>(n)); pos+=static_cast<size_t>(n); return value;
}
std::string pack(const Totals& totals) {
    std::string out;
    for(const auto& e:totals) {
        putString(out,e.first.first); putString(out,e.first.second); put64(out,e.second);
    }
    return out;
}
Totals unpack(const std::string& data) {
    Totals totals; size_t pos=0;
    while(pos<data.size()) {
        auto hour=getString(data,pos); auto light=getString(data,pos);
        auto cars=get64(data,pos); add(totals,{hour,light},cars);
    }
    return totals;
}
// Safe only AFTER ownership reduction: every candidate has a complete total.
// Any excluded light already has N better lights on its owner in that hour.
Totals selectCandidates(const Totals& complete,size_t top) {
    using Entry=std::pair<std::string,Count>;
    std::map<std::string,std::vector<Entry>> hours;
    for(const auto& e:complete) hours[e.first.first].push_back({e.first.second,e.second});
    Totals candidates;
    auto better=[](const Entry& a,const Entry& b){return a.second!=b.second?a.second>b.second:a.first<b.first;};
    for(auto& h:hours) {
        size_t n=std::min(top,h.second.size());
        std::partial_sort(h.second.begin(),h.second.begin()+n,h.second.end(),better);
        for(size_t i=0;i<n;++i) add(candidates,{h.first,h.second[i].first},h.second[i].second);
    }
    return candidates;
}

// 5. Blocking MPI exchange with variable message sizes.
// Unique name avoids conflict with std::exchange.
std::string mpiExchange(
    const std::vector<std::string>& messages,
    int ranks) {

    std::vector<int> sendCounts(ranks);
    std::vector<int> receiveCounts(ranks);
    std::vector<int> sendOffsets(ranks);
    std::vector<int> receiveOffsets(ranks);

    long long sendTotal = 0;
    long long receiveTotal = 0;

    for (int i = 0; i < ranks; ++i) {
        if (messages[i].size() >
            static_cast<size_t>(INT_MAX)) {
            throw std::runtime_error(
                "MPI message exceeds INT_MAX");
        }

        long long length =
            static_cast<long long>(messages[i].size());

        if (sendTotal + length > INT_MAX) {
            throw std::runtime_error(
                "MPI send buffer exceeds INT_MAX");
        }

        sendOffsets[i] = static_cast<int>(sendTotal);
        sendCounts[i] = static_cast<int>(length);
        sendTotal += length;
    }

    MPI_Alltoall(
        sendCounts.data(), 1, MPI_INT,
        receiveCounts.data(), 1, MPI_INT,
        MPI_COMM_WORLD);

    for (int i = 0; i < ranks; ++i) {
        if (receiveTotal + receiveCounts[i] > INT_MAX) {
            throw std::runtime_error(
                "MPI receive buffer exceeds INT_MAX");
        }

        receiveOffsets[i] =
            static_cast<int>(receiveTotal);

        receiveTotal += receiveCounts[i];
    }

    std::string sendData;
    sendData.reserve(static_cast<size_t>(sendTotal));

    for (const auto& message : messages) {
        sendData += message;
    }

    // Keep a valid pointer even when no bytes are received.
    std::vector<char> received(
        static_cast<size_t>(
            std::max<long long>(1, receiveTotal)));

    char dummy = 0;

    MPI_Alltoallv(
        sendData.empty() ? &dummy : sendData.data(),
        sendCounts.data(),
        sendOffsets.data(),
        MPI_CHAR,
        received.data(),
        receiveCounts.data(),
        receiveOffsets.data(),
        MPI_CHAR,
        MPI_COMM_WORLD);

    return std::string(
        received.data(),
        static_cast<size_t>(receiveTotal));
}

int main(int argc, char** argv) {
    int provided = 0;

    MPI_Init_thread(
        &argc, &argv,
        MPI_THREAD_FUNNELED,
        &provided);

    int rank = 0;
    int ranks = 0;

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &ranks);

    try {
        if (provided < MPI_THREAD_FUNNELED) {
            throw std::runtime_error(
                "MPI thread support is insufficient");
        }

        if (argc != 6 && argc != 7) {
            throw std::runtime_error(
                "Usage: ./traffic_mpi FILE TOP_N "
                "PRODUCERS CONSUMERS BUFFER_RECORDS [--all-totals]");
        }

        bool allTotals = argc == 7;
        if(allTotals && std::string(argv[6])!="--all-totals") throw std::runtime_error("Unknown option");
        std::string filename = argv[1];
        size_t top = positive(argv[2]);
        size_t producers = positive(argv[3]);
        size_t consumers = positive(argv[4]);
        size_t capacity = positive(argv[5]);

        if (producers > 128 || consumers > 128) {
            throw std::runtime_error(
                "Use at most 128 threads per type");
        }

        MPI_Barrier(MPI_COMM_WORLD);
        double start = MPI_Wtime();

        // 6. Split the shared file into byte ranges.
        std::ifstream input(filename, std::ios::binary);

        if (!input) {
            throw std::runtime_error(
                "Cannot open input file");
        }

        std::string header;
        std::getline(input, header);

        if (!header.empty() && header.back() == '\r') {
            header.pop_back();
        }

        if (header != "timestamp,traffic_light_id,cars") {
            throw std::runtime_error(
                "CSV header is missing or incorrect");
        }

        auto position = input.tellg();

        input.clear();
        input.seekg(0, std::ios::end);

        auto endPosition = input.tellg();

        if (endPosition == std::streampos(-1)) {
            throw std::runtime_error(
                "Cannot determine input file size");
        }

        long long fileEnd =
            static_cast<long long>(endPosition);

        long long dataStart =
            position == std::streampos(-1)
            ? fileEnd
            : static_cast<long long>(position);

        long long bytes = fileEnd - dataStart;

        auto boundary = [&](int index) {
            return dataStart +
                   (bytes / ranks) * index +
                   ((bytes % ranks) * index) / ranks;
        };

        long long begin = boundary(rank);
        long long end = boundary(rank + 1);

        input.clear();

        // If a partition starts inside a line, skip the remainder.
        // The previous rank processes that entire line.
        if (begin > dataStart) {
            input.seekg(begin - 1);

            char previous = 0;
            input.get(previous);

            if (previous != '\n') {
                std::string skipped;
                std::getline(input, skipped);
            }
        } else {
            input.seekg(begin);
        }

        // 7. Local producer-consumer processing.
        Buffer buffer(capacity);

        std::mutex fileMutex;
        std::mutex errorMutex;
        std::exception_ptr error;

        std::atomic<size_t> active(producers);

        std::vector<Totals> local(consumers);
        std::vector<Count> records(consumers, 0);

        std::vector<std::thread> workers;
        std::vector<std::thread> readers;

        auto saveError = [&] {
            {
                std::lock_guard<std::mutex> lock(errorMutex);

                if (!error) {
                    error = std::current_exception();
                }
            }

            buffer.cancel();
        };

        // Cancel and join started threads if thread creation fails.
        try {
            for (size_t i = 0; i < consumers; ++i) {
                workers.emplace_back([&, i] {
                    try {
                        Batch batch;

                        while (buffer.pop(batch)) {
                            for (const auto& line : batch) {
                                mapRecord(line, local[i]);
                                ++records[i];
                            }
                        }
                    } catch (...) {
                        saveError();
                    }
                });
            }

            for (size_t i = 0; i < producers; ++i) {
                readers.emplace_back([&] {
                    try {
                        while (true) {
                            Batch batch;

                            {
                                std::lock_guard<std::mutex>
                                    lock(fileMutex);

                                size_t limit =
                                    std::min<size_t>(
                                        256, capacity);

                                for (size_t j = 0;
                                     j < limit; ++j) {

                                    auto offset = input.tellg();

                                    if (offset ==
                                        std::streampos(-1)) {
                                        break;
                                    }

                                    if (static_cast<long long>(
                                            offset) >= end) {
                                        break;
                                    }

                                    std::string line;

                                    if (!std::getline(
                                            input, line)) {
                                        break;
                                    }

                                    if (!line.empty() &&
                                        line.back() == '\r') {
                                        line.pop_back();
                                    }

                                    batch.push_back(
                                        std::move(line));
                                }
                            }

                            if (batch.empty()) break;

                            if (!buffer.push(
                                    std::move(batch))) {
                                break;
                            }
                        }
                    } catch (...) {
                        saveError();
                    }

                    if (active.fetch_sub(1) == 1) {
                        buffer.close();
                    }
                });
            }
        } catch (...) {
            buffer.cancel();

            for (auto& thread : readers) thread.join();
            for (auto& thread : workers) thread.join();

            throw;
        }

        for (auto& thread : readers) thread.join();
        for (auto& thread : workers) thread.join();

        if (error) std::rethrow_exception(error);

        if (input.bad()) {
            throw std::runtime_error("Input read failed");
        }

        // Merge private consumer totals within this rank.
        Totals mapped;
        Count localRecords = 0;

        for (size_t i = 0; i < consumers; ++i) {
            localRecords += records[i];

            for (const auto& entry : local[i]) {
                add(mapped, entry.first, entry.second);
            }
        }

        double mapEnd = MPI_Wtime();
        // 8. Shuffle each key to its reducer rank.
        std::vector<Totals> buckets(ranks);

        for (const auto& entry : mapped) {
            int destination = owner(entry.first, ranks);

            add(
                buckets[destination],
                entry.first,
                entry.second);
        }

        std::vector<std::string> messages(ranks);

        for (int i = 0; i < ranks; ++i) {
            messages[i] = pack(buckets[i]);
        }

        // Reduce: combine matching keys from all ranks.
        Count shuffleBytes=0;
        for(const auto& m:messages) shuffleBytes+=m.size();
        double packEnd=MPI_Wtime();
        std::string exchanged=mpiExchange(messages,ranks);
        double exchangeEnd=MPI_Wtime();
        Totals reduced=unpack(exchanged);
        double reduceEnd=MPI_Wtime();

        Count reducedKeys =
            static_cast<Count>(reduced.size());

        // 9. Collect completed totals at rank 0.
        messages.assign(ranks, "");
        Totals candidates=allTotals?reduced:selectCandidates(reduced,top);
        messages[0] = pack(candidates);
        Count candidateCount=candidates.size(), collectionBytes=messages[0].size();
        double candidateEnd=MPI_Wtime();

        std::string finalData =
            mpiExchange(messages, ranks);

        double collectionEnd=MPI_Wtime();
        Count totalRecords = 0;

        MPI_Reduce(
            &localRecords,
            &totalRecords,
            1,
            MPI_UNSIGNED_LONG_LONG,
            MPI_SUM,
            0,
            MPI_COMM_WORLD);

        std::vector<Count> rankRecords(ranks);
        std::vector<Count> rankKeys(ranks);

        MPI_Gather(
            &localRecords,
            1,
            MPI_UNSIGNED_LONG_LONG,
            rankRecords.data(),
            1,
            MPI_UNSIGNED_LONG_LONG,
            0,
            MPI_COMM_WORLD);

        MPI_Gather(
            &reducedKeys,
            1,
            MPI_UNSIGNED_LONG_LONG,
            rankKeys.data(),
            1,
            MPI_UNSIGNED_LONG_LONG,
            0,
            MPI_COMM_WORLD);

        // 10. Sort complete totals and select top N per hour.
        using Ranking =
            std::vector<std::pair<std::string, Count>>;

        std::map<std::string, Ranking> rankings;

        if (rank == 0) {
            Totals finalTotals = unpack(finalData);

            for (const auto& entry : finalTotals) {
                rankings[entry.first.first].push_back({
                    entry.first.second,
                    entry.second
                });
            }

            for (auto& hour : rankings) {
                std::sort(
                    hour.second.begin(),
                    hour.second.end(),
                    [](const auto& a, const auto& b) {
                        return a.second != b.second
                            ? a.second > b.second
                            : a.first < b.first;
                    });

                if (hour.second.size() > top) {
                    hour.second.resize(top);
                }
            }
        }

        double rankingEnd=MPI_Wtime();
        double phases[7]={mapEnd-start,packEnd-mapEnd,exchangeEnd-packEnd,reduceEnd-exchangeEnd,
                          candidateEnd-reduceEnd,collectionEnd-candidateEnd,rankingEnd-collectionEnd};
        double maxPhases[7]={};
        MPI_Reduce(phases,maxPhases,7,MPI_DOUBLE,MPI_MAX,0,MPI_COMM_WORLD);
        Count stats[4]={shuffleBytes,collectionBytes,reducedKeys,candidateCount},globalStats[4]={};
        MPI_Reduce(stats,globalStats,4,MPI_UNSIGNED_LONG_LONG,MPI_SUM,0,MPI_COMM_WORLD);
        // Timing includes file processing, MPI and sorting.
        // MPI startup and console printing are excluded.
        double elapsed = MPI_Wtime() - start;
        double seconds = 0;

        MPI_Reduce(
            &elapsed,
            &seconds,
            1,
            MPI_DOUBLE,
            MPI_MAX,
            0,
            MPI_COMM_WORLD);

        // 11. Print results only from rank 0.
        if (rank == 0) {
            std::cout
                << "Mode: Optimised MPI MapReduce\n"
                << "Input file: " << filename << '\n'
                << "MPI processes: " << ranks << '\n'
                << "Top N: " << top << '\n'
                << "Producers per rank: "
                << producers << '\n'
                << "Consumers per rank: "
                << consumers << '\n'
                << "Buffer records per rank: "
                << capacity << '\n';

            const char* names[7]={"map","pack","shuffle","reduce","candidates","collection","ranking_and_metadata"};
            for(int i=0;i<7;++i) std::cout << "Phase " << names[i] << " max seconds: " << std::fixed << std::setprecision(6) << maxPhases[i] << '\n';
            std::cout << "Shuffle bytes: " << globalStats[0] << "\nCollection bytes: " << globalStats[1]
                      << "\nComplete keys: " << globalStats[2] << "\nCollected candidates: " << globalStats[3] << '\n';
            if(allTotals) {
                for(const auto& e:unpack(finalData)) std::cout << "TOTAL " << std::quoted(e.first.first) << ' ' << std::quoted(e.first.second) << ' ' << e.second << '\n';
            }
            for (int i = 0; i < ranks; ++i) {
                std::cout
                    << "Rank " << i
                    << " records: " << rankRecords[i]
                    << " reduced keys: " << rankKeys[i]
                    << '\n';
            }

            for (const auto& hour : rankings) {
                std::cout
                    << "Hour: " << hour.first << '\n';

                size_t number = 0;

                for (const auto& light : hour.second) {
                    std::cout
                        << "Rank " << ++number << ": "
                        << light.first << " - "
                        << light.second << " cars\n";
                }
            }

            std::cout
                << "Records processed: "
                << totalRecords << '\n'
                << "Hours processed: "
                << rankings.size() << '\n'
                << "Execution time: "
                << std::fixed << std::setprecision(6)
                << seconds << " seconds\n";
        }

        MPI_Finalize();
        return 0;

    } catch (const std::exception& error) {
        std::cerr
            << "MPI rank " << rank
            << " error: " << error.what()
            << std::endl;

        MPI_Abort(MPI_COMM_WORLD, 1);
        return 1;
    }
}