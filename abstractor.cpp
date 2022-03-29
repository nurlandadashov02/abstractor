/*
 * author: Nurlan Dadashov (2019400300)
 * The main idea of the project is to utilize threads to process files in a parallel way. This will result in a much faster processing.
 * Threads race to read the abstract, but this is protected by mutex. Similarity scores between query and each abstract is calculated.
 * Then N abstracts with highest scores are selected and summarized.
 * Code is working and fully functional.
 */

#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <algorithm>
#include <cstring>
#include <set>
#include <map>
#include <iomanip>
#include <zconf.h>
#include <queue>

using namespace std;

queue<string> abstracts_paths; // keeps list of abstracts that would be processed by a thread
ofstream output_file;  // output file
multimap<string, double> result; // keeps similarity scores of abstracts
set<string> WordSet1; // set version of query
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;  // mutex for printing thread output

void* runner(void* arg);

int main(int argc, char* argv[]) {
    string inp = argv[1];
    ifstream input_file(inp);
    output_file.open(argv[2]);

    // reading input
    int T, A, N;
    string line;
    getline(input_file, line);
    istringstream string_stream(line);
    string_stream >> T >> A >> N;

    string query, q_word;
    getline(input_file, query);
    istringstream query_stream(query);
    while (query_stream >> q_word) {
        WordSet1.insert(q_word);  // query -> set
    }

    for(int i = 0; i < A; i++) {
        getline(input_file, line);
        abstracts_paths.push(line);
    }

    input_file.close();

    vector<pthread_t> tids(T); // keeps thread ids

    // creating T threads
    for(int i = 0; i < T; i++) {
        int *ptr = (int *) malloc(sizeof(int));
        *ptr = i;
        pthread_create(&tids.at(i), NULL, &runner, ptr);
    }

    // wait for each thread to finish
    for(int i = 0; i < T; i++) {
        pthread_join(tids.at(i), NULL);
    }

    output_file << "###" << endl;

    // converting map to a vector
    vector<pair<string, double>> results(result.begin(), result.end());

    // sorting by scores in descending order
    sort(results.rbegin(), results.rend(), [](pair<string, double> a, pair<string, double> b) {
        return a.second < b.second;
    });

    // printing N abstracts with highest scores
    for(int i = 0; i < N; i++) {
        output_file << "Result " << i + 1 << ":" << endl;
        output_file << "File: " << results.at(i).first << endl;
        output_file << "Score: " << fixed << setprecision(4) << results.at(i).second << endl;

        // reading the abstract
        ifstream abstract_file("../abstracts/" + results.at(i).first);
        stringstream abstract_stream;
        abstract_stream << abstract_file.rdbuf();

        abstract_file.close();

        vector<vector<string>> sentences;
        string word;

        vector<string> sentence;

        // splitting text to sentences
        while (abstract_stream >> word) {
            if(word == ".") {
                sentences.push_back(sentence);
                sentence.clear();
            }
            else {
                sentence.push_back(word);
            }
        }

        output_file << "Summary: ";

        // Summarizing
        for(vector<string> _sentence : sentences) {
            bool found = false;
            // checking if query word exists in a sentence
            for(string _query : WordSet1) {
                if(find(_sentence.begin(), _sentence.end(), _query) != _sentence.end()) {
                    for(string _word : _sentence) {
                        output_file << _word << " ";
                    }

                    output_file << ". ";
                    break;
                }
            }
        }

        output_file << endl;

        output_file << "###" << endl;
    }

    output_file.close();

    pthread_exit(NULL);
}

// code executed in thread
void* runner(void* arg) {
    int *thread = (int *)arg; // thread index ( will determine its name -> 'A' + thread)

    // processing abstracts
    while(true) {
        usleep(100000); // sleep for 100 msec
        pthread_mutex_lock(&mutex);
        if(abstracts_paths.empty()) { // if there are no abstracts left, unlock mutex and end loop
            pthread_mutex_unlock(&mutex);
            break;
        }
        string file = abstracts_paths.front(); // file_name of abstract
        abstracts_paths.pop();
        output_file << "Thread " << (char) ('A' + *thread) << " is calculating " << file << endl;
        pthread_mutex_unlock(&mutex);

        // reading abstract
        ifstream abstract_file("../abstracts/" + file);
        stringstream string_stream;
        string_stream << abstract_file.rdbuf();

        abstract_file.close();

        set<string> WordSet2;
        string word;

        while (string_stream >> word) {
            WordSet2.insert(word); // finding unique words in abstract
        }

        // Find the intersecting words of two sets:
        set<string> Intersection_WS1_WS2;
        set_intersection(WordSet1.begin(), WordSet1.end(), WordSet2.begin(), WordSet2.end(), inserter(Intersection_WS1_WS2, Intersection_WS1_WS2.begin()));

        // Find the union of two sets:
        set<string> Union_WS1_WS2;
        set_union(WordSet1.begin(), WordSet1.end(), WordSet2.begin(), WordSet2.end(), inserter(Union_WS1_WS2, Union_WS1_WS2.begin()));

        size_t s1 = Intersection_WS1_WS2.size();
        size_t s2 = Union_WS1_WS2.size();

        // Calculate the Jaccard Similarity using the mathematical formula given:
        double Jaccard_Similarity = s1 * 1.0 / s2;

        result.insert(make_pair(file, Jaccard_Similarity));
    }

    return NULL;
}