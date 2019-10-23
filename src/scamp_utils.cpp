#include <cmath>
#include <iomanip>
#include <iostream>
#include <list>

#include "common.h"
#include "scamp_utils.h"

void write_matrix(const std::string &mp, bool output_pearson,
                  const std::vector<std::vector<float>> &matrix, int window) {
  std::ofstream mp_out(mp);
  for (auto row : matrix) {
    for (auto elem : row) {
      if (output_pearson) {
        mp_out << std::setprecision(10) << elem << " ";
      } else {
        mp_out << std::setprecision(10) << ConvertToEuclidean(elem, window)
               << " ";
      }
    }
    mp_out << std::endl;
  }
}

std::vector<std::vector<double>> reduce_all_neighbors(SCAMP::ProfileData *data,
                                                      int height, int width,
                                                      int output_height,
                                                      int output_width) {
  int reduced_cols = std::ceil(width / static_cast<double>(output_width));
  int reduced_rows = std::ceil(height / static_cast<double>(output_height));
  std::vector<std::vector<double>> result(
      output_height, std::vector<double>(output_width, -1.0));
  for (auto pq : data->match_value) {
    while (!pq.empty()) {
      SCAMP::SCAMPmatch elem = pq.top();
      pq.pop();
      int row = elem.row / reduced_rows;
      int col = elem.col / reduced_cols;
      if (row >= output_height || col >= output_height) {
        std::cout
            << "Warning: row: " << elem.row << " col: " << elem.col
            << " corr: " << elem.corr
            << " did not fit into reduced matrix, there is a bug somewhere..."
            << std::endl;
      } else if (result[row][col] < elem.corr) {
        result[row][col] = elem.corr;
      }
    }
  }
  return result;
}

std::ifstream &read_value(std::ifstream &s, double &d, int count) {
  std::string line;
  double parsed;

  s >> line;
  if (line.empty()) {
    if (s.peek() != EOF) {
      std::cout << "WARNING: got empty line #" << count + 1
                << " in input file\n"
                << std::endl;
    }
    d = NAN;
    return s;
  }

  try {
    parsed = std::stod(line);
  } catch (std::invalid_argument &e) {
    std::cout << line[0] << std::endl;
    std::cout << "FATAL ERROR: invalid argument: Could not parse line number "
              << count + 1 << " from input file.\n";
    exit(1);
  } catch (std::out_of_range &e) {
    std::cout << line[0] << std::endl;
    std::cout << "FATAL ERROR: out of range: Could not parse line number "
              << count + 1 << " from input file.\n";
    exit(1);
  }
  d = parsed;
  return s;
}

// Reads input time series from file
void readFile(const std::string &filename, std::vector<double> &v) {
  std::ifstream f(filename);
  if (f.fail()) {
    std::cout << "Unable to open" << filename
              << "for reading, please make sure it exists" << std::endl;
    exit(1);
  }
  std::cout << "Reading data from " << filename << std::endl;
  double num;
  while (read_value(f, num, v.size()) && f.peek() != EOF) {
    v.push_back(num);
  }
  std::cout << "Read " << v.size() << " values from file " << filename
            << std::endl;
}

std::vector<int> ParseIntList(const std::string &s) {
  // TODO(zpzim): check regex for formatting
  if (s.empty()) {
    return std::vector<int>();
  }
  std::stringstream ss(s);
  std::vector<int> result;
  while (ss.good()) {
    std::string substr;
    std::getline(ss, substr, ',');
    result.push_back(std::stoi(substr));
  }
  return result;
}

SCAMP::SCAMPPrecisionType GetPrecisionType(bool doublep, bool mixedp,
                                           bool singlep) {
  if (doublep) {
    return SCAMP::PRECISION_DOUBLE;
  }
  if (mixedp) {
    return SCAMP::PRECISION_MIXED;
  }
  if (singlep) {
    return SCAMP::PRECISION_SINGLE;
  }
  return SCAMP::PRECISION_INVALID;
}

SCAMP::SCAMPProfileType ParseProfileType(const std::string &s) {
  if (s == "1NN_INDEX") {
    return SCAMP::PROFILE_TYPE_1NN_INDEX;
  }
  if (s == "SUM_THRESH") {
    return SCAMP::PROFILE_TYPE_SUM_THRESH;
  }
  if (s == "1NN") {
    return SCAMP::PROFILE_TYPE_1NN;
  }
  if (s == "ALL_NEIGHBORS") {
    return SCAMP::PROFILE_TYPE_APPROX_ALL_NEIGHBORS;
  }
  return SCAMP::PROFILE_TYPE_INVALID;
}

double ConvertToEuclidean(double val, int window) {
  // If there was no match, we can't do a valid conversion, just return NaN
  if (val < -1) {
    return NAN;
  }
  return std::sqrt(std::max(2.0 * window * (1.0 - val), 0.0));
}

bool WriteProfileToFile(const std::string &mp, const std::string &mpi,
                        SCAMP::Profile &p, bool output_pearson, int window) {
  switch (p.type) {
    case SCAMP::PROFILE_TYPE_1NN_INDEX: {
      std::ofstream mp_out(mp);
      std::ofstream mpi_out(mpi);
      auto arr = p.data[0].uint64_value;
      for (const uint64_t elem : arr) {
        SCAMP::mp_entry e;
        e.ulong = elem;
        if (output_pearson) {
          mp_out << std::setprecision(10) << e.floats[0] << std::endl;
        } else {
          mp_out << std::setprecision(10)
                 << ConvertToEuclidean(e.floats[0], window) << std::endl;
        }
        int index;
        // If there was no match, set index to -1
        if (e.floats[0] < -1) {
          index = -1;
        } else {
          index = e.ints[1] + 1;
        }
        mpi_out << index << std::endl;
      }
      break;
    }
    case SCAMP::PROFILE_TYPE_1NN: {
      std::ofstream mp_out(mp);
      auto arr = p.data[0].float_value;
      for (const float elem : arr) {
        if (output_pearson) {
          mp_out << std::setprecision(10) << elem << std::endl;
        } else {
          mp_out << std::setprecision(10) << ConvertToEuclidean(elem, window)
                 << std::endl;
        }
      }
      break;
    }
    case SCAMP::PROFILE_TYPE_SUM_THRESH: {
      std::ofstream mp_out(mp);
      auto arr = p.data[0].double_value;
      for (const double elem : arr) {
        mp_out << std::setprecision(10) << elem << std::endl;
      }
      break;
    }
    case SCAMP::PROFILE_TYPE_APPROX_ALL_NEIGHBORS: {
      if (!p.data[0].matrix_value.empty()) {
        write_matrix(mp, output_pearson, p.data[0].matrix_value, window);
        break;
      }
      std::ofstream mp_out(mp);
      auto arr = p.data[0].match_value;
      for (auto &pq : arr) {
        std::list<SCAMP::SCAMPmatch> elems;
        while (!pq.empty()) {
          elems.push_front(pq.top());
          pq.pop();
        }
        for (auto &elem : elems) {
          if (output_pearson) {
            mp_out << elem.col << " " << elem.row << " "
                   << std::setprecision(10) << elem.corr << std::endl;
          } else {
            mp_out << elem.col << " " << elem.row << " "
                   << std::setprecision(10)
                   << ConvertToEuclidean(elem.corr, window) << std::endl;
          }
        }
      }
      break;
    }
    default:
      break;
  }
  return true;
}

bool InitProfileMemory(SCAMP::SCAMPArgs *args) {
  int64_t profile_a_size = args->timeseries_a.size() - args->window + 1;
  int64_t profile_b_size = args->has_b
                               ? args->timeseries_b.size() - args->window + 1
                               : profile_a_size;
  if (profile_a_size <= 0 ||
      (args->keep_rows_separate && profile_b_size <= 0)) {
    // Invalid input
    return false;
  }

  args->profile_a.Alloc(profile_a_size);

  if (args->keep_rows_separate) {
    args->profile_b.Alloc(profile_b_size);
  }
  return true;
}
