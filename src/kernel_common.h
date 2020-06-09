#pragma once
#include "common.h"
#include "tile.h"

namespace SCAMP {

static constexpr int NUM_EXTRA_OPERANDS = 3;

template <typename T>
struct SCAMPKernelInputArgs {
  SCAMPKernelInputArgs(Tile *t, bool transpose, bool ab_join);
  T *__restrict__ cov;
  const T *__restrict__ dfa;
  const T *__restrict__ dfb;
  const T *__restrict__ dga;
  const T *__restrict__ dgb;
  const T *__restrict__ normsa;
  const T *__restrict__ normsb;
  const T *__restrict__ extras[NUM_EXTRA_OPERANDS];
  unsigned long long int *profile_a_length;
  unsigned long long int *profile_b_length;
  int64_t max_matches_per_tile;
  int32_t n_x;
  int32_t n_y;
  int32_t exclusion_lower;
  int32_t exclusion_upper;
  OptionalArgs opt;
  void Print();
};

template <typename T>
SCAMPKernelInputArgs<T>::SCAMPKernelInputArgs(Tile *t, bool transpose,
                                              bool ab_join) {
  cov = t->QT();
  dfa = transpose ? t->dfb() : t->dfa();
  dfb = transpose ? t->dfa() : t->dfb();
  dga = transpose ? t->dgb() : t->dga();
  dgb = transpose ? t->dga() : t->dgb();
  normsa = transpose ? t->normsb() : t->normsa();
  normsb = transpose ? t->normsa() : t->normsb();
  n_x = transpose ? t->get_tile_height() : t->get_tile_width();
  n_y = transpose ? t->get_tile_width() : t->get_tile_height();
  n_x = n_x - t->info()->mp_window + 1;
  n_y = n_y - t->info()->mp_window + 1;
  std::pair<int, int> exclusion =
      ab_join ? t->get_exclusion_for_ab_join(!transpose)
              : t->get_exclusion_for_self_join(!transpose);
  exclusion_lower = exclusion.first;
  exclusion_upper = exclusion.second;
  opt = t->info()->opt_args;
  profile_a_length =
      transpose ? t->get_mutable_b_dev_length() : t->get_mutable_a_dev_length();
  profile_b_length =
      transpose ? t->get_mutable_a_dev_length() : t->get_mutable_b_dev_length();
  max_matches_per_tile = t->info()->max_matches_per_tile;
}

}  // namespace SCAMP
