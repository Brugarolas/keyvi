/* * keyvi - A key value store.
 *
 * Copyright 2021 Hendrik Muhs<hendrik.muhs@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef KEYVI_DICTIONARY_MATCHING_NEAR_MATCHING_H_
#define KEYVI_DICTIONARY_MATCHING_NEAR_MATCHING_H_

#include <cstdint>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "keyvi/dictionary/fsa/automata.h"
#include "keyvi/dictionary/fsa/codepoint_state_traverser.h"
#include "keyvi/dictionary/fsa/comparable_state_traverser.h"
#include "keyvi/dictionary/fsa/traverser_types.h"
#include "keyvi/dictionary/fsa/zip_state_traverser.h"
#include "keyvi/dictionary/match.h"

// #define ENABLE_TRACING
#include "keyvi/dictionary/util/trace.h"

namespace keyvi {
namespace dictionary {
namespace matching {

template <class innerTraverserType = fsa::ComparableStateTraverser<fsa::NearStateTraverser>>
class NearMatching final {
 private:
  using fsa_start_state_payloads_t = std::vector<
      std::tuple<fsa::automata_t, uint64_t, fsa::traversal::TraversalPayload<fsa::traversal::NearTransition>>>;

 public:
  /**
   * Create a near matcher from a single Fsa
   *
   * @param fsa the fsa
   * @param query the query
   * @param minimum_exact_prefix the minimum exact prefix to match before matching approximate
   * @param greedy if true matches everything below minimum prefix
   */
  static NearMatching FromSingleFsa(const fsa::automata_t& fsa, const std::string& query,
                                    const size_t minimum_exact_prefix, const bool greedy = false) {
    uint64_t state = fsa->GetStartState();

    if (query.size() < minimum_exact_prefix) {
      return NearMatching();
    }

    TRACE("GetNear %s, matching prefix first", key.substr(0, minimum_prefix_length).c_str());
    for (size_t i = 0; i < minimum_exact_prefix; ++i) {
      state = fsa->TryWalkTransition(state, query[i]);

      if (!state) {
        return NearMatching();
      }
    }

    return FromSingleFsa(fsa, state, query, minimum_exact_prefix, greedy);
  }

  /**
   * Create a near matcher from a single Fsa
   *
   * @param fsa the fsa
   * @param query the query
   * @param exact_prefix the exact prefix that already matched
   * @param greedy if true matches everything below minimum prefix
   */
  static NearMatching FromSingleFsa(const fsa::automata_t& fsa, const uint64_t start_state, const std::string& query,
                                    const size_t exact_prefix, const bool greedy = false) {
    // todo: check if the prefix is already an exact match
    Match first_match;
    std::shared_ptr<std::string> near_key = std::make_shared<std::string>(query.substr(exact_prefix));

    auto payload = fsa::traversal::TraversalPayload<fsa::traversal::NearTransition>(near_key);

    // todo: switch to make_unique, requires C++14
    std::unique_ptr<fsa::ComparableStateTraverser<fsa::NearStateTraverser>> traverser;
    traverser.reset(
        new fsa::ComparableStateTraverser<fsa::NearStateTraverser>(fsa, start_state, std::move(payload), true, 0));

    return NearMatching(std::move(traverser), std::move(first_match), query.substr(0, exact_prefix), greedy);
  }

  /**
   * Create a near matcher from multiple Fsas
   *
   * @param fsas a vector of fsas
   * @param query the query
   * @param minimum_exact_prefix the minimum exact prefix to match before matching approximate
   * @param greedy if true matches everything below minimum prefix, if false everything at the longest matched prefix
   */
  static NearMatching FromMulipleFsas(const std::vector<fsa::automata_t>& fsas, const std::string& query,
                                      const size_t minimum_exact_prefix = 2, const bool greedy = false) {
    fsa_start_state_payloads_t fsa_start_state_payloads = FilterWithExactPrefix(fsas, query, minimum_exact_prefix);

    return FromMulipleFsas(std::move(fsa_start_state_payloads), query, minimum_exact_prefix, greedy);
  }

  /**
   * Create a near matcher with already matched exact prefix.
   *
   * @param fsa_start_state_pairs pairs of fsa and current state
   * @param query the query
   * @param exact_prefix the exact prefix that already matched
   * @param greedy if true matches everything below minimum prefix, if false everything at the longest matched prefix
   */

  static NearMatching FromMulipleFsas(fsa_start_state_payloads_t&& fsa_start_state_payloads, const std::string& query,
                                      const size_t exact_prefix, const bool greedy = false) {
    if (fsa_start_state_payloads.size() == 0) {
      return NearMatching();
    }

    std::shared_ptr<std::string> near_key = std::make_shared<std::string>(query.substr(exact_prefix));
    std::vector<std::tuple<fsa::automata_t, uint64_t, fsa::traversal::TraversalPayload<fsa::traversal::NearTransition>>>
        fsas_with_payload;
    Match first_match;

    /*for (auto fsa_state : fsa_start_state_pairs) {
      // todo: check if the prefix is already an exact match

    }*/

    // todo: switch to make_unique, requires C++14
    std::unique_ptr<fsa::ZipStateTraverser<fsa::NearStateTraverser>> traverser;
    traverser.reset(new fsa::ZipStateTraverser<fsa::NearStateTraverser>(std::move(fsa_start_state_payloads)));

    return NearMatching<fsa::ZipStateTraverser<fsa::NearStateTraverser>>(std::move(traverser), std::move(first_match),
                                                                         query.substr(0, exact_prefix), greedy);
  }

  static inline fsa_start_state_payloads_t FilterWithExactPrefix(const std::vector<fsa::automata_t>& fsas,
                                                                 const std::string& query,
                                                                 const size_t minimum_exact_prefix = 2) {
    std::shared_ptr<std::string> near_key = std::make_shared<std::string>(query.substr(minimum_exact_prefix));
    fsa_start_state_payloads_t fsa_start_state_payloads;

    for (const fsa::automata_t& fsa : fsas) {
      uint64_t state = fsa->GetStartState();
      size_t depth = 0;
      while (state != 0 && depth < minimum_exact_prefix) {
        state = fsa->TryWalkTransition(state, query[depth++]);
      }

      if (state && depth == minimum_exact_prefix) {
        auto payload = fsa::traversal::TraversalPayload<fsa::traversal::NearTransition>(near_key);
        fsa_start_state_payloads.emplace_back(fsa, state, std::move(payload));
      }
    }
    return fsa_start_state_payloads;
  }

  Match FirstMatch() const { return first_match_; }

  Match NextMatch() {
    for (; traverser_ptr_ && traverser_ptr_->GetDepth() > matched_depth_;) {
      if (traverser_ptr_->IsFinalState()) {
        // optimize? fill vector upfront?
        std::string match_str =
            exact_prefix_ + std::string(reinterpret_cast<const char*>(traverser_ptr_->GetStateLabels().data()),
                                        traverser_ptr_->GetDepth());

        // length should be query.size???
        Match m(0, traverser_ptr_->GetDepth() + exact_prefix_.size(), match_str,
                exact_prefix_.size() + traverser_ptr_->GetTraversalPayload().exact_depth, traverser_ptr_->GetFsa(),
                traverser_ptr_->GetStateValue());

        if (!greedy_) {
          // remember the depth
          TRACE("found a match, remember depth, only allow matches with same depth %ld",
                traverser_ptr_->GetTraversalPayload().exact_depth);
          matched_depth_ = traverser_ptr_->GetTraversalPayload().exact_depth;
        }

        (*traverser_ptr_)++;
        return m;
      }
      (*traverser_ptr_)++;
    }

    return Match();
  }

 private:
  NearMatching(std::unique_ptr<innerTraverserType>&& traverser, Match&& first_match, std::string&& minimum_exact_prefix,
               const bool greedy)
      : traverser_ptr_(std::move(traverser)),
        exact_prefix_(std::move(minimum_exact_prefix)),
        first_match_(std::move(first_match)),
        greedy_(greedy) {}

  NearMatching() {}

 private:
  std::unique_ptr<innerTraverserType> traverser_ptr_;
  const std::string exact_prefix_;
  const Match first_match_;
  const bool greedy_ = false;
  size_t matched_depth_ = 0;
};

} /* namespace matching */
} /* namespace dictionary */
} /* namespace keyvi */
#endif  // KEYVI_DICTIONARY_MATCHING_NEAR_MATCHING_H_
